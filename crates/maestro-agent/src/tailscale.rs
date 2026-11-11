use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use maestro_core::{ByteSize, Millicores, cluster_dns_label, cluster_domain};
use maestro_runtime::{BindMount, ContainerLimits, ContainerSpec, NetworkMode};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::dns::{DnsRecords, PeerClusters};

pub const TAILSCALE_IMAGE: &str = "ghcr.io/tailscale/tailscale:v1.98.3";
pub const TAILSCALE_CONTAINER_ID: &str = "maestro-tailscale";
const HOSTNAME_PREFIX: &str = "maestro-";
const STATE_MOUNT: &str = "/var/lib/tailscale";
const SOCKET_MOUNT: &str = "/var/run/tailscale";
const SOCKET_FILE: &str = "tailscaled.sock";
const SERVE_CONFIG_FILE: &str = "serve.json";
pub const PANEL_SOCKET_FILE: &str = "panel.sock";
const STATUS_PATH: &str = "/localapi/v0/status";
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(15);
const STATUS_TIMEOUT: Duration = Duration::from_secs(5);
const DNS_PORT: u16 = 53;
const SYSTEM_SLICE: &str = "maestro-system.slice";
const SYSTEM_LABEL: &str = "maestro.system";
const SYSTEM_OOM_SCORE_ADJ: i32 = -900;

pub struct TailscaleNode {
    pub auth_key: String,
    pub cluster: String,
    pub name_suffix: String,
    pub serve_panel: bool,
    pub routes: Vec<String>,
    pub state_dir: PathBuf,
    pub socket_dir: PathBuf,
}

pub struct TailnetDiscovery {
    pub socket_dir: PathBuf,
    pub cluster: String,
    pub voters: usize,
    pub serve_panel: bool,
    pub peers: PeerClusters,
    pub records: DnsRecords,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Status {
    #[serde(default)]
    peer: HashMap<String, Peer>,
    #[serde(default, rename = "Self")]
    self_node: Option<Peer>,
    #[serde(default, rename = "MagicDNSSuffix")]
    magic_dns_suffix: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Peer {
    #[serde(default)]
    host_name: String,
    #[serde(default, rename = "TailscaleIPs")]
    tailscale_ips: Vec<IpAddr>,
    #[serde(default)]
    online: bool,
}

pub fn tailscale_container(node: &TailscaleNode) -> ContainerSpec {
    ContainerSpec {
        id: TAILSCALE_CONTAINER_ID.to_string(),
        image: TAILSCALE_IMAGE.to_string(),
        entrypoint: None,
        args: None,
        env: [
            ("TS_AUTHKEY", node.auth_key.clone()),
            ("TS_STATE_DIR", STATE_MOUNT.to_string()),
            ("TS_SOCKET", format!("{SOCKET_MOUNT}/{SOCKET_FILE}")),
            ("TS_HOSTNAME", hostname(&node.cluster, &node.name_suffix)),
            ("TS_USERSPACE", "true".to_string()),
            ("TS_ROUTES", node.routes.join(",")),
            ("TS_EXTRA_ARGS", "--accept-dns=false".to_string()),
        ]
        .into_iter()
        .chain(node.serve_panel.then(|| {
            (
                "TS_SERVE_CONFIG",
                format!("{SOCKET_MOUNT}/{SERVE_CONFIG_FILE}"),
            )
        }))
        .map(|(key, value)| (key.to_string(), value))
        .collect(),
        labels: BTreeMap::from([(SYSTEM_LABEL.to_string(), "tailscale".to_string())]),
        limits: ContainerLimits {
            cpu: Millicores::new(1000),
            memory: ByteSize::new(512 << 20),
            pids: 1024,
            oom_score_adj: SYSTEM_OOM_SCORE_ADJ,
        },
        cgroup_parent: SYSTEM_SLICE.to_string(),
        mounts: vec![
            BindMount {
                source: node.state_dir.clone(),
                destination: STATE_MOUNT.to_string(),
                read_only: false,
            },
            BindMount {
                source: node.socket_dir.clone(),
                destination: SOCKET_MOUNT.to_string(),
                read_only: false,
            },
        ],
        network: NetworkMode::Bridge,
    }
}

/// Serves the panel over HTTPS on the node's tailnet name and, once the tailnet's MagicDNS suffix
/// is known, over plain HTTP on `<cluster>.maestro`. Tailscale matches plain HTTP requests by their
/// Host header with the MagicDNS suffix appended, so `http_host` must carry it.
pub fn write_panel_serve_config(socket_dir: &Path, http_host: Option<&str>) -> std::io::Result<()> {
    let handlers = serde_json::json!({
        "Handlers": { "/": { "Proxy": format!("unix:{SOCKET_MOUNT}/{PANEL_SOCKET_FILE}") } }
    });
    let ports = std::iter::once(("443".to_string(), serde_json::json!({ "HTTPS": true })))
        .chain(http_host.map(|_| ("80".to_string(), serde_json::json!({ "HTTP": true }))))
        .collect::<serde_json::Map<_, _>>();
    let sites = std::iter::once(("${TS_CERT_DOMAIN}:443".to_string(), handlers.clone()))
        .chain(http_host.map(|host| (format!("{host}:80"), handlers.clone())))
        .collect::<serde_json::Map<_, _>>();
    let config = serde_json::json!({ "TCP": ports, "Web": sites }).to_string();
    let path = socket_dir.join(SERVE_CONFIG_FILE);
    match std::fs::read_to_string(&path).ok() == Some(config.clone()) {
        true => Ok(()),
        false => std::fs::write(path, config),
    }
}

pub async fn discover_tailnet(discovery: TailnetDiscovery) {
    let mut ticker = tokio::time::interval(DISCOVERY_INTERVAL);
    loop {
        ticker.tick().await;
        match status(&discovery.socket_dir.join(SOCKET_FILE)).await {
            Ok(status) => {
                discovery
                    .peers
                    .replace(peer_clusters(&status, &discovery.cluster));
                discovery.records.replace_tailnet(dashboard_records(
                    &status,
                    &discovery.cluster,
                    discovery.voters,
                ));
                let http_host = (discovery.serve_panel && !status.magic_dns_suffix.is_empty())
                    .then(|| {
                        format!(
                            "{}.{}",
                            cluster_domain(&discovery.cluster),
                            status.magic_dns_suffix.trim_end_matches('.')
                        )
                    });
                let written = http_host
                    .map(|host| write_panel_serve_config(&discovery.socket_dir, Some(&host)));
                if let Some(Err(error)) = written {
                    tracing::warn!(%error, "failed to update the tailscale serve config");
                }
            }
            Err(error) => tracing::debug!(%error, "tailscale status is unavailable"),
        }
    }
}

fn hostname(cluster: &str, name_suffix: &str) -> String {
    format!("{HOSTNAME_PREFIX}{cluster}-{name_suffix}")
}

fn peer_clusters(status: &Status, own_cluster: &str) -> HashMap<String, Vec<SocketAddr>> {
    let own_label = cluster_dns_label(own_cluster);
    status
        .peer
        .values()
        .filter(|peer| peer.online)
        .filter_map(|peer| {
            let cluster = cluster_dns_label(peer_cluster(&peer.host_name)?);
            let address = peer.tailscale_ips.iter().find(|ip| ip.is_ipv4())?;
            (cluster != own_label).then(|| (cluster, SocketAddr::new(*address, DNS_PORT)))
        })
        .fold(HashMap::new(), |mut clusters, (cluster, address)| {
            clusters
                .entry(cluster)
                .or_insert_with(Vec::new)
                .push(address);
            clusters
        })
}

/// `<cluster>.maestro` resolves to the tailnet addresses of this cluster's voters, which serve the
/// panel; voters are named by their position in `cluster.voters`.
fn dashboard_records(status: &Status, cluster: &str, voters: usize) -> HashMap<String, Vec<IpAddr>> {
    let voter_names = (1..=voters)
        .map(|position| hostname(cluster, &position.to_string()))
        .collect::<Vec<_>>();
    let mut addresses = status
        .self_node
        .iter()
        .chain(status.peer.values().filter(|peer| peer.online))
        .filter(|node| {
            voter_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&node.host_name))
        })
        .filter_map(|node| node.tailscale_ips.iter().find(|ip| ip.is_ipv4()).copied())
        .collect::<Vec<_>>();
    addresses.sort();
    match addresses.is_empty() {
        true => HashMap::new(),
        false => HashMap::from([(cluster_domain(cluster), addresses)]),
    }
}

fn peer_cluster(host_name: &str) -> Option<&str> {
    let (cluster, node_id) = host_name.strip_prefix(HOSTNAME_PREFIX)?.rsplit_once('-')?;
    let is_node_id =
        !node_id.is_empty() && node_id.chars().all(|character| character.is_ascii_digit());
    (is_node_id && !cluster.is_empty()).then_some(cluster)
}

async fn status(socket: &Path) -> Result<Status, String> {
    let exchange = async {
        let mut stream = tokio::net::UnixStream::connect(socket)
            .await
            .map_err(|error| error.to_string())?;
        stream
            .write_all(
                format!("GET {STATUS_PATH} HTTP/1.0\r\nHost: local-tailscaled.sock\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .map_err(|error| error.to_string())?;
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .map_err(|error| error.to_string())?;
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .unwrap_or_default();
        serde_json::from_str(body).map_err(|error| error.to_string())
    };
    tokio::time::timeout(STATUS_TIMEOUT, exchange)
        .await
        .unwrap_or_else(|_| Err("tailscale status timed out".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_other_clusters_among_online_peers() {
        let status: Status = serde_json::from_str(
            r#"{"Peer": {
                "a": {"HostName": "maestro-prod-123", "TailscaleIPs": ["100.64.0.2", "fd7a::2"], "Online": true},
                "b": {"HostName": "maestro-prod-456", "TailscaleIPs": ["100.64.0.3"], "Online": true},
                "c": {"HostName": "maestro-dev-789", "TailscaleIPs": ["100.64.0.4"], "Online": true},
                "d": {"HostName": "maestro-staging-1", "TailscaleIPs": ["100.64.0.5"], "Online": false},
                "e": {"HostName": "laptop", "TailscaleIPs": ["100.64.0.6"], "Online": true}
            }}"#,
        )
        .unwrap();
        let mut clusters = peer_clusters(&status, "dev");
        clusters.values_mut().for_each(|addresses| addresses.sort());
        assert_eq!(
            clusters,
            HashMap::from([(
                "prod".to_string(),
                vec![
                    "100.64.0.2:53".parse().unwrap(),
                    "100.64.0.3:53".parse().unwrap()
                ]
            )])
        );
        assert_eq!(peer_cluster("maestro-my-cluster-42"), Some("my-cluster"));
    }

    #[test]
    fn points_the_cluster_domain_at_its_online_voters() {
        let status: Status = serde_json::from_str(
            r#"{
                "MagicDNSSuffix": "tail1234.ts.net",
                "Self": {"HostName": "maestro-Prod-1", "TailscaleIPs": ["100.64.0.1"], "Online": true},
                "Peer": {
                    "a": {"HostName": "maestro-Prod-2", "TailscaleIPs": ["fd7a::2", "100.64.0.2"], "Online": true},
                    "b": {"HostName": "maestro-Prod-3", "TailscaleIPs": ["100.64.0.3"], "Online": false},
                    "c": {"HostName": "maestro-Prod-691149370204", "TailscaleIPs": ["100.64.0.4"], "Online": true},
                    "d": {"HostName": "maestro-dev-1", "TailscaleIPs": ["100.64.0.5"], "Online": true}
                }
            }"#,
        )
        .unwrap();
        assert_eq!(status.magic_dns_suffix, "tail1234.ts.net");
        assert_eq!(
            dashboard_records(&status, "Prod", 3),
            HashMap::from([(
                "prod.maestro".to_string(),
                vec!["100.64.0.1".parse().unwrap(), "100.64.0.2".parse().unwrap()]
            )])
        );
        assert!(dashboard_records(&status, "staging", 3).is_empty());
    }

    fn node(serve_panel: bool) -> TailscaleNode {
        TailscaleNode {
            auth_key: "tskey".to_string(),
            cluster: "prod".to_string(),
            name_suffix: "42".to_string(),
            serve_panel,
            routes: vec!["10.200.1.0/24".to_string(), "10.0.0.0/16".to_string()],
            state_dir: PathBuf::from("/var/lib/maestro/tailscale"),
            socket_dir: PathBuf::from("/run/maestro/tailscale"),
        }
    }

    #[test]
    fn runs_in_its_own_network_namespace_in_userspace_mode() {
        let spec = tailscale_container(&node(false));
        assert_eq!(spec.env["TS_USERSPACE"], "true");
        assert_eq!(spec.env["TS_ROUTES"], "10.200.1.0/24,10.0.0.0/16");
        assert_eq!(spec.env["TS_HOSTNAME"], "maestro-prod-42");
        assert_eq!(spec.network, NetworkMode::Bridge);
        assert!(!spec.env.contains_key("TS_SERVE_CONFIG"));
    }

    #[test]
    fn serves_the_panel_over_https_through_a_unix_socket() {
        let spec = tailscale_container(&node(true));
        assert_eq!(spec.env["TS_SERVE_CONFIG"], "/var/run/tailscale/serve.json");
        let directory = tempfile::tempdir().unwrap();
        let read_config = || -> serde_json::Value {
            serde_json::from_slice(&std::fs::read(directory.path().join(SERVE_CONFIG_FILE)).unwrap())
                .unwrap()
        };
        write_panel_serve_config(directory.path(), None).unwrap();
        let config = read_config();
        assert_eq!(config["TCP"]["443"]["HTTPS"], true);
        assert!(config["TCP"].get("80").is_none());
        assert_eq!(
            config["Web"]["${TS_CERT_DOMAIN}:443"]["Handlers"]["/"]["Proxy"],
            "unix:/var/run/tailscale/panel.sock"
        );
        write_panel_serve_config(directory.path(), Some("prod.maestro.tail1234.ts.net")).unwrap();
        let config = read_config();
        assert_eq!(config["TCP"]["80"]["HTTP"], true);
        assert_eq!(
            config["Web"]["prod.maestro.tail1234.ts.net:80"]["Handlers"]["/"]["Proxy"],
            "unix:/var/run/tailscale/panel.sock"
        );
    }
}
