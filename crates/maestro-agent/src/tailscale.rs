use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use maestro_core::{ByteSize, Millicores};
use maestro_runtime::{BindMount, ContainerLimits, ContainerSpec, NetworkMode};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::dns::PeerClusters;

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

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Status {
    #[serde(default)]
    peer: HashMap<String, Peer>,
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

pub fn write_panel_serve_config(socket_dir: &Path) -> std::io::Result<()> {
    let config = serde_json::json!({
        "TCP": { "443": { "HTTPS": true } },
        "Web": {
            "${TS_CERT_DOMAIN}:443": {
                "Handlers": { "/": { "Proxy": format!("unix:{SOCKET_MOUNT}/{PANEL_SOCKET_FILE}") } }
            }
        }
    });
    std::fs::write(socket_dir.join(SERVE_CONFIG_FILE), config.to_string())
}

pub async fn discover_peer_clusters(socket_dir: PathBuf, cluster: String, peers: PeerClusters) {
    let mut ticker = tokio::time::interval(DISCOVERY_INTERVAL);
    loop {
        ticker.tick().await;
        match status(&socket_dir.join(SOCKET_FILE)).await {
            Ok(status) => peers.replace(peer_clusters(&status, &cluster)),
            Err(error) => tracing::debug!(%error, "tailscale status is unavailable"),
        }
    }
}

fn hostname(cluster: &str, name_suffix: &str) -> String {
    format!("{HOSTNAME_PREFIX}{cluster}-{name_suffix}")
}

fn peer_clusters(status: &Status, own_cluster: &str) -> HashMap<String, Vec<SocketAddr>> {
    status
        .peer
        .values()
        .filter(|peer| peer.online)
        .filter_map(|peer| {
            let cluster = peer_cluster(&peer.host_name)?;
            let address = peer.tailscale_ips.iter().find(|ip| ip.is_ipv4())?;
            (cluster != own_cluster)
                .then(|| (cluster.to_string(), SocketAddr::new(*address, DNS_PORT)))
        })
        .fold(HashMap::new(), |mut clusters, (cluster, address)| {
            clusters
                .entry(cluster)
                .or_insert_with(Vec::new)
                .push(address);
            clusters
        })
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
        write_panel_serve_config(directory.path()).unwrap();
        let config: serde_json::Value = serde_json::from_slice(
            &std::fs::read(directory.path().join(SERVE_CONFIG_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(config["TCP"]["443"]["HTTPS"], true);
        assert_eq!(
            config["Web"]["${TS_CERT_DOMAIN}:443"]["Handlers"]["/"]["Proxy"],
            "unix:/var/run/tailscale/panel.sock"
        );
    }
}
