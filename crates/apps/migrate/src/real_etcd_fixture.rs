use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::net::{Ipv4Addr, TcpListener};
use std::path::Path;
use std::time::Duration;

use cluster::{
    CertificateValidity, ClusterCertificateAuthority, ClusterPorts, EmbeddedEtcdSettings,
    StoreMember, StoreProviderConfig,
};
use etcd_client::{Certificate, Client, ConnectOptions, Identity, TlsOptions};
use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};
use serde_json::json;
use time::{Duration as CertificateDuration, OffsetDateTime};
use tokio::io::AsyncWriteExt;

use crate::legacy_fixtures::{CLUSTER_ID, cluster_state_for};
use crate::legacy_node_tests::node_entries;
use crate::legacy_tests::cutover_service_snapshot;
use crate::{CutoverEtcdConnection, LegacyEntry, LegacySnapshot};

type FixtureResult<Value> = Result<Value, Box<dyn std::error::Error>>;

const ORIGINAL_HOSTS: [Ipv4Addr; 3] = [
    Ipv4Addr::new(10, 0, 0, 10),
    Ipv4Addr::new(10, 0, 0, 11),
    Ipv4Addr::new(10, 0, 0, 12),
];

pub(crate) fn three_member_cutover_snapshot(
    extra: Vec<LegacyEntry>,
) -> FixtureResult<LegacySnapshot> {
    let base = cutover_service_snapshot(extra)?;
    let mut entries = base
        .entries()
        .iter()
        .filter(|entry| !is_topology_key(entry.key()))
        .cloned()
        .collect::<Vec<_>>();
    entries.extend(node_entries("node-a", "master", 10, 1));
    entries.extend(node_entries("node-b", "voter", 11, 2));
    entries.extend(node_entries("node-c", "voter", 12, 3));
    entries.extend(cluster_state_for(&[10, 11, 12]));
    Ok(LegacySnapshot::new(entries)?)
}

pub(crate) fn bind_three_member_topology(
    snapshot: LegacySnapshot,
    host_addresses: [Ipv4Addr; 3],
    ports: ClusterPorts,
) -> FixtureResult<LegacySnapshot> {
    let api_port = ports.wireguard;
    let endpoints = host_addresses
        .iter()
        .map(|host_address| {
            json!({
                "hostIp": host_address,
                "apiPort": api_port,
                "gatewayPort": ports.gateway,
                "etcdClientPort": ports.store_client,
                "etcdPeerPort": ports.store_peer
            })
        })
        .collect::<Vec<_>>();
    let bootstrap_host = host_addresses
        .first()
        .copied()
        .ok_or("rehearsal topology has no bootstrap host")?;
    let mut entries = Vec::new();
    for entry in snapshot.entries() {
        let mut key = entry.key().to_owned();
        let mut value = entry.value().to_vec();
        match entry.key() {
            "/maetro/system/cluster-meta" => {
                let mut document: serde_json::Value = serde_json::from_slice(&value)?;
                set_json(&mut document, "/bootstrapHostIp", json!(bootstrap_host))?;
                set_json(&mut document, "/initialVoterHostIps", json!(host_addresses))?;
                set_json(&mut document, "/initialVoterEndpoints", json!(endpoints))?;
                value = serde_json::to_vec(&document)?;
            }
            candidate if candidate.starts_with("/maetro/cluster/node-records/") => {
                let node_id = candidate
                    .strip_prefix("/maetro/cluster/node-records/")
                    .ok_or("node record prefix disappeared")?;
                let host_address = host_for_node(&host_addresses, node_id)?;
                let mut document: serde_json::Value = serde_json::from_slice(&value)?;
                set_json(
                    &mut document,
                    "/lastInfo/clusterHostIp",
                    json!(host_address),
                )?;
                set_json(&mut document, "/lastInfo/clusterApiPort", json!(api_port))?;
                set_json(
                    &mut document,
                    "/lastInfo/clusterGatewayPort",
                    json!(ports.gateway),
                )?;
                value = serde_json::to_vec(&document)?;
            }
            candidate if candidate.starts_with("/maetro/cluster/control-addresses/") => {
                let mut document: serde_json::Value = serde_json::from_slice(&value)?;
                let node_id = document
                    .get("nodeId")
                    .and_then(serde_json::Value::as_str)
                    .ok_or("control address has no node ID")?;
                let host_address = host_for_node(&host_addresses, node_id)?;
                set_json(&mut document, "/hostIp", json!(host_address))?;
                set_json(&mut document, "/apiPort", json!(api_port))?;
                set_json(&mut document, "/gatewayPort", json!(ports.gateway))?;
                set_json(&mut document, "/etcdClientPort", json!(ports.store_client))?;
                set_json(&mut document, "/etcdPeerPort", json!(ports.store_peer))?;
                key = format!(
                    "/maetro/cluster/control-addresses/{:08x}-{api_port:04x}",
                    u32::from_be_bytes(host_address.octets())
                );
                value = serde_json::to_vec(&document)?;
            }
            candidate if candidate.starts_with("/maetro/cluster/voters/") => {
                let mut document: serde_json::Value = serde_json::from_slice(&value)?;
                let peer_url = document
                    .pointer("/peerUrls/0")
                    .and_then(serde_json::Value::as_str)
                    .ok_or("voter has no peer URL")?;
                let index = ORIGINAL_HOSTS
                    .iter()
                    .position(|host| peer_url.contains(&host.to_string()))
                    .ok_or("voter peer URL has an unknown original host")?;
                let host_address = host_addresses
                    .get(index)
                    .copied()
                    .ok_or("voter host index is out of range")?;
                set_json(
                    &mut document,
                    "/name",
                    json!(format!(
                        "maestro-{:08x}-{api_port:04x}",
                        u32::from_be_bytes(host_address.octets())
                    )),
                )?;
                set_json(
                    &mut document,
                    "/peerUrls",
                    json!([format!("https://{host_address}:{}", ports.store_peer)]),
                )?;
                set_json(
                    &mut document,
                    "/clientUrls",
                    json!([format!("https://{host_address}:{}", ports.store_client)]),
                )?;
                value = serde_json::to_vec(&document)?;
            }
            _ => {}
        }
        entries.push(LegacyEntry::new(key, value));
    }
    Ok(LegacySnapshot::new(entries)?)
}

pub(crate) fn provider_configs(
    root: &Path,
    host_addresses: [Ipv4Addr; 3],
    ports: ClusterPorts,
    store_secret: &str,
) -> FixtureResult<BTreeMap<NodeId, StoreProviderConfig>> {
    let node_ids = ["node-a", "node-b", "node-c"]
        .into_iter()
        .map(NodeId::new)
        .collect::<Result<Vec<_>, _>>()?;
    let known_members = node_ids
        .iter()
        .cloned()
        .zip(host_addresses)
        .map(|(node_id, host_address)| {
            (
                node_id.clone(),
                StoreMember {
                    node_id,
                    host_address,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let authority = ClusterCertificateAuthority::generate(CLUSTER_ID, certificate_validity()?)?;
    node_ids
        .into_iter()
        .enumerate()
        .map(|(index, node_id)| {
            let member = known_members
                .get(&node_id)
                .cloned()
                .ok_or("provider member is missing")?;
            let role = if index == 0 {
                NodeRole::Master
            } else {
                NodeRole::ControlPlane
            };
            let security = authority.issue_node_certificate_with_ip_sans(
                &node_id,
                &format!("{node_id}.internal"),
                member.host_address,
                &host_addresses,
                role,
                certificate_validity()?,
            )?;
            let config = StoreProviderConfig::new(
                ClusterId::new(CLUSTER_ID)?,
                member,
                known_members.clone(),
                ports,
                root.join(node_id.as_str()).join("store"),
                SecretValue::new(store_secret),
                security,
            )?;
            Ok((node_id, config))
        })
        .collect()
}

pub(crate) fn cutover_connection(
    config: &StoreProviderConfig,
    endpoint: String,
) -> FixtureResult<CutoverEtcdConnection> {
    let security = config.security();
    Ok(CutoverEtcdConnection::new(
        vec![endpoint],
        security.trust_root_pem.as_bytes().to_vec(),
        security.identity.certificate_pem.as_bytes().to_vec(),
        security
            .identity
            .private_key_pem
            .expose()
            .as_bytes()
            .to_vec(),
    )?)
}

pub(crate) async fn raw_client(
    config: &StoreProviderConfig,
    endpoint: &str,
) -> Result<Client, etcd_client::Error> {
    let security = config.security();
    let tls = TlsOptions::new()
        .ca_certificate(Certificate::from_pem(security.trust_root_pem.as_bytes()))
        .identity(Identity::from_pem(
            security.identity.certificate_pem.as_bytes(),
            security.identity.private_key_pem.expose().as_bytes(),
        ));
    Client::connect([endpoint], Some(ConnectOptions::new().with_tls(tls))).await
}

pub(crate) async fn save_native_snapshot(client: &mut Client, path: &Path) -> FixtureResult<()> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;

    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options.open(path)?;
    let mut file = tokio::fs::File::from_std(file);
    let mut snapshot = client.snapshot().await?;
    while let Some(chunk) = snapshot.message().await? {
        file.write_all(chunk.blob()).await?;
    }
    file.sync_all().await?;
    Ok(())
}

pub(crate) fn allocate_shared_ports(host_addresses: [Ipv4Addr; 3]) -> FixtureResult<ClusterPorts> {
    let mut reservations = Vec::new();
    let mut ports = Vec::new();
    while ports.len() < 4 {
        let (port, listeners) = reserve_shared_port(&host_addresses, &ports)?;
        ports.push(port);
        reservations.push(listeners);
    }
    let [gateway, store_client, store_peer, wireguard]: [u16; 4] = ports
        .try_into()
        .map_err(|_| "failed to allocate rehearsal ports")?;
    drop(reservations);
    Ok(ClusterPorts::new(
        gateway,
        store_client,
        store_peer,
        wireguard,
    )?)
}

pub(crate) fn embedded_settings() -> Result<EmbeddedEtcdSettings, cluster::StoreProviderError> {
    EmbeddedEtcdSettings::new(
        Duration::from_secs(30),
        Duration::from_secs(2),
        Duration::from_millis(100),
        Duration::from_secs(1),
    )
}

fn reserve_shared_port(
    host_addresses: &[Ipv4Addr; 3],
    excluded: &[u16],
) -> FixtureResult<(u16, Vec<TcpListener>)> {
    let first_address = host_addresses.first().copied().ok_or("no host addresses")?;
    for _attempt in 0..128 {
        let first = TcpListener::bind((first_address, 0))?;
        let port = first.local_addr()?.port();
        if excluded.contains(&port) {
            continue;
        }
        let mut listeners = vec![first];
        let mut available = true;
        for host_address in host_addresses.iter().skip(1) {
            match TcpListener::bind((*host_address, port)) {
                Ok(listener) => listeners.push(listener),
                Err(_) => {
                    available = false;
                    break;
                }
            }
        }
        if available {
            return Ok((port, listeners));
        }
    }
    Err("could not reserve one port on all rehearsal addresses".into())
}

fn host_for_node(host_addresses: &[Ipv4Addr; 3], node_id: &str) -> FixtureResult<Ipv4Addr> {
    let index = match node_id {
        "node-a" => 0,
        "node-b" => 1,
        "node-c" => 2,
        _ => return Err(format!("unknown rehearsal node `{node_id}`").into()),
    };
    host_addresses
        .get(index)
        .copied()
        .ok_or_else(|| "rehearsal host index is out of range".into())
}

fn is_topology_key(key: &str) -> bool {
    key == "/maetro/system/cluster-meta"
        || key.starts_with("/maetro/cluster/node-records/")
        || key.starts_with("/maetro/cluster/subnets/")
        || key.starts_with("/maetro/cluster/control-addresses/")
        || key.starts_with("/maetro/cluster/voters/")
}

fn set_json(
    document: &mut serde_json::Value,
    pointer: &str,
    value: serde_json::Value,
) -> FixtureResult<()> {
    let target = document
        .pointer_mut(pointer)
        .ok_or_else(|| std::io::Error::other(format!("fixture has no `{pointer}`")))?;
    *target = value;
    Ok(())
}

fn certificate_validity() -> FixtureResult<CertificateValidity> {
    let now = OffsetDateTime::now_utc();
    Ok(CertificateValidity::new(
        now - CertificateDuration::days(1),
        now + CertificateDuration::days(3_650),
    )?)
}
