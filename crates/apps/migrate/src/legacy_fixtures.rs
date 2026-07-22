use serde_json::json;

use crate::LegacyEntry;

pub(crate) const CLUSTER_ID: &str = "0123456789abcdef0123456789abcdef";

pub(crate) fn cluster_meta() -> LegacyEntry {
    cluster_meta_for(&[10])
}

pub(crate) fn cluster_meta_for(host_octets: &[u8]) -> LegacyEntry {
    let host_ips = host_octets
        .iter()
        .map(|octet| format!("10.0.0.{octet}"))
        .collect::<Vec<_>>();
    let endpoints = host_ips
        .iter()
        .map(|host_ip| {
            json!({
                "hostIp": host_ip,
                "apiPort": 3000,
                "gatewayPort": 3002,
                "etcdClientPort": 2379,
                "etcdPeerPort": 2380
            })
        })
        .collect::<Vec<_>>();
    LegacyEntry::new(
        "/maetro/system/cluster-meta",
        json!({
            "clusterId": CLUSTER_ID,
            "name": "Test Cluster",
            "bootstrapHostIp": host_ips.first().cloned().unwrap_or_default(),
            "initialVoterHostIps": host_ips,
            "initialVoterEndpoints": endpoints
        })
        .to_string()
        .into_bytes(),
    )
}

pub(crate) fn cluster_state() -> Vec<LegacyEntry> {
    cluster_state_for(&[10])
}

pub(crate) fn cluster_state_for(host_octets: &[u8]) -> Vec<LegacyEntry> {
    let mut entries = vec![cluster_meta_for(host_octets)];
    entries.extend(
        host_octets
            .iter()
            .copied()
            .map(|host_octet| voter(host_octet, u64::from(host_octet))),
    );
    entries
}

pub(crate) fn voter(host_octet: u8, member_id: u64) -> LegacyEntry {
    let host_ip = format!("10.0.0.{host_octet}");
    let name = format!(
        "maestro-{:08x}-0bb8",
        u32::from_be_bytes([10, 0, 0, host_octet])
    );
    LegacyEntry::new(
        format!("/maetro/cluster/voters/{member_id:016x}"),
        json!({
            "memberId": member_id,
            "name": name,
            "peerUrls": [format!("https://{host_ip}:2380")],
            "clientUrls": [format!("https://{host_ip}:2379")]
        })
        .to_string()
        .into_bytes(),
    )
}
