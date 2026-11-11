use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ids::{ReplicaId, ServiceId};

pub const DNS_ROOT: &str = "maestro";
const FALLBACK_CLUSTER_LABEL: &str = "maestro";
const MAX_DNS_LABEL_LENGTH: usize = 63;
const AFFINITY_TOKEN_DOMAIN: &[u8] = b"maestro-affinity-token\0";
const AFFINITY_TOKEN_LENGTH: usize = 32;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteTable {
    pub routes: BTreeMap<ServiceId, ServiceRoute>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_ips: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRoute {
    pub hosts: Vec<String>,
    pub backends: Vec<Backend>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_header: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Backend {
    pub replica_id: ReplicaId,
    pub address: SocketAddr,
}

pub fn affinity_token(replica_id: &ReplicaId) -> String {
    let digest = Sha256::new()
        .chain_update(AFFINITY_TOKEN_DOMAIN)
        .chain_update(replica_id.as_str())
        .finalize();
    hex::encode(digest)[..AFFINITY_TOKEN_LENGTH].to_string()
}

/// The cluster's own DNS zone, `<cluster>.maestro`; it names the admin dashboard, and services and
/// replicas live one label below it.
pub fn cluster_domain(cluster: &str) -> String {
    format!("{}.{DNS_ROOT}", cluster_dns_label(cluster))
}

pub fn internal_name(cluster: &str, label: &str) -> String {
    format!("{label}.{}", cluster_domain(cluster))
}

/// Cluster names are free-form, so DNS uses a sanitized label: lowercase letters, digits and
/// single dashes, at most 63 characters.
pub fn cluster_dns_label(cluster: &str) -> String {
    let dashed = cluster
        .to_ascii_lowercase()
        .chars()
        .map(
            |character| match character.is_ascii_lowercase() || character.is_ascii_digit() {
                true => character,
                false => '-',
            },
        )
        .collect::<String>();
    let label = dashed
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
        .chars()
        .take(MAX_DNS_LABEL_LENGTH)
        .collect::<String>()
        .trim_end_matches('-')
        .to_string();
    match label.is_empty() {
        true => FALLBACK_CLUSTER_LABEL.to_string(),
        false => label,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_cluster_names_into_dns_labels() {
        assert_eq!(cluster_dns_label("baton-sandbox"), "baton-sandbox");
        assert_eq!(cluster_dns_label("Baton_Sandbox"), "baton-sandbox");
        assert_eq!(cluster_dns_label("prod.east"), "prod-east");
        assert_eq!(cluster_dns_label("--Prod  East!!"), "prod-east");
        assert_eq!(cluster_dns_label("_.-"), "maestro");
        assert_eq!(cluster_dns_label(&"a-".repeat(40)).len(), 63);
        assert!(!cluster_dns_label(&"ab-".repeat(30)).ends_with('-'));
    }

    #[test]
    fn names_live_under_the_cluster_domain() {
        assert_eq!(cluster_domain("Baton Sandbox"), "baton-sandbox.maestro");
        assert_eq!(
            internal_name("baton-sandbox", "web"),
            "web.baton-sandbox.maestro"
        );
    }
}
