use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use http::Uri;
use kernel_api::{AnnotationKey, BuiltinResource, NodeId};
use serde::{Deserialize, Serialize};

use crate::LegacyEntry;
use crate::legacy_convert::LegacyPlanError;
use crate::legacy_nodes::{LegacyControlEndpoint, LegacyNodeCatalog};

const VOTER_PREFIX: &str = "/maetro/cluster/voters/";
const MEMBER_ANNOTATION: &str = "migration.maestro.dev/legacy-store-member";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyMembershipCatalog {
    members: BTreeMap<NodeId, LegacyStoreMember>,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

impl LegacyMembershipCatalog {
    pub(crate) fn decode(
        entries: &[LegacyEntry],
        nodes: &LegacyNodeCatalog,
    ) -> Result<Self, LegacyMembershipError> {
        let mut records = BTreeMap::new();
        let mut unclaimed = Vec::new();
        for entry in entries {
            if let Some(suffix) = entry.key().strip_prefix(VOTER_PREFIX) {
                let member_id = parse_member_key(entry.key(), suffix)?;
                let member: LegacyStoreMember = decode_json(entry)?;
                validate_member(entry.key(), member_id, &member)?;
                records.insert(member_id, member);
            } else {
                unclaimed.push(entry.clone());
            }
        }
        let members = match_members(records, nodes)?;
        Ok(Self { members, unclaimed })
    }

    pub(crate) fn annotate_nodes(
        &self,
        resources: &mut [BuiltinResource],
    ) -> Result<(), LegacyPlanError> {
        for (node_id, member) in &self.members {
            let node = resources.iter_mut().find_map(|resource| match resource {
                BuiltinResource::Node(node) if &node.meta.id == node_id => Some(node),
                _ => None,
            });
            let node = node.ok_or_else(|| LegacyPlanError::InvalidClusterState {
                resource_id: node_id.to_string(),
                message: "converted control-plane node is missing".to_owned(),
            })?;
            let value = serde_json::to_string(member).map_err(|error| {
                LegacyPlanError::InvalidClusterState {
                    resource_id: node_id.to_string(),
                    message: format!("could not preserve legacy store member: {error}"),
                }
            })?;
            node.meta
                .annotations
                .insert(AnnotationKey(MEMBER_ANNOTATION.to_owned()), value);
        }
        Ok(())
    }
}

fn match_members(
    records: BTreeMap<u64, LegacyStoreMember>,
    nodes: &LegacyNodeCatalog,
) -> Result<BTreeMap<NodeId, LegacyStoreMember>, LegacyMembershipError> {
    let endpoints = nodes.control_plane_endpoints();
    if records.len() != endpoints.len() || !matches!(records.len(), 1 | 3) {
        return Err(invalid(
            VOTER_PREFIX,
            format!(
                "current voter records must cover all {} control-plane nodes",
                endpoints.len()
            ),
        ));
    }
    let mut matched = BTreeMap::new();
    for member in records.into_values() {
        let key = member_key(member.member_id);
        let peer_value = member
            .peer_urls
            .first()
            .ok_or_else(|| invalid(&key, "member has no peer URL"))?;
        let client_value = member
            .client_urls
            .first()
            .ok_or_else(|| invalid(&key, "member has no client URL"))?;
        let peer = parse_member_url(peer_value)?;
        let client = parse_member_url(client_value)?;
        if peer.scheme != client.scheme || peer.host != client.host {
            return Err(invalid(
                &key,
                "peer and client URLs must use the same scheme and IPv4 host",
            ));
        }
        let candidates = endpoints
            .iter()
            .filter(|(_, endpoint)| endpoint_matches(**endpoint, peer, client))
            .map(|(node_id, _)| node_id.clone())
            .collect::<Vec<_>>();
        let [node_id] = candidates.as_slice() else {
            return Err(invalid(
                key,
                "member URLs do not identify exactly one active control-plane reservation",
            ));
        };
        if matched.insert(node_id.clone(), member).is_some() {
            return Err(invalid(
                node_id.to_string(),
                "more than one voter record identifies this control-plane node",
            ));
        }
    }
    let covered = matched.keys().cloned().collect::<BTreeSet<_>>();
    let expected = endpoints.keys().cloned().collect::<BTreeSet<_>>();
    if covered != expected {
        return Err(invalid(
            VOTER_PREFIX,
            "voter records do not cover the current control-plane topology",
        ));
    }
    Ok(matched)
}

fn endpoint_matches(endpoint: LegacyControlEndpoint, peer: MemberUrl, client: MemberUrl) -> bool {
    endpoint.host_ip == peer.host
        && endpoint.etcd_peer_port == peer.port
        && endpoint.etcd_client_port == client.port
}

fn validate_member(
    key: &str,
    key_member_id: u64,
    member: &LegacyStoreMember,
) -> Result<(), LegacyMembershipError> {
    if member.member_id == 0 || member.member_id != key_member_id {
        return Err(invalid(
            key,
            "member id is zero or disagrees with the voter key",
        ));
    }
    if member.name.is_empty()
        || member.name.len() > 256
        || !member
            .name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(invalid(key, "member name is empty, oversized, or unsafe"));
    }
    if member.peer_urls.len() != 1 || member.client_urls.len() != 1 {
        return Err(invalid(
            key,
            "member must advertise exactly one peer URL and one client URL",
        ));
    }
    Ok(())
}

fn parse_member_key(key: &str, suffix: &str) -> Result<u64, LegacyMembershipError> {
    if suffix.len() != 16
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            key,
            "voter key must end in exactly 16 lowercase hexadecimal digits",
        ));
    }
    u64::from_str_radix(suffix, 16)
        .map_err(|error| invalid(key, format!("member id is invalid: {error}")))
}

fn parse_member_url(value: &str) -> Result<MemberUrl, LegacyMembershipError> {
    let uri = value
        .parse::<Uri>()
        .map_err(|error| invalid(value, format!("member URL is invalid: {error}")))?;
    let scheme = match uri.scheme_str() {
        Some("http") => "http",
        Some("https") => "https",
        _ => return Err(invalid(value, "member URL scheme must be http or https")),
    };
    let authority = uri
        .authority()
        .ok_or_else(|| invalid(value, "member URL has no authority"))?;
    let host = authority
        .host()
        .parse::<Ipv4Addr>()
        .map_err(|error| invalid(value, format!("member URL has no IPv4 host: {error}")))?;
    let port = authority
        .port_u16()
        .filter(|port| *port != 0)
        .ok_or_else(|| invalid(value, "member URL has no valid port"))?;
    if value != format!("{scheme}://{host}:{port}") {
        return Err(invalid(
            value,
            "member URL must be a canonical endpoint without a path or query",
        ));
    }
    Ok(MemberUrl { scheme, host, port })
}

fn decode_json<Value: serde::de::DeserializeOwned>(
    entry: &LegacyEntry,
) -> Result<Value, LegacyMembershipError> {
    serde_json::from_slice(entry.value())
        .map_err(|error| invalid(entry.key(), format!("invalid JSON: {error}")))
}

fn member_key(member_id: u64) -> String {
    format!("{VOTER_PREFIX}{member_id:016x}")
}

fn invalid(key: impl Into<String>, message: impl Into<String>) -> LegacyMembershipError {
    LegacyMembershipError::InvalidState {
        key: key.into(),
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyMembershipError {
    #[error("legacy voter state at `{key}` is invalid: {message}")]
    InvalidState { key: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LegacyStoreMember {
    member_id: u64,
    name: String,
    peer_urls: Vec<String>,
    client_urls: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemberUrl {
    scheme: &'static str,
    host: Ipv4Addr,
    port: u16,
}
