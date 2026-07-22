use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

use kernel_api::{AnnotationKey, BuiltinResource, NodeRole};
use sha2::{Digest, Sha256};

use crate::LegacyEntry;
use crate::legacy_convert::LegacyPlanError;
use crate::legacy_derived_schema::{
    LegacyControllerStats, LegacyDerivedSummary, LegacyDnsRecordSet, LegacyImageHolder,
    LegacyNodeDisk, LegacyRbacReady, LegacyTraefikServiceIdentity, LegacyUnschedulableReplica,
};
use crate::legacy_nodes::LegacyNodeCatalog;
use crate::legacy_services::LegacyServiceCatalog;

const STATS_PREFIX: &str = "/maetro/cluster/stats/";
const DISKS_PREFIX: &str = "/maetro/cluster/disks/";
const DNS_PREFIX: &str = "/maetro/cluster/dns/";
const IMAGE_HOLDERS_PREFIX: &str = "/maetro/cluster/image-holders/";
const NODE_IMAGE_HOLDERS_PREFIX: &str = "/maetro/cluster/node-image-holders/";
const SERVICE_MAP_PREFIX: &str = "/maetro/cluster/traefik-service-map/";
const UNSCHEDULABLE_KEY: &str = "/maetro/cluster/unschedulable";
const LEADER_KEY: &str = "/maetro/cluster/leader";
const RBAC_READY_KEY: &str = "/maetro/system/rbac-ready";
const SUMMARY_ANNOTATION: &str = "migration.maestro.dev/legacy-derived-state";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyDerivedCatalog {
    summary: LegacyDerivedSummary,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

impl LegacyDerivedCatalog {
    pub(crate) fn decode(
        entries: &[LegacyEntry],
        nodes: &LegacyNodeCatalog,
        services: &LegacyServiceCatalog,
    ) -> Result<Self, LegacyDerivedError> {
        let mut stats = BTreeMap::new();
        let mut disks = BTreeMap::new();
        let mut dns = BTreeMap::new();
        let mut holders_by_image = BTreeMap::new();
        let mut holders_by_node = BTreeMap::new();
        let mut service_mappings = BTreeMap::new();
        let mut unschedulable = Vec::new();
        let mut rbac_ready = None;
        let mut unclaimed = Vec::new();

        for entry in entries {
            if entry.key() == LEADER_KEY
                || entry
                    .key()
                    .strip_prefix(LEADER_KEY)
                    .is_some_and(|suffix| suffix.starts_with('/'))
            {
                return Err(LegacyDerivedError::ActiveControlPlane {
                    key: entry.key().to_owned(),
                });
            }
            match classify_key(entry.key())? {
                Some(DerivedKey::Stats(node_id)) => {
                    require_node(entry.key(), &node_id, nodes)?;
                    let value: LegacyControllerStats = decode_json(entry)?;
                    validate_stats(entry.key(), &value)?;
                    stats.insert(node_id, value);
                }
                Some(DerivedKey::Disks(node_id)) => {
                    require_node(entry.key(), &node_id, nodes)?;
                    let value: Vec<LegacyNodeDisk> = decode_json(entry)?;
                    validate_disks(entry.key(), &value)?;
                    disks.insert(node_id, value);
                }
                Some(DerivedKey::Dns(service_id)) => {
                    require_service(entry.key(), &service_id, services)?;
                    let value: LegacyDnsRecordSet = decode_json(entry)?;
                    validate_dns(entry.key(), &service_id, &value)?;
                    dns.insert(service_id, value);
                }
                Some(DerivedKey::ImageHolder { digest, node_id }) => {
                    require_node(entry.key(), &node_id, nodes)?;
                    let value: LegacyImageHolder = decode_json(entry)?;
                    validate_holder(entry.key(), &digest, &node_id, &value)?;
                    holders_by_image.insert((value.image.clone(), node_id), value);
                }
                Some(DerivedKey::NodeImageHolder { node_id, digest }) => {
                    require_node(entry.key(), &node_id, nodes)?;
                    let value: LegacyImageHolder = decode_json(entry)?;
                    validate_holder(entry.key(), &digest, &node_id, &value)?;
                    holders_by_node.insert((value.image.clone(), node_id), value);
                }
                Some(DerivedKey::ServiceMap(label)) => {
                    let value: LegacyTraefikServiceIdentity = decode_json(entry)?;
                    validate_service_mapping(entry.key(), &value, nodes, services)?;
                    service_mappings.insert(label, value);
                }
                Some(DerivedKey::Unschedulable) => {
                    unschedulable = decode_json(entry)?;
                    validate_unschedulable(entry.key(), &unschedulable, services)?;
                }
                Some(DerivedKey::RbacReady) => {
                    let value: LegacyRbacReady = decode_json(entry)?;
                    validate_rbac(entry.key(), &value, nodes)?;
                    rbac_ready = Some(value);
                }
                None => unclaimed.push(entry.clone()),
            }
        }
        if holders_by_image != holders_by_node {
            return Err(invalid(
                IMAGE_HOLDERS_PREFIX,
                "image-holder forward and reverse indexes disagree",
            ));
        }
        Ok(Self {
            summary: LegacyDerivedSummary {
                controller_stats: stats.len(),
                disk_snapshots: disks.len(),
                dns_record_sets: dns.len(),
                image_holders: holders_by_image.len(),
                traefik_service_mappings: service_mappings.len(),
                unschedulable_replicas: unschedulable.len(),
                rbac_version: rbac_ready.map(|ready| ready.version),
            },
            unclaimed,
        })
    }

    pub(crate) fn annotate_master(
        &self,
        resources: &mut [BuiltinResource],
    ) -> Result<(), LegacyPlanError> {
        let value = serde_json::to_string(&self.summary).map_err(|error| {
            LegacyPlanError::InvalidClusterState {
                resource_id: "legacy-derived-state".to_owned(),
                message: format!("could not preserve derived-state summary: {error}"),
            }
        })?;
        let master = resources.iter_mut().find_map(|resource| match resource {
            BuiltinResource::Node(node) if node.spec.role == NodeRole::Master => Some(node),
            _ => None,
        });
        let master = master.ok_or_else(|| LegacyPlanError::InvalidClusterState {
            resource_id: "legacy-derived-state".to_owned(),
            message: "converted master node is missing".to_owned(),
        })?;
        master
            .meta
            .annotations
            .insert(AnnotationKey(SUMMARY_ANNOTATION.to_owned()), value);
        Ok(())
    }
}

fn validate_stats(key: &str, stats: &LegacyControllerStats) -> Result<(), LegacyDerivedError> {
    if stats.reported_at_ms < 0 || stats.version.trim().is_empty() {
        return Err(invalid(
            key,
            "controller stats have an invalid timestamp or version",
        ));
    }
    let mut sink_ids = BTreeSet::new();
    for sink in &stats.sinks {
        if sink.id.trim().is_empty() || !sink_ids.insert(&sink.id) {
            return Err(invalid(
                key,
                "controller stats contain an empty or repeated sink id",
            ));
        }
    }
    Ok(())
}

fn validate_disks(key: &str, disks: &[LegacyNodeDisk]) -> Result<(), LegacyDerivedError> {
    let mut names = BTreeSet::new();
    let mut mounts = BTreeSet::new();
    for disk in disks {
        if disk.name.trim().is_empty()
            || disk.mount_point.trim().is_empty()
            || disk.file_system.trim().is_empty()
            || disk.available_bytes > disk.total_bytes
            || !names.insert(&disk.name)
            || !mounts.insert(&disk.mount_point)
        {
            return Err(invalid(
                key,
                "disk snapshot contains an invalid or repeated disk",
            ));
        }
    }
    Ok(())
}

fn validate_dns(
    key: &str,
    service_id: &str,
    records: &LegacyDnsRecordSet,
) -> Result<(), LegacyDerivedError> {
    if records.service_id != service_id || !valid_dns_name(&records.stable_fqdn) {
        return Err(invalid(
            key,
            "DNS key, service identity, or stable name is invalid",
        ));
    }
    let mut values = BTreeSet::new();
    for address in &records.addresses {
        let parsed = address
            .parse::<IpAddr>()
            .map_err(|error| invalid(key, format!("DNS address is invalid: {error}")))?;
        if address != &parsed.to_string() || !values.insert((String::new(), address.clone())) {
            return Err(invalid(key, "DNS addresses must be canonical and unique"));
        }
    }
    for (name, address) in &records.replica_records {
        let parsed = address
            .parse::<IpAddr>()
            .map_err(|error| invalid(key, format!("replica DNS address is invalid: {error}")))?;
        if !valid_dns_name(name)
            || address != &parsed.to_string()
            || !values.insert((name.clone(), address.clone()))
        {
            return Err(invalid(
                key,
                "replica DNS records must be canonical and unique",
            ));
        }
    }
    Ok(())
}

fn validate_holder(
    key: &str,
    digest: &str,
    node_id: &str,
    holder: &LegacyImageHolder,
) -> Result<(), LegacyDerivedError> {
    let expected = hex::encode(Sha256::digest(holder.image.as_bytes()));
    if holder.image.trim().is_empty()
        || holder.node_id != node_id
        || holder.available_at_ms < 0
        || digest != expected
    {
        return Err(invalid(key, "image-holder key and payload disagree"));
    }
    Ok(())
}

fn validate_service_mapping(
    key: &str,
    mapping: &LegacyTraefikServiceIdentity,
    nodes: &LegacyNodeCatalog,
    services: &LegacyServiceCatalog,
) -> Result<(), LegacyDerivedError> {
    require_deployment(key, &mapping.service_id, &mapping.deployment_id, services)?;
    if let Some(node_id) = &mapping.node_id {
        require_node(key, node_id, nodes)?;
    }
    Ok(())
}

fn validate_unschedulable(
    key: &str,
    entries: &[LegacyUnschedulableReplica],
    services: &LegacyServiceCatalog,
) -> Result<(), LegacyDerivedError> {
    let mut slots = BTreeSet::new();
    for entry in entries {
        require_deployment(key, &entry.service_id, &entry.deployment_id, services)?;
        if entry.reason.trim().is_empty()
            || !slots.insert((&entry.service_id, &entry.deployment_id, entry.replica_index))
        {
            return Err(invalid(
                key,
                "unschedulable observations are empty or repeated",
            ));
        }
    }
    Ok(())
}

fn validate_rbac(
    key: &str,
    ready: &LegacyRbacReady,
    nodes: &LegacyNodeCatalog,
) -> Result<(), LegacyDerivedError> {
    if ready.version != 3 {
        return Err(invalid(
            key,
            "RBAC marker has an unsupported schema version",
        ));
    }
    require_node(key, &ready.initialized_by, nodes)
}

fn require_node(
    key: &str,
    node_id: &str,
    nodes: &LegacyNodeCatalog,
) -> Result<(), LegacyDerivedError> {
    if nodes.contains(node_id) {
        Ok(())
    } else {
        Err(invalid(key, format!("references missing node `{node_id}`")))
    }
}

fn require_service(
    key: &str,
    service_id: &str,
    services: &LegacyServiceCatalog,
) -> Result<(), LegacyDerivedError> {
    if services.services.contains_key(service_id) {
        Ok(())
    } else {
        Err(invalid(
            key,
            format!("references missing service `{service_id}`"),
        ))
    }
}

fn require_deployment(
    key: &str,
    service_id: &str,
    deployment_id: &str,
    services: &LegacyServiceCatalog,
) -> Result<(), LegacyDerivedError> {
    let Some(service) = services.services.get(service_id) else {
        return Err(invalid(
            key,
            format!("references missing service `{service_id}`"),
        ));
    };
    let found = service
        .deployments
        .iter()
        .any(|record| record.deployment.id == deployment_id);
    if found {
        Ok(())
    } else {
        Err(invalid(
            key,
            format!("references missing deployment `{deployment_id}`"),
        ))
    }
}

fn valid_dns_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        })
}

fn classify_key(key: &str) -> Result<Option<DerivedKey>, LegacyDerivedError> {
    if let Some(node_id) = key.strip_prefix(STATS_PREFIX) {
        return Ok(Some(DerivedKey::Stats(segment(key, node_id)?)));
    }
    if let Some(node_id) = key.strip_prefix(DISKS_PREFIX) {
        return Ok(Some(DerivedKey::Disks(segment(key, node_id)?)));
    }
    if let Some(service_id) = key.strip_prefix(DNS_PREFIX) {
        return Ok(Some(DerivedKey::Dns(segment(key, service_id)?)));
    }
    if let Some(suffix) = key.strip_prefix(IMAGE_HOLDERS_PREFIX) {
        let (digest, node_id) = pair(key, suffix)?;
        validate_digest(key, digest)?;
        return Ok(Some(DerivedKey::ImageHolder {
            digest: digest.to_owned(),
            node_id: node_id.to_owned(),
        }));
    }
    if let Some(suffix) = key.strip_prefix(NODE_IMAGE_HOLDERS_PREFIX) {
        let (node_id, digest) = pair(key, suffix)?;
        validate_digest(key, digest)?;
        return Ok(Some(DerivedKey::NodeImageHolder {
            node_id: node_id.to_owned(),
            digest: digest.to_owned(),
        }));
    }
    if let Some(label) = key.strip_prefix(SERVICE_MAP_PREFIX) {
        return Ok(Some(DerivedKey::ServiceMap(segment(key, label)?)));
    }
    if key == UNSCHEDULABLE_KEY {
        return Ok(Some(DerivedKey::Unschedulable));
    }
    if key == RBAC_READY_KEY {
        return Ok(Some(DerivedKey::RbacReady));
    }
    Ok(None)
}

fn segment(key: &str, value: &str) -> Result<String, LegacyDerivedError> {
    if value.is_empty() || value.contains('/') {
        Err(invalid(key, "key suffix must be one non-empty segment"))
    } else {
        Ok(value.to_owned())
    }
}

fn pair<'a>(key: &str, value: &'a str) -> Result<(&'a str, &'a str), LegacyDerivedError> {
    let Some((left, right)) = value.split_once('/') else {
        return Err(invalid(key, "key suffix must contain two segments"));
    };
    if left.is_empty() || right.is_empty() || right.contains('/') {
        Err(invalid(
            key,
            "key suffix must contain two non-empty segments",
        ))
    } else {
        Ok((left, right))
    }
}

fn validate_digest(key: &str, digest: &str) -> Result<(), LegacyDerivedError> {
    if digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(invalid(
            key,
            "image digest segment is not lowercase SHA-256",
        ))
    }
}

fn decode_json<Value: serde::de::DeserializeOwned>(
    entry: &LegacyEntry,
) -> Result<Value, LegacyDerivedError> {
    serde_json::from_slice(entry.value())
        .map_err(|error| invalid(entry.key(), format!("invalid JSON: {error}")))
}

fn invalid(key: impl Into<String>, message: impl Into<String>) -> LegacyDerivedError {
    LegacyDerivedError::InvalidState {
        key: key.into(),
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DerivedKey {
    Stats(String),
    Disks(String),
    Dns(String),
    ImageHolder { digest: String, node_id: String },
    NodeImageHolder { node_id: String, digest: String },
    ServiceMap(String),
    Unschedulable,
    RbacReady,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyDerivedError {
    #[error("legacy control-plane lease `{key}` is still active")]
    ActiveControlPlane { key: String },
    #[error("legacy derived state at `{key}` is invalid: {message}")]
    InvalidState { key: String, message: String },
}
