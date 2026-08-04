use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

use kernel_api::{
    AssignmentId, BuiltinResource, DeploymentId, DnsName, Generation, NodeId, Object, ObjectMeta,
    OwnerReference, Ownership, PlacementHistorySpec, PlacementHistoryStatus, ResourceId,
    ResourceKind, ResourceRevision, ServiceId, Timestamp,
};

use crate::LegacyEntry;
use crate::legacy_convert::{LegacyPlanError, invalid_generated};
use crate::legacy_placement_schema::LegacyPlacementHistory;

const PLACEMENTS_PREFIX: &str = "/maetro/cluster/placements/";
const INDEX_PREFIX: &str = "/maetro/cluster/placement-index/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyPlacementCatalog {
    placements: BTreeMap<String, LegacyPlacementHistory>,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

impl LegacyPlacementCatalog {
    pub(crate) fn decode(entries: &[LegacyEntry]) -> Result<Self, LegacyPlacementError> {
        let mut placements = BTreeMap::new();
        let mut indexes = BTreeMap::new();
        let mut unclaimed = Vec::new();
        for entry in entries {
            match classify_key(entry.key())? {
                Some(PlacementKey::Record {
                    node_id,
                    assignment_id,
                }) => {
                    let placement: LegacyPlacementHistory = decode_json(entry)?;
                    validate_record(entry.key(), &node_id, &assignment_id, &placement)?;
                    if placements.insert(assignment_id, placement).is_some() {
                        return Err(invalid(
                            entry.key(),
                            "assignment has more than one placement record",
                        ));
                    }
                }
                Some(PlacementKey::Index {
                    service_id,
                    deployment_id,
                    replica_index,
                    assignment_id,
                }) => {
                    let target = utf8(entry)?;
                    let expected_target = format!(
                        "{PLACEMENTS_PREFIX}{}/{}",
                        target_node(target)?,
                        assignment_id
                    );
                    if target != expected_target {
                        return Err(invalid(
                            entry.key(),
                            "placement index value is not its canonical record key",
                        ));
                    }
                    let index = PlacementIndex {
                        service_id,
                        deployment_id,
                        replica_index,
                        target: target.to_owned(),
                    };
                    if indexes.insert(assignment_id, index).is_some() {
                        return Err(invalid(
                            entry.key(),
                            "assignment has more than one placement index",
                        ));
                    }
                }
                None => unclaimed.push(entry.clone()),
            }
        }
        validate_indexes(&placements, &indexes)?;
        Ok(Self {
            placements,
            unclaimed,
        })
    }

    pub(crate) fn convert(&self) -> Result<Vec<BuiltinResource>, LegacyPlanError> {
        self.placements.values().map(convert_placement).collect()
    }
}

fn validate_record(
    key: &str,
    key_node_id: &str,
    key_assignment_id: &str,
    placement: &LegacyPlacementHistory,
) -> Result<(), LegacyPlacementError> {
    validate_id::<NodeId>(key, key_node_id, "node")?;
    validate_id::<AssignmentId>(key, key_assignment_id, "assignment")?;
    validate_id::<ServiceId>(key, &placement.service_id, "service")?;
    validate_id::<DeploymentId>(key, &placement.deployment_id, "deployment")?;
    if placement.node_id != key_node_id || placement.assignment_id != key_assignment_id {
        return Err(invalid(
            key,
            "placement key and payload identities disagree",
        ));
    }
    let address = placement
        .cluster_host_ip
        .parse::<IpAddr>()
        .map_err(|error| invalid(key, format!("cluster host address is invalid: {error}")))?;
    if placement.cluster_host_ip != address.to_string()
        || placement.cluster_api_port == 0
        || !valid_hostname(&placement.container_hostname)
        || placement.started_at_ms < 0
        || placement
            .ended_at_ms
            .is_some_and(|ended| ended < placement.started_at_ms)
    {
        return Err(invalid(
            key,
            "placement endpoint, hostname, or timestamps are invalid",
        ));
    }
    Ok(())
}

fn validate_indexes(
    placements: &BTreeMap<String, LegacyPlacementHistory>,
    indexes: &BTreeMap<String, PlacementIndex>,
) -> Result<(), LegacyPlacementError> {
    if placements.len() != indexes.len() {
        return Err(invalid(
            INDEX_PREFIX,
            "placement records and indexes do not have one-to-one coverage",
        ));
    }
    for (assignment_id, placement) in placements {
        let index = indexes.get(assignment_id).ok_or_else(|| {
            invalid(
                format!("{PLACEMENTS_PREFIX}{}/{assignment_id}", placement.node_id),
                "placement has no reverse index",
            )
        })?;
        let expected_target = format!("{PLACEMENTS_PREFIX}{}/{assignment_id}", placement.node_id);
        if index.service_id != placement.service_id
            || index.deployment_id != placement.deployment_id
            || index.replica_index != placement.replica_index
            || index.target != expected_target
        {
            return Err(invalid(
                format!("{INDEX_PREFIX}{assignment_id}"),
                "placement index and record disagree",
            ));
        }
    }
    Ok(())
}

fn convert_placement(legacy: &LegacyPlacementHistory) -> Result<BuiltinResource, LegacyPlanError> {
    let assignment_id = parse_id::<AssignmentId>(&legacy.assignment_id, "placement assignment id")?;
    let service_id = parse_id::<ServiceId>(&legacy.service_id, "placement service id")?;
    let deployment_id = parse_id::<DeploymentId>(&legacy.deployment_id, "placement deployment id")?;
    let node_id = parse_id::<NodeId>(&legacy.node_id, "placement node id")?;
    let cluster_host_address =
        legacy
            .cluster_host_ip
            .parse()
            .map_err(|error| LegacyPlanError::InvalidClusterState {
                resource_id: legacy.assignment_id.clone(),
                message: format!("placement host address is invalid: {error}"),
            })?;
    let deployment_kind = ResourceKind::new("Deployment")
        .map_err(|error| invalid_generated("PlacementHistory", error))?;
    Ok(BuiltinResource::PlacementHistory(Object {
        meta: ObjectMeta {
            id: assignment_id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision(0),
            generation: Generation(1),
            owner_refs: vec![OwnerReference {
                resource: ResourceId::new(deployment_kind, deployment_id.clone().into()),
                ownership: Ownership::Informational,
            }],
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: PlacementHistorySpec {
            service_id,
            deployment_id,
            replica_index: legacy.replica_index,
            node_id,
            cluster_host_address,
            cluster_api_port: legacy.cluster_api_port,
            container_hostname: legacy.container_hostname.clone(),
        },
        status: PlacementHistoryStatus {
            started_at: Timestamp(legacy.started_at_ms),
            ended_at: legacy.ended_at_ms.map(Timestamp),
        },
    }))
}

fn classify_key(key: &str) -> Result<Option<PlacementKey>, LegacyPlacementError> {
    if let Some(suffix) = key.strip_prefix(PLACEMENTS_PREFIX) {
        let (node_id, assignment_id) = pair(key, suffix)?;
        return Ok(Some(PlacementKey::Record {
            node_id: node_id.to_owned(),
            assignment_id: assignment_id.to_owned(),
        }));
    }
    if let Some(suffix) = key.strip_prefix(INDEX_PREFIX) {
        let (service_id, deployment_id, raw_replica_index, assignment_id) = quadruple(key, suffix)?;
        let replica_index = raw_replica_index
            .parse::<u32>()
            .map_err(|error| invalid(key, format!("replica index is invalid: {error}")))?;
        if raw_replica_index != replica_index.to_string() {
            return Err(invalid(key, "replica index is not canonical"));
        }
        return Ok(Some(PlacementKey::Index {
            service_id: service_id.to_owned(),
            deployment_id: deployment_id.to_owned(),
            replica_index,
            assignment_id: assignment_id.to_owned(),
        }));
    }
    Ok(None)
}

fn pair<'a>(key: &str, suffix: &'a str) -> Result<(&'a str, &'a str), LegacyPlacementError> {
    let Some((left, right)) = suffix.split_once('/') else {
        return Err(invalid(key, "key suffix must contain two segments"));
    };
    if left.is_empty() || right.is_empty() || right.contains('/') {
        Err(invalid(key, "key suffix must contain two segments"))
    } else {
        Ok((left, right))
    }
}

fn quadruple<'a>(
    key: &str,
    suffix: &'a str,
) -> Result<(&'a str, &'a str, &'a str, &'a str), LegacyPlacementError> {
    let mut parts = suffix.split('/');
    let values = (parts.next(), parts.next(), parts.next(), parts.next());
    let (Some(first), Some(second), Some(third), Some(fourth)) = values else {
        return Err(invalid(key, "key suffix must contain four segments"));
    };
    if [first, second, third, fourth]
        .into_iter()
        .any(str::is_empty)
        || parts.next().is_some()
    {
        Err(invalid(key, "key suffix must contain four segments"))
    } else {
        Ok((first, second, third, fourth))
    }
}

fn target_node(target: &str) -> Result<&str, LegacyPlacementError> {
    let suffix = target.strip_prefix(PLACEMENTS_PREFIX).ok_or_else(|| {
        invalid(
            target,
            "placement index target is outside the placement prefix",
        )
    })?;
    pair(target, suffix).map(|(node_id, _)| node_id)
}

fn valid_hostname(value: &str) -> bool {
    DnsName::parse_case_insensitive(value).is_ok()
}

fn validate_id<Id>(key: &str, value: &str, kind: &str) -> Result<(), LegacyPlacementError>
where
    Id: TryFrom<String>,
    <Id as TryFrom<String>>::Error: std::fmt::Display,
{
    Id::try_from(value.to_owned())
        .map(|_| ())
        .map_err(|error| invalid(key, format!("{kind} id is invalid: {error}")))
}

fn parse_id<Id>(value: &str, field: &'static str) -> Result<Id, LegacyPlanError>
where
    Id: TryFrom<String>,
    <Id as TryFrom<String>>::Error: std::fmt::Display,
{
    Id::try_from(value.to_owned()).map_err(|error| LegacyPlanError::InvalidIdentifier {
        field,
        value: value.to_owned(),
        message: error.to_string(),
    })
}

fn decode_json<Value: serde::de::DeserializeOwned>(
    entry: &LegacyEntry,
) -> Result<Value, LegacyPlacementError> {
    serde_json::from_slice(entry.value())
        .map_err(|error| invalid(entry.key(), format!("invalid JSON: {error}")))
}

fn utf8(entry: &LegacyEntry) -> Result<&str, LegacyPlacementError> {
    std::str::from_utf8(entry.value())
        .map_err(|error| invalid(entry.key(), format!("value is not UTF-8: {error}")))
}

fn invalid(key: impl Into<String>, message: impl Into<String>) -> LegacyPlacementError {
    LegacyPlacementError::InvalidState {
        key: key.into(),
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlacementIndex {
    service_id: String,
    deployment_id: String,
    replica_index: u32,
    target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PlacementKey {
    Record {
        node_id: String,
        assignment_id: String,
    },
    Index {
        service_id: String,
        deployment_id: String,
        replica_index: u32,
        assignment_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyPlacementError {
    #[error("legacy placement state at `{key}` is invalid: {message}")]
    InvalidState { key: String, message: String },
}
