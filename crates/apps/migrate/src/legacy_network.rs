use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};

use kernel_api::{
    AnnotationKey, Assignment, BuiltinResource, Generation, IngressBlocklist, IngressBlocklistId,
    IngressBlocklistSpec, IngressBlocklistStatus, IngressRoute, Object, ObjectMeta,
    ResourceRevision, ServiceId, Timestamp, TrafficGenerationId, TrafficGenerationPhase,
    TrafficGenerationSpec, TrafficGenerationStatus, TrafficRoute, TrafficTarget,
};

use crate::LegacyEntry;
use crate::legacy_convert::{LegacyPlanError, annotations, owner, stable_id};
use crate::legacy_schema::LegacyTrafficGeneration;

const TRAFFIC_PREFIX: &str = "/maetro/cluster/traffic/";
const BLOCKLIST_PREFIX: &str = "/maetro/cluster/ingress-blocklist/";
const BLOCKLIST_APPLIED_KEY: &str = "/maetro/cluster/ingress-blocklist-applied";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyNetworkCatalog {
    traffic: BTreeMap<String, LegacyTrafficGeneration>,
    blocklist: BTreeSet<IpAddr>,
    blocklist_applied: Option<String>,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

impl LegacyNetworkCatalog {
    pub(crate) fn decode(entries: &[LegacyEntry]) -> Result<Self, LegacyNetworkError> {
        let mut traffic = BTreeMap::new();
        let mut blocklist = BTreeSet::new();
        let mut blocklist_applied = None;
        let mut unclaimed = Vec::new();
        for entry in entries {
            if let Some(service_id) = entry.key().strip_prefix(TRAFFIC_PREFIX) {
                if service_id.is_empty() || service_id.contains('/') {
                    return Err(invalid(entry.key(), "traffic key is malformed"));
                }
                let generation: LegacyTrafficGeneration = decode_json(entry)?;
                if generation.service_id != service_id {
                    return Err(invalid(
                        entry.key(),
                        "traffic key and payload service identities disagree",
                    ));
                }
                traffic.insert(service_id.to_owned(), generation);
            } else if let Some(raw_address) = entry.key().strip_prefix(BLOCKLIST_PREFIX) {
                if raw_address.is_empty() || raw_address.contains('/') {
                    return Err(invalid(entry.key(), "blocklist key is malformed"));
                }
                let value = utf8(entry)?;
                let address = raw_address.parse::<IpAddr>().map_err(|error| {
                    invalid(
                        entry.key(),
                        format!("blocklist address is invalid: {error}"),
                    )
                })?;
                if raw_address != address.to_string() || value != raw_address {
                    return Err(invalid(
                        entry.key(),
                        "blocklist key and value must contain the same canonical address",
                    ));
                }
                blocklist.insert(address);
            } else if entry.key() == BLOCKLIST_APPLIED_KEY {
                let digest = utf8(entry)?.to_owned();
                if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(invalid(entry.key(), "applied blocklist digest is invalid"));
                }
                blocklist_applied = Some(digest);
            } else {
                unclaimed.push(entry.clone());
            }
        }
        Ok(Self {
            traffic,
            blocklist,
            blocklist_applied,
            unclaimed,
        })
    }

    pub(crate) fn convert(
        &self,
        resources: &[BuiltinResource],
    ) -> Result<Vec<BuiltinResource>, LegacyPlanError> {
        let mut converted = self
            .traffic
            .iter()
            .map(|(service_id, traffic)| convert_traffic(service_id, traffic, resources))
            .collect::<Result<Vec<_>, _>>()?;
        if !self.blocklist.is_empty() || self.blocklist_applied.is_some() {
            converted.push(BuiltinResource::IngressBlocklist(convert_blocklist(self)?));
        }
        Ok(converted)
    }
}

fn convert_traffic(
    key_service_id: &str,
    legacy: &LegacyTrafficGeneration,
    resources: &[BuiltinResource],
) -> Result<BuiltinResource, LegacyPlanError> {
    let service_id =
        ServiceId::new(key_service_id).map_err(|error| LegacyPlanError::InvalidIdentifier {
            field: "traffic service id",
            value: key_service_id.to_owned(),
            message: error.to_string(),
        })?;
    let service = find_service(resources, &service_id)?;
    let deployment_id = kernel_api::DeploymentId::new(&legacy.deployment_id).map_err(|error| {
        LegacyPlanError::InvalidIdentifier {
            field: "traffic deployment id",
            value: legacy.deployment_id.clone(),
            message: error.to_string(),
        }
    })?;
    let deployment = resources.iter().find_map(|resource| match resource {
        BuiltinResource::Deployment(deployment) if deployment.meta.id == deployment_id => {
            Some(deployment)
        }
        _ => None,
    });
    if deployment.is_none_or(|deployment| deployment.spec.service_id != service_id) {
        return Err(invalid_plan(
            key_service_id,
            "traffic refers to a missing or foreign deployment",
        ));
    }
    if legacy.traffic_epoch == 0
        || legacy.generation.trim().is_empty()
        || legacy.drain_old_after_ms < legacy.switched_at_ms
    {
        return Err(invalid_plan(
            key_service_id,
            "traffic has a zero epoch, empty generation, or inverted drain deadline",
        ));
    }

    let routes = captured_routes(resources, &service_id);
    let mut assignment_ids = legacy.active_assignment_ids.clone();
    assignment_ids.sort();
    let original_count = assignment_ids.len();
    assignment_ids.dedup();
    if assignment_ids.len() != original_count {
        return Err(invalid_plan(
            key_service_id,
            "traffic repeats an active assignment",
        ));
    }
    let mut assignments = assignment_ids
        .iter()
        .map(|assignment_id| find_assignment(resources, assignment_id, &service_id, &deployment_id))
        .collect::<Result<Vec<_>, _>>()?;
    assignments.sort_by(|left, right| {
        left.spec
            .replica_index
            .cmp(&right.spec.replica_index)
            .then_with(|| left.meta.id.cmp(&right.meta.id))
    });
    validate_active_nodes(key_service_id, legacy, &assignments)?;
    for assignment in &assignments {
        validate_ready_replica(resources, assignment)?;
    }

    let ports = routes
        .iter()
        .map(|route| route.target_port)
        .collect::<BTreeSet<_>>();
    let targets = assignments
        .iter()
        .flat_map(|assignment| {
            ports.iter().map(move |port| TrafficTarget {
                assignment_id: assignment.meta.id.clone(),
                node_id: assignment.spec.node_id.clone(),
                endpoint: SocketAddr::new(assignment.spec.workload_address, *port),
            })
        })
        .collect();
    let generation_id = migrated_traffic_id(legacy)?;
    let annotations = annotations([
        (
            "migration.maestro.dev/legacy-traffic-generation",
            legacy.generation.clone(),
        ),
        (
            "migration.maestro.dev/legacy-routing-fingerprint",
            legacy.routing_fingerprint.clone(),
        ),
        (
            "migration.maestro.dev/legacy-drain-old-after-ms",
            legacy.drain_old_after_ms.to_string(),
        ),
        (
            "migration.maestro.dev/legacy-active-assignment-ids",
            encode_annotation(key_service_id, &assignment_ids)?,
        ),
        (
            "migration.maestro.dev/legacy-active-node-ids",
            encode_annotation(key_service_id, &legacy.active_node_ids)?,
        ),
    ]);
    Ok(BuiltinResource::TrafficGeneration(Object {
        meta: ObjectMeta {
            id: generation_id,
            labels: BTreeMap::new(),
            annotations,
            revision: ResourceRevision(0),
            generation: Generation(1),
            owner_refs: vec![owner("Service", service.meta.id.clone().into())?],
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: TrafficGenerationSpec {
            service_id,
            deployment_id,
            epoch: legacy.traffic_epoch,
            routes,
            targets,
        },
        status: TrafficGenerationStatus {
            phase: TrafficGenerationPhase::Active,
            staged_at: Timestamp(legacy.switched_at_ms),
            activated_at: Some(Timestamp(legacy.switched_at_ms)),
            retired_at: None,
            conditions: Vec::new(),
        },
    }))
}

fn convert_blocklist(catalog: &LegacyNetworkCatalog) -> Result<IngressBlocklist, LegacyPlanError> {
    let mut annotations = BTreeMap::new();
    if let Some(digest) = &catalog.blocklist_applied {
        annotations.insert(
            AnnotationKey("migration.maestro.dev/legacy-applied-digest".to_owned()),
            digest.clone(),
        );
    }
    Ok(Object {
        meta: ObjectMeta {
            id: IngressBlocklistId::new("global").map_err(|error| {
                crate::legacy_convert::invalid_generated("IngressBlocklist", error)
            })?,
            labels: BTreeMap::new(),
            annotations,
            revision: ResourceRevision(0),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: IngressBlocklistSpec {
            addresses: catalog.blocklist.iter().copied().collect(),
        },
        status: IngressBlocklistStatus {
            applied_generation: Generation(0),
            configuration_digest: None,
            conditions: Vec::new(),
        },
    })
}

fn captured_routes(resources: &[BuiltinResource], service_id: &ServiceId) -> Vec<TrafficRoute> {
    let mut routes = resources
        .iter()
        .filter_map(|resource| match resource {
            BuiltinResource::IngressRoute(route) if &route.spec.service_id == service_id => {
                Some(capture_route(route))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    routes.sort_by(|left, right| left.route_id.cmp(&right.route_id));
    routes
}

fn capture_route(route: &IngressRoute) -> TrafficRoute {
    let mut hosts = route.spec.hosts.clone();
    hosts.sort();
    TrafficRoute {
        route_id: route.meta.id.clone(),
        route_generation: route.meta.generation,
        hosts,
        path_prefix: route.spec.path_prefix.clone(),
        target_port: route.spec.target_port,
        session_affinity: route.spec.session_affinity.clone(),
    }
}

fn find_service<'a>(
    resources: &'a [BuiltinResource],
    service_id: &ServiceId,
) -> Result<&'a kernel_api::Service, LegacyPlanError> {
    resources
        .iter()
        .find_map(|resource| match resource {
            BuiltinResource::Service(service) if &service.meta.id == service_id => Some(service),
            _ => None,
        })
        .ok_or_else(|| invalid_plan(service_id.as_str(), "traffic service is missing"))
}

fn find_assignment<'a>(
    resources: &'a [BuiltinResource],
    assignment_id: &str,
    service_id: &ServiceId,
    deployment_id: &kernel_api::DeploymentId,
) -> Result<&'a Assignment, LegacyPlanError> {
    let assignment = resources.iter().find_map(|resource| match resource {
        BuiltinResource::Assignment(assignment) if assignment.meta.id.as_str() == assignment_id => {
            Some(assignment)
        }
        _ => None,
    });
    assignment
        .filter(|assignment| {
            &assignment.spec.service_id == service_id
                && &assignment.spec.deployment_id == deployment_id
                && assignment.meta.deletion_timestamp.is_none()
        })
        .ok_or_else(|| {
            invalid_plan(
                service_id.as_str(),
                format!("traffic assignment `{assignment_id}` is missing or foreign"),
            )
        })
}

fn validate_ready_replica(
    resources: &[BuiltinResource],
    assignment: &Assignment,
) -> Result<(), LegacyPlanError> {
    let ready = resources.iter().any(|resource| match resource {
        BuiltinResource::ReplicaState(replica) => {
            replica.spec.assignment_id == assignment.meta.id
                && replica.status.phase == kernel_api::DeploymentPhase::Ready
        }
        _ => false,
    });
    if ready {
        Ok(())
    } else {
        Err(invalid_plan(
            assignment.spec.service_id.as_str(),
            format!(
                "active traffic assignment `{}` has no ready replica state",
                assignment.meta.id
            ),
        ))
    }
}

fn validate_active_nodes(
    service_id: &str,
    legacy: &LegacyTrafficGeneration,
    assignments: &[&Assignment],
) -> Result<(), LegacyPlanError> {
    if legacy.active_node_ids.is_empty() {
        return Ok(());
    }
    let legacy_nodes = legacy.active_node_ids.iter().collect::<BTreeSet<_>>();
    let assignment_nodes = assignments
        .iter()
        .map(|assignment| assignment.spec.node_id.as_str())
        .collect::<BTreeSet<_>>();
    if legacy_nodes.len() == legacy.active_node_ids.len()
        && legacy_nodes
            .iter()
            .map(|node| node.as_str())
            .eq(assignment_nodes)
    {
        Ok(())
    } else {
        Err(invalid_plan(
            service_id,
            "traffic active node identities disagree with active assignments",
        ))
    }
}

fn migrated_traffic_id(
    legacy: &LegacyTrafficGeneration,
) -> Result<TrafficGenerationId, LegacyPlanError> {
    let candidate = format!("traffic-{}", legacy.generation);
    TrafficGenerationId::new(&candidate)
        .or_else(|_| TrafficGenerationId::new(stable_id("legacy-traffic", &legacy.generation)))
        .map_err(|error| crate::legacy_convert::invalid_generated("TrafficGeneration", error))
}

fn encode_annotation(
    service_id: &str,
    value: &impl serde::Serialize,
) -> Result<String, LegacyPlanError> {
    serde_json::to_string(value).map_err(|error| {
        invalid_plan(
            service_id,
            format!("could not preserve traffic metadata: {error}"),
        )
    })
}

fn decode_json<Value: serde::de::DeserializeOwned>(
    entry: &LegacyEntry,
) -> Result<Value, LegacyNetworkError> {
    serde_json::from_slice(entry.value())
        .map_err(|error| invalid(entry.key(), format!("invalid JSON: {error}")))
}

fn utf8(entry: &LegacyEntry) -> Result<&str, LegacyNetworkError> {
    std::str::from_utf8(entry.value())
        .map_err(|error| invalid(entry.key(), format!("value is not UTF-8: {error}")))
}

fn invalid_plan(service_id: impl Into<String>, message: impl Into<String>) -> LegacyPlanError {
    LegacyPlanError::InvalidClusterState {
        resource_id: service_id.into(),
        message: message.into(),
    }
}

fn invalid(key: impl Into<String>, message: impl Into<String>) -> LegacyNetworkError {
    LegacyNetworkError::InvalidState {
        key: key.into(),
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyNetworkError {
    #[error("legacy network state at `{key}` is invalid: {message}")]
    InvalidState { key: String, message: String },
}
