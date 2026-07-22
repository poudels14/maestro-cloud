use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

use kernel_api::{
    AnnotationKey, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    BuiltinResource, DeploymentId, Generation, NodeId, Object, ObjectMeta, ReplicaState,
    ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus, ResourceRevision, ServiceId, Timestamp,
    WorkloadId,
};

use crate::LegacyEntry;
use crate::legacy_convert::{
    LegacyPlanError, annotations, deployment_phase, invalid_generated, owner,
};
use crate::legacy_nodes::LegacyNodeCatalog;
use crate::legacy_schema::{
    LegacyAssignment, LegacyAssignmentManifest, LegacyDeploymentStatus, LegacyImageAssignment,
    LegacyReplicaState,
};
use crate::legacy_services::LegacyServiceCatalog;

const ASSIGNMENTS_PREFIX: &str = "/maetro/cluster/assignments/";
const REPLICAS_PREFIX: &str = "/maetro/cluster/replica-states/";
const LEGACY_IMAGE_ASSIGNMENTS_ANNOTATION: &str = "migration.maestro.dev/legacy-image-assignments";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyClusterCatalog {
    manifest_nodes: BTreeSet<String>,
    assignments: BTreeMap<String, AssignmentRecord>,
    replicas: BTreeMap<String, ReplicaRecord>,
    images: Vec<ImageRecord>,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AssignmentRecord {
    manifest_generation: u64,
    assignment: LegacyAssignment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicaRecord {
    node_id: String,
    state: LegacyReplicaState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImageRecord {
    node_id: String,
    manifest_generation: u64,
    image: LegacyImageAssignment,
}

impl LegacyClusterCatalog {
    pub(crate) fn decode(entries: &[LegacyEntry]) -> Result<Self, LegacyClusterError> {
        let mut assignments = BTreeMap::new();
        let mut manifest_nodes = BTreeSet::new();
        let mut replicas = BTreeMap::new();
        let mut images = Vec::new();
        let mut unclaimed = Vec::new();
        let mut addresses = BTreeSet::new();

        for entry in entries {
            match classify_key(entry.key())? {
                Some(ClusterKey::Assignments { node_id }) => {
                    manifest_nodes.insert(node_id.clone());
                    let manifest: LegacyAssignmentManifest = decode_json(entry)?;
                    validate_manifest_identity(entry.key(), &node_id, &manifest)?;
                    let mut manifest_images = BTreeSet::new();
                    for assignment in manifest.assignments {
                        if assignment.node_id != node_id {
                            return Err(invalid(
                                entry.key(),
                                format!(
                                    "assignment `{}` targets node `{}` instead of `{node_id}`",
                                    assignment.assignment_id, assignment.node_id
                                ),
                            ));
                        }
                        if assignment.placement_epoch == 0
                            || assignment.replaces_assignment_id.as_deref()
                                == Some(assignment.assignment_id.as_str())
                        {
                            return Err(invalid(
                                entry.key(),
                                format!(
                                    "assignment `{}` has a zero epoch or replaces itself",
                                    assignment.assignment_id
                                ),
                            ));
                        }
                        let address = assignment.container_ip.ok_or_else(|| {
                            invalid(
                                entry.key(),
                                format!(
                                    "assignment `{}` has no reserved workload address",
                                    assignment.assignment_id
                                ),
                            )
                        })?;
                        if !addresses.insert(address) {
                            return Err(invalid(
                                entry.key(),
                                format!("workload address `{address}` is assigned more than once"),
                            ));
                        }
                        let assignment_id = assignment.assignment_id.clone();
                        if assignments
                            .insert(
                                assignment_id.clone(),
                                AssignmentRecord {
                                    manifest_generation: manifest.generation,
                                    assignment,
                                },
                            )
                            .is_some()
                        {
                            return Err(invalid(
                                entry.key(),
                                format!("assignment `{assignment_id}` occurs more than once"),
                            ));
                        }
                    }
                    for image in manifest.images {
                        if image.image.trim().is_empty()
                            || !manifest_images.insert(image.image.clone())
                        {
                            return Err(invalid(
                                entry.key(),
                                "image assignments must be non-empty and unique per manifest",
                            ));
                        }
                        images.push(ImageRecord {
                            node_id: node_id.clone(),
                            manifest_generation: manifest.generation,
                            image,
                        });
                    }
                }
                Some(ClusterKey::Replica {
                    node_id,
                    assignment_id,
                }) => {
                    let state: LegacyReplicaState = decode_json(entry)?;
                    validate_replica_identity(entry.key(), &node_id, &assignment_id, &state)?;
                    if replicas
                        .insert(assignment_id.clone(), ReplicaRecord { node_id, state })
                        .is_some()
                    {
                        return Err(invalid(
                            entry.key(),
                            format!("replica `{assignment_id}` occurs on more than one node"),
                        ));
                    }
                }
                None => unclaimed.push(entry.clone()),
            }
        }
        for (assignment_id, replica) in &replicas {
            let assignment = assignments.get(assignment_id).ok_or_else(|| {
                invalid(
                    format!("{REPLICAS_PREFIX}{}/{assignment_id}", replica.node_id),
                    "replica state has no desired assignment",
                )
            })?;
            if assignment.assignment.node_id != replica.node_id {
                return Err(invalid(
                    format!("{REPLICAS_PREFIX}{}/{assignment_id}", replica.node_id),
                    "replica state belongs to a different assignment node",
                ));
            }
        }
        Ok(Self {
            manifest_nodes,
            assignments,
            replicas,
            images,
            unclaimed,
        })
    }

    pub(crate) fn convert(
        &self,
        services: &LegacyServiceCatalog,
        nodes: &LegacyNodeCatalog,
        resources: &mut [BuiltinResource],
    ) -> Result<Vec<BuiltinResource>, LegacyPlanError> {
        self.validate_node_references(nodes)?;
        self.annotate_image_assignments(services, resources)?;
        let mut converted = Vec::new();
        for (assignment_id, record) in &self.assignments {
            let deployment = find_deployment(services, &record.assignment)?;
            let replica = self.replicas.get(assignment_id);
            let assignment = convert_assignment(record, replica)?;
            converted.push(BuiltinResource::Assignment(assignment.clone()));
            if let Some(replica) = replica {
                validate_replica(record, replica, deployment)?;
                converted.push(BuiltinResource::ReplicaState(convert_replica(
                    &assignment,
                    replica,
                )?));
            }
        }
        Ok(converted)
    }

    fn validate_node_references(&self, nodes: &LegacyNodeCatalog) -> Result<(), LegacyPlanError> {
        for node_id in &self.manifest_nodes {
            if !nodes.contains(node_id) {
                return Err(invalid_assignment(
                    node_id,
                    "assignment manifest belongs to a missing durable node",
                ));
            }
        }
        for image in &self.images {
            if !nodes.contains(&image.node_id) || !nodes.contains(&image.image.source_node_id) {
                return Err(invalid_assignment(
                    &image.image.deployment_id,
                    "image placement refers to a missing destination or source node",
                ));
            }
        }
        Ok(())
    }

    fn annotate_image_assignments(
        &self,
        services: &LegacyServiceCatalog,
        resources: &mut [BuiltinResource],
    ) -> Result<(), LegacyPlanError> {
        let mut placements = BTreeMap::<(String, String), Vec<serde_json::Value>>::new();
        for record in &self.images {
            let deployment = find_deployment_by_id(
                services,
                &record.image.service_id,
                &record.image.deployment_id,
            )?;
            let build = deployment.deployment.build.as_ref().ok_or_else(|| {
                invalid_assignment(
                    &record.image.deployment_id,
                    "image placement refers to a deployment without build output",
                )
            })?;
            if build.docker_image_id != record.image.image
                || build.source_node_id.as_deref() != Some(&record.image.source_node_id)
            {
                return Err(invalid_assignment(
                    &record.image.deployment_id,
                    "image placement disagrees with deployment build metadata",
                ));
            }
            parse_node_id(&record.node_id, "image placement node id")?;
            parse_node_id(&record.image.source_node_id, "image source node id")?;
            placements
                .entry((
                    record.image.service_id.clone(),
                    record.image.deployment_id.clone(),
                ))
                .or_default()
                .push(serde_json::json!({
                    "nodeId": record.node_id,
                    "manifestGeneration": record.manifest_generation,
                    "image": record.image.image,
                    "sourceNodeId": record.image.source_node_id,
                }));
        }
        for resource in resources {
            let BuiltinResource::Deployment(deployment) = resource else {
                continue;
            };
            let key = (
                deployment.spec.service_id.to_string(),
                deployment.meta.id.to_string(),
            );
            if let Some(value) = placements.remove(&key) {
                deployment.meta.annotations.insert(
                    AnnotationKey(LEGACY_IMAGE_ASSIGNMENTS_ANNOTATION.to_owned()),
                    serde_json::to_string(&value).map_err(|error| {
                        invalid_assignment(&key.1, format!("could not preserve images: {error}"))
                    })?,
                );
            }
        }
        if let Some(((service_id, deployment_id), _)) = placements.first_key_value() {
            return Err(invalid_assignment(
                deployment_id,
                format!("converted deployment for service `{service_id}` is missing"),
            ));
        }
        Ok(())
    }
}

fn convert_assignment(
    record: &AssignmentRecord,
    replica: Option<&ReplicaRecord>,
) -> Result<Assignment, LegacyPlanError> {
    let legacy = &record.assignment;
    let assignment_id = parse_assignment_id(&legacy.assignment_id)?;
    let status = replica.map(|replica| replica.state.status);
    let workload_id = status
        .filter(|status| has_workload(*status))
        .map(|_| WorkloadId::new(legacy.assignment_id.clone()))
        .transpose()
        .map_err(|error| invalid_generated("Workload", error))?;
    let deletion_timestamp = status
        .filter(|status| is_terminal(*status))
        .map(|_| Timestamp(legacy.created_at_ms));
    Ok(Object {
        meta: ObjectMeta {
            id: assignment_id,
            labels: BTreeMap::new(),
            annotations: annotations([
                (
                    "migration.maestro.dev/legacy-manifest-generation",
                    record.manifest_generation.to_string(),
                ),
                (
                    "migration.maestro.dev/legacy-created-at-ms",
                    legacy.created_at_ms.to_string(),
                ),
            ]),
            revision: ResourceRevision(0),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp,
        },
        spec: AssignmentSpec {
            service_id: parse_service_id(&legacy.service_id)?,
            deployment_id: parse_deployment_id(&legacy.deployment_id)?,
            restart_generation: Generation(1),
            replica_index: legacy.replica_index,
            node_id: parse_node_id(&legacy.node_id, "assignment node id")?,
            placement_epoch: legacy.placement_epoch,
            workload_address: IpAddr::V4(legacy.container_ip.ok_or_else(|| {
                invalid_assignment(
                    &legacy.assignment_id,
                    "reserved workload address is missing",
                )
            })?),
            replaces_assignment_id: legacy
                .replaces_assignment_id
                .as_deref()
                .map(parse_assignment_id)
                .transpose()?,
        },
        status: AssignmentStatus {
            phase: status.map_or(AssignmentPhase::Pending, assignment_phase),
            workload_id,
            conditions: Vec::new(),
        },
    })
}

fn convert_replica(
    assignment: &Assignment,
    record: &ReplicaRecord,
) -> Result<ReplicaState, LegacyPlanError> {
    let legacy = &record.state;
    let mut replica_annotations = BTreeMap::new();
    if let Some(endpoint) = &legacy.endpoint {
        replica_annotations.insert(
            AnnotationKey("migration.maestro.dev/legacy-endpoint".to_owned()),
            serde_json::to_string(endpoint).map_err(|error| {
                invalid_assignment(
                    assignment.meta.id.as_str(),
                    format!("could not preserve replica endpoint: {error}"),
                )
            })?,
        );
    }
    if let Some(error) = &legacy.error {
        replica_annotations.insert(
            AnnotationKey("migration.maestro.dev/legacy-error".to_owned()),
            error.clone(),
        );
    }
    let replica_id = ReplicaStateId::new(assignment.meta.id.as_str())
        .map_err(|error| invalid_generated("ReplicaState", error))?;
    Ok(Object {
        meta: ObjectMeta {
            id: replica_id,
            labels: BTreeMap::new(),
            annotations: replica_annotations,
            revision: ResourceRevision(0),
            generation: Generation(1),
            owner_refs: vec![owner("Assignment", assignment.meta.id.clone().into())?],
            finalizers: BTreeSet::new(),
            deletion_timestamp: assignment.meta.deletion_timestamp,
        },
        spec: ReplicaStateSpec {
            service_id: assignment.spec.service_id.clone(),
            deployment_id: assignment.spec.deployment_id.clone(),
            assignment_id: assignment.meta.id.clone(),
            replica_index: assignment.spec.replica_index,
        },
        status: ReplicaStateStatus {
            phase: deployment_phase(legacy.status),
            node_id: Some(assignment.spec.node_id.clone()),
            workload_id: assignment.status.workload_id.clone(),
            healthcheck_failures: legacy.healthcheck_failures,
            restart_attempts: legacy.restart_attempts,
            restart_pending_attempt: None,
            restart_not_before: None,
            conditions: Vec::new(),
        },
    })
}

fn validate_replica(
    assignment: &AssignmentRecord,
    replica: &ReplicaRecord,
    _deployment: &crate::legacy_services::LegacyDeploymentRecord,
) -> Result<(), LegacyPlanError> {
    let desired = &assignment.assignment;
    let state = &replica.state;
    if state.service_id.as_deref() != Some(desired.service_id.as_str())
        || state.deployment_id.as_deref() != Some(desired.deployment_id.as_str())
        || state.replica_index != desired.replica_index
    {
        return Err(invalid_assignment(
            &desired.assignment_id,
            "replica payload disagrees with its assignment identity",
        ));
    }
    if let Some(endpoint) = &state.endpoint {
        let address = endpoint.container_ip.parse().map_err(|error| {
            invalid_assignment(
                &desired.assignment_id,
                format!("replica endpoint address is invalid: {error}"),
            )
        })?;
        if Some(address) != desired.container_ip {
            return Err(invalid_assignment(
                &desired.assignment_id,
                "replica endpoint address disagrees with its assignment",
            ));
        }
        if endpoint.container_hostname.trim().is_empty()
            || endpoint.ingress_container_port == 0
            || endpoint.gateway.port == 0
        {
            return Err(invalid_assignment(
                &desired.assignment_id,
                "replica endpoint contains an empty hostname or zero port",
            ));
        }
    }
    Ok(())
}

fn find_deployment<'a>(
    services: &'a LegacyServiceCatalog,
    assignment: &LegacyAssignment,
) -> Result<&'a crate::legacy_services::LegacyDeploymentRecord, LegacyPlanError> {
    find_deployment_by_id(services, &assignment.service_id, &assignment.deployment_id)
}

fn find_deployment_by_id<'a>(
    services: &'a LegacyServiceCatalog,
    service_id: &str,
    deployment_id: &str,
) -> Result<&'a crate::legacy_services::LegacyDeploymentRecord, LegacyPlanError> {
    services
        .services
        .get(service_id)
        .and_then(|service| {
            service
                .deployments
                .iter()
                .find(|record| record.deployment.id == deployment_id)
        })
        .ok_or_else(|| {
            invalid_assignment(
                deployment_id,
                format!("referenced service `{service_id}` or deployment is missing"),
            )
        })
}

fn validate_manifest_identity(
    key: &str,
    key_node_id: &str,
    manifest: &LegacyAssignmentManifest,
) -> Result<(), LegacyClusterError> {
    if manifest.node_id == key_node_id {
        Ok(())
    } else {
        Err(invalid(
            key,
            format!(
                "key names node `{key_node_id}` but payload names `{}`",
                manifest.node_id
            ),
        ))
    }
}

fn validate_replica_identity(
    key: &str,
    node_id: &str,
    assignment_id: &str,
    state: &LegacyReplicaState,
) -> Result<(), LegacyClusterError> {
    if state.node_id.as_deref() != Some(node_id)
        || state.assignment_id.as_deref() != Some(assignment_id)
    {
        Err(invalid(
            key,
            "replica key and payload node or assignment identity disagree",
        ))
    } else {
        Ok(())
    }
}

fn assignment_phase(status: LegacyDeploymentStatus) -> AssignmentPhase {
    match status {
        LegacyDeploymentStatus::Queued => AssignmentPhase::Pending,
        LegacyDeploymentStatus::Building
        | LegacyDeploymentStatus::PendingReady
        | LegacyDeploymentStatus::Ready => AssignmentPhase::Running,
        LegacyDeploymentStatus::Draining => AssignmentPhase::Draining,
        LegacyDeploymentStatus::Crashed => AssignmentPhase::Failed,
        LegacyDeploymentStatus::Terminated
        | LegacyDeploymentStatus::Removed
        | LegacyDeploymentStatus::Canceled => AssignmentPhase::Stopped,
    }
}

fn has_workload(status: LegacyDeploymentStatus) -> bool {
    !matches!(
        status,
        LegacyDeploymentStatus::Queued
            | LegacyDeploymentStatus::Terminated
            | LegacyDeploymentStatus::Removed
            | LegacyDeploymentStatus::Canceled
    )
}

fn is_terminal(status: LegacyDeploymentStatus) -> bool {
    matches!(
        status,
        LegacyDeploymentStatus::Terminated
            | LegacyDeploymentStatus::Removed
            | LegacyDeploymentStatus::Canceled
    )
}

enum ClusterKey {
    Assignments {
        node_id: String,
    },
    Replica {
        node_id: String,
        assignment_id: String,
    },
}

fn classify_key(key: &str) -> Result<Option<ClusterKey>, LegacyClusterError> {
    if let Some(node_id) = key.strip_prefix(ASSIGNMENTS_PREFIX) {
        if node_id.is_empty() || node_id.contains('/') {
            return Err(invalid(key, "assignment manifest key is malformed"));
        }
        return Ok(Some(ClusterKey::Assignments {
            node_id: node_id.to_owned(),
        }));
    }
    let Some(remainder) = key.strip_prefix(REPLICAS_PREFIX) else {
        return Ok(None);
    };
    let Some((node_id, assignment_id)) = remainder.split_once('/') else {
        return Err(invalid(key, "replica state key is malformed"));
    };
    if node_id.is_empty() || assignment_id.is_empty() || assignment_id.contains('/') {
        return Err(invalid(key, "replica state key is malformed"));
    }
    Ok(Some(ClusterKey::Replica {
        node_id: node_id.to_owned(),
        assignment_id: assignment_id.to_owned(),
    }))
}

fn decode_json<Value: serde::de::DeserializeOwned>(
    entry: &LegacyEntry,
) -> Result<Value, LegacyClusterError> {
    serde_json::from_slice(entry.value())
        .map_err(|error| invalid(entry.key(), format!("invalid JSON: {error}")))
}

fn parse_assignment_id(value: &str) -> Result<AssignmentId, LegacyPlanError> {
    AssignmentId::new(value).map_err(|error| LegacyPlanError::InvalidIdentifier {
        field: "assignment id",
        value: value.to_owned(),
        message: error.to_string(),
    })
}

fn parse_service_id(value: &str) -> Result<ServiceId, LegacyPlanError> {
    ServiceId::new(value).map_err(|error| LegacyPlanError::InvalidIdentifier {
        field: "assignment service id",
        value: value.to_owned(),
        message: error.to_string(),
    })
}

fn parse_deployment_id(value: &str) -> Result<DeploymentId, LegacyPlanError> {
    DeploymentId::new(value).map_err(|error| LegacyPlanError::InvalidIdentifier {
        field: "assignment deployment id",
        value: value.to_owned(),
        message: error.to_string(),
    })
}

fn parse_node_id(value: &str, field: &'static str) -> Result<NodeId, LegacyPlanError> {
    NodeId::new(value).map_err(|error| LegacyPlanError::InvalidIdentifier {
        field,
        value: value.to_owned(),
        message: error.to_string(),
    })
}

fn invalid_assignment(
    assignment_id: impl Into<String>,
    message: impl Into<String>,
) -> LegacyPlanError {
    LegacyPlanError::InvalidAssignment {
        assignment_id: assignment_id.into(),
        message: message.into(),
    }
}

fn invalid(key: impl Into<String>, message: impl Into<String>) -> LegacyClusterError {
    LegacyClusterError::InvalidState {
        key: key.into(),
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyClusterError {
    #[error("legacy cluster state at `{key}` is invalid: {message}")]
    InvalidState { key: String, message: String },
}
