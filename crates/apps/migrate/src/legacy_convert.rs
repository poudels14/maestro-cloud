use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    AnnotationKey, ArtifactTemplate, BUILD_WATCH_REVISION_ANNOTATION, Build, BuildId, BuildPhase,
    BuildSpec, BuildStatus, BuiltinResource, Deployment, DeploymentGoal, DeploymentId,
    DeploymentPhase, DeploymentSpec, DeploymentStatus, Generation, Object, ObjectMeta,
    OwnerReference, Ownership, ResourceId, ResourceKind, ResourceName, ResourceRevision,
    RolloutState, Service, ServiceId, ServiceStatus, Timestamp,
};
use sha2::{Digest, Sha256};

use crate::legacy_cluster::LegacyClusterCatalog;
use crate::legacy_config::{ConvertedServiceConfig, convert_service_config};
use crate::legacy_derived::LegacyDerivedCatalog;
use crate::legacy_identity::LegacyClusterIdentity;
use crate::legacy_maintenance::LegacyMaintenanceCatalog;
use crate::legacy_membership::LegacyMembershipCatalog;
use crate::legacy_network::LegacyNetworkCatalog;
use crate::legacy_node_lifecycle::LegacyNodeLifecycleCatalog;
use crate::legacy_nodes::LegacyNodeCatalog;
use crate::legacy_placements::LegacyPlacementCatalog;
use crate::legacy_requests::LegacyRequestCatalog;
use crate::legacy_resources::{convert_policy, convert_preview, convert_route};
use crate::legacy_schema::{LegacyDeployment, LegacyDeploymentStatus};
use crate::legacy_services::{LegacyDeploymentRecord, LegacyServiceCatalog, LegacyServiceState};
use crate::legacy_webhooks::LegacyWebhookCatalog;
use crate::{LegacySnapshot, MigrationPlan, PlanError};

const SERVICE_KIND: &str = "Service";
const DEPLOYMENT_KIND: &str = "Deployment";

/// Converts one complete stopped-control-plane snapshot into canonical typed resources.
///
/// The current implementation deliberately rejects unclaimed legacy key families so every
/// cutover slice remains all-or-nothing.
pub fn plan_legacy_snapshot(
    snapshot: &LegacySnapshot,
    master_secret: &str,
) -> Result<MigrationPlan, LegacyPlanError> {
    let catalog = LegacyServiceCatalog::decode(snapshot, master_secret).map_err(|error| {
        LegacyPlanError::DecodeLegacyState {
            message: error.to_string(),
        }
    })?;
    let nodes = LegacyNodeCatalog::decode(&catalog.unclaimed).map_err(|error| {
        LegacyPlanError::DecodeLegacyState {
            message: error.to_string(),
        }
    })?;
    let identity = LegacyClusterIdentity::decode(&nodes.unclaimed, &nodes).map_err(|error| {
        LegacyPlanError::DecodeLegacyState {
            message: error.to_string(),
        }
    })?;
    let membership =
        LegacyMembershipCatalog::decode(&identity.unclaimed, &nodes).map_err(|error| {
            LegacyPlanError::DecodeLegacyState {
                message: error.to_string(),
            }
        })?;
    let maintenance =
        LegacyMaintenanceCatalog::decode(&membership.unclaimed, &nodes).map_err(|error| {
            LegacyPlanError::DecodeLegacyState {
                message: error.to_string(),
            }
        })?;
    let lifecycle = LegacyNodeLifecycleCatalog::decode(&maintenance.unclaimed, &nodes, &membership)
        .map_err(|error| LegacyPlanError::DecodeLegacyState {
            message: error.to_string(),
        })?;
    let cluster = LegacyClusterCatalog::decode(&lifecycle.unclaimed).map_err(|error| {
        LegacyPlanError::DecodeLegacyState {
            message: error.to_string(),
        }
    })?;
    let network = LegacyNetworkCatalog::decode(&cluster.unclaimed).map_err(|error| {
        LegacyPlanError::DecodeLegacyState {
            message: error.to_string(),
        }
    })?;
    let derived =
        LegacyDerivedCatalog::decode(&network.unclaimed, &nodes, &catalog).map_err(|error| {
            LegacyPlanError::DecodeLegacyState {
                message: error.to_string(),
            }
        })?;
    let placements = LegacyPlacementCatalog::decode(&derived.unclaimed).map_err(|error| {
        LegacyPlanError::DecodeLegacyState {
            message: error.to_string(),
        }
    })?;
    let requests = LegacyRequestCatalog::decode(&placements.unclaimed).map_err(|error| {
        LegacyPlanError::DecodeLegacyState {
            message: error.to_string(),
        }
    })?;
    let webhooks =
        LegacyWebhookCatalog::decode(&requests.unclaimed, master_secret).map_err(|error| {
            LegacyPlanError::DecodeLegacyState {
                message: error.to_string(),
            }
        })?;
    if let Some(entry) = webhooks.unclaimed.first() {
        return Err(LegacyPlanError::UnsupportedLegacyKey {
            key: entry.key().to_owned(),
        });
    }
    let mut resources = convert_catalog(&catalog)?;
    resources.extend(nodes.convert()?);
    identity.annotate_master(&mut resources)?;
    derived.annotate_master(&mut resources)?;
    membership.annotate_nodes(&mut resources)?;
    maintenance.annotate_master(&mut resources)?;
    requests.annotate_master(&mut resources)?;
    lifecycle.annotate_nodes(&mut resources)?;
    resources.extend(lifecycle.convert_removed()?);
    let cluster_resources = cluster.convert(&catalog, &nodes, &mut resources)?;
    resources.extend(cluster_resources);
    resources.extend(placements.convert()?);
    let network_resources = network.convert(&resources)?;
    resources.extend(network_resources);
    resources.extend(webhooks.convert());
    MigrationPlan::with_request_barriers(
        identity.cluster_id().clone(),
        snapshot.digest(),
        resources,
        requests.barriers(),
    )
    .map_err(Into::into)
}

fn convert_catalog(
    catalog: &LegacyServiceCatalog,
) -> Result<Vec<BuiltinResource>, LegacyPlanError> {
    let mut resources = Vec::new();
    let mut services = BTreeMap::<ServiceId, Service>::new();
    for state in catalog.services.values() {
        let converted = convert_current_config(state)?;
        let service = convert_service(state, &converted)?;
        if let Some(route) = converted.ingress {
            resources.push(BuiltinResource::IngressRoute(convert_route(
                &service, route,
            )?));
        }
        if let Some(policy) = converted.egress {
            resources.push(BuiltinResource::FirewallPolicy(convert_policy(
                &service, policy,
            )?));
        }
        for record in &state.deployments {
            let (deployment, build) = convert_deployment(&service.meta.id, record)?;
            resources.push(BuiltinResource::Deployment(deployment));
            resources.extend(build.map(BuiltinResource::Build));
        }
        services.insert(service.meta.id.clone(), service);
    }
    for state in catalog.services.values() {
        if let Some(source) = &state.info.config.preview_source {
            resources.push(BuiltinResource::Preview(convert_preview(
                state, source, &services,
            )?));
        }
    }
    resources.extend(services.into_values().map(BuiltinResource::Service));
    Ok(resources)
}

fn convert_current_config(
    state: &LegacyServiceState,
) -> Result<ConvertedServiceConfig, LegacyPlanError> {
    let latest = state.deployments.last();
    let data = latest.map(|record| record.data.clone()).unwrap_or_default();
    let artifact = latest
        .map(|record| uploaded_image_artifact(&record.deployment))
        .transpose()?
        .flatten();
    convert_service_config(&state.info.config, &data, None, artifact)
}

fn convert_service(
    state: &LegacyServiceState,
    converted: &ConvertedServiceConfig,
) -> Result<Service, LegacyPlanError> {
    let service_id = parse_service_id(&state.info.config.id, "service config id")?;
    let generation = Generation(state.next_history_index.max(1));
    let active_deployment_id = state
        .deployments
        .iter()
        .rev()
        .find(|record| {
            matches!(
                record.deployment.status,
                LegacyDeploymentStatus::Ready
                    | LegacyDeploymentStatus::Building
                    | LegacyDeploymentStatus::PendingReady
            )
        })
        .map(|record| parse_deployment_id(&record.deployment.id))
        .transpose()?;
    let mut annotations = annotations([(
        "migration.maestro.dev/legacy-history-next-index",
        state.next_history_index.to_string(),
    )]);
    if let Some(archive) = state
        .deployments
        .last()
        .and_then(|record| record.deployment.upload_archive.as_ref())
    {
        annotations.insert(
            AnnotationKey("migration.maestro.dev/legacy-upload-archive".to_owned()),
            archive.clone(),
        );
    }
    if let Some(revision) = state
        .deployments
        .last()
        .and_then(|record| record.deployment.git_commit.as_ref())
        .map(|commit| commit.reference.trim())
        .filter(|revision| !revision.is_empty())
        && matches!(
            converted.spec.artifact,
            ArtifactTemplate::Build {
                template: kernel_api::BuildTemplate { watch: true, .. }
            }
        )
    {
        annotations.insert(
            AnnotationKey(BUILD_WATCH_REVISION_ANNOTATION.to_owned()),
            revision.to_owned(),
        );
    }
    Ok(Object {
        meta: ObjectMeta {
            id: service_id,
            labels: BTreeMap::new(),
            annotations,
            revision: ResourceRevision(0),
            generation,
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: converted.spec.clone(),
        status: ServiceStatus {
            active_deployment_id,
            replica_override: state.info.replicas_override,
            rollout: if state.info.deploy_frozen {
                RolloutState::Frozen
            } else {
                RolloutState::Active
            },
            rollout_bypass_generation: None,
            conditions: Vec::new(),
        },
    })
}

fn convert_deployment(
    service_id: &ServiceId,
    record: &LegacyDeploymentRecord,
) -> Result<(Deployment, Option<Build>), LegacyPlanError> {
    let legacy = &record.deployment;
    let uploaded_artifact = uploaded_image_artifact(legacy)?;
    let pinned_revision = legacy
        .git_commit
        .as_ref()
        .map(|commit| commit.reference.as_str());
    let converted = convert_service_config(
        &legacy.config,
        &record.data,
        pinned_revision,
        uploaded_artifact,
    )?;
    let deployment_id = parse_deployment_id(&legacy.id)?;
    let service_generation = Generation(record.history_index.checked_add(1).ok_or_else(|| {
        LegacyPlanError::GenerationOverflow {
            service_id: service_id.to_string(),
        }
    })?);
    let build_id = matches!(converted.spec.artifact, ArtifactTemplate::Build { .. })
        .then(|| legacy_build_id(&deployment_id))
        .transpose()?;
    let phase = deployment_phase(legacy.status);
    let image_digest = legacy
        .build
        .as_ref()
        .map(|build| build.docker_image_id.clone());
    if matches!(
        phase,
        DeploymentPhase::PendingReady | DeploymentPhase::Ready | DeploymentPhase::Draining
    ) && image_digest.is_none()
    {
        return Err(LegacyPlanError::InvalidDeployment {
            deployment_id: legacy.id.clone(),
            message: "active deployment has no resolved image".to_owned(),
        });
    }
    let mut deployment_annotations = annotations([(
        "migration.maestro.dev/legacy-history-index",
        record.history_index.to_string(),
    )]);
    if let Some(commit) = &legacy.git_commit {
        deployment_annotations.insert(
            AnnotationKey("migration.maestro.dev/git-reference".to_owned()),
            commit.reference.clone(),
        );
        deployment_annotations.insert(
            AnnotationKey("migration.maestro.dev/git-message".to_owned()),
            commit.message.clone(),
        );
    }
    if let Some(source_node_id) = legacy
        .build
        .as_ref()
        .and_then(|build| build.source_node_id.as_ref())
    {
        deployment_annotations.insert(
            AnnotationKey("migration.maestro.dev/source-node-id".to_owned()),
            source_node_id.clone(),
        );
    }
    if let Some(archive) = &legacy.upload_archive {
        deployment_annotations.insert(
            AnnotationKey("migration.maestro.dev/legacy-upload-archive".to_owned()),
            archive.clone(),
        );
    }
    let deployment = Object {
        meta: ObjectMeta {
            id: deployment_id.clone(),
            labels: BTreeMap::new(),
            annotations: deployment_annotations,
            revision: ResourceRevision(0),
            generation: Generation(1),
            owner_refs: vec![owner(SERVICE_KIND, service_id.clone().into())?],
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: DeploymentSpec {
            service_id: service_id.clone(),
            service_generation,
            restart_generation: Generation(1),
            bypass_rollout_freeze: false,
            service: converted.spec,
            goal: deployment_goal(legacy.status),
            build_id: build_id.clone(),
        },
        status: DeploymentStatus {
            phase,
            created_at: timestamp("deployment.createdAt", legacy.created_at)?,
            ready_at: optional_timestamp("deployment.deployedAt", legacy.deployed_at)?,
            draining_at: optional_timestamp("deployment.drainedAt", legacy.drained_at)?,
            image_digest: image_digest.clone(),
            conditions: Vec::new(),
        },
    };
    let build = build_id
        .map(|build_id| convert_build(&deployment, build_id, image_digest, pinned_revision))
        .transpose()?;
    Ok((deployment, build))
}

fn uploaded_image_artifact(
    legacy: &LegacyDeployment,
) -> Result<Option<ArtifactTemplate>, LegacyPlanError> {
    let Some(archive) = &legacy.upload_archive else {
        return Ok(None);
    };
    let image = legacy
        .build
        .as_ref()
        .map(|build| build.docker_image_id.trim())
        .filter(|image| !image.is_empty())
        .ok_or_else(|| LegacyPlanError::InvalidDeployment {
            deployment_id: legacy.id.clone(),
            message: format!(
                "uploaded archive `{archive}` has no resolved image; finish or remove the \
                 incomplete deployment before cutover"
            ),
        })?;
    Ok(Some(ArtifactTemplate::Image {
        reference: image.to_owned(),
    }))
}

fn convert_build(
    deployment: &Deployment,
    build_id: BuildId,
    image_digest: Option<String>,
    source_revision: Option<&str>,
) -> Result<Build, LegacyPlanError> {
    let ArtifactTemplate::Build { template } = &deployment.spec.service.artifact else {
        return Err(LegacyPlanError::InvalidDeployment {
            deployment_id: deployment.meta.id.to_string(),
            message: "non-build deployment unexpectedly received a build id".to_owned(),
        });
    };
    let phase = if image_digest.is_some() {
        BuildPhase::Succeeded
    } else {
        match deployment.status.phase {
            DeploymentPhase::Canceled => BuildPhase::Canceled,
            DeploymentPhase::Crashed
            | DeploymentPhase::Terminated
            | DeploymentPhase::Removed
            | DeploymentPhase::Draining => BuildPhase::Failed,
            DeploymentPhase::Queued
            | DeploymentPhase::Building
            | DeploymentPhase::PendingReady
            | DeploymentPhase::Ready => BuildPhase::Queued,
        }
    };
    Ok(Object {
        meta: ObjectMeta {
            id: build_id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision(0),
            generation: Generation(1),
            owner_refs: vec![owner(DEPLOYMENT_KIND, deployment.meta.id.clone().into())?],
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: BuildSpec {
            service_id: deployment.spec.service_id.clone(),
            deployment_id: deployment.meta.id.clone(),
            template: template.clone(),
        },
        status: BuildStatus {
            phase,
            image_digest,
            source_revision: source_revision.map(str::to_owned),
            conditions: Vec::new(),
        },
    })
}

pub(crate) fn owner(kind: &str, id: ResourceName) -> Result<OwnerReference, LegacyPlanError> {
    Ok(OwnerReference {
        resource: ResourceId::new(
            ResourceKind::new(kind).map_err(|error| invalid_generated(kind, error))?,
            id,
        ),
        ownership: Ownership::Controller,
    })
}

pub(crate) fn deployment_phase(status: LegacyDeploymentStatus) -> DeploymentPhase {
    match status {
        LegacyDeploymentStatus::Queued => DeploymentPhase::Queued,
        LegacyDeploymentStatus::Building => DeploymentPhase::Building,
        LegacyDeploymentStatus::PendingReady => DeploymentPhase::PendingReady,
        LegacyDeploymentStatus::Ready => DeploymentPhase::Ready,
        LegacyDeploymentStatus::Crashed => DeploymentPhase::Crashed,
        LegacyDeploymentStatus::Terminated => DeploymentPhase::Terminated,
        LegacyDeploymentStatus::Removed => DeploymentPhase::Removed,
        LegacyDeploymentStatus::Draining => DeploymentPhase::Draining,
        LegacyDeploymentStatus::Canceled => DeploymentPhase::Canceled,
    }
}

fn deployment_goal(status: LegacyDeploymentStatus) -> DeploymentGoal {
    match status {
        LegacyDeploymentStatus::Canceled => DeploymentGoal::Cancel,
        LegacyDeploymentStatus::Draining | LegacyDeploymentStatus::Removed => {
            DeploymentGoal::Remove
        }
        _ => DeploymentGoal::Run,
    }
}

pub(crate) fn timestamp(field: &'static str, value: u64) -> Result<Timestamp, LegacyPlanError> {
    i64::try_from(value)
        .map(Timestamp)
        .map_err(|_| LegacyPlanError::TimestampOverflow { field, value })
}

pub(crate) fn optional_timestamp(
    field: &'static str,
    value: Option<u64>,
) -> Result<Option<Timestamp>, LegacyPlanError> {
    value.map(|value| timestamp(field, value)).transpose()
}

pub(crate) fn add_seconds(
    timestamp: Timestamp,
    seconds: u64,
    field: &'static str,
) -> Result<Timestamp, LegacyPlanError> {
    let milliseconds = i64::try_from(seconds)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000))
        .and_then(|duration| timestamp.0.checked_add(duration))
        .ok_or(LegacyPlanError::TimestampArithmetic { field })?;
    Ok(Timestamp(milliseconds))
}

pub(crate) fn parse_service_id(
    value: &str,
    field: &'static str,
) -> Result<ServiceId, LegacyPlanError> {
    ServiceId::new(value).map_err(|error| LegacyPlanError::InvalidIdentifier {
        field,
        value: value.to_owned(),
        message: error.to_string(),
    })
}

fn parse_deployment_id(value: &str) -> Result<DeploymentId, LegacyPlanError> {
    DeploymentId::new(value).map_err(|error| LegacyPlanError::InvalidIdentifier {
        field: "deployment id",
        value: value.to_owned(),
        message: error.to_string(),
    })
}

fn legacy_build_id(deployment_id: &DeploymentId) -> Result<BuildId, LegacyPlanError> {
    let candidate = format!("{deployment_id}-build");
    let value = if candidate.len() <= 253 {
        candidate
    } else {
        stable_id("legacy-build", deployment_id.as_str())
    };
    BuildId::new(value).map_err(|error| invalid_generated("Build", error))
}

pub(crate) fn stable_id(prefix: &str, value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(prefix.as_bytes());
    digest.update([0]);
    digest.update(value.as_bytes());
    let encoded = hex::encode(digest.finalize());
    let suffix = encoded.chars().take(32).collect::<String>();
    format!("{prefix}-{suffix}")
}

pub(crate) fn annotations<const N: usize>(
    values: [(&'static str, String); N],
) -> BTreeMap<AnnotationKey, String> {
    values
        .into_iter()
        .map(|(key, value)| (AnnotationKey(key.to_owned()), value))
        .collect()
}

pub(crate) fn invalid_generated(kind: &str, error: impl std::fmt::Display) -> LegacyPlanError {
    LegacyPlanError::InvalidGeneratedResource {
        kind: kind.to_owned(),
        message: error.to_string(),
    }
}

/// A legacy snapshot cannot be converted without losing or corrupting state.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LegacyPlanError {
    /// Strict legacy decoding or authenticated sidecar recovery failed.
    #[error("could not decode legacy state: {message}")]
    DecodeLegacyState {
        /// Decoder detail without secret values.
        message: String,
    },
    /// A key family has not yet been added to the complete cutover planner.
    #[error("legacy key `{key}` is not supported by the cutover planner")]
    UnsupportedLegacyKey {
        /// Exact unclaimed legacy key.
        key: String,
    },
    /// A legacy identifier is invalid under the canonical resource contract.
    #[error("legacy {field} `{value}` is invalid: {message}")]
    InvalidIdentifier {
        /// Legacy field containing the identifier.
        field: &'static str,
        /// Rejected identifier.
        value: String,
        /// Canonical validation detail.
        message: String,
    },
    /// A service configuration violates native workload invariants.
    #[error("legacy service `{service_id}` is invalid: {message}")]
    InvalidServiceConfig {
        /// Owning service.
        service_id: String,
        /// Validation detail.
        message: String,
    },
    /// A legacy service feature has no lossless native representation yet.
    #[error("legacy service `{service_id}` field `{field}` is unsupported: {message}")]
    UnsupportedServiceField {
        /// Owning service.
        service_id: String,
        /// Legacy field path.
        field: String,
        /// Missing parity detail.
        message: String,
    },
    /// A deterministic child resource could not satisfy canonical identity rules.
    #[error("could not construct migrated {kind}: {message}")]
    InvalidGeneratedResource {
        /// Resource kind being constructed.
        kind: String,
        /// Canonical validation detail.
        message: String,
    },
    /// A deployment is internally inconsistent.
    #[error("legacy deployment `{deployment_id}` is invalid: {message}")]
    InvalidDeployment {
        /// Legacy deployment identity.
        deployment_id: String,
        /// Validation detail.
        message: String,
    },
    /// A scheduled workload record is internally inconsistent.
    #[error("legacy assignment `{assignment_id}` is invalid: {message}")]
    InvalidAssignment {
        /// Legacy assignment or related deployment identity.
        assignment_id: String,
        /// Validation detail.
        message: String,
    },
    /// Legacy cluster runtime state cannot be represented safely.
    #[error("legacy cluster resource `{resource_id}` is invalid: {message}")]
    InvalidClusterState {
        /// Legacy service or singleton resource identity.
        resource_id: String,
        /// Validation detail.
        message: String,
    },
    /// A preview cannot be attached to its canonical source resources.
    #[error("legacy preview service `{service_id}` is invalid: {message}")]
    InvalidPreview {
        /// Derived service identity.
        service_id: String,
        /// Validation detail.
        message: String,
    },
    /// A millisecond timestamp cannot fit the canonical signed representation.
    #[error("legacy {field} value {value} does not fit a canonical timestamp")]
    TimestampOverflow {
        /// Legacy timestamp field.
        field: &'static str,
        /// Rejected unsigned milliseconds.
        value: u64,
    },
    /// Adding a bounded lifecycle duration overflowed the timestamp representation.
    #[error("legacy {field} timestamp arithmetic overflowed")]
    TimestampArithmetic {
        /// Computed lifecycle timestamp.
        field: &'static str,
    },
    /// A history index cannot be represented as a service generation.
    #[error("legacy service `{service_id}` history generation overflowed")]
    GenerationOverflow {
        /// Owning service.
        service_id: String,
    },
    /// Canonical resource serialization or destination uniqueness failed.
    #[error(transparent)]
    Plan(#[from] PlanError),
}
