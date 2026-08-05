use std::sync::Arc;
use std::sync::Mutex;

use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentPhase, ConditionType, Deployment, EnvironmentName,
    ReplicaState, ResourceKind, ResourceName, SecretMountSpec, SecretValue,
};
use kernel_store::{CasOutcome, Clock, ExpectedVersion, Keyspace, PutRequest, Store};
use runtime::{
    AddressRequest, ArtifactDigest, NetworkHandle, NetworkProvider, RuntimeError, ShutdownRequest,
    ValueSourceError, ValueSourceResolver, WorkloadHandle, WorkloadRuntime, WorkloadState,
    WorkloadUser, WorkloadUserNamespace,
};

#[path = "assignment_reconcile.rs"]
mod reconcile;
mod watch;

use crate::ArtifactReplicationAgent;
use crate::StatusClock;
use crate::assignment_error::AssignmentAgentError;
#[cfg(unix)]
use crate::assignment_node_api::mount_node_api;
#[cfg(not(unix))]
use crate::assignment_plan::node_api_user;
use crate::assignment_plan::{
    WorkloadRuntimeInputs, workload_id, workload_spec_with_environment, workload_user,
};
use crate::assignment_resource::decode_assignment;
use crate::assignment_restart::{
    RestartReservation, finish_pending_restart, reserve_restart, restart_failure,
};
use crate::assignment_status::{
    AssignmentOutcome, ConvergeFailure, desired_status, runtime_status_message,
};
use crate::assignment_types::{
    AssignmentAgentSettings, AssignmentReconcileReport, ConvergedAssignment,
};
use crate::secret_mount::SecretMountManager;
use crate::system_host_ports::{runtime_host_ports, validate_system_host_ports};
#[cfg(unix)]
use crate::{NodeApiServices, node_api_mount::NodeApiMountManager};

const ASSIGNMENT_KIND: &str = "Assignment";
const DEPLOYMENT_KIND: &str = "Deployment";
const REPLICA_STATE_KIND: &str = "ReplicaState";
const RUNTIME_RETRY_REASON: &str = "RuntimeRetry";
const MAX_CAS_ATTEMPTS: usize = 16;

#[derive(Clone)]
struct ResolvedDeployment {
    deployment: Deployment,
    environment: std::collections::BTreeMap<String, SecretValue>,
}

fn validate_runtime_environment(
    values: &std::collections::BTreeMap<String, SecretValue>,
) -> Result<(), ConvergeFailure> {
    for (key, value) in values {
        if EnvironmentName::parse(key).is_err() {
            return Err(ConvergeFailure::failed(
                "ExternalValueSourceRejected",
                format!("environment key `{key}` must match [A-Za-z_][A-Za-z0-9_]*"),
            ));
        }
        if value.expose().contains('\0') {
            return Err(ConvergeFailure::failed(
                "ExternalValueSourceRejected",
                format!("environment value `{key}` contains NUL"),
            ));
        }
    }
    Ok(())
}

/// Level-triggered node reconciler for assignment lifecycle, adoption, and garbage collection.
pub struct AssignmentAgent {
    store: Arc<dyn Store>,
    runtime: Arc<dyn WorkloadRuntime>,
    network: Arc<dyn NetworkProvider>,
    settings: AssignmentAgentSettings,
    keyspace: Keyspace,
    assignment_kind: ResourceKind,
    deployment_kind: ResourceKind,
    replica_kind: ResourceKind,
    monotonic_clock: Arc<dyn Clock>,
    status_clock: Arc<dyn StatusClock>,
    artifact_replication: Option<Arc<ArtifactReplicationAgent>>,
    value_sources: Option<Arc<dyn ValueSourceResolver>>,
    resolved_deployments:
        Mutex<std::collections::BTreeMap<kernel_api::DeploymentId, ResolvedDeployment>>,
    secrets: SecretMountManager,
    #[cfg(unix)]
    node_api: NodeApiMountManager,
}

impl AssignmentAgent {
    /// Creates a node agent without starting background work.
    pub fn new(
        store: Arc<dyn Store>,
        runtime: Arc<dyn WorkloadRuntime>,
        network: Arc<dyn NetworkProvider>,
        settings: AssignmentAgentSettings,
        #[cfg(unix)] node_api_services: Option<NodeApiServices>,
        monotonic_clock: Arc<dyn Clock>,
        status_clock: Arc<dyn StatusClock>,
    ) -> Result<Self, AssignmentAgentError> {
        if settings.stop_timeout.is_zero()
            || settings.resync_interval.is_zero()
            || settings.restart_backoff_base.is_zero()
            || settings.restart_backoff_max < settings.restart_backoff_base
            || settings.reconcile_timeout.is_zero()
        {
            return Err(AssignmentAgentError::ZeroDeadline);
        }
        validate_system_host_ports(
            runtime.as_ref(),
            settings.network.addressing,
            &settings.system_host_ports,
        )?;
        let secrets = SecretMountManager::new(settings.secrets_root.clone())?;
        #[cfg(unix)]
        let node_api = NodeApiMountManager::new(settings.node_api_root.clone(), node_api_services)?;
        Ok(Self {
            keyspace: Keyspace::new(&settings.cluster_id),
            assignment_kind: ResourceKind::new(ASSIGNMENT_KIND)?,
            deployment_kind: ResourceKind::new(DEPLOYMENT_KIND)?,
            replica_kind: ResourceKind::new(REPLICA_STATE_KIND)?,
            store,
            runtime,
            network,
            settings,
            monotonic_clock,
            status_clock,
            artifact_replication: None,
            value_sources: None,
            resolved_deployments: Mutex::new(std::collections::BTreeMap::new()),
            secrets,
            #[cfg(unix)]
            node_api,
        })
    }

    /// Requires registry-free build artifacts to be local before workload creation.
    pub fn with_artifact_replication(mut self, replication: Arc<ArtifactReplicationAgent>) -> Self {
        self.artifact_replication = Some(replication);
        self
    }

    /// Resolves external deploy environment and secret references immediately before workload creation.
    pub fn with_value_source_resolver(mut self, resolver: Arc<dyn ValueSourceResolver>) -> Self {
        self.value_sources = Some(resolver);
        self
    }

    /// Reconciles one linearizable assignment snapshot and all owned runtime objects.
    pub async fn reconcile_once(&self) -> Result<AssignmentReconcileReport, AssignmentAgentError> {
        let (report, _cursor) = self.reconcile_with_cursor().await?;
        Ok(report)
    }

    async fn converge_assignment(
        &self,
        assignment: &Assignment,
        deployment: &Deployment,
        replica: Option<&ReplicaState>,
        network: &NetworkHandle,
        dns_server: Option<std::net::IpAddr>,
    ) -> Result<ConvergedAssignment, ConvergeFailure> {
        if matches!(
            &deployment.spec.service.artifact,
            ArtifactTemplate::Build { template } if template.registry.is_none()
        ) && let Some(replication) = self.artifact_replication.as_ref()
        {
            let digest = deployment.status.image_digest.as_deref().ok_or_else(|| {
                ConvergeFailure::pending(
                    "ArtifactUnavailable",
                    "deployment artifact is not available yet".to_owned(),
                )
            })?;
            let digest = ArtifactDigest::new(digest.to_owned()).map_err(|error| {
                ConvergeFailure::failed("ArtifactReplicationRejected", error.to_string())
            })?;
            replication.ensure_local(&digest).await.map_err(|error| {
                ConvergeFailure::pending("ArtifactReplicationUnavailable", error.to_string())
            })?;
        }
        let resolved = self.resolve_deployment_sources(deployment).await?;
        let deployment = &resolved.deployment;
        let resolved_secrets = deployment
            .spec
            .service
            .secrets
            .as_ref()
            .map(|secrets| {
                secrets
                    .values()
                    .iter()
                    .map(|(key, value)| (key.clone(), value.masked()))
                    .collect()
            })
            .unwrap_or_default();
        let workload_id = workload_id(assignment)?;
        let user_namespace = self.runtime.prepare_user_namespace(&workload_id).await?;
        let workload_user = workload_user(deployment);
        let secret_owner = host_secret_owner(user_namespace, workload_user)?;
        let mut additional_mounts = Vec::new();
        let secret_mount = match deployment.spec.service.secrets.as_ref() {
            Some(secrets) => Some(
                self.secrets
                    .materialize(&workload_id, secrets, secret_owner)
                    .await?,
            ),
            None => None,
        };
        additional_mounts.extend(secret_mount);
        #[cfg(unix)]
        if let Some(node_api_mount) = mount_node_api(
            &self.node_api,
            assignment,
            deployment,
            &workload_id,
            user_namespace,
        )
        .await?
        {
            additional_mounts.push(node_api_mount);
        }
        #[cfg(not(unix))]
        if node_api_user(deployment)?.is_some() {
            return Err(ConvergeFailure::failed(
                "NodeApiUnsupported",
                "node API workloads require a Unix host".to_owned(),
            ));
        }
        let spec = workload_spec_with_environment(
            &self.settings.cluster_id,
            assignment,
            deployment,
            resolved.environment.clone(),
            WorkloadRuntimeInputs {
                dns_server,
                user_namespace,
                additional_mounts,
                published_ports: runtime_host_ports(
                    self.settings.network.addressing,
                    self.settings
                        .system_host_ports
                        .get(&assignment.spec.service_id)
                        .cloned()
                        .unwrap_or_default(),
                ),
            },
        )?;
        let handle = self.runtime.create(&spec).await?;
        let before = self.runtime.status(&handle).await?;
        match before.state {
            WorkloadState::Created | WorkloadState::Running => {}
            WorkloadState::Stopped => {
                let replica = replica.ok_or_else(|| {
                    ConvergeFailure::pending(
                        "ReplicaStateMissing",
                        "an exited workload cannot restart until its ReplicaState is available"
                            .to_owned(),
                    )
                })?;
                match reserve_restart(
                    self.store.as_ref(),
                    &self.keyspace,
                    &self.replica_kind,
                    replica,
                    &assignment.meta.id,
                    deployment.spec.service.max_restarts,
                    self.settings.restart_backoff_base,
                    self.settings.restart_backoff_max,
                    self.status_clock.now(),
                )
                .await
                .map_err(restart_failure)?
                {
                    RestartReservation::Reserved { not_before }
                        if self.status_clock.now() < not_before =>
                    {
                        return Err(ConvergeFailure::pending_at(
                            "RestartBackoff",
                            format!("runtime restart is delayed until {}", not_before.0),
                            not_before,
                        ));
                    }
                    RestartReservation::Reserved { .. } => {}
                    RestartReservation::Exhausted { maximum } => {
                        return Err(ConvergeFailure::failed(
                            "RestartLimitReached",
                            format!(
                                "workload exhausted its restart limit of {} attempts",
                                maximum
                            ),
                        ));
                    }
                }
            }
            WorkloadState::Paused => {
                return Err(ConvergeFailure::pending(
                    RUNTIME_RETRY_REASON,
                    "runtime workload is paused".to_owned(),
                ));
            }
            WorkloadState::Failed => {
                return Err(ConvergeFailure::failed(
                    "RuntimeFailed",
                    runtime_status_message(&before),
                ));
            }
        }
        let needs_start = matches!(
            before.state,
            WorkloadState::Created | WorkloadState::Stopped
        );
        let request = assignment
            .spec
            .workload_address
            .map_or(AddressRequest::Any, AddressRequest::Exact);
        let reservation = self
            .network
            .allocate_address(network, handle.workload_id(), request)
            .await?;
        let attachment = self.network.attach(&handle, network, &reservation).await?;
        if needs_start {
            self.runtime.start(&handle).await?;
        }
        let status = self.runtime.status(&handle).await?;
        if status.state == WorkloadState::Running {
            let restarted = match replica {
                Some(replica) => finish_pending_restart(
                    self.store.as_ref(),
                    &self.keyspace,
                    &self.replica_kind,
                    replica,
                    &assignment.meta.id,
                    self.status_clock.now(),
                )
                .await
                .map_err(restart_failure)?,
                None => false,
            };
            Ok(ConvergedAssignment {
                handle,
                workload_address: attachment.address,
                restarted,
                replica_id: replica.map(|replica| replica.meta.id.clone()),
                resolved_secrets,
            })
        } else {
            Err(ConvergeFailure::pending(
                RUNTIME_RETRY_REASON,
                runtime_status_message(&status),
            ))
        }
    }

    async fn resolve_deployment_sources(
        &self,
        deployment: &Deployment,
    ) -> Result<ResolvedDeployment, ConvergeFailure> {
        if let Some(deployment) = self
            .resolved_deployments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&deployment.meta.id)
            .cloned()
        {
            return Ok(deployment);
        }
        let mut deployment = deployment.clone();
        let mut resolved_source = false;
        let environment_sources = std::mem::take(&mut deployment.spec.service.environment_sources);
        let mut environment = std::collections::BTreeMap::new();
        for source in environment_sources {
            resolved_source = true;
            environment.extend(self.resolve_value_source(&source).await?);
        }
        resolve_value_templates(
            &deployment.spec.environment_template,
            &mut environment,
            "ExternalEnvironmentTemplateRejected",
            "external environment",
        )?;
        environment.extend(
            deployment
                .spec
                .service
                .environment
                .iter()
                .map(|(key, value)| (key.clone(), SecretValue::new(value.clone()))),
        );
        if let Some(SecretMountSpec::Dotenv { source, items, .. }) =
            deployment.spec.service.secrets.as_mut()
            && let Some(source) = source.take()
        {
            resolved_source = true;
            let values = self.resolve_value_source(&source).await?;
            let inline = std::mem::take(items);
            *items = values;
            items.extend(inline);
            resolve_value_templates(
                &deployment.spec.environment_template,
                items,
                "SecretMountTemplateRejected",
                "secret mount",
            )?;
        }
        validate_runtime_environment(&environment)?;
        if resolved_source {
            deployment.spec.service.validate().map_err(|error| {
                ConvergeFailure::failed("ExternalValueSourceRejected", error.to_string())
            })?;
        }
        let resolved = ResolvedDeployment {
            deployment,
            environment,
        };
        self.resolved_deployments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(resolved.deployment.meta.id.clone(), resolved.clone());
        Ok(resolved)
    }

    async fn resolve_value_source(
        &self,
        source: &str,
    ) -> Result<std::collections::BTreeMap<String, SecretValue>, ConvergeFailure> {
        let resolver = self.value_sources.as_ref().ok_or_else(|| {
            ConvergeFailure::failed(
                "ExternalValueSourceUnsupported",
                "deployment contains an external value source but no resolver is configured"
                    .to_owned(),
            )
        })?;
        resolver.resolve(source).await.map_err(|error| match error {
            ValueSourceError::Unavailable { message } => {
                ConvergeFailure::pending("ExternalValueSourceUnavailable", message)
            }
            ValueSourceError::Rejected { message } => {
                ConvergeFailure::failed("ExternalValueSourceRejected", message)
            }
        })
    }

    async fn remove_workload(
        &self,
        handle: &WorkloadHandle,
        network: &NetworkHandle,
    ) -> Result<(), AssignmentAgentError> {
        let attachments = self.network.inspect(handle).await?.attachments;
        let stop = self
            .runtime
            .stop(
                handle,
                ShutdownRequest {
                    timeout: self.settings.stop_timeout,
                },
            )
            .await;
        if matches!(stop, Err(RuntimeError::Timeout { .. })) {
            self.runtime.kill(handle).await?;
        } else {
            stop?;
        }
        let mut configured_network_cleaned = false;
        for attachment in attachments {
            self.network.detach(handle, &attachment.network).await?;
            self.network
                .release_address(&attachment.network, handle.workload_id())
                .await?;
            configured_network_cleaned |= attachment.network == *network;
        }
        if !configured_network_cleaned {
            self.network.detach(handle, network).await?;
            self.network
                .release_address(network, handle.workload_id())
                .await?;
        }
        self.runtime.remove(handle).await?;
        self.secrets.cleanup(handle.workload_id()).await?;
        #[cfg(unix)]
        self.node_api.cleanup(handle.workload_id()).await?;
        Ok(())
    }

    async fn update_status(
        &self,
        assignment: &Assignment,
        outcome: AssignmentOutcome<'_>,
    ) -> Result<(), AssignmentAgentError> {
        let resource_name = ResourceName::new(assignment.meta.id.as_str())?;
        let key = self
            .keyspace
            .resource(&self.assignment_kind, &resource_name);
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            let stored = self.store.get(&key).await?.ok_or_else(|| {
                AssignmentAgentError::AssignmentDisappeared {
                    assignment_id: assignment.meta.id.to_string(),
                }
            })?;
            let mut current = decode_assignment(&stored)?;
            if current.spec.node_id != self.settings.node_id {
                return Err(AssignmentAgentError::AssignmentMoved {
                    assignment_id: current.meta.id.to_string(),
                });
            }
            let desired = desired_status(&current, outcome, self.status_clock.now());
            if current.status == desired {
                return Ok(());
            }
            current.status = desired;
            current.meta.revision = stored.version.resource_revision();
            let value = serde_json::to_vec(&current).map_err(|error| {
                AssignmentAgentError::SerializeResource {
                    message: error.to_string(),
                }
            })?;
            let result = self
                .store
                .put_cas(PutRequest {
                    key: key.clone(),
                    value,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await?;
            if matches!(result, CasOutcome::Applied(_)) {
                trace_assignment_transition(&current);
                return Ok(());
            }
        }
        Err(AssignmentAgentError::Contention {
            assignment_id: assignment.meta.id.to_string(),
        })
    }
}

fn resolve_value_templates(
    context: &kernel_api::EnvironmentTemplateContext,
    values: &mut std::collections::BTreeMap<String, SecretValue>,
    rejection_reason: &'static str,
    value_kind: &'static str,
) -> Result<(), ConvergeFailure> {
    for (key, value) in values.iter_mut() {
        if !value.expose().contains("${{") {
            continue;
        }
        let resolved = context.resolve(value.expose()).map_err(|error| {
            ConvergeFailure::failed(
                rejection_reason,
                format!("{value_kind} key `{key}` has an invalid template: {error}"),
            )
        })?;
        *value = SecretValue::new(resolved);
    }
    Ok(())
}

pub(crate) fn host_secret_owner(
    user_namespace: Option<WorkloadUserNamespace>,
    workload_user: Option<WorkloadUser>,
) -> Result<Option<WorkloadUser>, RuntimeError> {
    workload_user
        .map(|user| user_namespace.map_or_else(|| Ok(user), |namespace| namespace.host_user(user)))
        .transpose()
}

fn trace_assignment_transition(assignment: &Assignment) {
    let condition = assignment
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::RuntimeReady);
    let reason = condition.map_or("Unknown", |condition| condition.reason.0.as_str());
    let detail = condition.map_or("assignment status changed", |condition| {
        condition.message.as_str()
    });
    match assignment.status.phase {
        AssignmentPhase::Failed => tracing::error!(
            target: "maestro::controller",
            kind = ASSIGNMENT_KIND,
            resource_id = %assignment.meta.id,
            service_id = %assignment.spec.service_id,
            deployment_id = %assignment.spec.deployment_id,
            node_id = %assignment.spec.node_id,
            reason,
            error = detail,
            "assignment failed: {detail}"
        ),
        AssignmentPhase::Pending => tracing::warn!(
            target: "maestro::controller",
            kind = ASSIGNMENT_KIND,
            resource_id = %assignment.meta.id,
            service_id = %assignment.spec.service_id,
            deployment_id = %assignment.spec.deployment_id,
            node_id = %assignment.spec.node_id,
            reason,
            error = detail,
            "assignment is pending: {detail}"
        ),
        AssignmentPhase::Running => tracing::info!(
            target: "maestro::controller",
            kind = ASSIGNMENT_KIND,
            resource_id = %assignment.meta.id,
            service_id = %assignment.spec.service_id,
            deployment_id = %assignment.spec.deployment_id,
            node_id = %assignment.spec.node_id,
            reason,
            detail,
            "assignment is running"
        ),
        AssignmentPhase::Draining => tracing::info!(
            target: "maestro::controller",
            kind = ASSIGNMENT_KIND,
            resource_id = %assignment.meta.id,
            service_id = %assignment.spec.service_id,
            deployment_id = %assignment.spec.deployment_id,
            node_id = %assignment.spec.node_id,
            reason,
            detail,
            "assignment is draining"
        ),
        AssignmentPhase::Stopped => tracing::info!(
            target: "maestro::controller",
            kind = ASSIGNMENT_KIND,
            resource_id = %assignment.meta.id,
            service_id = %assignment.spec.service_id,
            deployment_id = %assignment.spec.deployment_id,
            node_id = %assignment.spec.node_id,
            reason,
            detail,
            "assignment stopped"
        ),
    }
}
