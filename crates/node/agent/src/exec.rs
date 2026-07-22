use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, ClusterId, Deployment, ExecPolicy, NodeId,
    ResourceKind, ResourceName,
};
use kernel_store::{Keyspace, Store, StoreError, StoreKey, Version};
use runtime::{
    ExecInput, ExecMode, ExecOutput, ExecRequest, ExecSession, RuntimeCapability, RuntimeError,
    WorkloadRuntime, WorkloadState,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const ASSIGNMENT_KIND: &str = "Assignment";
const DEPLOYMENT_KIND: &str = "Deployment";

/// Node-local authorization and concurrency settings for runtime exec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeExecSettings {
    /// Cluster owning target assignments and runtime objects.
    pub cluster_id: ClusterId,
    /// Node on which every accepted target must be running.
    pub node_id: NodeId,
    /// Maximum sessions held open concurrently on this node.
    pub maximum_sessions: usize,
}

/// Policy-enforcing bridge from assignment identity to a native runtime exec session.
pub struct NodeExecService {
    store: Arc<dyn Store>,
    runtime: Arc<dyn WorkloadRuntime>,
    settings: NodeExecSettings,
    keyspace: Keyspace,
    assignment_kind: ResourceKind,
    deployment_kind: ResourceKind,
    sessions: Arc<Semaphore>,
}

impl NodeExecService {
    /// Builds an exec service without opening a listener or a session.
    pub fn new(
        store: Arc<dyn Store>,
        runtime: Arc<dyn WorkloadRuntime>,
        settings: NodeExecSettings,
    ) -> Result<Self, NodeExecError> {
        if settings.maximum_sessions == 0 {
            return Err(NodeExecError::ZeroSessionLimit);
        }
        Ok(Self {
            keyspace: Keyspace::new(&settings.cluster_id),
            assignment_kind: ResourceKind::new(ASSIGNMENT_KIND)?,
            deployment_kind: ResourceKind::new(DEPLOYMENT_KIND)?,
            sessions: Arc::new(Semaphore::new(settings.maximum_sessions)),
            store,
            runtime,
            settings,
        })
    }

    /// Resolves, authorizes, and opens one native exec session without shell reinterpretation.
    pub async fn open(
        &self,
        assignment_id: &AssignmentId,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, NodeExecError> {
        validate_request(&request)?;
        self.validate_capabilities(request.mode)?;
        let permit = self.sessions.clone().try_acquire_owned().map_err(|_| {
            NodeExecError::SessionLimitReached {
                maximum: self.settings.maximum_sessions,
            }
        })?;
        let assignment_key = self.resource_key(&self.assignment_kind, assignment_id.as_str())?;
        let (assignment, assignment_version) = self.load_assignment(&assignment_key).await?;
        if &assignment.meta.id != assignment_id {
            return Err(NodeExecError::ResourceIdentityMismatch {
                kind: ASSIGNMENT_KIND,
                key: assignment_key.to_string(),
            });
        }
        self.validate_assignment(&assignment)?;
        let deployment_key = self.resource_key(
            &self.deployment_kind,
            assignment.spec.deployment_id.as_str(),
        )?;
        let (deployment, deployment_version) = self.load_deployment(&deployment_key).await?;
        validate_deployment(&assignment, &deployment)?;

        let workload_id = assignment.status.workload_id.as_ref().ok_or_else(|| {
            NodeExecError::WorkloadUnavailable {
                assignment_id: assignment.meta.id.to_string(),
            }
        })?;
        let matching = self
            .runtime
            .list(&self.settings.cluster_id, &self.settings.node_id)
            .await?
            .into_iter()
            .filter(|workload| {
                workload.metadata.assignment_id == assignment.meta.id
                    && &workload.metadata.workload_id == workload_id
            })
            .collect::<Vec<_>>();
        let [observed] = matching.as_slice() else {
            return Err(NodeExecError::WorkloadUnavailable {
                assignment_id: assignment.meta.id.to_string(),
            });
        };
        if observed.status.state != WorkloadState::Running
            || self.runtime.status(&observed.handle).await?.state != WorkloadState::Running
        {
            return Err(NodeExecError::WorkloadNotRunning {
                assignment_id: assignment.meta.id.to_string(),
            });
        }
        self.require_version(&assignment_key, assignment_version)
            .await?;
        self.require_version(&deployment_key, deployment_version)
            .await?;
        let session = self.runtime.exec(&observed.handle, request).await?;
        Ok(Box::new(LimitedExecSession {
            session,
            _permit: permit,
        }))
    }

    fn validate_capabilities(&self, mode: ExecMode) -> Result<(), NodeExecError> {
        let capabilities = self.runtime.capabilities();
        if !capabilities.supports(RuntimeCapability::Exec) {
            return Err(NodeExecError::CapabilityUnavailable {
                capability: RuntimeCapability::Exec,
            });
        }
        if matches!(mode, ExecMode::Terminal { .. })
            && !capabilities.supports(RuntimeCapability::InteractiveExec)
        {
            return Err(NodeExecError::CapabilityUnavailable {
                capability: RuntimeCapability::InteractiveExec,
            });
        }
        Ok(())
    }

    fn validate_assignment(&self, assignment: &Assignment) -> Result<(), NodeExecError> {
        if assignment.spec.node_id != self.settings.node_id {
            return Err(NodeExecError::AssignmentOnAnotherNode {
                assignment_id: assignment.meta.id.to_string(),
                node_id: assignment.spec.node_id.to_string(),
            });
        }
        if assignment.meta.deletion_timestamp.is_some()
            || assignment.status.phase != AssignmentPhase::Running
        {
            return Err(NodeExecError::WorkloadNotRunning {
                assignment_id: assignment.meta.id.to_string(),
            });
        }
        Ok(())
    }

    fn resource_key(&self, kind: &ResourceKind, id: &str) -> Result<StoreKey, NodeExecError> {
        Ok(self
            .keyspace
            .resource(kind, &ResourceName::new(id.to_owned())?))
    }

    async fn load_assignment(
        &self,
        key: &StoreKey,
    ) -> Result<(Assignment, Version), NodeExecError> {
        let stored = self
            .store
            .get(key)
            .await?
            .ok_or_else(|| NodeExecError::ResourceMissing {
                kind: ASSIGNMENT_KIND,
                key: key.to_string(),
            })?;
        let assignment = serde_json::from_slice(&stored.value).map_err(|error| {
            NodeExecError::MalformedResource {
                kind: ASSIGNMENT_KIND,
                key: key.to_string(),
                message: error.to_string(),
            }
        })?;
        Ok((assignment, stored.version))
    }

    async fn load_deployment(
        &self,
        key: &StoreKey,
    ) -> Result<(Deployment, Version), NodeExecError> {
        let stored = self
            .store
            .get(key)
            .await?
            .ok_or_else(|| NodeExecError::ResourceMissing {
                kind: DEPLOYMENT_KIND,
                key: key.to_string(),
            })?;
        let deployment = serde_json::from_slice(&stored.value).map_err(|error| {
            NodeExecError::MalformedResource {
                kind: DEPLOYMENT_KIND,
                key: key.to_string(),
                message: error.to_string(),
            }
        })?;
        Ok((deployment, stored.version))
    }

    async fn require_version(
        &self,
        key: &StoreKey,
        expected: Version,
    ) -> Result<(), NodeExecError> {
        if self.store.get(key).await?.map(|stored| stored.version) == Some(expected) {
            Ok(())
        } else {
            Err(NodeExecError::PolicyChanged {
                key: key.to_string(),
            })
        }
    }
}

fn validate_request(request: &ExecRequest) -> Result<(), NodeExecError> {
    if request.command.executable.is_empty()
        || matches!(
            request.mode,
            ExecMode::Terminal { columns: 0, .. } | ExecMode::Terminal { rows: 0, .. }
        )
    {
        Err(NodeExecError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn validate_deployment(
    assignment: &Assignment,
    deployment: &Deployment,
) -> Result<(), NodeExecError> {
    if assignment.spec.deployment_id != deployment.meta.id
        || assignment.spec.service_id != deployment.spec.service_id
    {
        Err(NodeExecError::DeploymentMismatch {
            assignment_id: assignment.meta.id.to_string(),
        })
    } else if deployment.meta.deletion_timestamp.is_some() {
        Err(NodeExecError::DeploymentUnavailable {
            deployment_id: deployment.meta.id.to_string(),
        })
    } else if deployment.spec.service.exec == ExecPolicy::Denied {
        Err(NodeExecError::PolicyDenied {
            service_id: deployment.spec.service_id.to_string(),
        })
    } else {
        Ok(())
    }
}

struct LimitedExecSession {
    session: Box<dyn ExecSession>,
    _permit: OwnedSemaphorePermit,
}

#[async_trait]
impl ExecSession for LimitedExecSession {
    async fn send(&mut self, input: ExecInput) -> Result<(), RuntimeError> {
        self.session.send(input).await
    }

    async fn next(&mut self) -> Result<Option<ExecOutput>, RuntimeError> {
        self.session.next().await
    }

    async fn kill(&mut self) -> Result<(), RuntimeError> {
        self.session.kill().await
    }
}

/// Why a node-local exec session was rejected before or by the runtime.
#[derive(Debug, thiserror::Error)]
pub enum NodeExecError {
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    #[error("node exec maximumSessions must be positive")]
    ZeroSessionLimit,
    #[error("exec request requires a non-empty command and non-zero terminal dimensions")]
    InvalidRequest,
    #[error("runtime capability `{capability:?}` is unavailable")]
    CapabilityUnavailable { capability: RuntimeCapability },
    #[error("this node already has {maximum} active exec sessions")]
    SessionLimitReached { maximum: usize },
    #[error("{kind} resource at `{key}` is missing")]
    ResourceMissing { kind: &'static str, key: String },
    #[error("malformed {kind} resource at `{key}`: {message}")]
    MalformedResource {
        kind: &'static str,
        key: String,
        message: String,
    },
    #[error("{kind} resource at `{key}` contains a different identity")]
    ResourceIdentityMismatch { kind: &'static str, key: String },
    #[error("assignment `{assignment_id}` belongs to node `{node_id}`")]
    AssignmentOnAnotherNode {
        assignment_id: String,
        node_id: String,
    },
    #[error("assignment `{assignment_id}` does not have a running workload")]
    WorkloadNotRunning { assignment_id: String },
    #[error("assignment `{assignment_id}` does not have exactly one owned runtime workload")]
    WorkloadUnavailable { assignment_id: String },
    #[error("assignment `{assignment_id}` does not match its deployment identity")]
    DeploymentMismatch { assignment_id: String },
    #[error("deployment `{deployment_id}` is being deleted")]
    DeploymentUnavailable { deployment_id: String },
    #[error("service `{service_id}` denies exec")]
    PolicyDenied { service_id: String },
    #[error("exec authorization resource `{key}` changed while the target was resolved")]
    PolicyChanged { key: String },
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}
