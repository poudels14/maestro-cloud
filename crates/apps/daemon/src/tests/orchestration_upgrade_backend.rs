use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use clustertest::{FixtureNodeName, MaintenanceAttempt};
use kernel_api::{
    ConditionState, Node, NodeId, NodeInstanceId, ResourceKind, ResourceName, UpgradeOperation,
};
use kernel_store::{CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store};
use upgrade::{NodeUpgradeBackend, NodeUpgradeBackendError, NodeUpgradeRequest};

use super::orchestration::HarnessResult;

pub(super) struct RecordingUpgradeBackend {
    store: Arc<InMemoryStore>,
    keys: Keyspace,
    state: Mutex<UpgradeBackendState>,
}

#[derive(Default)]
struct UpgradeBackendState {
    failed_node: Option<NodeId>,
    failed_requests: BTreeSet<String>,
    accepted_requests: BTreeSet<String>,
    attempts: Vec<MaintenanceAttempt>,
}

impl RecordingUpgradeBackend {
    pub(super) fn new(store: Arc<InMemoryStore>, keys: Keyspace) -> Self {
        Self {
            store,
            keys,
            state: Mutex::new(UpgradeBackendState::default()),
        }
    }

    pub(super) fn fail_once_on(&self, node: Option<NodeId>) {
        lock(&self.state).failed_node = node;
    }

    pub(super) fn attempt_count(&self) -> usize {
        lock(&self.state).attempts.len()
    }

    pub(super) fn attempts_since(&self, cursor: usize) -> Vec<MaintenanceAttempt> {
        lock(&self.state)
            .attempts
            .get(cursor..)
            .unwrap_or_default()
            .to_vec()
    }

    async fn observed_nodes(&self) -> HarnessResult<Vec<Node>> {
        self.store
            .list(&self.keys.resource_kind(&ResourceKind::new("Node")?))
            .await?
            .values
            .into_iter()
            .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
            .collect()
    }

    async fn apply_nodes(&self, request: &NodeUpgradeRequest) -> HarnessResult<()> {
        for target in &request.targets {
            let key = self.keys.resource(
                &ResourceKind::new("Node")?,
                &ResourceName::from(target.node_id.clone()),
            );
            let stored =
                self.store.get(&key).await?.ok_or_else(|| {
                    format!("upgrade target node `{}` disappeared", target.node_id)
                })?;
            let mut node: Node = serde_json::from_slice(&stored.value)?;
            if request.operation == UpgradeOperation::Upgrade {
                node.status.version = request.target_version.clone();
            }
            node.status.instance_id = NodeInstanceId::new(format!(
                "{}-{}-{}",
                node.status.instance_id,
                match request.operation {
                    UpgradeOperation::Upgrade => "upgrade",
                    UpgradeOperation::Restart => "restart",
                },
                request.target_version.replace('.', "-")
            ))?;
            let outcome = self
                .store
                .put_cas(PutRequest {
                    key,
                    value: serde_json::to_vec(&node)?,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await?;
            if !matches!(outcome, CasOutcome::Applied(_)) {
                return Err(format!("upgrade target node `{}` conflicted", target.node_id).into());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl NodeUpgradeBackend for RecordingUpgradeBackend {
    async fn apply(&self, request: &NodeUpgradeRequest) -> Result<(), NodeUpgradeBackendError> {
        let request_key = format!(
            "{}:{:?}:{}",
            request.run_id,
            request.operation,
            request
                .targets
                .iter()
                .map(|target| target.node_id.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
        if lock(&self.state).accepted_requests.contains(&request_key) {
            return Ok(());
        }
        let nodes = self
            .observed_nodes()
            .await
            .map_err(|error| unavailable(error.to_string()))?;
        let drained_nodes = nodes
            .iter()
            .filter(|node| maintained(node))
            .map(|node| FixtureNodeName::new(node.meta.id.to_string()))
            .collect::<BTreeSet<_>>();
        let should_fail = {
            let mut state = lock(&self.state);
            state
                .attempts
                .extend(request.targets.iter().map(|target| MaintenanceAttempt {
                    node: FixtureNodeName::new(target.node_id.to_string()),
                    drained_nodes: drained_nodes.clone(),
                }));
            let failed_node = state.failed_node.clone();
            failed_node.as_ref().is_some_and(|failed| {
                request.targets.len() == 1
                    && request.targets.first().map(|target| &target.node_id) == Some(failed)
                    && state.failed_requests.insert(request_key.clone())
            })
        };
        if should_fail {
            return Err(unavailable("injected first upgrade failure"));
        }
        self.apply_nodes(request)
            .await
            .map_err(|error| unavailable(error.to_string()))?;
        lock(&self.state).accepted_requests.insert(request_key);
        Ok(())
    }
}

pub(super) fn maintained(node: &Node) -> bool {
    node.status.conditions.iter().any(|condition| {
        condition.condition_type == kernel_api::ConditionType::Maintenance
            && condition.state == ConditionState::True
    })
}

fn unavailable(message: impl Into<String>) -> NodeUpgradeBackendError {
    NodeUpgradeBackendError::Unavailable {
        message: message.into(),
    }
}

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(|error| error.into_inner())
}
