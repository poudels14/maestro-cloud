use std::future::Future;
use std::sync::Arc;

use maestro_core::{NodeId, ReplicaPlan};
use maestro_store::{Change, ChangeKey, SecretKey, StateMachine, replica_plans};
use tokio::sync::{Mutex, broadcast};

use crate::error::AgentError;

pub trait PlanSource: Send + Sync + 'static {
    fn plans(&self) -> impl Future<Output = Result<Vec<ReplicaPlan>, AgentError>> + Send;

    fn changed(&self) -> impl Future<Output = ()> + Send;
}

pub struct LocalPlanSource {
    state: Arc<StateMachine>,
    key: Arc<SecretKey>,
    node_id: NodeId,
    changes: Mutex<broadcast::Receiver<Change>>,
}

impl LocalPlanSource {
    pub fn new(state: Arc<StateMachine>, key: Arc<SecretKey>, node_id: NodeId) -> Self {
        let changes = Mutex::new(state.subscribe());
        Self {
            state,
            key,
            node_id,
            changes,
        }
    }

    fn is_relevant(&self, change: &Change) -> bool {
        change.keys.iter().any(|key| match key {
            ChangeKey::NodeAssignments { node_id } => *node_id == self.node_id,
            ChangeKey::Everything | ChangeKey::Deployment { .. } => true,
            ChangeKey::Service { .. }
            | ChangeKey::Node { .. }
            | ChangeKey::ServiceControls { .. }
            | ChangeKey::ClusterFreeze
            | ChangeKey::Unschedulable
            | ChangeKey::Blocklist
            | ChangeKey::SlackWebhooks
            | ChangeKey::PreviewSource { .. }
            | ChangeKey::MaintenanceRun => false,
        })
    }
}

impl PlanSource for LocalPlanSource {
    async fn plans(&self) -> Result<Vec<ReplicaPlan>, AgentError> {
        Ok(replica_plans(&self.state, &self.key, self.node_id)?)
    }

    async fn changed(&self) {
        let mut changes = self.changes.lock().await;
        loop {
            match changes.recv().await {
                Ok(change) if self.is_relevant(&change) => break,
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => break,
                Err(broadcast::error::RecvError::Closed) => std::future::pending::<()>().await,
            }
        }
    }
}
