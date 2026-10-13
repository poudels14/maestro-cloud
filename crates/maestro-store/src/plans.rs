use maestro_core::{NodeId, ReplicaPlan};

use crate::crypto::{SecretError, SecretKey};
use crate::error::StoreError;
use crate::state::StateMachine;

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Secret(#[from] SecretError),
}

pub fn replica_plans(
    state: &StateMachine,
    key: &SecretKey,
    node_id: NodeId,
) -> Result<Vec<ReplicaPlan>, PlanError> {
    state
        .assignments(node_id)?
        .iter()
        .filter_map(|assignment| {
            let deployment = state
                .deployment(&assignment.service_id, &assignment.deployment_id)
                .map_err(PlanError::from)
                .transpose()?;
            Some(deployment.and_then(|deployment| {
                let spec = key.open_spec(&deployment.spec)?;
                Ok(ReplicaPlan::new(assignment, &spec))
            }))
        })
        .collect()
}
