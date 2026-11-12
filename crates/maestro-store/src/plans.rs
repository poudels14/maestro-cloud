use maestro_core::{
    DISCOVERY_HOST_ENV, DISCOVERY_KEY_ENV, DISCOVERY_PORT, NodeId, ReplicaPlan, subnet_gateway,
};

use crate::crypto::{DiscoveryKeys, SecretError, SecretKey};
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
    discovery_keys: &DiscoveryKeys,
    node_id: NodeId,
) -> Result<Vec<ReplicaPlan>, PlanError> {
    let gateway = state
        .node(node_id)?
        .and_then(|node| node.subnet)
        .and_then(|subnet| subnet_gateway(&subnet));
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
                let image_source = deployment
                    .built_on
                    .filter(|built_on| *built_on != node_id)
                    .map(|built_on| state.node(built_on))
                    .transpose()?
                    .flatten()
                    .map(|node| node.endpoint);
                let plan = ReplicaPlan::new(assignment, &spec);
                let discovery_env = gateway
                    .map(|gateway| {
                        [
                            (
                                DISCOVERY_HOST_ENV.to_string(),
                                format!("http://{gateway}:{DISCOVERY_PORT}"),
                            ),
                            (
                                DISCOVERY_KEY_ENV.to_string(),
                                discovery_keys.key(&assignment.service_id),
                            ),
                        ]
                    })
                    .into_iter()
                    .flatten();
                Ok(ReplicaPlan {
                    image_source,
                    env: plan.env.into_iter().chain(discovery_env).collect(),
                    ..plan
                })
            }))
        })
        .collect()
}
