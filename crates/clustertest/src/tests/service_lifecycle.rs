use async_trait::async_trait;

use super::lifecycle::{LifecycleWorld, LifecycleWorldError};
use crate::{
    FixtureName, FixtureNodeName, NodeDrainState, ServiceFreezeState, ServiceLifecycleCluster,
    scenarios,
};

#[async_trait]
impl ServiceLifecycleCluster for LifecycleWorld {
    fn topology_nodes(&self) -> Vec<FixtureNodeName> {
        self.topology_nodes.clone()
    }

    async fn restart_deployment(
        &mut self,
        deployment_id: &Self::DeploymentId,
    ) -> Result<(), Self::Error> {
        let deployment = self.deployment_mut(*deployment_id)?;
        deployment.phase = crate::DeploymentPhase::Queued;
        deployment.replicas.clear();
        Ok(())
    }

    async fn remove_deployment(
        &mut self,
        deployment_id: &Self::DeploymentId,
    ) -> Result<(), Self::Error> {
        self.deployment_mut(*deployment_id)?;
        self.removing_deployments.insert(*deployment_id);
        Ok(())
    }

    async fn delete_service(&mut self, service: &FixtureName) -> Result<(), Self::Error> {
        self.deleting_services.insert(service.clone());
        Ok(())
    }

    async fn set_service_frozen(
        &mut self,
        service: &FixtureName,
        state: ServiceFreezeState,
    ) -> Result<(), Self::Error> {
        match state {
            ServiceFreezeState::Frozen => {
                self.frozen_services.insert(service.clone());
            }
            ServiceFreezeState::Active => {
                self.frozen_services.remove(service);
            }
        }
        Ok(())
    }

    async fn set_node_draining(
        &mut self,
        node: &FixtureNodeName,
        state: NodeDrainState,
    ) -> Result<(), Self::Error> {
        if !self.topology_nodes.contains(node) {
            return Err(LifecycleWorldError("unknown node"));
        }
        match state {
            NodeDrainState::Draining => {
                self.draining_nodes.insert(node.clone());
            }
            NodeDrainState::Available => {
                self.draining_nodes.remove(node);
            }
        }
        Ok(())
    }

    async fn set_service_node_affinity(
        &mut self,
        service: &FixtureName,
        node: &FixtureNodeName,
    ) -> Result<(), Self::Error> {
        if !self.topology_nodes.contains(node) {
            return Err(LifecycleWorldError("unknown affinity node"));
        }
        self.service_affinity.insert(service.clone(), node.clone());
        Ok(())
    }
}

#[tokio::test]
async fn every_lifecycle_verb_passes_one_and_three_node_topologies()
-> Result<(), crate::ScenarioError> {
    for node_count in [1, 3] {
        scenarios::rollout_reaches_ready(&mut LifecycleWorld::with_node_count(node_count)).await?;
        scenarios::redeploy_drains_previous(&mut LifecycleWorld::with_node_count(node_count))
            .await?;
        scenarios::queued_deployment_can_be_canceled(&mut LifecycleWorld::with_node_count(
            node_count,
        ))
        .await?;
        scenarios::replica_override_round_trips(&mut LifecycleWorld::with_node_count(node_count))
            .await?;
        scenarios::drained_deployment_finalizes(&mut LifecycleWorld::with_node_count(node_count))
            .await?;
        scenarios::restart_recycles_workloads_in_place(&mut LifecycleWorld::with_node_count(
            node_count,
        ))
        .await?;
        scenarios::remove_deployment_retains_history(&mut LifecycleWorld::with_node_count(
            node_count,
        ))
        .await?;
        scenarios::delete_service_collects_owned_state(&mut LifecycleWorld::with_node_count(
            node_count,
        ))
        .await?;
        scenarios::freeze_and_unfreeze_gate_rollout(&mut LifecycleWorld::with_node_count(
            node_count,
        ))
        .await?;
        scenarios::drain_and_restore_move_placement(&mut LifecycleWorld::with_node_count(
            node_count,
        ))
        .await?;
        scenarios::hard_node_affinity_pins_placement(&mut LifecycleWorld::with_node_count(
            node_count,
        ))
        .await?;
    }
    Ok(())
}
