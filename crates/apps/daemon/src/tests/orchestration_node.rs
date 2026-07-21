use kernel_api::{
    Condition, ConditionReason, ConditionState, ConditionType, Node, NodeId, ResourceKind,
    ResourceName, Timestamp,
};
use kernel_store::{CasOutcome, ExpectedVersion, PutRequest, Store};

use super::orchestration::RolloutWorld;

impl RolloutWorld {
    pub(super) async fn set_node_draining(
        &self,
        node_id: &NodeId,
        draining: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = self.keys.resource(
            &ResourceKind::new("Node")?,
            &ResourceName::from(node_id.clone()),
        );
        let stored = self.store.get(&key).await?.ok_or("node missing")?;
        let mut node: Node = serde_json::from_slice(&stored.value)?;
        node.status
            .conditions
            .retain(|condition| condition.condition_type.0 != "Draining");
        node.status.conditions.push(Condition {
            condition_type: ConditionType("Draining".to_string()),
            state: if draining {
                ConditionState::True
            } else {
                ConditionState::False
            },
            reason: ConditionReason(if draining {
                "Requested".to_string()
            } else {
                "Restored".to_string()
            }),
            message: if draining {
                "node drain requested".to_string()
            } else {
                "node restored to scheduling".to_string()
            },
            observed_generation: node.meta.generation,
            last_transition_time: Timestamp(10_000),
        });
        let outcome = self
            .store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&node)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err("node drain update conflicted".into())
        }
    }
}
