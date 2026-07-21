use std::collections::BTreeMap;

use kernel_api::{Assignment, NodeId, PlacementConstraint};

use super::orchestration::RolloutWorld;

#[tokio::test]
async fn hard_node_affinity_places_every_replica_on_the_selected_node()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        let world = RolloutWorld::new(node_count).await?;
        let selected = NodeId::new(format!("node-{node_count}"))?;
        world
            .set_placement(PlacementConstraint {
                node_id: Some(selected.clone()),
                labels: BTreeMap::new(),
            })
            .await?;
        world.converge().await?;

        let assignments = world.list::<Assignment>("Assignment").await?;
        assert_eq!(assignments.len(), usize::from(node_count));
        assert!(
            assignments
                .iter()
                .all(|assignment| assignment.spec.node_id == selected)
        );
    }
    Ok(())
}
