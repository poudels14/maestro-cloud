use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::{
    FixtureNodeName, ReplicaCount, ReplicaIndex, ScheduledAssignment, SchedulingCluster,
    SchedulingSnapshot, scenarios::scheduler_scales_replicas_across_nodes,
};

struct SchedulingWorld {
    assignments: BTreeMap<u32, ScheduledAssignment<String>>,
    nodes: Vec<FixtureNodeName>,
}

impl SchedulingWorld {
    fn new() -> Self {
        Self {
            assignments: BTreeMap::new(),
            nodes: (1..=3)
                .map(|node_number| FixtureNodeName::new(format!("node-{node_number}")))
                .collect(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("scheduling world failed")]
struct SchedulingWorldError;

#[async_trait]
impl SchedulingCluster for SchedulingWorld {
    type AssignmentId = String;
    type Error = SchedulingWorldError;

    async fn scale(
        &mut self,
        replicas: ReplicaCount,
    ) -> Result<SchedulingSnapshot<Self::AssignmentId>, Self::Error> {
        self.assignments
            .retain(|replica_index, _| *replica_index < replicas.get());
        for replica_index in 0..replicas.get() {
            if !self.assignments.contains_key(&replica_index) {
                let load = self.assignments.values().fold(
                    BTreeMap::<FixtureNodeName, usize>::new(),
                    |mut counts, assignment| {
                        *counts.entry(assignment.node.clone()).or_default() += 1;
                        counts
                    },
                );
                let node = self
                    .nodes
                    .iter()
                    .min_by_key(|node| (load.get(*node).copied().unwrap_or(0), node.as_str()))
                    .cloned()
                    .ok_or(SchedulingWorldError)?;
                self.assignments.insert(
                    replica_index,
                    ScheduledAssignment {
                        id: format!("assignment-{replica_index}"),
                        replica_index: ReplicaIndex::new(replica_index),
                        node,
                    },
                );
            }
        }
        Ok(SchedulingSnapshot {
            assignments: self.assignments.values().cloned().collect(),
            unschedulable_replicas: Vec::new(),
            orphaned_workloads: 0,
        })
    }
}

#[tokio::test]
async fn scheduling_scenario_scales_and_retains_assignments() {
    let mut cluster = SchedulingWorld::new();

    scheduler_scales_replicas_across_nodes(&mut cluster)
        .await
        .expect("scheduling scenario");
}
