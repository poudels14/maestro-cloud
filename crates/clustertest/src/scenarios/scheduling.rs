use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::{
    FixtureNodeName, ReplicaCount, ReplicaIndex, ScenarioError, ScheduledAssignment,
    SchedulingCluster, SchedulingSnapshot,
};

/// Proves deterministic spread, stable assignments, scale-down, and workload GC.
pub async fn scheduler_scales_replicas_across_nodes<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: SchedulingCluster,
{
    let initial = scale(cluster, ReplicaCount::new(1), "initial scheduling").await?;
    assert_schedulable(&initial, 1)?;
    assert_placement(&initial, [("node-1", 1)])?;

    let scaled_up = scale(cluster, ReplicaCount::new(5), "scale-up scheduling").await?;
    assert_schedulable(&scaled_up, 5)?;
    assert_placement(&scaled_up, [("node-1", 2), ("node-2", 2), ("node-3", 1)])?;
    assert_assignment_retained(&initial, &scaled_up, ReplicaIndex::new(0))?;

    let scaled_down = scale(cluster, ReplicaCount::new(2), "scale-down scheduling").await?;
    assert_schedulable(&scaled_down, 2)?;
    assert_placement(&scaled_down, [("node-1", 1), ("node-2", 1)])?;
    for survivor in &scaled_down.assignments {
        assert_assignment_retained(&scaled_up, &scaled_down, survivor.replica_index)?;
    }
    if scaled_down.orphaned_workloads == 0 {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "scale-down left {} workload(s) without assignments",
            scaled_down.orphaned_workloads
        )))
    }
}

async fn scale<Cluster>(
    cluster: &mut Cluster,
    replicas: ReplicaCount,
    operation: &'static str,
) -> Result<SchedulingSnapshot<Cluster::AssignmentId>, ScenarioError>
where
    Cluster: SchedulingCluster,
{
    cluster
        .scale(replicas)
        .await
        .map_err(|error| ScenarioError::Driver {
            operation,
            message: error.to_string(),
        })
}

fn assert_schedulable<AssignmentId>(
    snapshot: &SchedulingSnapshot<AssignmentId>,
    expected_assignments: usize,
) -> Result<(), ScenarioError> {
    if !snapshot.unschedulable_replicas.is_empty() {
        Err(ScenarioError::Assertion(format!(
            "scheduler reported unschedulable replicas: {:?}",
            snapshot.unschedulable_replicas
        )))
    } else if snapshot.assignments.len() != expected_assignments {
        Err(ScenarioError::Assertion(format!(
            "scheduler produced {} assignments, expected {expected_assignments}",
            snapshot.assignments.len()
        )))
    } else {
        Ok(())
    }
}

fn assert_placement<AssignmentId, const NODE_COUNT: usize>(
    snapshot: &SchedulingSnapshot<AssignmentId>,
    expected: [(&str, usize); NODE_COUNT],
) -> Result<(), ScenarioError> {
    let actual = snapshot.assignments.iter().fold(
        BTreeMap::<FixtureNodeName, usize>::new(),
        |mut counts, assignment| {
            *counts.entry(assignment.node.clone()).or_default() += 1;
            counts
        },
    );
    let expected = expected
        .into_iter()
        .map(|(node, count)| (FixtureNodeName::new(node), count))
        .collect::<BTreeMap<_, _>>();
    if actual == expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "scheduler placement was {actual:?}, expected {expected:?}"
        )))
    }
}

fn assert_assignment_retained<AssignmentId: Debug + Eq>(
    before: &SchedulingSnapshot<AssignmentId>,
    after: &SchedulingSnapshot<AssignmentId>,
    replica_index: ReplicaIndex,
) -> Result<(), ScenarioError> {
    let before_assignment = assignment(before, replica_index)?;
    let after_assignment = assignment(after, replica_index)?;
    if before_assignment.id == after_assignment.id {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "replica {} changed assignment from {:?} to {:?}",
            replica_index.get(),
            before_assignment.id,
            after_assignment.id
        )))
    }
}

fn assignment<AssignmentId: Debug>(
    snapshot: &SchedulingSnapshot<AssignmentId>,
    replica_index: ReplicaIndex,
) -> Result<&ScheduledAssignment<AssignmentId>, ScenarioError> {
    snapshot
        .assignments
        .iter()
        .find(|assignment| assignment.replica_index == replica_index)
        .ok_or_else(|| {
            ScenarioError::Assertion(format!(
                "scheduler snapshot has no assignment for replica {}",
                replica_index.get()
            ))
        })
}
