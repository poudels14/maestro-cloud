use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};

use kernel_api::{
    AnnotationKey, Condition, ConditionReason, ConditionState, ConditionType, Generation, Node,
    NodeId, NodeInstanceId, NodeRole, NodeSpec, NodeStatus, Object, ObjectMeta, ResourceRevision,
    Timestamp, WorkloadNetworkMode,
};

use crate::{NodeSchedulingAction, set_node_scheduling};

#[test]
fn restore_releases_only_the_migrated_cutover_freeze() -> Result<(), Box<dyn std::error::Error>> {
    let mut node = migrated_node()?;
    node.status.conditions.push(condition(
        "Maintenance",
        ConditionState::True,
        "UpgradeInProgress",
    ));

    assert!(set_node_scheduling(
        &mut node,
        NodeSchedulingAction::Restore,
        Timestamp(2_000),
    ));
    assert!(
        !node
            .status
            .conditions
            .iter()
            .any(|condition| condition.reason.0 == "CutoverPending")
    );
    assert!(
        node.status
            .conditions
            .iter()
            .any(|condition| condition.reason.0 == "UpgradeInProgress")
    );
    assert!(node.status.conditions.iter().any(|condition| {
        condition.condition_type.0 == "Draining"
            && condition.state == ConditionState::False
            && condition.reason.0 == "Restored"
    }));
    assert!(!set_node_scheduling(
        &mut node,
        NodeSchedulingAction::Restore,
        Timestamp(3_000),
    ));
    Ok(())
}

#[test]
fn restore_does_not_release_an_unmarked_cutover_condition() -> Result<(), Box<dyn std::error::Error>>
{
    let mut node = migrated_node()?;
    node.meta.annotations.clear();

    assert!(set_node_scheduling(
        &mut node,
        NodeSchedulingAction::Restore,
        Timestamp(2_000),
    ));
    assert!(
        node.status
            .conditions
            .iter()
            .any(|condition| condition.reason.0 == "CutoverPending")
    );
    Ok(())
}

fn migrated_node() -> Result<Node, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: ObjectMeta {
            id: NodeId::new("node-1")?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::from([(
                AnnotationKey("migration.maestro.dev/legacy-node-record".to_owned()),
                "{}".to_owned(),
            )]),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: NodeSpec {
            hostname: "node-1.internal".to_owned(),
            host_address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            role: NodeRole::Hybrid,
            workload_network_mode: WorkloadNetworkMode::ClusterRouted,
            scheduling_labels: BTreeMap::new(),
        },
        status: NodeStatus {
            instance_id: NodeInstanceId::new("instance-1")?,
            version: "0.5.0".to_owned(),
            last_seen: Timestamp(1_000),
            conditions: vec![condition(
                "Maintenance",
                ConditionState::True,
                "CutoverPending",
            )],
        },
    })
}

fn condition(condition_type: &str, state: ConditionState, reason: &str) -> Condition {
    Condition {
        condition_type: ConditionType(condition_type.to_owned()),
        state,
        reason: ConditionReason(reason.to_owned()),
        message: reason.to_owned(),
        observed_generation: Generation(1),
        last_transition_time: Timestamp(1_000),
    }
}
