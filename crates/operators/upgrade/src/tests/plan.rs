use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus, DeploymentId,
    Generation, Node, NodeId, NodeInstanceId, NodeRole, NodeSpec, NodeStatus, Object, ObjectMeta,
    ResourceRevision, ServiceId, Timestamp, UpgradeMode, UpgradePhase, UpgradeRun, UpgradeRunId,
    UpgradeRunSpec, UpgradeRunStatus,
};

use crate::{
    UpgradeDispatchOutcome, UpgradeInput, UpgradePlan, UpgradePlanAction, UpgradePlanError,
    UpgradeSettings, plan_upgrade, record_dispatch_outcome,
};

#[test]
fn rolling_upgrade_drains_retries_restarts_and_advances_one_node_at_a_time() -> Result<(), String> {
    let settings = settings(3);
    let mut nodes = topology_with_worker();
    let mut current_run = run(UpgradeMode::Rolling);

    let initialized = plan_upgrade(input(current_run, &nodes, Vec::new(), 10_000), settings)
        .expect("initialize rolling upgrade");
    assert_eq!(initialized.run.status.phase, UpgradePhase::Draining);
    assert_eq!(
        status_ids(&initialized),
        ["worker-1", "node-2", "node-3", "node-1"]
    );
    assert_eq!(initialized.node_updates.len(), 1);
    current_run = initialized.run;
    apply_updates(&mut nodes, initialized.node_updates);
    assert_eq!(maintained_nodes(&nodes), BTreeSet::from(["worker-1"]));

    let waiting = plan_upgrade(
        input(current_run, &nodes, vec![assignment("worker-1")], 10_000),
        settings,
    )
    .expect("wait for rolling drain");
    assert_eq!(waiting.run.status.phase, UpgradePhase::Draining);
    assert_eq!(
        waiting.action,
        UpgradePlanAction::Requeue(Duration::from_secs(1))
    );

    let applying = plan_upgrade(input(waiting.run, &nodes, Vec::new(), 10_000), settings)
        .expect("finish rolling drain");
    assert_eq!(applying.run.status.phase, UpgradePhase::Applying);
    let dispatch = plan_upgrade(
        input(applying.run.clone(), &nodes, Vec::new(), 10_000),
        settings,
    )
    .expect("plan rolling dispatch");
    let UpgradePlanAction::Dispatch(request) = dispatch.action else {
        return Err("applying phase did not emit dispatch".to_string());
    };
    assert_eq!(
        request
            .targets
            .iter()
            .map(|target| target.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["worker-1"]
    );

    let retry = record_dispatch_outcome(
        input(applying.run, &nodes, Vec::new(), 10_000),
        settings,
        UpgradeDispatchOutcome::Retryable {
            message: "node API unavailable".to_string(),
        },
    )
    .expect("record retryable dispatch");
    assert_eq!(retry.run.status.nodes.first().unwrap().attempts, 1);
    assert_eq!(
        retry.run.status.nodes.first().unwrap().retry_at,
        Some(Timestamp(15_000))
    );
    let before_retry = plan_upgrade(
        input(retry.run.clone(), &nodes, Vec::new(), 12_000),
        settings,
    )
    .expect("respect persisted retry deadline");
    assert_eq!(
        before_retry.action,
        UpgradePlanAction::Requeue(Duration::from_secs(3))
    );
    assert!(matches!(
        plan_upgrade(
            input(retry.run.clone(), &nodes, Vec::new(), 15_000),
            settings
        )
        .expect("retry deadline elapsed")
        .action,
        UpgradePlanAction::Dispatch(_)
    ));

    let restarting = record_dispatch_outcome(
        input(retry.run, &nodes, Vec::new(), 15_000),
        settings,
        UpgradeDispatchOutcome::Accepted,
    )
    .expect("record accepted dispatch");
    assert_eq!(restarting.run.status.phase, UpgradePhase::Restarting);
    let unchanged = plan_upgrade(
        input(restarting.run.clone(), &nodes, Vec::new(), 15_000),
        settings,
    )
    .expect("wait for daemon identity change");
    assert_eq!(unchanged.run.status.phase, UpgradePhase::Restarting);

    upgrade_node(&mut nodes, "worker-1");
    let verifying = plan_upgrade(input(restarting.run, &nodes, Vec::new(), 16_000), settings)
        .expect("observe restarted daemon");
    assert_eq!(verifying.run.status.phase, UpgradePhase::Verifying);
    let next = plan_upgrade(input(verifying.run, &nodes, Vec::new(), 16_000), settings)
        .expect("verify and advance rolling upgrade");
    assert_eq!(next.run.status.phase, UpgradePhase::Draining);
    assert_eq!(
        next.run.status.nodes.first().unwrap().phase,
        UpgradePhase::Completed
    );
    assert_eq!(
        next.run.status.nodes.get(1).unwrap().phase,
        UpgradePhase::Draining
    );
    apply_updates(&mut nodes, next.node_updates);
    assert_eq!(maintained_nodes(&nodes), BTreeSet::from(["node-2"]));
    Ok(())
}

#[test]
fn all_node_mode_dispatches_and_verifies_one_batch_without_waiting_for_placement() {
    let settings = settings(2);
    let mut nodes = topology_three_voters();
    let initialized = plan_upgrade(
        input(run(UpgradeMode::AllNodes), &nodes, Vec::new(), 10_000),
        settings,
    )
    .expect("initialize all-node upgrade");
    assert!(
        initialized
            .run
            .status
            .nodes
            .iter()
            .all(|status| status.phase == UpgradePhase::Draining)
    );
    apply_updates(&mut nodes, initialized.node_updates);
    assert_eq!(maintained_nodes(&nodes).len(), 3);

    let assignments = nodes
        .iter()
        .map(|node| assignment(node.meta.id.as_str()))
        .collect();
    let applying = plan_upgrade(
        input(initialized.run, &nodes, assignments, 10_000),
        settings,
    )
    .expect("all-node drain is a scheduling barrier, not a placement wait");
    assert_eq!(applying.run.status.phase, UpgradePhase::Applying);
    let dispatch = plan_upgrade(
        input(applying.run.clone(), &nodes, Vec::new(), 10_000),
        settings,
    )
    .expect("plan all-node dispatch");
    assert!(matches!(
        dispatch.action,
        UpgradePlanAction::Dispatch(ref request) if request.targets.len() == 3
    ));
    let restarting = record_dispatch_outcome(
        input(applying.run, &nodes, Vec::new(), 10_000),
        settings,
        UpgradeDispatchOutcome::Accepted,
    )
    .expect("record all-node dispatch");
    for node_id in ["node-1", "node-2", "node-3"] {
        upgrade_node(&mut nodes, node_id);
    }
    let verifying = plan_upgrade(input(restarting.run, &nodes, Vec::new(), 11_000), settings)
        .expect("observe all restarted daemons");
    let completed = plan_upgrade(input(verifying.run, &nodes, Vec::new(), 11_000), settings)
        .expect("verify all-node batch");
    assert_eq!(completed.run.status.phase, UpgradePhase::Completed);
    assert!(
        completed
            .run
            .status
            .nodes
            .iter()
            .all(|status| status.phase == UpgradePhase::Completed)
    );
    apply_updates(&mut nodes, completed.node_updates);
    assert!(maintained_nodes(&nodes).is_empty());
}

#[test]
fn exhausted_dispatch_restores_current_node_and_cancels_pending_nodes() {
    let settings = settings(1);
    let mut nodes = topology_with_worker();
    let initialized = plan_upgrade(
        input(run(UpgradeMode::Rolling), &nodes, Vec::new(), 10_000),
        settings,
    )
    .expect("initialize rolling upgrade");
    apply_updates(&mut nodes, initialized.node_updates);
    let applying = plan_upgrade(input(initialized.run, &nodes, Vec::new(), 10_000), settings)
        .expect("finish rolling drain");
    let failed = record_dispatch_outcome(
        input(applying.run, &nodes, Vec::new(), 10_000),
        settings,
        UpgradeDispatchOutcome::Retryable {
            message: "still unavailable".to_string(),
        },
    )
    .expect("exhaust dispatch budget");
    assert_eq!(failed.run.status.phase, UpgradePhase::Failed);
    assert_eq!(
        failed.run.status.nodes.first().unwrap().phase,
        UpgradePhase::Failed
    );
    assert!(
        failed
            .run
            .status
            .nodes
            .iter()
            .skip(1)
            .all(|status| status.phase == UpgradePhase::Canceled)
    );
    apply_updates(&mut nodes, failed.node_updates);
    assert!(maintained_nodes(&nodes).is_empty());
}

#[test]
fn rolling_upgrade_rejects_a_two_voter_control_plane() {
    let nodes = vec![
        node("node-1", NodeRole::Master),
        node("node-2", NodeRole::Hybrid),
    ];
    assert!(matches!(
        plan_upgrade(
            input(run(UpgradeMode::Rolling), &nodes, Vec::new(), 10_000),
            settings(2),
        ),
        Err(UpgradePlanError::UnsafeTwoVoterRollingUpgrade)
    ));
}

fn settings(max_attempts: u32) -> UpgradeSettings {
    UpgradeSettings::new(Duration::from_secs(5), Duration::from_secs(1), max_attempts).unwrap()
}

fn input(run: UpgradeRun, nodes: &[Node], assignments: Vec<Assignment>, now: i64) -> UpgradeInput {
    UpgradeInput {
        run,
        nodes: nodes.to_vec(),
        assignments,
        live_nodes: nodes.iter().map(|node| node.meta.id.clone()).collect(),
        leader_id: NodeId::new("node-1").unwrap(),
        now: Timestamp(now),
    }
}

fn run(mode: UpgradeMode) -> UpgradeRun {
    Object {
        meta: metadata(UpgradeRunId::new("upgrade-1").unwrap()),
        spec: UpgradeRunSpec {
            target_version: "2.0.0".to_string(),
            mode,
            node_ids: Vec::new(),
        },
        status: UpgradeRunStatus {
            phase: UpgradePhase::Pending,
            nodes: Vec::new(),
            conditions: Vec::new(),
        },
    }
}

fn topology_with_worker() -> Vec<Node> {
    vec![
        node("node-1", NodeRole::Master),
        node("worker-1", NodeRole::Worker),
        node("node-3", NodeRole::ControlPlane),
        node("node-2", NodeRole::Hybrid),
    ]
}

fn topology_three_voters() -> Vec<Node> {
    vec![
        node("node-1", NodeRole::Master),
        node("node-2", NodeRole::Hybrid),
        node("node-3", NodeRole::ControlPlane),
    ]
}

fn node(id: &str, role: NodeRole) -> Node {
    Object {
        meta: metadata(NodeId::new(id).unwrap()),
        spec: NodeSpec {
            hostname: format!("{id}.internal"),
            host_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            role,
            scheduling_labels: BTreeMap::new(),
        },
        status: NodeStatus {
            instance_id: NodeInstanceId::new(format!("instance-{id}-1")).unwrap(),
            version: "1.0.0".to_string(),
            last_seen: Timestamp(10_000),
            conditions: Vec::new(),
        },
    }
}

fn assignment(node_id: &str) -> Assignment {
    Object {
        meta: metadata(AssignmentId::new(format!("assignment-{node_id}")).unwrap()),
        spec: AssignmentSpec {
            service_id: ServiceId::new("api").unwrap(),
            deployment_id: DeploymentId::new("deployment-1").unwrap(),
            restart_generation: Generation(1),
            replica_index: 0,
            node_id: NodeId::new(node_id).unwrap(),
            placement_epoch: 1,
            workload_address: IpAddr::V4(Ipv4Addr::new(10, 42, 1, 2)),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Running,
            workload_id: None,
            conditions: Vec::new(),
        },
    }
}

fn metadata<Id>(id: Id) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision::default(),
        generation: Generation(1),
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}

fn apply_updates(nodes: &mut [Node], updates: Vec<Node>) {
    for update in updates {
        let current = nodes
            .iter_mut()
            .find(|node| node.meta.id == update.meta.id)
            .expect("updated node exists");
        *current = update;
    }
}

fn upgrade_node(nodes: &mut [Node], node_id: &str) {
    let node = nodes
        .iter_mut()
        .find(|node| node.meta.id.as_str() == node_id)
        .expect("upgrade node exists");
    node.status.instance_id = NodeInstanceId::new(format!("instance-{node_id}-2")).unwrap();
    node.status.version = "2.0.0".to_string();
}

fn maintained_nodes(nodes: &[Node]) -> BTreeSet<&str> {
    nodes
        .iter()
        .filter(|node| {
            node.status.conditions.iter().any(|condition| {
                condition.condition_type.0 == "Maintenance"
                    && condition.state == kernel_api::ConditionState::True
            })
        })
        .map(|node| node.meta.id.as_str())
        .collect()
}

fn status_ids(plan: &UpgradePlan) -> Vec<&str> {
    plan.run
        .status
        .nodes
        .iter()
        .map(|status| status.node_id.as_str())
        .collect()
}
