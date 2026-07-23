use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kernel_api::{
    ConditionState, Node, NodeId, NodeUpgradeStatus, Timestamp, UpgradeMode, UpgradeOperation,
    UpgradePhase,
};
use semver::Version;

use crate::conditions::{
    MaintenanceAction, reject_foreign_maintenance, set_maintenance, set_ready_condition,
};
use crate::{
    NodeUpgradeRequest, NodeUpgradeTarget, UpgradeDispatchOutcome, UpgradeInput, UpgradePlan,
    UpgradePlanAction, UpgradePlanError, UpgradeSettings,
};

const ARTIFACT_REPLICATION_READY_CONDITION: &str = "ArtifactReplicationReady";

/// Computes the next durable upgrade state without performing side effects.
pub fn plan_upgrade(
    input: UpgradeInput,
    settings: UpgradeSettings,
) -> Result<UpgradePlan, UpgradePlanError> {
    let nodes = index_nodes(&input.nodes)?;
    match input.run.status.phase {
        UpgradePhase::Pending => {
            let target = parse_target(&input.run.spec.target_version)?;
            initialize(input, nodes, &target)
        }
        UpgradePhase::Draining => plan_draining(input, nodes, settings),
        UpgradePhase::Applying => plan_dispatch(input, nodes),
        UpgradePhase::Restarting => plan_restart(input, nodes, settings),
        UpgradePhase::Verifying => {
            let target = parse_target(&input.run.spec.target_version)?;
            plan_verification(input, nodes, &target, settings)
        }
        UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::Canceled => {
            plan_terminal(input, nodes)
        }
    }
}

/// Applies one adapter outcome to an `Applying` run without repeating the side effect.
pub fn record_dispatch_outcome(
    input: UpgradeInput,
    settings: UpgradeSettings,
    outcome: UpgradeDispatchOutcome,
) -> Result<UpgradePlan, UpgradePlanError> {
    if input.run.status.phase != UpgradePhase::Applying {
        return Err(UpgradePlanError::UnexpectedDispatchPhase {
            phase: input.run.status.phase,
        });
    }
    let nodes = index_nodes(&input.nodes)?;
    let mut run = input.run;
    let applying = status_indices(&run, UpgradePhase::Applying);
    if applying.is_empty() {
        return Err(UpgradePlanError::EmptyBatch {
            phase: UpgradePhase::Applying,
        });
    }
    for index in &applying {
        let status = run
            .status
            .nodes
            .get_mut(*index)
            .ok_or(UpgradePlanError::CorruptStatusIndex)?;
        status.attempts = status.attempts.saturating_add(1);
    }
    match outcome {
        UpgradeDispatchOutcome::Accepted => {
            transition_statuses(&mut run, &applying, UpgradePhase::Restarting)?;
            run.status.phase = transition(run.status.phase, UpgradePhase::Restarting)?;
            let message = if run.spec.operation == UpgradeOperation::Restart {
                "restart request accepted; waiting for new daemon identities"
            } else {
                "upgrade request accepted; waiting for new daemon identities"
            };
            set_ready_condition(
                &mut run,
                ConditionState::False,
                "Restarting",
                message,
                input.now,
            );
            Ok(plan(
                run,
                Vec::new(),
                UpgradePlanAction::Requeue(Duration::ZERO),
            ))
        }
        UpgradeDispatchOutcome::Retryable { message } => {
            let exhausted = applying.iter().any(|index| {
                run.status
                    .nodes
                    .get(*index)
                    .is_some_and(|status| status.attempts >= settings.max_attempts)
            });
            if exhausted {
                fail_run(run, nodes, input.now, "UpgradeRetriesExhausted", message)
            } else {
                let retry_at = add_duration(input.now, settings.retry_delay);
                for index in applying {
                    let status = run
                        .status
                        .nodes
                        .get_mut(index)
                        .ok_or(UpgradePlanError::CorruptStatusIndex)?;
                    status.retry_at = Some(retry_at);
                }
                set_ready_condition(
                    &mut run,
                    ConditionState::False,
                    "UpgradeRetryScheduled",
                    &message,
                    input.now,
                );
                Ok(plan(
                    run,
                    Vec::new(),
                    UpgradePlanAction::Requeue(settings.retry_delay),
                ))
            }
        }
        UpgradeDispatchOutcome::Rejected { message } => {
            fail_run(run, nodes, input.now, "UpgradeRejected", message)
        }
    }
}

fn initialize(
    input: UpgradeInput,
    nodes: BTreeMap<NodeId, Node>,
    target: &Version,
) -> Result<UpgradePlan, UpgradePlanError> {
    if nodes.is_empty() {
        return Err(UpgradePlanError::NoNodes);
    }
    if !input.run.status.nodes.is_empty() {
        return Err(UpgradePlanError::UnexpectedPendingProgress);
    }
    if !nodes.contains_key(&input.leader_id) {
        return Err(UpgradePlanError::LeaderMissing {
            node_id: input.leader_id,
        });
    }
    validate_live_nodes(&nodes, &input.live_nodes)?;
    let selected = selected_nodes(&input.run, &nodes)?;
    for node in &selected {
        reject_foreign_maintenance(node, &input.run)?;
    }
    let mut pending = selected
        .into_iter()
        .filter_map(|node| {
            if input.run.spec.operation == UpgradeOperation::Restart {
                Some(Ok(node))
            } else {
                match parse_node_version(node) {
                    Ok(version) if version < *target => Some(Ok(node)),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                }
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if pending.is_empty() {
        return Err(UpgradePlanError::TargetAlreadySatisfied {
            target: target.to_string(),
        });
    }
    validate_quorum(&input.run, &nodes, &pending)?;
    pending.sort_by(|left, right| {
        maintenance_order(left, &input.leader_id)
            .cmp(&maintenance_order(right, &input.leader_id))
            .then_with(|| left.meta.id.cmp(&right.meta.id))
    });
    let mut run = input.run;
    run.status.nodes = pending
        .iter()
        .map(|node| NodeUpgradeStatus {
            node_id: node.meta.id.clone(),
            phase: UpgradePhase::Pending,
            previous_instance_id: None,
            observed_version: Some(node.status.version.clone()),
            attempts: 0,
            retry_at: None,
        })
        .collect();
    let batch = start_next_batch(&mut run)?;
    run.status.phase = transition(run.status.phase, UpgradePhase::Draining)?;
    let mut updates = BTreeMap::new();
    for node_id in batch {
        set_maintenance(
            &mut updates,
            &nodes,
            &run,
            &node_id,
            MaintenanceAction::Reserve,
            input.now,
        )?;
    }
    set_ready_condition(
        &mut run,
        ConditionState::False,
        "Draining",
        "selected nodes are leaving workload scheduling",
        input.now,
    );
    Ok(plan(
        run,
        updates.into_values().collect(),
        UpgradePlanAction::Requeue(Duration::ZERO),
    ))
}

fn plan_draining(
    input: UpgradeInput,
    nodes: BTreeMap<NodeId, Node>,
    settings: UpgradeSettings,
) -> Result<UpgradePlan, UpgradePlanError> {
    let mut run = input.run;
    let draining = status_indices(&run, UpgradePhase::Draining);
    if draining.is_empty() {
        return Err(UpgradePlanError::EmptyBatch {
            phase: UpgradePhase::Draining,
        });
    }
    let draining_ids = status_ids(&run, &draining)?;
    let offline = draining_ids
        .iter()
        .filter(|node_id| !input.live_nodes.contains(*node_id))
        .cloned()
        .collect::<Vec<_>>();
    if !offline.is_empty() {
        return Err(UpgradePlanError::OfflineNodes { node_ids: offline });
    }
    let mut updates = BTreeMap::new();
    for node_id in &draining_ids {
        set_maintenance(
            &mut updates,
            &nodes,
            &run,
            node_id,
            MaintenanceAction::Reserve,
            input.now,
        )?;
    }
    let artifacts_pending = draining_ids.iter().any(|node_id| {
        nodes.get(node_id).is_none_or(|node| {
            !node.status.conditions.iter().any(|condition| {
                condition.condition_type.0 == ARTIFACT_REPLICATION_READY_CONDITION
                    && condition.state == kernel_api::ConditionState::True
            })
        })
    });
    if artifacts_pending {
        set_ready_condition(
            &mut run,
            ConditionState::False,
            "ArtifactReplicationPending",
            "selected nodes are waiting for retained artifacts to acquire peer copies",
            input.now,
        );
        return Ok(plan(
            run,
            updates.into_values().collect(),
            UpgradePlanAction::Requeue(settings.observation_interval),
        ));
    }
    let assignments_remain = input.assignments.iter().any(|assignment| {
        assignment.meta.deletion_timestamp.is_none()
            && draining_ids.contains(&assignment.spec.node_id)
    });
    if run.spec.mode == UpgradeMode::Rolling && assignments_remain {
        return Ok(plan(
            run,
            updates.into_values().collect(),
            UpgradePlanAction::Requeue(settings.observation_interval),
        ));
    }
    for index in &draining {
        let status = run
            .status
            .nodes
            .get_mut(*index)
            .ok_or(UpgradePlanError::CorruptStatusIndex)?;
        let node = nodes
            .get(&status.node_id)
            .ok_or_else(|| UpgradePlanError::NodeMissing {
                node_id: status.node_id.clone(),
            })?;
        status.previous_instance_id = Some(node.status.instance_id.clone());
        status.observed_version = Some(node.status.version.clone());
        status.retry_at = None;
    }
    transition_statuses(&mut run, &draining, UpgradePhase::Applying)?;
    run.status.phase = transition(run.status.phase, UpgradePhase::Applying)?;
    let message = if run.spec.operation == UpgradeOperation::Restart {
        "node restart dispatch is ready"
    } else {
        "node upgrade dispatch is ready"
    };
    set_ready_condition(
        &mut run,
        ConditionState::False,
        "Applying",
        message,
        input.now,
    );
    Ok(plan(
        run,
        updates.into_values().collect(),
        UpgradePlanAction::Requeue(Duration::ZERO),
    ))
}

fn plan_dispatch(
    input: UpgradeInput,
    nodes: BTreeMap<NodeId, Node>,
) -> Result<UpgradePlan, UpgradePlanError> {
    let applying = status_indices(&input.run, UpgradePhase::Applying);
    if applying.is_empty() {
        return Err(UpgradePlanError::EmptyBatch {
            phase: UpgradePhase::Applying,
        });
    }
    let retry_at = applying
        .iter()
        .filter_map(|index| input.run.status.nodes.get(*index)?.retry_at)
        .max_by_key(|timestamp| timestamp.0);
    if let Some(retry_at) = retry_at
        && retry_at.0 > input.now.0
    {
        return Ok(plan(
            input.run,
            Vec::new(),
            UpgradePlanAction::Requeue(duration_between(input.now, retry_at)),
        ));
    }
    let targets = applying
        .iter()
        .map(|index| {
            let status = input
                .run
                .status
                .nodes
                .get(*index)
                .ok_or(UpgradePlanError::CorruptStatusIndex)?;
            if !nodes.contains_key(&status.node_id) {
                return Err(UpgradePlanError::NodeMissing {
                    node_id: status.node_id.clone(),
                });
            }
            Ok(NodeUpgradeTarget {
                node_id: status.node_id.clone(),
                previous_instance_id: status.previous_instance_id.clone().ok_or_else(|| {
                    UpgradePlanError::PreviousInstanceMissing {
                        node_id: status.node_id.clone(),
                    }
                })?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let request = NodeUpgradeRequest {
        run_id: input.run.meta.id.clone(),
        operation: input.run.spec.operation,
        target_version: input.run.spec.target_version.clone(),
        targets,
    };
    Ok(plan(
        input.run,
        Vec::new(),
        UpgradePlanAction::Dispatch(request),
    ))
}

fn plan_restart(
    input: UpgradeInput,
    nodes: BTreeMap<NodeId, Node>,
    settings: UpgradeSettings,
) -> Result<UpgradePlan, UpgradePlanError> {
    let mut run = input.run;
    let restarting = status_indices(&run, UpgradePhase::Restarting);
    if restarting.is_empty() {
        return Err(UpgradePlanError::EmptyBatch {
            phase: UpgradePhase::Restarting,
        });
    }
    for index in &restarting {
        let status = run
            .status
            .nodes
            .get(*index)
            .ok_or(UpgradePlanError::CorruptStatusIndex)?;
        let node = nodes
            .get(&status.node_id)
            .ok_or_else(|| UpgradePlanError::NodeMissing {
                node_id: status.node_id.clone(),
            })?;
        if status.previous_instance_id.as_ref() == Some(&node.status.instance_id) {
            return Ok(plan(
                run,
                Vec::new(),
                UpgradePlanAction::Requeue(settings.observation_interval),
            ));
        }
    }
    for index in &restarting {
        let status = run
            .status
            .nodes
            .get_mut(*index)
            .ok_or(UpgradePlanError::CorruptStatusIndex)?;
        let node = nodes
            .get(&status.node_id)
            .ok_or_else(|| UpgradePlanError::NodeMissing {
                node_id: status.node_id.clone(),
            })?;
        status.observed_version = Some(node.status.version.clone());
    }
    transition_statuses(&mut run, &restarting, UpgradePhase::Verifying)?;
    run.status.phase = transition(run.status.phase, UpgradePhase::Verifying)?;
    let message = if run.spec.operation == UpgradeOperation::Restart {
        "new daemon identities are reporting; verifying node health"
    } else {
        "new daemon identities are reporting; verifying target versions"
    };
    set_ready_condition(
        &mut run,
        ConditionState::False,
        "Verifying",
        message,
        input.now,
    );
    Ok(plan(
        run,
        Vec::new(),
        UpgradePlanAction::Requeue(Duration::ZERO),
    ))
}

fn plan_verification(
    input: UpgradeInput,
    nodes: BTreeMap<NodeId, Node>,
    target: &Version,
    settings: UpgradeSettings,
) -> Result<UpgradePlan, UpgradePlanError> {
    let operation = input.run.spec.operation;
    let mut run = input.run;
    let verifying = status_indices(&run, UpgradePhase::Verifying);
    if verifying.is_empty() {
        return Err(UpgradePlanError::EmptyBatch {
            phase: UpgradePhase::Verifying,
        });
    }
    for index in &verifying {
        let status = run
            .status
            .nodes
            .get(*index)
            .ok_or(UpgradePlanError::CorruptStatusIndex)?;
        let node = nodes
            .get(&status.node_id)
            .ok_or_else(|| UpgradePlanError::NodeMissing {
                node_id: status.node_id.clone(),
            })?;
        let version_pending =
            operation == UpgradeOperation::Upgrade && parse_node_version(node)? < *target;
        if !input.live_nodes.contains(&status.node_id) || version_pending {
            return Ok(plan(
                run,
                Vec::new(),
                UpgradePlanAction::Requeue(settings.observation_interval),
            ));
        }
    }
    let mut updates = BTreeMap::new();
    for index in &verifying {
        let node_id = {
            let status = run
                .status
                .nodes
                .get_mut(*index)
                .ok_or(UpgradePlanError::CorruptStatusIndex)?;
            let node = nodes
                .get(&status.node_id)
                .ok_or_else(|| UpgradePlanError::NodeMissing {
                    node_id: status.node_id.clone(),
                })?;
            status.observed_version = Some(node.status.version.clone());
            status.phase = transition(status.phase, UpgradePhase::Completed)?;
            status.node_id.clone()
        };
        set_maintenance(
            &mut updates,
            &nodes,
            &run,
            &node_id,
            MaintenanceAction::Release,
            input.now,
        )?;
    }
    if run.spec.mode == UpgradeMode::Rolling
        && run
            .status
            .nodes
            .iter()
            .any(|status| status.phase == UpgradePhase::Pending)
    {
        let batch = start_next_batch(&mut run)?;
        for node_id in batch {
            set_maintenance(
                &mut updates,
                &nodes,
                &run,
                &node_id,
                MaintenanceAction::Reserve,
                input.now,
            )?;
        }
        run.status.phase = transition(run.status.phase, UpgradePhase::Draining)?;
        set_ready_condition(
            &mut run,
            ConditionState::False,
            "Draining",
            "next rolling node is leaving workload scheduling",
            input.now,
        );
    } else {
        run.status.phase = transition(run.status.phase, UpgradePhase::Completed)?;
        let (reason, message) = if operation == UpgradeOperation::Restart {
            (
                "RestartCompleted",
                "every selected node restarted and returned healthy",
            )
        } else {
            (
                "UpgradeCompleted",
                "every selected node reports the requested version",
            )
        };
        set_ready_condition(&mut run, ConditionState::True, reason, message, input.now);
    }
    Ok(plan(
        run,
        updates.into_values().collect(),
        UpgradePlanAction::Requeue(Duration::ZERO),
    ))
}

fn plan_terminal(
    input: UpgradeInput,
    nodes: BTreeMap<NodeId, Node>,
) -> Result<UpgradePlan, UpgradePlanError> {
    let mut updates = BTreeMap::new();
    for status in &input.run.status.nodes {
        set_maintenance(
            &mut updates,
            &nodes,
            &input.run,
            &status.node_id,
            MaintenanceAction::Release,
            input.now,
        )?;
    }
    Ok(plan(
        input.run,
        updates.into_values().collect(),
        UpgradePlanAction::Done,
    ))
}

fn fail_run(
    mut run: kernel_api::UpgradeRun,
    nodes: BTreeMap<NodeId, Node>,
    now: Timestamp,
    reason: &str,
    message: String,
) -> Result<UpgradePlan, UpgradePlanError> {
    let applying = status_indices(&run, UpgradePhase::Applying);
    transition_statuses(&mut run, &applying, UpgradePhase::Failed)?;
    let pending = status_indices(&run, UpgradePhase::Pending);
    transition_statuses(&mut run, &pending, UpgradePhase::Canceled)?;
    run.status.phase = transition(run.status.phase, UpgradePhase::Failed)?;
    set_ready_condition(&mut run, ConditionState::False, reason, &message, now);
    let mut updates = BTreeMap::new();
    for status in &run.status.nodes {
        set_maintenance(
            &mut updates,
            &nodes,
            &run,
            &status.node_id,
            MaintenanceAction::Release,
            now,
        )?;
    }
    Ok(plan(
        run,
        updates.into_values().collect(),
        UpgradePlanAction::Done,
    ))
}

fn selected_nodes<'a>(
    run: &kernel_api::UpgradeRun,
    nodes: &'a BTreeMap<NodeId, Node>,
) -> Result<Vec<&'a Node>, UpgradePlanError> {
    if run.spec.node_ids.is_empty() {
        return Ok(nodes.values().collect());
    }
    let unique = run.spec.node_ids.iter().collect::<BTreeSet<_>>();
    if unique.len() != run.spec.node_ids.len() {
        return Err(UpgradePlanError::DuplicateSelection);
    }
    run.spec
        .node_ids
        .iter()
        .map(|node_id| {
            nodes
                .get(node_id)
                .ok_or_else(|| UpgradePlanError::NodeMissing {
                    node_id: node_id.clone(),
                })
        })
        .collect()
}

fn validate_quorum(
    run: &kernel_api::UpgradeRun,
    nodes: &BTreeMap<NodeId, Node>,
    pending: &[&Node],
) -> Result<(), UpgradePlanError> {
    if run.spec.mode != UpgradeMode::Rolling
        || !pending.iter().any(|node| node.spec.role.is_control_plane())
    {
        return Ok(());
    }
    let voters = nodes
        .values()
        .filter(|node| node.spec.role.is_control_plane())
        .count();
    if voters == 2 {
        Err(UpgradePlanError::UnsafeTwoVoterRollingUpgrade)
    } else {
        Ok(())
    }
}

fn maintenance_order(node: &Node, leader_id: &NodeId) -> u8 {
    if &node.meta.id == leader_id {
        2
    } else if node.spec.role.is_control_plane() {
        1
    } else {
        0
    }
}

fn start_next_batch(run: &mut kernel_api::UpgradeRun) -> Result<Vec<NodeId>, UpgradePlanError> {
    let pending = status_indices(run, UpgradePhase::Pending);
    if pending.is_empty() {
        return Err(UpgradePlanError::EmptyPendingBatch);
    }
    let selected = match run.spec.mode {
        UpgradeMode::Rolling => pending.into_iter().take(1).collect::<Vec<_>>(),
        UpgradeMode::AllNodes => pending,
    };
    let ids = status_ids(run, &selected)?.into_iter().collect::<Vec<_>>();
    transition_statuses(run, &selected, UpgradePhase::Draining)?;
    Ok(ids)
}

fn transition_statuses(
    run: &mut kernel_api::UpgradeRun,
    indices: &[usize],
    target: UpgradePhase,
) -> Result<(), UpgradePlanError> {
    for index in indices {
        let status = run
            .status
            .nodes
            .get_mut(*index)
            .ok_or(UpgradePlanError::CorruptStatusIndex)?;
        status.phase = transition(status.phase, target)?;
        if target != UpgradePhase::Applying {
            status.retry_at = None;
        }
    }
    Ok(())
}

fn transition(from: UpgradePhase, to: UpgradePhase) -> Result<UpgradePhase, UpgradePlanError> {
    if from.can_transition_to(to) {
        Ok(to)
    } else {
        Err(UpgradePlanError::InvalidTransition { from, to })
    }
}

fn status_indices(run: &kernel_api::UpgradeRun, phase: UpgradePhase) -> Vec<usize> {
    run.status
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(index, status)| (status.phase == phase).then_some(index))
        .collect()
}

fn status_ids(
    run: &kernel_api::UpgradeRun,
    indices: &[usize],
) -> Result<BTreeSet<NodeId>, UpgradePlanError> {
    indices
        .iter()
        .map(|index| {
            run.status
                .nodes
                .get(*index)
                .map(|status| status.node_id.clone())
                .ok_or(UpgradePlanError::CorruptStatusIndex)
        })
        .collect()
}

fn index_nodes(nodes: &[Node]) -> Result<BTreeMap<NodeId, Node>, UpgradePlanError> {
    let mut indexed = BTreeMap::new();
    for node in nodes
        .iter()
        .filter(|node| node.meta.deletion_timestamp.is_none())
    {
        if indexed.insert(node.meta.id.clone(), node.clone()).is_some() {
            return Err(UpgradePlanError::DuplicateNode {
                node_id: node.meta.id.clone(),
            });
        }
    }
    Ok(indexed)
}

fn validate_live_nodes(
    nodes: &BTreeMap<NodeId, Node>,
    live_nodes: &BTreeSet<NodeId>,
) -> Result<(), UpgradePlanError> {
    let offline = nodes
        .keys()
        .filter(|node_id| !live_nodes.contains(*node_id))
        .cloned()
        .collect::<Vec<_>>();
    if offline.is_empty() {
        Ok(())
    } else {
        Err(UpgradePlanError::OfflineNodes { node_ids: offline })
    }
}

fn parse_target(value: &str) -> Result<Version, UpgradePlanError> {
    Version::parse(value.trim()).map_err(|error| UpgradePlanError::InvalidTargetVersion {
        value: value.to_string(),
        message: error.to_string(),
    })
}

fn parse_node_version(node: &Node) -> Result<Version, UpgradePlanError> {
    Version::parse(node.status.version.trim()).map_err(|error| {
        UpgradePlanError::InvalidNodeVersion {
            node_id: node.meta.id.clone(),
            value: node.status.version.clone(),
            message: error.to_string(),
        }
    })
}

fn add_duration(now: Timestamp, duration: Duration) -> Timestamp {
    let millis = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
    Timestamp(now.0.saturating_add(millis))
}

fn duration_between(now: Timestamp, future: Timestamp) -> Duration {
    Duration::from_millis(u64::try_from(future.0.saturating_sub(now.0)).unwrap_or(u64::MAX))
}

fn plan(
    run: kernel_api::UpgradeRun,
    node_updates: Vec<Node>,
    action: UpgradePlanAction,
) -> UpgradePlan {
    UpgradePlan {
        run,
        node_updates,
        action,
    }
}
