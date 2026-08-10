use std::collections::BTreeMap;
use std::time::Duration;

use kernel_api::{
    ConditionState, ConditionType, Node, NodeId, NodeUpgradeStatus, Timestamp, UpgradeMode,
    UpgradeOperation, UpgradePhase,
};
use semver::Version;

use crate::conditions::{
    MaintenanceAction, has_foreign_maintenance, set_maintenance, set_ready_condition,
};
use crate::plan_support::{
    add_duration, duration_between, maintenance_order, parse_node_version, plan, selected_nodes,
    start_next_batch, status_ids, status_indices, transition, transition_statuses,
    validate_live_nodes, validate_quorum,
};
use crate::{
    NodeUpgradeRequest, NodeUpgradeTarget, UpgradeInput, UpgradePlan, UpgradePlanAction,
    UpgradePlanError, UpgradeSettings,
};

pub(crate) fn initialize(
    input: UpgradeInput,
    nodes: BTreeMap<NodeId, Node>,
    target: &Version,
    observation_interval: Duration,
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
    let blocked = selected
        .iter()
        .filter(|node| has_foreign_maintenance(node, &input.run))
        .map(|node| node.meta.id.to_string())
        .collect::<Vec<_>>();
    if !blocked.is_empty() {
        let mut run = input.run;
        set_ready_condition(
            &mut run,
            ConditionState::False,
            "MaintenanceBlocked",
            &format!(
                "waiting for existing maintenance to release nodes: {}",
                blocked.join(", ")
            ),
            input.now,
        );
        return Ok(plan(
            run,
            Vec::new(),
            UpgradePlanAction::Requeue(observation_interval),
        ));
    }
    let mut pending = selected
        .iter()
        .copied()
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
    if input.run.spec.mode == UpgradeMode::AllNodes {
        let pending_voters = pending
            .iter()
            .filter(|node| node.spec.role.is_control_plane())
            .count();
        let configured_voters = nodes
            .values()
            .filter(|node| node.spec.role.is_control_plane())
            .count();
        if pending_voters >= 2 {
            let selected_voters = selected
                .iter()
                .filter(|node| node.spec.role.is_control_plane())
                .count();
            if configured_voters != 3 || selected_voters != configured_voters {
                return Err(UpgradePlanError::UnsafePartialControlPlaneBatch);
            }
            let pending_ids = pending
                .iter()
                .map(|node| node.meta.id.clone())
                .collect::<std::collections::BTreeSet<_>>();
            pending.extend(selected.iter().copied().filter(|node| {
                node.spec.role.is_control_plane() && !pending_ids.contains(&node.meta.id)
            }));
        }
    }
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

pub(crate) fn plan_draining(
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
                condition.condition_type == ConditionType::ArtifactReplicationReady
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
        set_ready_condition(
            &mut run,
            ConditionState::False,
            "WorkloadDrainPending",
            "selected nodes are waiting for workload assignments to drain",
            input.now,
        );
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

pub(crate) fn plan_dispatch(
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
        // A routine all-node reboot keeps the existing etcd membership intact. Store
        // reconstruction is an emergency recovery operation and must never be armed
        // automatically merely because every voter is in the same upgrade batch.
        store_recovery: None,
    };
    Ok(plan(
        input.run,
        Vec::new(),
        UpgradePlanAction::Dispatch(request),
    ))
}

pub(crate) fn plan_restart(
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

pub(crate) fn plan_verification(
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
    let mut lagging = Vec::new();
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
        if !input.live_nodes.contains(&status.node_id) {
            return Ok(plan(
                run,
                Vec::new(),
                UpgradePlanAction::Requeue(settings.observation_interval),
            ));
        }
        let version = parse_node_version(node)?;
        if operation == UpgradeOperation::Upgrade && version < *target {
            lagging.push((
                *index,
                status.node_id.clone(),
                node.status.instance_id.clone(),
                node.status.version.clone(),
            ));
        }
    }
    if !lagging.is_empty() {
        let summary = lagging
            .iter()
            .map(|(_, node_id, _, version)| format!("{node_id} ({version})"))
            .collect::<Vec<_>>()
            .join(", ");
        if lagging.iter().any(|(index, _, _, _)| {
            run.status
                .nodes
                .get(*index)
                .is_some_and(|status| status.attempts >= settings.max_attempts)
        }) {
            return fail_run(
                run,
                nodes,
                input.now,
                "UpgradeVerificationRetriesExhausted",
                format!(
                    "nodes returned below target {target} after {} attempts: {summary}",
                    settings.max_attempts
                ),
            );
        }
        let retry_at = add_duration(input.now, settings.retry_delay);
        for (index, _, instance_id, version) in &lagging {
            let status = run
                .status
                .nodes
                .get_mut(*index)
                .ok_or(UpgradePlanError::CorruptStatusIndex)?;
            status.phase = transition(status.phase, UpgradePhase::Applying)?;
            status.previous_instance_id = Some(instance_id.clone());
            status.observed_version = Some(version.clone());
            status.retry_at = Some(retry_at);
        }
        run.status.phase = transition(run.status.phase, UpgradePhase::Applying)?;
        set_ready_condition(
            &mut run,
            ConditionState::False,
            "UpgradeVerificationRetry",
            &format!(
                "nodes returned below target {target}; restaging after verification: {summary}"
            ),
            input.now,
        );
        return Ok(plan(
            run,
            Vec::new(),
            UpgradePlanAction::Requeue(settings.retry_delay),
        ));
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

pub(crate) fn plan_terminal(
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

pub(crate) fn fail_run(
    mut run: kernel_api::UpgradeRun,
    nodes: BTreeMap<NodeId, Node>,
    now: Timestamp,
    reason: &str,
    message: String,
) -> Result<UpgradePlan, UpgradePlanError> {
    for phase in [
        UpgradePhase::Draining,
        UpgradePhase::Applying,
        UpgradePhase::Restarting,
        UpgradePhase::Verifying,
    ] {
        let active = status_indices(&run, phase);
        transition_statuses(&mut run, &active, UpgradePhase::Failed)?;
    }
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
