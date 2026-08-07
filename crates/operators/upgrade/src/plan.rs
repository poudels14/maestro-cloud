use std::time::Duration;

use kernel_api::{ConditionState, UpgradeOperation, UpgradePhase};

use crate::conditions::set_ready_condition;
use crate::plan_phases::{
    fail_run, initialize, plan_dispatch, plan_draining, plan_restart, plan_terminal,
    plan_verification,
};
use crate::plan_support::{
    add_duration, index_nodes, parse_target, plan, status_indices, transition, transition_statuses,
};
use crate::{
    UpgradeDispatchOutcome, UpgradeInput, UpgradePlan, UpgradePlanAction, UpgradePlanError,
    UpgradeSettings,
};

/// Computes the next durable upgrade state without performing side effects.
pub fn plan_upgrade(
    input: UpgradeInput,
    settings: UpgradeSettings,
) -> Result<UpgradePlan, UpgradePlanError> {
    let nodes = index_nodes(&input.nodes)?;
    match input.run.status.phase {
        UpgradePhase::Pending => {
            let target = parse_target(&input.run.spec.target_version)?;
            initialize(input, nodes, &target, settings.observation_interval)
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
