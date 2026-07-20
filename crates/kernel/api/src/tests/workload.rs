use crate::{BuildPhase, DeploymentPhase};

#[test]
fn deployment_transition_matrix_matches_the_harvested_lifecycle() {
    let phases = [
        DeploymentPhase::Queued,
        DeploymentPhase::Building,
        DeploymentPhase::PendingReady,
        DeploymentPhase::Ready,
        DeploymentPhase::Crashed,
        DeploymentPhase::Terminated,
        DeploymentPhase::Removed,
        DeploymentPhase::Draining,
        DeploymentPhase::Canceled,
    ];

    for current in phases {
        for target in phases {
            let expected = match target {
                DeploymentPhase::PendingReady => current == DeploymentPhase::Building,
                DeploymentPhase::Ready => matches!(
                    current,
                    DeploymentPhase::Building | DeploymentPhase::PendingReady
                ),
                DeploymentPhase::Crashed => !matches!(
                    current,
                    DeploymentPhase::Crashed
                        | DeploymentPhase::Canceled
                        | DeploymentPhase::Terminated
                ),
                DeploymentPhase::Draining => matches!(
                    current,
                    DeploymentPhase::Ready
                        | DeploymentPhase::PendingReady
                        | DeploymentPhase::Building
                ),
                DeploymentPhase::Terminated => current != DeploymentPhase::Terminated,
                DeploymentPhase::Queued
                | DeploymentPhase::Building
                | DeploymentPhase::Removed
                | DeploymentPhase::Canceled => true,
            };
            assert_eq!(current.can_transition_to(target), expected);
        }
    }
}

#[test]
fn build_terminal_phases_do_not_restart_themselves() {
    assert!(BuildPhase::Queued.can_transition_to(BuildPhase::Preparing));
    assert!(BuildPhase::Building.can_transition_to(BuildPhase::Succeeded));
    assert!(!BuildPhase::Succeeded.can_transition_to(BuildPhase::Building));
    assert!(!BuildPhase::Failed.can_transition_to(BuildPhase::Queued));
    assert!(BuildPhase::Canceled.can_transition_to(BuildPhase::Canceled));
}
