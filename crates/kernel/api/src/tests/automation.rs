use crate::{PreviewPhase, UpgradePhase, UpgradeRunSpec};

#[test]
fn preview_transition_matrix_is_exhaustive() {
    let phases = [
        PreviewPhase::Pending,
        PreviewPhase::Active,
        PreviewPhase::Closing,
        PreviewPhase::Expired,
        PreviewPhase::Failed,
        PreviewPhase::Canceled,
    ];

    for current in phases {
        for target in phases {
            let expected = current == target
                || matches!(
                    (current, target),
                    (
                        PreviewPhase::Pending,
                        PreviewPhase::Active
                            | PreviewPhase::Closing
                            | PreviewPhase::Expired
                            | PreviewPhase::Failed
                            | PreviewPhase::Canceled
                    ) | (
                        PreviewPhase::Active,
                        PreviewPhase::Pending
                            | PreviewPhase::Closing
                            | PreviewPhase::Expired
                            | PreviewPhase::Failed
                    ) | (
                        PreviewPhase::Closing,
                        PreviewPhase::Pending
                            | PreviewPhase::Active
                            | PreviewPhase::Expired
                            | PreviewPhase::Failed
                    ) | (
                        PreviewPhase::Failed,
                        PreviewPhase::Pending
                            | PreviewPhase::Active
                            | PreviewPhase::Closing
                            | PreviewPhase::Expired
                    ) | (PreviewPhase::Expired, PreviewPhase::Pending)
                        | (
                            PreviewPhase::Canceled,
                            PreviewPhase::Pending
                                | PreviewPhase::Active
                                | PreviewPhase::Closing
                                | PreviewPhase::Expired
                                | PreviewPhase::Failed
                        )
                );
            assert_eq!(
                current.can_transition_to(target),
                expected,
                "{current:?} -> {target:?}"
            );
        }
    }
}

#[test]
fn upgrade_transition_matrix_is_exhaustive() {
    let phases = [
        UpgradePhase::Pending,
        UpgradePhase::Draining,
        UpgradePhase::Applying,
        UpgradePhase::Restarting,
        UpgradePhase::Verifying,
        UpgradePhase::Completed,
        UpgradePhase::Failed,
        UpgradePhase::Canceled,
    ];

    for current in phases {
        for target in phases {
            let expected = current == target
                || matches!(
                    (current, target),
                    (
                        UpgradePhase::Pending,
                        UpgradePhase::Draining | UpgradePhase::Completed | UpgradePhase::Canceled
                    ) | (
                        UpgradePhase::Draining,
                        UpgradePhase::Applying | UpgradePhase::Failed | UpgradePhase::Canceled
                    ) | (
                        UpgradePhase::Applying,
                        UpgradePhase::Restarting | UpgradePhase::Verifying | UpgradePhase::Failed
                    ) | (
                        UpgradePhase::Restarting,
                        UpgradePhase::Verifying | UpgradePhase::Failed
                    ) | (
                        UpgradePhase::Verifying,
                        UpgradePhase::Draining
                            | UpgradePhase::Applying
                            | UpgradePhase::Completed
                            | UpgradePhase::Failed
                    ) | (
                        UpgradePhase::Failed,
                        UpgradePhase::Pending | UpgradePhase::Canceled
                    )
                );
            assert_eq!(
                current.can_transition_to(target),
                expected,
                "{current:?} -> {target:?}"
            );
        }
    }
}

#[test]
fn upgrade_operation_is_required_by_the_canonical_resource_schema() {
    let error = serde_json::from_value::<UpgradeRunSpec>(serde_json::json!({
        "targetVersion": "2.0.0",
        "mode": "rolling"
    }))
    .expect_err("missing operation must be rejected");

    assert!(error.to_string().contains("operation"));
}
