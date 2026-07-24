use crate::{PreviewPhase, UpgradePhase, UpgradeRunSpec};

#[test]
fn closing_preview_can_reopen_but_expired_preview_is_terminal() {
    assert!(PreviewPhase::Pending.can_transition_to(PreviewPhase::Closing));
    assert!(PreviewPhase::Active.can_transition_to(PreviewPhase::Closing));
    assert!(PreviewPhase::Closing.can_transition_to(PreviewPhase::Pending));
    assert!(PreviewPhase::Closing.can_transition_to(PreviewPhase::Active));
    assert!(PreviewPhase::Closing.can_transition_to(PreviewPhase::Expired));
    assert!(!PreviewPhase::Expired.can_transition_to(PreviewPhase::Active));
}

#[test]
fn upgrade_modes_share_one_retryable_state_machine() {
    assert!(UpgradePhase::Pending.can_transition_to(UpgradePhase::Draining));
    assert!(UpgradePhase::Applying.can_transition_to(UpgradePhase::Restarting));
    assert!(UpgradePhase::Verifying.can_transition_to(UpgradePhase::Draining));
    assert!(UpgradePhase::Failed.can_transition_to(UpgradePhase::Pending));
    assert!(!UpgradePhase::Completed.can_transition_to(UpgradePhase::Pending));
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
