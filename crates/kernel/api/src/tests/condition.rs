use crate::{Condition, ConditionReason, ConditionState, ConditionType, Generation, Timestamp};

#[test]
fn condition_wire_shape_is_generic_and_typed() {
    let condition = Condition {
        condition_type: ConditionType::Ready,
        state: ConditionState::Unknown,
        reason: ConditionReason("Reconciling".to_string()),
        message: "waiting for a healthy replica".to_string(),
        observed_generation: Generation(3),
        last_transition_time: Timestamp(1_721_500_000_000),
    };

    let value = serde_json::to_value(condition).expect("serialize condition");

    assert_eq!(
        value,
        serde_json::json!({
            "type": "READY",
            "status": "unknown",
            "reason": "Reconciling",
            "message": "waiting for a healthy replica",
            "observedGeneration": 3,
            "lastTransitionTime": 1_721_500_000_000_i64
        })
    );
}
