use crate::TrafficGenerationPhase;

#[test]
fn traffic_generations_only_move_forward_through_cutover() {
    assert!(TrafficGenerationPhase::Staged.can_transition_to(TrafficGenerationPhase::Active));
    assert!(TrafficGenerationPhase::Active.can_transition_to(TrafficGenerationPhase::Retired));
    assert!(!TrafficGenerationPhase::Retired.can_transition_to(TrafficGenerationPhase::Active));
    assert!(!TrafficGenerationPhase::Staged.can_transition_to(TrafficGenerationPhase::Retired));
}
