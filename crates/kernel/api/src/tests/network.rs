use crate::TrafficGenerationPhase;

#[test]
fn traffic_generation_transition_matrix_is_exhaustive() {
    let phases = [
        TrafficGenerationPhase::Staged,
        TrafficGenerationPhase::Active,
        TrafficGenerationPhase::Retired,
    ];

    for current in phases {
        for target in phases {
            let expected = current == target
                || matches!(
                    (current, target),
                    (
                        TrafficGenerationPhase::Staged,
                        TrafficGenerationPhase::Active
                    ) | (
                        TrafficGenerationPhase::Active,
                        TrafficGenerationPhase::Retired
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
