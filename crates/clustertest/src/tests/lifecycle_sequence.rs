use super::lifecycle::LifecycleWorld;
use crate::{
    FixtureName, FixtureVersion, LifecycleOperation, ReplicaCount, ReplicaIndex,
    scenarios::lifecycle_operation_sequence_preserves_invariants,
};

#[tokio::test]
async fn mixed_lifecycle_sequence_preserves_fast_fake_invariants() {
    let mut operations = Vec::new();
    for generation in 1..=12 {
        let service = FixtureName::new(format!("sequence-service-{}", generation % 3));
        operations.push(LifecycleOperation::Rollout {
            service: service.clone(),
            version: FixtureVersion::new(format!("v{generation}")),
            replicas: ReplicaCount::new((generation % 3) + 1),
        });
        operations.push(LifecycleOperation::Reconcile);
        if generation % 2 == 0 {
            operations.push(LifecycleOperation::CrashLatest {
                service: service.clone(),
                replica: ReplicaIndex::new(generation % 3),
            });
        }
        if generation % 4 == 0 {
            operations.push(LifecycleOperation::CancelLatest { service });
        }
        operations.push(LifecycleOperation::Advance {
            millis: generation * 10,
        });
    }
    let mut cluster = LifecycleWorld::new();

    lifecycle_operation_sequence_preserves_invariants(&mut cluster, &operations)
        .await
        .expect("mixed lifecycle operation sequence");
}
