use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{Service, Timestamp};
use kernel_controller::{Backoff, RuntimeConfig};
use kernel_store::{CasOutcome, Clock, ExpectedVersion, MonotonicTime, PutRequest, Store};
use tokio::sync::Notify;

use super::scheduler::World;
use crate::{SchedulerReconciler, SchedulerSettings, TimestampClock};

#[tokio::test]
async fn scheduler_runtime_watches_liveness_resyncs_grace_and_finalizes_assignments()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(1).await?;
    let wall_clock = Arc::new(ManualTimestampClock::new(1_000));
    let monotonic_clock = Arc::new(ManualMonotonicClock::default());
    let reconciler = Arc::new(SchedulerReconciler::new(
        kernel_api::ClusterId::new("cluster-1")?,
        SchedulerSettings {
            replacement_grace: Duration::from_secs(30),
            deployment_drain_grace: Duration::from_secs(30),
        },
        wall_clock.clone(),
    )?);
    let runtime = Arc::new(reconciler.runtime(
        Arc::new(world.fenced.clone()),
        monotonic_clock.clone(),
        RuntimeConfig::new(
            Duration::from_secs(30),
            Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
        )?,
    ));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.run(shutdown_rx).await })
    };

    let original = wait_for_one_assignment(&world, None).await?;
    world.remove_liveness(&original.spec.node_id).await?;
    wait_for_stable_assignment(&world, &original).await?;

    wall_clock.set(40_000);
    monotonic_clock.advance(Duration::from_secs(30));
    let replacement = wait_for_one_assignment(&world, Some(&original.meta.id)).await?;
    assert_ne!(replacement.spec.node_id, original.spec.node_id);
    assert_eq!(replacement.spec.placement_epoch, 2);

    mark_service_deleting(&world, Timestamp(41_000)).await?;
    wait_for_service_and_assignments_removed(&world).await?;

    shutdown_tx.send_replace(true);
    task.await??;
    Ok(())
}

async fn mark_service_deleting(
    world: &World,
    deleted_at: Timestamp,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = world.keys.resource(
        &kernel_api::ResourceKind::new("Service")?,
        &kernel_api::ResourceName::new("api")?,
    );
    let stored = world.store.get(&key).await?.ok_or("service missing")?;
    let mut service: Service = serde_json::from_slice(&stored.value)?;
    service.meta.deletion_timestamp = Some(deleted_at);
    let outcome = world
        .store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&service)?,
            expected: ExpectedVersion::Exact(stored.version),
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("service deletion marker conflicted".into())
    }
}

async fn wait_for_one_assignment(
    world: &World,
    previous: Option<&kernel_api::AssignmentId>,
) -> Result<kernel_api::Assignment, Box<dyn std::error::Error>> {
    for _ in 0..1_000 {
        let assignments = world.assignments().await?;
        if let [assignment] = assignments.as_slice()
            && previous.is_none_or(|previous| &assignment.meta.id != previous)
        {
            return Ok(assignment.clone());
        }
        tokio::task::yield_now().await;
    }
    Err("scheduler did not converge to one expected assignment".into())
}

async fn wait_for_stable_assignment(
    world: &World,
    expected: &kernel_api::Assignment,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    if world.assignments().await? == [expected.clone()] {
        Ok(())
    } else {
        Err("assignment moved before the node replacement grace elapsed".into())
    }
}

async fn wait_for_service_and_assignments_removed(
    world: &World,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = world.keys.resource(
        &kernel_api::ResourceKind::new("Service")?,
        &kernel_api::ResourceName::new("api")?,
    );
    for _ in 0..1_000 {
        if world.store.get(&key).await?.is_none() && world.assignments().await?.is_empty() {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err("scheduler finalizer did not remove service assignments".into())
}

#[derive(Default)]
struct ManualMonotonicClock {
    millis: AtomicU64,
    changed: Notify,
}

impl ManualMonotonicClock {
    fn advance(&self, duration: Duration) {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.millis.fetch_add(millis, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

#[async_trait]
impl Clock for ManualMonotonicClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(self.millis.load(Ordering::SeqCst)))
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        loop {
            let changed = self.changed.notified();
            if self.now() >= deadline {
                return;
            }
            changed.await;
        }
    }
}

struct ManualTimestampClock(AtomicI64);

impl ManualTimestampClock {
    fn new(millis: i64) -> Self {
        Self(AtomicI64::new(millis))
    }

    fn set(&self, millis: i64) {
        self.0.store(millis, Ordering::SeqCst);
    }
}

impl TimestampClock for ManualTimestampClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}
