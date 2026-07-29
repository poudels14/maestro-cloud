use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{NodeId, NodeInstanceId};
use kernel_controller::{
    ControllerError, FencedStore, LeaderElector, LeaderIdentity, LeadershipLease,
    LeadershipObservation, LeadershipToken, StoreLeaderElector,
};
use kernel_store::{Clock, InMemoryStore, Keyspace, MonotonicTime, StoreError, TokioClock};
use tokio::sync::{Notify, watch};

use crate::leadership::run_leadership;
use crate::{DaemonRoleSettings, LeaderWorkload, RoleError};

#[tokio::test]
async fn leader_workload_stops_before_recampaign_and_resignation()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let leader_key = Keyspace::new(&kernel_api::ClusterId::new("leader-runtime")?).leader();
    let identity = LeaderIdentity {
        node_id: NodeId::new("master")?,
        instance_id: NodeInstanceId::new("instance-1")?,
    };
    let elector = Arc::new(FailingOnceElector {
        inner: StoreLeaderElector::new(store.clone(), leader_key.clone()),
        fail_next_lease: AtomicBool::new(true),
    });
    let settings = DaemonRoleSettings::new(
        Duration::from_secs(30),
        Duration::from_secs(30),
        Duration::from_secs(30),
        Duration::from_secs(30),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(1),
        1_000,
        Duration::from_secs(5),
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(10),
    )?;
    let initial_lease = elector
        .campaign(identity.clone(), Duration::from_secs(5))
        .await?
        .ok_or("initial leadership campaign lost")?;
    let workload = Arc::new(RecordingWorkload::default());
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let task = tokio::spawn(run_leadership(
        store,
        leader_key,
        elector,
        identity.clone(),
        Some(initial_lease),
        Some(workload.clone()),
        clock.clone(),
        settings,
        shutdown_receiver,
    ));

    wait(&workload.started).await?;
    clock.advance(Duration::from_secs(1));
    wait(&workload.stopped).await?;
    clock.advance(Duration::from_secs(10));
    wait(&workload.started).await?;

    drop(shutdown);
    wait(&workload.stopped).await?;
    tokio::time::timeout(Duration::from_secs(1), task).await???;

    assert_eq!(
        workload
            .terms
            .lock()
            .map_err(|_| "leader term lock poisoned")?
            .as_slice(),
        &[identity.clone(), identity]
    );
    assert_eq!(
        workload
            .stopped_while_fenced
            .lock()
            .map_err(|_| "leader stop lock poisoned")?
            .as_slice(),
        &[true, true]
    );
    Ok(())
}

#[tokio::test]
async fn transient_campaign_failures_are_retried() -> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = kernel_api::ClusterId::new("leader-retry")?;
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let leader_key = Keyspace::new(&cluster_id).leader();
    let elector = Arc::new(UnavailableCampaignElector::default());
    let settings = DaemonRoleSettings::new(
        Duration::from_secs(30),
        Duration::from_secs(30),
        Duration::from_secs(30),
        Duration::from_secs(30),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(1),
        1_000,
        Duration::from_millis(100),
        Duration::from_millis(20),
        Duration::from_millis(10),
        Duration::from_secs(1),
    )?;
    let identity = LeaderIdentity {
        node_id: NodeId::new("master")?,
        instance_id: NodeInstanceId::new("instance-1")?,
    };
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let task = tokio::spawn(run_leadership(
        store,
        leader_key,
        elector.clone(),
        identity,
        None,
        None,
        Arc::new(TokioClock::new()),
        settings,
        shutdown_receiver,
    ));

    tokio::time::timeout(Duration::from_secs(1), async {
        while elector.attempts.load(Ordering::SeqCst) < 2 {
            elector.attempted.notified().await;
        }
    })
    .await?;
    assert!(!task.is_finished());
    shutdown.send(true)?;
    tokio::time::timeout(Duration::from_secs(1), task).await???;
    Ok(())
}

async fn wait(notify: &Notify) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(1), notify.notified()).await
}

#[derive(Default)]
struct ManualClock {
    millis: AtomicU64,
    advanced: Notify,
}

impl ManualClock {
    fn advance(&self, duration: Duration) {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.millis.fetch_add(millis, Ordering::SeqCst);
        self.advanced.notify_waiters();
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(self.millis.load(Ordering::SeqCst)))
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        while self.now() < deadline {
            self.advanced.notified().await;
        }
    }
}

struct FailingOnceElector {
    inner: StoreLeaderElector,
    fail_next_lease: AtomicBool,
}

#[async_trait]
impl LeaderElector for FailingOnceElector {
    async fn campaign(
        &self,
        identity: LeaderIdentity,
        ttl: Duration,
    ) -> Result<Option<Box<dyn LeadershipLease>>, ControllerError> {
        let Some(lease) = self.inner.campaign(identity, ttl).await? else {
            return Ok(None);
        };
        if self.fail_next_lease.swap(false, Ordering::SeqCst) {
            Ok(Some(Box::new(FailingLease {
                inner: lease,
                fail_keepalive: AtomicBool::new(true),
            })))
        } else {
            Ok(Some(lease))
        }
    }

    async fn observe(&self) -> Result<LeadershipObservation, ControllerError> {
        self.inner.observe().await
    }
}

#[derive(Default)]
struct UnavailableCampaignElector {
    attempts: AtomicU64,
    attempted: Notify,
}

#[async_trait]
impl LeaderElector for UnavailableCampaignElector {
    async fn campaign(
        &self,
        _identity: LeaderIdentity,
        _ttl: Duration,
    ) -> Result<Option<Box<dyn LeadershipLease>>, ControllerError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.attempted.notify_waiters();
        Err(StoreError::Unavailable {
            message: "injected campaign timeout".to_owned(),
        }
        .into())
    }

    async fn observe(&self) -> Result<LeadershipObservation, ControllerError> {
        Ok(LeadershipObservation::Vacant)
    }
}

struct FailingLease {
    inner: Box<dyn LeadershipLease>,
    fail_keepalive: AtomicBool,
}

#[async_trait]
impl LeadershipLease for FailingLease {
    fn token(&self) -> &LeadershipToken {
        self.inner.token()
    }

    async fn keep_alive(&self) -> Result<(), ControllerError> {
        if self.fail_keepalive.swap(false, Ordering::SeqCst) {
            Err(ControllerError::Contract {
                message: "injected keepalive failure".to_string(),
            })
        } else {
            self.inner.keep_alive().await
        }
    }

    async fn resign(&self) -> Result<(), ControllerError> {
        self.inner.resign().await
    }
}

#[derive(Default)]
struct RecordingWorkload {
    started: Notify,
    stopped: Notify,
    terms: Mutex<Vec<LeaderIdentity>>,
    stopped_while_fenced: Mutex<Vec<bool>>,
}

#[async_trait]
impl LeaderWorkload for RecordingWorkload {
    async fn run(
        &self,
        store: Arc<FencedStore>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), RoleError> {
        self.terms
            .lock()
            .map_err(|_| RoleError::new("leader term lock poisoned"))?
            .push(store.token().identity().clone());
        self.started.notify_one();
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
        let still_fenced = store.verify_leadership().await.is_ok();
        self.stopped_while_fenced
            .lock()
            .map_err(|_| RoleError::new("leader stop lock poisoned"))?
            .push(still_fenced);
        self.stopped.notify_one();
        Ok(())
    }
}
