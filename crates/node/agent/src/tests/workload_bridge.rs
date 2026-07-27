use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_store::{Clock, MonotonicTime};
use tokio::sync::{Notify, watch};

use crate::{
    WorkloadBridge, WorkloadBridgeAgent, WorkloadBridgeBackend, WorkloadBridgeBackendError,
    WorkloadBridgeError,
};

#[tokio::test]
async fn bridge_agent_applies_the_exact_desired_state() -> Result<(), Box<dyn std::error::Error>> {
    let applications = Arc::new(Mutex::new(Vec::new()));
    let backend = RecordingBridgeBackend {
        applications: applications.clone(),
    };
    let desired = WorkloadBridge::new(Ipv4Addr::new(10, 42, 1, 1), 24, 1_420)?;
    let agent = WorkloadBridgeAgent::new(
        desired.clone(),
        backend,
        Arc::new(TestClock),
        Duration::from_secs(30),
    )?;

    agent.reconcile_once().await?;

    assert_eq!(agent.desired(), &desired);
    assert_eq!(
        applications
            .lock()
            .map_err(|_| "bridge application lock poisoned")?
            .as_slice(),
        std::slice::from_ref(&desired)
    );
    Ok(())
}

#[tokio::test]
async fn bridge_agent_recovers_after_backend_failure() -> Result<(), Box<dyn std::error::Error>> {
    let applications = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicU64::new(0));
    let backend = FailFirstBridgeBackend {
        applications: applications.clone(),
        attempts: attempts.clone(),
    };
    let clock = Arc::new(ManualClock::default());
    let desired = WorkloadBridge::new(Ipv4Addr::new(10, 42, 1, 1), 24, 1_420)?;
    let agent = Arc::new(WorkloadBridgeAgent::new(
        desired.clone(),
        backend,
        clock.clone(),
        Duration::from_secs(30),
    )?);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let running_agent = agent.clone();
    let task = tokio::spawn(async move { running_agent.run(shutdown_rx).await });

    wait_for_sleeps(clock.as_ref(), 1).await?;
    assert!(!task.is_finished());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(
        applications
            .lock()
            .map_err(|_| "bridge application lock poisoned")?
            .is_empty()
    );

    clock.advance(Duration::from_secs(30));
    wait_for_applications(&applications, 1).await?;
    assert_eq!(
        applications
            .lock()
            .map_err(|_| "bridge application lock poisoned")?
            .as_slice(),
        std::slice::from_ref(&desired)
    );

    shutdown.send(true)?;
    task.await??;
    Ok(())
}

#[test]
fn bridge_settings_reject_unsafe_or_hot_looping_state() -> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        WorkloadBridge::new(Ipv4Addr::LOCALHOST, 24, 1_420),
        Err(WorkloadBridgeError::InvalidGateway { .. })
    ));
    assert!(matches!(
        WorkloadBridge::new(Ipv4Addr::new(10, 42, 1, 1), 31, 1_420),
        Err(WorkloadBridgeError::InvalidPrefixLength { .. })
    ));
    assert!(matches!(
        WorkloadBridge::new(Ipv4Addr::new(10, 42, 1, 1), 24, 0),
        Err(WorkloadBridgeError::ZeroMtu)
    ));
    let desired = WorkloadBridge::new(Ipv4Addr::new(10, 42, 1, 1), 24, 1_420)?;
    assert!(matches!(
        WorkloadBridgeAgent::new(
            desired,
            RecordingBridgeBackend {
                applications: Arc::new(Mutex::new(Vec::new())),
            },
            Arc::new(TestClock),
            Duration::ZERO,
        ),
        Err(WorkloadBridgeError::ZeroResyncInterval)
    ));
    Ok(())
}

struct RecordingBridgeBackend {
    applications: Arc<Mutex<Vec<WorkloadBridge>>>,
}

#[async_trait]
impl WorkloadBridgeBackend for RecordingBridgeBackend {
    async fn apply(&self, desired: &WorkloadBridge) -> Result<(), WorkloadBridgeBackendError> {
        self.applications
            .lock()
            .map_err(|_| WorkloadBridgeBackendError::new("bridge application lock poisoned"))?
            .push(desired.clone());
        Ok(())
    }
}

struct FailFirstBridgeBackend {
    applications: Arc<Mutex<Vec<WorkloadBridge>>>,
    attempts: Arc<AtomicU64>,
}

#[async_trait]
impl WorkloadBridgeBackend for FailFirstBridgeBackend {
    async fn apply(&self, desired: &WorkloadBridge) -> Result<(), WorkloadBridgeBackendError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(WorkloadBridgeBackendError::new(
                "injected bridge backend outage",
            ));
        }
        self.applications
            .lock()
            .map_err(|_| WorkloadBridgeBackendError::new("bridge application lock poisoned"))?
            .push(desired.clone());
        Ok(())
    }
}

struct TestClock;

#[async_trait]
impl Clock for TestClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}

#[derive(Default)]
struct ManualClock {
    milliseconds: AtomicU64,
    sleeps: AtomicU64,
    advanced: Notify,
}

impl ManualClock {
    fn advance(&self, duration: Duration) {
        let milliseconds = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.milliseconds.fetch_add(milliseconds, Ordering::SeqCst);
        self.advanced.notify_waiters();
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(
            self.milliseconds.load(Ordering::SeqCst),
        ))
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        self.sleeps.fetch_add(1, Ordering::SeqCst);
        loop {
            let advanced = self.advanced.notified();
            if self.now() >= deadline {
                return;
            }
            advanced.await;
        }
    }
}

async fn wait_for_sleeps(
    clock: &ManualClock,
    expected: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for _attempt in 0..128 {
        if clock.sleeps.load(Ordering::SeqCst) >= expected {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err(format!("workload bridge agent did not begin sleep {expected}").into())
}

async fn wait_for_applications(
    applications: &Mutex<Vec<WorkloadBridge>>,
    expected: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for _attempt in 0..128 {
        if applications
            .lock()
            .map_err(|_| "bridge application lock poisoned")?
            .len()
            >= expected
        {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err(format!("workload bridge agent did not apply desired state {expected} time(s)").into())
}
