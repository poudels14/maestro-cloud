use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_store::{Clock, MonotonicTime};

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
