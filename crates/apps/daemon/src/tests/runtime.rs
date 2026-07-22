use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kernel_api::{NodeId, NodeRole};

use crate::{
    Daemon, DaemonError, DaemonPlan, DaemonRole, RoleError, RoleFactory, RoleRuntime, RoleSpec,
};

use super::cluster_with_nodes;

#[tokio::test]
async fn startup_failure_rolls_back_already_started_roles() -> Result<(), Box<dyn std::error::Error>>
{
    let state = Arc::new(Mutex::new(Recorded::default()));
    let factory = RecordingFactory {
        state: state.clone(),
        fail_start: Some(DaemonRole::Controller),
        fail_shutdown: None,
        fail_runtime: None,
    };
    let plan = plan()?;
    let error = match Daemon::new(plan, factory).start().await {
        Ok(_) => return Err("controller startup unexpectedly succeeded".into()),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        DaemonError::Startup {
            role: DaemonRole::Controller,
            ..
        }
    ));
    let recorded = state.lock().map_err(|_| "recording lock poisoned")?;
    assert_eq!(
        recorded.events,
        vec![
            "start:agent".to_owned(),
            "start:controller".to_owned(),
            "stop:agent".to_owned(),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_is_reverse_ordered_and_continues_after_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(Mutex::new(Recorded::default()));
    let factory = RecordingFactory {
        state: state.clone(),
        fail_start: None,
        fail_shutdown: Some(DaemonRole::Controller),
        fail_runtime: None,
    };
    let running = Daemon::new(plan()?, factory).start().await?;
    assert_eq!(
        running.active_roles(),
        vec![DaemonRole::Agent, DaemonRole::Controller]
    );
    let error = match running.shutdown().await {
        Ok(()) => return Err("injected controller shutdown unexpectedly succeeded".into()),
        Err(error) => error,
    };
    assert!(matches!(error, DaemonError::Shutdown { .. }));
    let recorded = state.lock().map_err(|_| "recording lock poisoned")?;
    assert_eq!(
        recorded.events,
        vec![
            "start:agent".to_owned(),
            "start:controller".to_owned(),
            "stop:controller".to_owned(),
            "stop:agent".to_owned(),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn background_role_failure_is_observed() -> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(Mutex::new(Recorded::default()));
    let factory = RecordingFactory {
        state,
        fail_start: None,
        fail_shutdown: None,
        fail_runtime: Some(DaemonRole::Agent),
    };
    let mut running = Daemon::new(plan()?, factory).start().await?;
    let failure = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        running.wait_for_failure(),
    )
    .await?;
    assert!(matches!(
        failure,
        DaemonError::Runtime {
            role: DaemonRole::Agent,
            ..
        }
    ));
    running.shutdown().await?;
    Ok(())
}

fn plan() -> Result<DaemonPlan, Box<dyn std::error::Error>> {
    Ok(DaemonPlan::new(
        cluster_with_nodes(&[("master", NodeRole::Master)])?,
        NodeId::new("master")?,
        PathBuf::from("/var/lib/maestro"),
    )?)
}

#[derive(Default)]
struct Recorded {
    events: Vec<String>,
}

struct RecordingFactory {
    state: Arc<Mutex<Recorded>>,
    fail_start: Option<DaemonRole>,
    fail_shutdown: Option<DaemonRole>,
    fail_runtime: Option<DaemonRole>,
}

#[async_trait]
impl RoleFactory for RecordingFactory {
    async fn start(
        &self,
        _plan: &DaemonPlan,
        spec: &RoleSpec,
    ) -> Result<Box<dyn RoleRuntime>, RoleError> {
        record(&self.state, format!("start:{}", role_name(spec.role)))?;
        if self.fail_start == Some(spec.role) {
            return Err(RoleError::new("injected startup failure"));
        }
        Ok(Box::new(RecordingRuntime {
            role: spec.role,
            state: self.state.clone(),
            fail_shutdown: self.fail_shutdown == Some(spec.role),
            fail_runtime: self.fail_runtime == Some(spec.role),
        }))
    }
}

struct RecordingRuntime {
    role: DaemonRole,
    state: Arc<Mutex<Recorded>>,
    fail_shutdown: bool,
    fail_runtime: bool,
}

#[async_trait]
impl RoleRuntime for RecordingRuntime {
    fn is_finished(&self) -> bool {
        self.fail_runtime
    }

    async fn take_failure(&mut self) -> RoleError {
        RoleError::new("injected runtime failure")
    }

    async fn shutdown(self: Box<Self>) -> Result<(), RoleError> {
        record(&self.state, format!("stop:{}", role_name(self.role)))?;
        if self.fail_shutdown {
            Err(RoleError::new("injected shutdown failure"))
        } else {
            Ok(())
        }
    }
}

fn record(state: &Mutex<Recorded>, event: String) -> Result<(), RoleError> {
    state
        .lock()
        .map_err(|_| RoleError::new("recording lock poisoned"))?
        .events
        .push(event);
    Ok(())
}

fn role_name(role: DaemonRole) -> &'static str {
    match role {
        DaemonRole::Agent => "agent",
        DaemonRole::Controller => "controller",
    }
}
