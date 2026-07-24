use std::collections::VecDeque;
use std::future::pending;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, WorkloadId};
use test_util::{Mutex, MutexGuard};
use tokio::sync::broadcast;

use crate::{
    Capabilities, CgroupPath, EventCursor, EventRequest, ExecOutput, ExecRequest, ExecSession,
    LogCursor, LogFrame, LogRequest, LogSource, LogStream, ObservedWorkload, RuntimeCapability,
    RuntimeError, RuntimeEvent, RuntimeEventKind, RuntimeEventStream, ShutdownRequest,
    WorkloadHandle, WorkloadMetadata, WorkloadRuntime, WorkloadSpec, WorkloadState,
    WorkloadStatsReading, WorkloadStatsSnapshot, WorkloadStatus,
};

use crate::fake_state::{
    FakeDirective, FakeEventRecord, FakeState, FakeWorkload, checked_record, checked_record_mut,
    validate_handle,
};
use crate::fake_stream::{
    FakeEventStream, FakeExecSession, FakeLogStream, cursor_sequence, parse_cursor,
};

/// One programmable fake-runtime operation used for failure and hang injection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FakeRuntimeOperation {
    /// Workload creation.
    Create,
    /// Primary-process start.
    Start,
    /// Graceful stop.
    Stop,
    /// Forced stop.
    Kill,
    /// Backend-object removal.
    Remove,
    /// Point-in-time status read.
    Status,
    /// Ownership-label listing.
    List,
    /// Event-stream creation.
    Events,
    /// Log-stream creation.
    Logs,
    /// Interactive command creation.
    Exec,
    /// Workload resource sampling.
    Stats,
}

/// Diagnostic record of one operation accepted by the fake seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeRuntimeCall {
    /// Operation attempted by the caller.
    pub operation: FakeRuntimeOperation,
    /// Workload identity when the operation targets one object.
    pub workload_id: Option<WorkloadId>,
}

/// Deterministic in-memory workload runtime for reconcilers and conformance tests.
///
/// Clones share backend state, which deliberately models an agent restarting while the runtime
/// and its detached workloads remain alive.
#[derive(Clone)]
pub struct FakeRuntime {
    capabilities: Capabilities,
    state: Arc<Mutex<FakeState>>,
    stats_snapshot: Arc<Mutex<Option<WorkloadStatsSnapshot>>>,
    event_tx: broadcast::Sender<FakeEventRecord>,
}

impl FakeRuntime {
    /// Constructs a fake supporting exec and interactive terminal sessions.
    pub fn new() -> Self {
        Self::with_capabilities(Capabilities::new([
            RuntimeCapability::Exec,
            RuntimeCapability::InteractiveExec,
            RuntimeCapability::KillExec,
        ]))
    }

    /// Constructs a fake with an exact advertised capability set.
    pub fn with_capabilities(capabilities: Capabilities) -> Self {
        let (event_tx, _receiver) = broadcast::channel(256);
        Self {
            capabilities,
            state: Arc::new(Mutex::new(FakeState::default())),
            stats_snapshot: Arc::new(Mutex::new(None)),
            event_tx,
        }
    }

    /// Makes resource collection return one reusable runtime-native snapshot.
    pub fn set_stats_snapshot(&self, snapshot: WorkloadStatsSnapshot) {
        *self.stats_snapshot.lock() = Some(snapshot);
    }

    /// Makes the next selected operation return one matchable error, then restores normal behavior.
    pub fn fail_next(
        &self,
        operation: FakeRuntimeOperation,
        error: RuntimeError,
    ) -> Result<(), RuntimeError> {
        self.lock()?
            .directives
            .entry(operation)
            .or_default()
            .push_back(FakeDirective::Fail(error));
        Ok(())
    }

    /// Makes the next selected operation remain pending until its caller cancels the future.
    pub fn hang_next(&self, operation: FakeRuntimeOperation) -> Result<(), RuntimeError> {
        self.lock()?
            .directives
            .entry(operation)
            .or_default()
            .push_back(FakeDirective::Hang);
        Ok(())
    }

    /// Returns diagnostic operation history in invocation order.
    pub fn calls(&self) -> Result<Vec<FakeRuntimeCall>, RuntimeError> {
        Ok(self.lock()?.calls.clone())
    }

    /// Appends deterministic runtime-native log bytes for a managed workload.
    pub fn append_log(
        &self,
        workload_id: &WorkloadId,
        source: LogSource,
        payload: impl Into<Vec<u8>>,
    ) -> Result<LogCursor, RuntimeError> {
        let mut state = self.lock()?;
        let cursor = LogCursor::new(state.next_sequence().to_string());
        let record =
            state
                .workloads
                .get_mut(workload_id)
                .ok_or_else(|| RuntimeError::NotFound {
                    workload_id: workload_id.clone(),
                })?;
        record.logs.push(LogFrame {
            cursor: cursor.clone(),
            source,
            payload: payload.into(),
        });
        Ok(cursor)
    }

    /// Drops retained lifecycle history to model an event gap across a backend restart.
    ///
    /// Workload objects remain listable, so a correct agent converges by re-listing instead of
    /// assuming event delivery was complete.
    pub fn clear_event_history(&self) -> Result<(), RuntimeError> {
        self.lock()?.events.clear();
        Ok(())
    }

    async fn begin(
        &self,
        operation: FakeRuntimeOperation,
        workload_id: Option<WorkloadId>,
    ) -> Result<(), RuntimeError> {
        let directive = {
            let mut state = self.lock()?;
            state.calls.push(FakeRuntimeCall {
                operation,
                workload_id,
            });
            state
                .directives
                .get_mut(&operation)
                .and_then(VecDeque::pop_front)
        };
        match directive {
            Some(FakeDirective::Fail(error)) => Err(error),
            Some(FakeDirective::Hang) => pending().await,
            None => Ok(()),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, FakeState>, RuntimeError> {
        Ok(self.state.lock())
    }

    fn emit(
        &self,
        state: &mut FakeState,
        metadata: &WorkloadMetadata,
        kind: RuntimeEventKind,
        exit_code: Option<i32>,
    ) {
        let cursor = EventCursor::new(state.next_sequence().to_string());
        let record = FakeEventRecord {
            cluster_id: metadata.cluster_id.clone(),
            node_id: metadata.node_id.clone(),
            event: RuntimeEvent {
                cursor,
                workload_id: metadata.workload_id.clone(),
                kind,
                exit_code,
            },
        };
        state.events.push(record.clone());
        let _receiver_count = self.event_tx.send(record);
    }
}

impl Default for FakeRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WorkloadRuntime for FakeRuntime {
    fn capabilities(&self) -> Capabilities {
        self.capabilities.clone()
    }

    async fn create(&self, spec: &WorkloadSpec) -> Result<WorkloadHandle, RuntimeError> {
        let workload_id = spec.configuration().metadata.workload_id.clone();
        self.begin(FakeRuntimeOperation::Create, Some(workload_id.clone()))
            .await?;
        if matches!(spec, WorkloadSpec::Vm(_))
            && !self
                .capabilities
                .supports(RuntimeCapability::VirtualMachine)
        {
            return Err(RuntimeError::Unsupported {
                capability: RuntimeCapability::VirtualMachine,
            });
        }
        let fingerprint = serde_json::to_vec(spec).map_err(|error| RuntimeError::InvalidSpec {
            message: format!("failed to fingerprint fake workload spec: {error}"),
        })?;
        let mut state = self.lock()?;
        if let Some(existing) = state.workloads.get(&workload_id) {
            if existing.fingerprint == fingerprint {
                return Ok(existing.handle.clone());
            }
            return Err(RuntimeError::Conflict {
                workload_id,
                message: "backend object already exists with a different specification".to_owned(),
            });
        }
        let handle = WorkloadHandle::new(
            workload_id.clone(),
            format!("fake/{}", workload_id.as_str()),
        )?;
        let record = FakeWorkload {
            fingerprint,
            handle: handle.clone(),
            metadata: spec.configuration().metadata.clone(),
            status: WorkloadStatus {
                state: WorkloadState::Created,
                exit_code: None,
                detail: None,
            },
            logs: Vec::new(),
        };
        let metadata = record.metadata.clone();
        state.workloads.insert(workload_id, record);
        self.emit(&mut state, &metadata, RuntimeEventKind::Created, None);
        Ok(handle)
    }

    async fn start(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        let workload_id = handle.workload_id().clone();
        self.begin(FakeRuntimeOperation::Start, Some(workload_id.clone()))
            .await?;
        let mut state = self.lock()?;
        let metadata = {
            let record = checked_record_mut(&mut state, handle)?;
            if record.status.state == WorkloadState::Running {
                return Ok(());
            }
            if record.status.state == WorkloadState::Failed {
                return Err(RuntimeError::Conflict {
                    workload_id,
                    message: "failed workload must be removed before recreation".to_owned(),
                });
            }
            record.status = WorkloadStatus {
                state: WorkloadState::Running,
                exit_code: None,
                detail: None,
            };
            record.metadata.clone()
        };
        self.emit(&mut state, &metadata, RuntimeEventKind::Started, None);
        Ok(())
    }

    async fn stop(
        &self,
        handle: &WorkloadHandle,
        _request: ShutdownRequest,
    ) -> Result<(), RuntimeError> {
        let workload_id = handle.workload_id().clone();
        self.begin(FakeRuntimeOperation::Stop, Some(workload_id.clone()))
            .await?;
        let mut state = self.lock()?;
        stop_with_exit(self, &mut state, handle, 0)
    }

    async fn kill(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        let workload_id = handle.workload_id().clone();
        self.begin(FakeRuntimeOperation::Kill, Some(workload_id))
            .await?;
        let mut state = self.lock()?;
        stop_with_exit(self, &mut state, handle, 137)
    }

    async fn remove(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        let workload_id = handle.workload_id().clone();
        self.begin(FakeRuntimeOperation::Remove, Some(workload_id.clone()))
            .await?;
        let mut state = self.lock()?;
        let Some(record) = state.workloads.get(&workload_id) else {
            return Ok(());
        };
        validate_handle(record, handle)?;
        if record.status.state == WorkloadState::Running
            || record.status.state == WorkloadState::Paused
        {
            return Err(RuntimeError::Conflict {
                workload_id,
                message: "running workload cannot be removed".to_owned(),
            });
        }
        let metadata = record.metadata.clone();
        state.workloads.remove(handle.workload_id());
        self.emit(&mut state, &metadata, RuntimeEventKind::Removed, None);
        Ok(())
    }

    async fn status(&self, handle: &WorkloadHandle) -> Result<WorkloadStatus, RuntimeError> {
        let workload_id = handle.workload_id().clone();
        self.begin(FakeRuntimeOperation::Status, Some(workload_id))
            .await?;
        let state = self.lock()?;
        let record = checked_record(&state, handle)?;
        Ok(record.status.clone())
    }

    async fn list(
        &self,
        cluster_id: &ClusterId,
        node_id: &NodeId,
    ) -> Result<Vec<ObservedWorkload>, RuntimeError> {
        self.begin(FakeRuntimeOperation::List, None).await?;
        Ok(self
            .lock()?
            .workloads
            .values()
            .filter(|record| {
                &record.metadata.cluster_id == cluster_id && &record.metadata.node_id == node_id
            })
            .map(|record| ObservedWorkload {
                handle: record.handle.clone(),
                metadata: record.metadata.clone(),
                status: record.status.clone(),
            })
            .collect())
    }

    async fn events(
        &self,
        request: EventRequest,
    ) -> Result<Box<dyn RuntimeEventStream>, RuntimeError> {
        self.begin(FakeRuntimeOperation::Events, None).await?;
        let after = parse_cursor(request.after.as_ref().map(EventCursor::as_str))?;
        let receiver = self.event_tx.subscribe();
        let state = self.lock()?;
        let events = state
            .events
            .iter()
            .filter(|record| {
                cursor_sequence(record.event.cursor.as_str()).is_ok_and(|sequence| sequence > after)
                    && record.cluster_id == request.cluster_id
                    && record.node_id == request.node_id
            })
            .map(|record| record.event.clone())
            .collect();
        Ok(Box::new(FakeEventStream {
            events,
            receiver,
            cluster_id: request.cluster_id,
            node_id: request.node_id,
            after,
        }))
    }

    async fn logs(
        &self,
        handle: &WorkloadHandle,
        request: LogRequest,
    ) -> Result<Box<dyn LogStream>, RuntimeError> {
        let workload_id = handle.workload_id().clone();
        self.begin(FakeRuntimeOperation::Logs, Some(workload_id))
            .await?;
        let after = parse_cursor(request.after.as_ref().map(LogCursor::as_str))?;
        let state = self.lock()?;
        let record = checked_record(&state, handle)?;
        let frames = record
            .logs
            .iter()
            .filter(|frame| {
                cursor_sequence(frame.cursor.as_str()).is_ok_and(|sequence| sequence > after)
            })
            .cloned()
            .collect();
        Ok(Box::new(FakeLogStream { frames }))
    }

    async fn exec(
        &self,
        handle: &WorkloadHandle,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, RuntimeError> {
        let workload_id = handle.workload_id().clone();
        self.begin(FakeRuntimeOperation::Exec, Some(workload_id))
            .await?;
        if !self.capabilities.supports(RuntimeCapability::Exec) {
            return Err(RuntimeError::Unsupported {
                capability: RuntimeCapability::Exec,
            });
        }
        let state = self.lock()?;
        checked_record(&state, handle)?;
        drop(state);
        Ok(Box::new(FakeExecSession {
            outputs: VecDeque::from([
                ExecOutput::Stdout(request.command.executable.into_bytes()),
                ExecOutput::Exited { code: Some(0) },
            ]),
        }))
    }

    async fn stats(&self, handle: &WorkloadHandle) -> Result<WorkloadStatsReading, RuntimeError> {
        let workload_id = handle.workload_id().clone();
        self.begin(FakeRuntimeOperation::Stats, Some(workload_id))
            .await?;
        let state = self.lock()?;
        checked_record(&state, handle)?;
        drop(state);
        if let Some(snapshot) = *self.stats_snapshot.lock() {
            return Ok(WorkloadStatsReading::Snapshot(snapshot));
        }
        CgroupPath::new(PathBuf::from(format!(
            "/sys/fs/cgroup/maestro/{}",
            handle.workload_id()
        )))
        .map(WorkloadStatsReading::CgroupV2)
        .map_err(|error| RuntimeError::Rejected {
            message: error.to_string(),
        })
    }
}

fn stop_with_exit(
    runtime: &FakeRuntime,
    state: &mut FakeState,
    handle: &WorkloadHandle,
    exit_code: i32,
) -> Result<(), RuntimeError> {
    let metadata = {
        let record = checked_record_mut(state, handle)?;
        if record.status.state == WorkloadState::Stopped {
            return Ok(());
        }
        record.status = WorkloadStatus {
            state: WorkloadState::Stopped,
            exit_code: Some(exit_code),
            detail: None,
        };
        record.metadata.clone()
    };
    runtime.emit(state, &metadata, RuntimeEventKind::Exited, Some(exit_code));
    Ok(())
}
