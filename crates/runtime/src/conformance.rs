use std::future::Future;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId};

use crate::{
    EventRequest, ExecOutput, ExecRequest, LogMode, LogRequest, LogSource, RuntimeCapability,
    RuntimeError, RuntimeEvent, RuntimeEventKind, RuntimeEventStream, ShutdownRequest,
    WorkloadHandle, WorkloadRuntime, WorkloadSpec, WorkloadState,
};

/// Backend-specific fixtures for the workload runtime conformance battery.
#[derive(Debug, Clone)]
pub struct WorkloadRuntimeFixture {
    /// Valid backend-native workload specification.
    pub spec: WorkloadSpec,
    /// Different specification with the same stable workload identity.
    pub conflicting_spec: WorkloadSpec,
}

/// Expected behavior for one backend-native exec request.
#[derive(Debug, Clone)]
pub struct ExecConformanceFixture {
    /// Command and transport mode to execute inside the running workload.
    pub request: ExecRequest,
    /// Byte sequence that must occur in the collected standard output.
    pub stdout_marker: Vec<u8>,
    /// Byte sequence that must occur in standard error when the fixture emits it.
    pub stderr_marker: Option<Vec<u8>>,
}

/// Backend-specific expectations for a running workload's streaming surface.
#[derive(Debug, Clone)]
pub struct RunningWorkloadFixture {
    /// Cluster ownership label used by adoption and event filtering.
    pub cluster_id: ClusterId,
    /// Node ownership label used by adoption and event filtering.
    pub node_id: NodeId,
    /// Byte sequence that must occur in the workload's runtime-native stdout.
    pub stdout_marker: Vec<u8>,
    /// Byte sequence that must occur in the workload's runtime-native stderr.
    pub stderr_marker: Vec<u8>,
    /// Exec request required when the backend advertises exec support.
    pub exec: Option<ExecConformanceFixture>,
    /// Maximum wall-clock duration for each finite stream assertion.
    pub timeout: Duration,
}

/// Matchable failure from a runtime implementation or a violated contract invariant.
#[derive(Debug, thiserror::Error)]
pub enum WorkloadConformanceError {
    /// Backend returned a typed runtime error.
    #[error("runtime conformance operation failed: {0}")]
    Runtime(#[from] RuntimeError),
    /// Backend completed an operation but violated required observable semantics.
    #[error("runtime conformance invariant failed: {message}")]
    Invariant {
        /// Stable explanation of the violated contract.
        message: String,
    },
}

/// Exercises idempotent create/start/stop/remove, discovery, conflict, status, and cgroup semantics.
///
/// The caller owns isolation and supplies a backend-native fixture. On success no workload remains.
/// A failed battery may leave backend state for the caller's fixture cleanup to remove.
pub async fn exercise_workload_runtime(
    runtime: &dyn WorkloadRuntime,
    fixture: &WorkloadRuntimeFixture,
) -> Result<(), WorkloadConformanceError> {
    let configuration = fixture.spec.configuration();
    if fixture
        .conflicting_spec
        .configuration()
        .metadata
        .workload_id
        != configuration.metadata.workload_id
        || fixture.conflicting_spec == fixture.spec
    {
        return Err(WorkloadConformanceError::Invariant {
            message: "conflicting fixture must reuse the workload identity with different content"
                .to_owned(),
        });
    }

    let handle = runtime.create(&fixture.spec).await?;
    if runtime.create(&fixture.spec).await? != handle {
        return Err(invariant("idempotent create returned a different handle"));
    }
    if !matches!(
        runtime.create(&fixture.conflicting_spec).await,
        Err(RuntimeError::Conflict { .. })
    ) {
        return Err(invariant(
            "conflicting create did not return RuntimeError::Conflict",
        ));
    }
    if runtime.status(&handle).await?.state != WorkloadState::Created {
        return Err(invariant("created workload did not report Created"));
    }
    let listed = runtime
        .list(
            &configuration.metadata.cluster_id,
            &configuration.metadata.node_id,
        )
        .await?;
    if listed.len() != 1 || listed.first().map(|item| &item.handle) != Some(&handle) {
        return Err(invariant(
            "ownership listing did not return exactly the created workload",
        ));
    }

    runtime.start(&handle).await?;
    runtime.start(&handle).await?;
    if runtime.status(&handle).await?.state != WorkloadState::Running {
        return Err(invariant("started workload did not report Running"));
    }
    if !runtime.stats_handle(&handle).await?.as_path().is_absolute() {
        return Err(invariant("runtime returned a relative cgroup path"));
    }

    let shutdown = ShutdownRequest {
        timeout: Duration::from_secs(5),
    };
    runtime.stop(&handle, shutdown).await?;
    runtime.stop(&handle, shutdown).await?;
    if runtime.status(&handle).await?.state != WorkloadState::Stopped {
        return Err(invariant("stopped workload did not report Stopped"));
    }
    runtime.remove(&handle).await?;
    runtime.remove(&handle).await?;
    if !matches!(
        runtime.status(&handle).await,
        Err(RuntimeError::NotFound { .. })
    ) {
        return Err(invariant(
            "removed workload remained visible through point status",
        ));
    }
    if !runtime
        .list(
            &configuration.metadata.cluster_id,
            &configuration.metadata.node_id,
        )
        .await?
        .is_empty()
    {
        return Err(invariant(
            "removed workload remained visible through ownership listing",
        ));
    }
    Ok(())
}

/// Exercises adoption, logs, capability-gated exec, forced exit events, and removal events.
///
/// The supplied workload must be running and must already have emitted both configured log
/// markers. On success the workload has been killed and removed. The event subscription is opened
/// before either mutation, which proves that backend-native streams observe live transitions.
pub async fn exercise_running_workload(
    runtime: &dyn WorkloadRuntime,
    handle: &WorkloadHandle,
    fixture: &RunningWorkloadFixture,
) -> Result<(), WorkloadConformanceError> {
    validate_running_fixture(runtime, fixture)?;
    assert_adoptable(runtime, handle, fixture).await?;
    assert_logs(runtime, handle, fixture).await?;
    assert_exec(runtime, handle, fixture).await?;

    let mut events = tokio::time::timeout(
        fixture.timeout,
        runtime.events(EventRequest {
            cluster_id: fixture.cluster_id.clone(),
            node_id: fixture.node_id.clone(),
            after: None,
        }),
    )
    .await
    .map_err(|_elapsed| {
        invariant("runtime event subscription did not open before its deadline")
    })??;
    mutate_and_observe(
        runtime.kill(handle),
        events.as_mut(),
        handle,
        RuntimeEventKind::Exited,
        fixture.timeout,
    )
    .await?;
    if runtime.status(handle).await?.state != WorkloadState::Stopped {
        return Err(invariant("killed workload did not report Stopped"));
    }
    runtime.kill(handle).await?;

    mutate_and_observe(
        runtime.remove(handle),
        events.as_mut(),
        handle,
        RuntimeEventKind::Removed,
        fixture.timeout,
    )
    .await?;
    runtime.remove(handle).await?;
    if !runtime
        .list(&fixture.cluster_id, &fixture.node_id)
        .await?
        .is_empty()
    {
        return Err(invariant("removed workload remained adoptable"));
    }
    Ok(())
}

/// Proves a newly connected runtime can discover an externally retained running workload.
pub async fn assert_running_workload_adoptable(
    runtime: &dyn WorkloadRuntime,
    handle: &WorkloadHandle,
    fixture: &RunningWorkloadFixture,
) -> Result<(), WorkloadConformanceError> {
    validate_running_fixture(runtime, fixture)?;
    assert_adoptable(runtime, handle, fixture).await
}

fn validate_running_fixture(
    runtime: &dyn WorkloadRuntime,
    fixture: &RunningWorkloadFixture,
) -> Result<(), WorkloadConformanceError> {
    if fixture.timeout.is_zero()
        || fixture.stdout_marker.is_empty()
        || fixture.stderr_marker.is_empty()
    {
        return Err(invariant(
            "stream fixture requires a deadline and non-empty log markers",
        ));
    }
    let exec_supported = runtime.capabilities().supports(RuntimeCapability::Exec);
    if exec_supported != fixture.exec.is_some() {
        return Err(invariant(
            "stream fixture exec expectation disagrees with advertised capabilities",
        ));
    }
    if let Some(exec) = fixture.exec.as_ref()
        && (exec.stdout_marker.is_empty() || exec.stderr_marker.as_ref().is_some_and(Vec::is_empty))
    {
        return Err(invariant("exec fixture markers cannot be empty"));
    }
    Ok(())
}

async fn assert_adoptable(
    runtime: &dyn WorkloadRuntime,
    handle: &WorkloadHandle,
    fixture: &RunningWorkloadFixture,
) -> Result<(), WorkloadConformanceError> {
    let listed = runtime.list(&fixture.cluster_id, &fixture.node_id).await?;
    let point_status = runtime.status(handle).await?;
    if listed.len() != 1
        || listed.first().map(|workload| &workload.handle) != Some(handle)
        || listed.first().map(|workload| workload.status.state) != Some(WorkloadState::Running)
        || point_status.state != WorkloadState::Running
    {
        return Err(invariant(format!(
            "ownership listing did not expose exactly one running workload: expected={handle:?}, point_status={point_status:?}, observed={listed:?}"
        )));
    }
    Ok(())
}

async fn assert_logs(
    runtime: &dyn WorkloadRuntime,
    handle: &WorkloadHandle,
    fixture: &RunningWorkloadFixture,
) -> Result<(), WorkloadConformanceError> {
    let mut stream = tokio::time::timeout(
        fixture.timeout,
        runtime.logs(
            handle,
            LogRequest {
                after: None,
                mode: LogMode::Snapshot,
            },
        ),
    )
    .await
    .map_err(|_elapsed| invariant("runtime log snapshot did not open before its deadline"))??;
    let collected = tokio::time::timeout(fixture.timeout, async {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        while let Some(frame) = stream.next().await? {
            match frame.source {
                LogSource::Stdout => stdout.extend(frame.payload),
                LogSource::Stderr => stderr.extend(frame.payload),
            }
        }
        Ok::<_, RuntimeError>((stdout, stderr))
    })
    .await
    .map_err(|_elapsed| {
        invariant("runtime log snapshot did not terminate before its deadline")
    })??;
    if !contains_bytes(&collected.0, &fixture.stdout_marker)
        || !contains_bytes(&collected.1, &fixture.stderr_marker)
    {
        return Err(invariant(
            "runtime log snapshot omitted an expected stdout or stderr marker",
        ));
    }
    Ok(())
}

async fn assert_exec(
    runtime: &dyn WorkloadRuntime,
    handle: &WorkloadHandle,
    fixture: &RunningWorkloadFixture,
) -> Result<(), WorkloadConformanceError> {
    let Some(expected) = fixture.exec.as_ref() else {
        return Ok(());
    };
    let mut session = tokio::time::timeout(
        fixture.timeout,
        runtime.exec(handle, expected.request.clone()),
    )
    .await
    .map_err(|_elapsed| invariant("runtime exec session did not open before its deadline"))??;
    let collected = tokio::time::timeout(fixture.timeout, async {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        loop {
            match session.next().await? {
                Some(ExecOutput::Stdout(payload)) => stdout.extend(payload),
                Some(ExecOutput::Stderr(payload)) => stderr.extend(payload),
                Some(ExecOutput::Exited { code }) => {
                    return Ok::<_, RuntimeError>((stdout, stderr, code));
                }
                None => {
                    return Err(RuntimeError::Stream {
                        message: "exec stream ended without an exit result".to_owned(),
                    });
                }
            }
        }
    })
    .await
    .map_err(|_elapsed| invariant("runtime exec did not terminate before its deadline"))??;
    if collected.2 != Some(0)
        || !contains_bytes(&collected.0, &expected.stdout_marker)
        || expected
            .stderr_marker
            .as_ref()
            .is_some_and(|marker| !contains_bytes(&collected.1, marker))
    {
        return Err(invariant(
            "runtime exec omitted expected output or a successful exit result",
        ));
    }
    Ok(())
}

async fn mutate_and_observe<Mutation>(
    mutation: Mutation,
    events: &mut dyn RuntimeEventStream,
    handle: &WorkloadHandle,
    kind: RuntimeEventKind,
    timeout: Duration,
) -> Result<RuntimeEvent, WorkloadConformanceError>
where
    Mutation: Future<Output = Result<(), RuntimeError>>,
{
    let observation = async {
        loop {
            match events.next().await? {
                Some(event) if event.workload_id == *handle.workload_id() && event.kind == kind => {
                    return Ok::<_, RuntimeError>(event);
                }
                Some(_event) => {}
                None => {
                    return Err(RuntimeError::Stream {
                        message: "runtime event stream ended before the expected transition"
                            .to_owned(),
                    });
                }
            }
        }
    };
    let mutation = async {
        tokio::task::yield_now().await;
        mutation.await
    };
    let (observed, mutated) = tokio::join!(
        tokio::time::timeout(timeout, observation),
        tokio::time::timeout(timeout, mutation)
    );
    mutated.map_err(|_elapsed| {
        invariant(format!(
            "runtime {kind:?} mutation did not finish before its deadline"
        ))
    })??;
    observed
        .map_err(|_elapsed| invariant("runtime event did not arrive before its deadline"))?
        .map_err(WorkloadConformanceError::from)
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|candidate| candidate == needle)
}

fn invariant(message: impl Into<String>) -> WorkloadConformanceError {
    WorkloadConformanceError::Invariant {
        message: message.into(),
    }
}
