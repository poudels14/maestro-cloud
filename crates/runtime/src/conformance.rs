use std::time::Duration;

use crate::{RuntimeError, ShutdownRequest, WorkloadRuntime, WorkloadSpec, WorkloadState};

/// Backend-specific fixtures for the workload runtime conformance battery.
#[derive(Debug, Clone)]
pub struct WorkloadRuntimeFixture {
    /// Valid backend-native workload specification.
    pub spec: WorkloadSpec,
    /// Different specification with the same stable workload identity.
    pub conflicting_spec: WorkloadSpec,
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

fn invariant(message: impl Into<String>) -> WorkloadConformanceError {
    WorkloadConformanceError::Invariant {
        message: message.into(),
    }
}
