use std::path::{Path, PathBuf};

use kernel_api::WorkloadId;
use sha2::{Digest, Sha256};
use supervisor::{
    EnvironmentInheritance, ProcessCommand, ProcessEnvironment, ProcessHandle, ProcessLogFiles,
    ProcessSpec as SupervisedProcessSpec, ProcessUser, SupervisorError,
};

use crate::cgroup;
use crate::process_manifest::workload_directory;
use crate::{ProcessWorkload, RuntimeError, WorkloadHandle, WorkloadSpec};

pub(crate) struct ProcessPaths {
    pub(crate) directory: PathBuf,
    pub(crate) stdout: PathBuf,
    pub(crate) stderr: PathBuf,
}

pub(crate) fn process_paths(root: &Path, workload_id: &WorkloadId) -> ProcessPaths {
    let directory = workload_directory(root, workload_id);
    ProcessPaths {
        stdout: directory.join("stdout.log"),
        stderr: directory.join("stderr.log"),
        directory,
    }
}

pub(crate) fn build_supervised_spec(
    process: &ProcessWorkload,
    paths: &ProcessPaths,
) -> Result<SupervisedProcessSpec, RuntimeError> {
    if !process.configuration.mounts.is_empty() {
        return Err(RuntimeError::InvalidSpec {
            message: "process runtime does not support remapped filesystem mounts".to_owned(),
        });
    }
    let mut environment = process.configuration.environment.clone();
    environment.extend(
        process
            .configuration
            .secret_environment
            .iter()
            .map(|(name, value)| (name.clone(), value.expose().to_owned())),
    );
    let metadata = &process.configuration.metadata;
    environment.insert(
        "MAESTRO_CLUSTER_ID".to_owned(),
        metadata.cluster_id.to_string(),
    );
    environment.insert("MAESTRO_NODE_ID".to_owned(), metadata.node_id.to_string());
    environment.insert(
        "MAESTRO_ASSIGNMENT_ID".to_owned(),
        metadata.assignment_id.to_string(),
    );
    environment.insert(
        "MAESTRO_WORKLOAD_ID".to_owned(),
        metadata.workload_id.to_string(),
    );
    environment.insert(
        "HOSTNAME".to_owned(),
        process.configuration.hostname.clone(),
    );
    if let Some(address) = process.configuration.workload_address {
        environment.insert("MAESTRO_WORKLOAD_ADDRESS".to_owned(), address.to_string());
    }
    let spec = SupervisedProcessSpec {
        command: ProcessCommand {
            executable: PathBuf::from(&process.command.executable),
            arguments: process.command.arguments.clone(),
        },
        environment: ProcessEnvironment {
            inheritance: EnvironmentInheritance::Clear,
            variables: environment,
        },
        working_directory: Some(paths.directory.clone()),
        logs: ProcessLogFiles {
            stdout: paths.stdout.clone(),
            stderr: paths.stderr.clone(),
        },
        user: process.configuration.user.map(|user| ProcessUser {
            user_id: user.user_id,
            group_id: user.group_id,
        }),
    };
    spec.validate().map_err(runtime_supervisor_error)?;
    Ok(spec)
}

pub(crate) fn process_handle(workload_id: WorkloadId) -> Result<WorkloadHandle, RuntimeError> {
    WorkloadHandle::new(
        workload_id.clone(),
        format!("process/{}", workload_id.as_str()),
    )
}

pub(crate) fn validate_process_handle(handle: &WorkloadHandle) -> Result<(), RuntimeError> {
    let expected = format!("process/{}", handle.workload_id().as_str());
    if handle.backend_id() == expected {
        Ok(())
    } else {
        Err(RuntimeError::Conflict {
            workload_id: handle.workload_id().clone(),
            message: "workload handle does not belong to the process runtime".to_owned(),
        })
    }
}

pub(crate) fn spec_fingerprint(spec: &WorkloadSpec) -> Result<String, RuntimeError> {
    let bytes = serde_json::to_vec(spec).map_err(|error| RuntimeError::InvalidSpec {
        message: format!("failed to fingerprint process workload: {error}"),
    })?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

pub(crate) fn read_cgroup_path(process: ProcessHandle) -> Result<PathBuf, RuntimeError> {
    cgroup::read_cgroup_path(process.pid())
}

pub(crate) fn runtime_supervisor_error(error: SupervisorError) -> RuntimeError {
    let message = error.to_string();
    match error {
        SupervisorError::InvalidSpec { .. }
        | SupervisorError::IdentityMismatch { .. }
        | SupervisorError::NotOwned { .. } => RuntimeError::Rejected { message },
        SupervisorError::Io { .. }
        | SupervisorError::Signal { .. }
        | SupervisorError::StateUnavailable
        | SupervisorError::Task { .. } => RuntimeError::Unavailable { message },
    }
}

pub(crate) async fn blocking<Operation, Output>(
    operation: Operation,
) -> Result<Output, RuntimeError>
where
    Operation: FnOnce() -> Result<Output, RuntimeError> + Send + 'static,
    Output: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| RuntimeError::Unavailable {
            message: format!("process runtime filesystem task failed: {error}"),
        })?
}
