use std::collections::BTreeMap;
use std::path::PathBuf;

use kernel_api::{
    ArtifactTemplate, Assignment, ClusterId, Deployment, VolumeAccess, VolumeSource, WorkloadId,
};
use node_fabric::WORKLOAD_NODE_DIRECTORY;
use runtime::{
    ArtifactReference, ContainerWorkload, MountAccess, MountSource, WorkloadConfiguration,
    WorkloadMetadata, WorkloadMount, WorkloadSpec, WorkloadUser,
};

pub(crate) fn workload_spec(
    cluster_id: &ClusterId,
    assignment: &Assignment,
    deployment: &Deployment,
    additional_mounts: Vec<WorkloadMount>,
) -> Result<WorkloadSpec, WorkloadPlanError> {
    if assignment.spec.deployment_id != deployment.meta.id
        || assignment.spec.service_id != deployment.spec.service_id
    {
        return Err(WorkloadPlanError::DeploymentIdentityMismatch);
    }
    let workload_id = workload_id(assignment)?;
    let image = image_reference(deployment)?;
    let mut mounts = deployment
        .spec
        .service
        .volumes
        .iter()
        .map(|mount| workload_mount(assignment, mount))
        .collect::<Result<Vec<_>, _>>()?;
    mounts.extend(additional_mounts);
    let labels = workload_labels(assignment);
    Ok(WorkloadSpec::Container(ContainerWorkload {
        configuration: WorkloadConfiguration {
            metadata: WorkloadMetadata {
                cluster_id: cluster_id.clone(),
                node_id: assignment.spec.node_id.clone(),
                assignment_id: assignment.meta.id.clone(),
                workload_id,
                labels,
            },
            hostname: workload_hostname(assignment),
            environment: deployment.spec.service.environment.clone(),
            mounts,
            workload_address: Some(assignment.spec.workload_address),
            user: deployment.spec.service.user.map(|user| WorkloadUser {
                user_id: user.user_id,
                group_id: user.group_id,
            }),
        },
        image,
        command: deployment.spec.service.command.clone(),
    }))
}

pub(crate) fn workload_labels(assignment: &Assignment) -> BTreeMap<String, String> {
    let mut labels = assignment
        .meta
        .labels
        .iter()
        .map(|(key, value)| (key.0.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    labels.insert(
        "maestro.service-id".to_owned(),
        assignment.spec.service_id.to_string(),
    );
    labels.insert(
        "maestro.deployment-id".to_owned(),
        assignment.spec.deployment_id.to_string(),
    );
    labels.insert(
        "maestro.replica-index".to_owned(),
        assignment.spec.replica_index.to_string(),
    );
    labels
}

pub(crate) fn node_api_user(
    deployment: &Deployment,
) -> Result<Option<WorkloadUser>, WorkloadPlanError> {
    if deployment.spec.service.node_api.is_enabled() {
        validate_node_api_mount_targets(deployment)?;
    }
    match (
        deployment.spec.service.node_api.is_enabled(),
        deployment.spec.service.user,
    ) {
        (false, _) => Ok(None),
        (true, Some(user)) => Ok(Some(WorkloadUser {
            user_id: user.user_id,
            group_id: user.group_id,
        })),
        (true, None) => Err(WorkloadPlanError::NodeApiRequiresExplicitUser),
    }
}

fn validate_node_api_mount_targets(deployment: &Deployment) -> Result<(), WorkloadPlanError> {
    let reserved = std::path::Path::new(WORKLOAD_NODE_DIRECTORY);
    let targets = deployment
        .spec
        .service
        .volumes
        .iter()
        .map(|mount| mount.target.as_str())
        .chain(
            deployment
                .spec
                .service
                .secrets
                .iter()
                .map(|mount| mount.mount_path.as_str()),
        );
    for target in targets {
        if std::path::Path::new(target).starts_with(reserved) {
            return Err(WorkloadPlanError::ReservedNodeApiMount {
                target: target.to_owned(),
            });
        }
    }
    Ok(())
}

pub(crate) fn workload_id(assignment: &Assignment) -> Result<WorkloadId, WorkloadPlanError> {
    WorkloadId::new(assignment.meta.id.as_str()).map_err(WorkloadPlanError::from)
}

fn image_reference(deployment: &Deployment) -> Result<ArtifactReference, WorkloadPlanError> {
    let reference = if let Some(digest) = &deployment.status.image_digest {
        digest.clone()
    } else if let ArtifactTemplate::Image { reference } = &deployment.spec.service.artifact {
        reference.clone()
    } else {
        return Err(WorkloadPlanError::ArtifactUnavailable);
    };
    ArtifactReference::new(reference).map_err(|error| WorkloadPlanError::InvalidArtifact {
        message: error.to_string(),
    })
}

fn workload_mount(
    assignment: &Assignment,
    mount: &kernel_api::VolumeMountSpec,
) -> Result<WorkloadMount, WorkloadPlanError> {
    let source = match &mount.source {
        VolumeSource::HostPath { path, node_id } if node_id == &assignment.spec.node_id => {
            MountSource::HostPath(PathBuf::from(path))
        }
        VolumeSource::HostPath { node_id, .. } => {
            return Err(WorkloadPlanError::HostVolumeNodeMismatch {
                volume_node_id: node_id.to_string(),
                assignment_node_id: assignment.spec.node_id.to_string(),
            });
        }
        VolumeSource::Managed { name } => MountSource::ManagedVolume(name.clone()),
    };
    Ok(WorkloadMount {
        source,
        target: PathBuf::from(&mount.target),
        access: match mount.access {
            VolumeAccess::ReadWrite => MountAccess::ReadWrite,
            VolumeAccess::ReadOnly => MountAccess::ReadOnly,
        },
    })
}

fn workload_hostname(assignment: &Assignment) -> String {
    let mut hostname = format!(
        "{}-{}",
        assignment.spec.service_id, assignment.spec.replica_index
    )
    .replace(['_', '.'], "-");
    hostname.truncate(63);
    hostname
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum WorkloadPlanError {
    #[error("assignment and deployment identities do not match")]
    DeploymentIdentityMismatch,
    #[error("deployment artifact is not available yet")]
    ArtifactUnavailable,
    #[error("deployment artifact is invalid: {message}")]
    InvalidArtifact { message: String },
    #[error(transparent)]
    InvalidWorkloadId(#[from] kernel_api::InvalidIdentifier),
    #[error("node API workloads require an explicit numeric user and group")]
    NodeApiRequiresExplicitUser,
    #[error("workload mount `{target}` overlaps the reserved `/run/maestro` node API directory")]
    ReservedNodeApiMount { target: String },
    #[error(
        "host volume belongs to node `{volume_node_id}` but assignment targets `{assignment_node_id}`"
    )]
    HostVolumeNodeMismatch {
        volume_node_id: String,
        assignment_node_id: String,
    },
}
