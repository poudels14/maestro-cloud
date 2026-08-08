use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{ArtifactTemplate, Deployment, DeploymentId, DeploymentPhase, ServiceId};
use runtime::ArtifactDigest;

use crate::ArtifactReplicationError;

pub(crate) fn retained_digests(
    deployments: &[Deployment],
) -> Result<BTreeSet<ArtifactDigest>, ArtifactReplicationError> {
    let registry_free = deployments
        .iter()
        .map(registry_free_build)
        .collect::<Result<Vec<_>, _>>()?;
    let mut latest = BTreeMap::<ServiceId, (kernel_api::Timestamp, DeploymentId)>::new();
    for (deployment, registry_free) in deployments.iter().zip(&registry_free) {
        if !registry_free || deployment.status.image_digest.is_none() {
            continue;
        }
        let candidate = (deployment.status.created_at, deployment.meta.id.clone());
        latest
            .entry(deployment.spec.service_id.clone())
            .and_modify(|current| {
                if candidate > *current {
                    *current = candidate.clone();
                }
            })
            .or_insert(candidate);
    }
    deployments
        .iter()
        .zip(registry_free)
        .filter(|(deployment, registry_free)| {
            *registry_free
                && (requires_artifact(deployment.status.phase)
                    || latest
                        .get(&deployment.spec.service_id)
                        .is_some_and(|(_, id)| id == &deployment.meta.id))
        })
        .filter_map(|(deployment, _)| deployment.status.image_digest.as_deref())
        .map(|digest| ArtifactDigest::new(digest.to_owned()).map_err(Into::into))
        .collect()
}

fn registry_free_build(deployment: &Deployment) -> Result<bool, ArtifactReplicationError> {
    let ArtifactTemplate::Build { template } = &deployment.spec.service.artifact else {
        return Ok(false);
    };
    if template.registry.is_some() {
        return Ok(false);
    }
    deployment
        .status
        .image_digest
        .as_deref()
        .map(ArtifactDigest::new)
        .transpose()?
        .map_or(Ok(true), |digest| digest.is_internal().map_err(Into::into))
}

pub(crate) fn preserved_digests(
    deployments: &[Deployment],
) -> Result<BTreeSet<ArtifactDigest>, ArtifactReplicationError> {
    let mut preserved = retained_digests(deployments)?;
    for digest in deployments
        .iter()
        .filter(|deployment| requires_artifact(deployment.status.phase))
        .filter_map(|deployment| deployment.status.image_digest.as_deref())
    {
        preserved.insert(ArtifactDigest::new(digest.to_owned())?);
    }
    Ok(preserved)
}

fn requires_artifact(phase: DeploymentPhase) -> bool {
    matches!(
        phase,
        DeploymentPhase::Building
            | DeploymentPhase::Publishing
            | DeploymentPhase::Starting
            | DeploymentPhase::PendingReady
            | DeploymentPhase::Retrying
            | DeploymentPhase::Ready
            | DeploymentPhase::Recovering
            | DeploymentPhase::Stopping
            | DeploymentPhase::Stopped
            | DeploymentPhase::Draining
    )
}
