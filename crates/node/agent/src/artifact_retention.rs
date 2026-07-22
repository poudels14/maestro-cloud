use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{ArtifactTemplate, Deployment, DeploymentId, DeploymentPhase, ServiceId};
use runtime::ArtifactDigest;

use crate::ArtifactReplicationError;

pub(crate) fn retained_digests(
    deployments: &[Deployment],
) -> Result<BTreeSet<ArtifactDigest>, ArtifactReplicationError> {
    let mut latest = BTreeMap::<ServiceId, (kernel_api::Timestamp, DeploymentId)>::new();
    for deployment in deployments.iter().filter(|deployment| {
        matches!(
            deployment.spec.service.artifact,
            ArtifactTemplate::Build { .. }
        ) && deployment.status.image_digest.is_some()
    }) {
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
        .filter(|deployment| {
            matches!(
                deployment.spec.service.artifact,
                ArtifactTemplate::Build { .. }
            ) && (matches!(
                deployment.status.phase,
                DeploymentPhase::Building
                    | DeploymentPhase::PendingReady
                    | DeploymentPhase::Ready
                    | DeploymentPhase::Draining
            ) || latest
                .get(&deployment.spec.service_id)
                .is_some_and(|(_, id)| id == &deployment.meta.id))
        })
        .filter_map(|deployment| deployment.status.image_digest.as_deref())
        .map(|digest| ArtifactDigest::new(digest.to_owned()).map_err(Into::into))
        .collect()
}

pub(crate) fn preserved_digests(
    deployments: &[Deployment],
) -> Result<BTreeSet<ArtifactDigest>, ArtifactReplicationError> {
    let mut preserved = retained_digests(deployments)?;
    for digest in deployments
        .iter()
        .filter(|deployment| {
            matches!(
                deployment.status.phase,
                DeploymentPhase::Building
                    | DeploymentPhase::PendingReady
                    | DeploymentPhase::Ready
                    | DeploymentPhase::Draining
            )
        })
        .filter_map(|deployment| deployment.status.image_digest.as_deref())
    {
        preserved.insert(ArtifactDigest::new(digest.to_owned())?);
    }
    Ok(preserved)
}
