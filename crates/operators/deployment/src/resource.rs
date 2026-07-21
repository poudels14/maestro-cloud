use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    ArtifactTemplate, Build, BuildId, BuildPhase, BuildSpec, BuildStatus, Deployment,
    DeploymentGoal, DeploymentId, DeploymentPhase, DeploymentSpec, DeploymentStatus, Generation,
    InvalidIdentifier, Object, ObjectMeta, OwnerReference, Ownership, ResourceId, ResourceKind,
    ResourceName, Service, ServiceId, Timestamp,
};
use sha2::{Digest, Sha256};

use crate::DeploymentPlanError;

const SERVICE_KIND: &str = "Service";
const DEPLOYMENT_KIND: &str = "Deployment";

pub(crate) fn new_deployment(
    cluster_id: &kernel_api::ClusterId,
    service: &Service,
    now: Timestamp,
) -> Result<Deployment, DeploymentPlanError> {
    let deployment_id = DeploymentId::new(stable_id(
        "deployment",
        &[
            cluster_id.as_str(),
            service.meta.id.as_str(),
            &service.meta.generation.0.to_string(),
        ],
    ))?;
    let build_id = matches!(service.spec.artifact, ArtifactTemplate::Build { .. })
        .then(|| {
            BuildId::new(stable_id(
                "build",
                &[cluster_id.as_str(), deployment_id.as_str()],
            ))
        })
        .transpose()?;
    Ok(Object {
        meta: child_metadata(deployment_id, SERVICE_KIND, service.meta.id.clone().into())?,
        spec: DeploymentSpec {
            service_id: service.meta.id.clone(),
            service_generation: service.meta.generation,
            service: service.spec.clone(),
            goal: DeploymentGoal::Run,
            build_id,
        },
        status: DeploymentStatus {
            phase: DeploymentPhase::Queued,
            created_at: now,
            ready_at: None,
            draining_at: None,
            image_digest: None,
            conditions: Vec::new(),
        },
    })
}

pub(crate) fn new_build(
    deployment: &Deployment,
    build_id: BuildId,
) -> Result<Build, DeploymentPlanError> {
    let ArtifactTemplate::Build { template } = &deployment.spec.service.artifact else {
        return Err(DeploymentPlanError::UnexpectedBuild {
            deployment_id: deployment.meta.id.clone(),
            build_id,
        });
    };
    Ok(Object {
        meta: child_metadata(build_id, DEPLOYMENT_KIND, deployment.meta.id.clone().into())?,
        spec: BuildSpec {
            service_id: deployment.spec.service_id.clone(),
            deployment_id: deployment.meta.id.clone(),
            template: template.clone(),
        },
        status: BuildStatus {
            phase: BuildPhase::Queued,
            image_digest: None,
            source_revision: None,
            conditions: Vec::new(),
        },
    })
}

fn child_metadata<Id>(
    id: Id,
    owner_kind: &str,
    owner_id: ResourceName,
) -> Result<ObjectMeta<Id>, InvalidIdentifier> {
    Ok(ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: Default::default(),
        generation: Generation(1),
        owner_refs: vec![OwnerReference {
            resource: ResourceId::new(ResourceKind::new(owner_kind)?, owner_id),
            ownership: Ownership::Controller,
        }],
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    })
}

fn stable_id(prefix: &str, parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(part.as_bytes());
        hash.update([0]);
    }
    let digest = hash.finalize();
    let suffix = digest
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}-{suffix}")
}

pub(crate) fn validate_ownership(
    services: &BTreeMap<ServiceId, Service>,
    deployments: &BTreeMap<DeploymentId, Deployment>,
) -> Result<(), DeploymentPlanError> {
    for deployment in deployments.values() {
        if !services.contains_key(&deployment.spec.service_id) {
            return Err(DeploymentPlanError::MissingService {
                deployment_id: deployment.meta.id.clone(),
                service_id: deployment.spec.service_id.clone(),
            });
        }
    }
    Ok(())
}

pub(crate) fn index<Id, Resource>(
    resources: Vec<Resource>,
    id: impl Fn(&Resource) -> Id,
    kind: &'static str,
) -> Result<BTreeMap<Id, Resource>, DeploymentPlanError>
where
    Id: Clone + Ord + std::fmt::Display,
{
    let mut indexed = BTreeMap::new();
    for resource in resources {
        let resource_id = id(&resource);
        if indexed.insert(resource_id.clone(), resource).is_some() {
            return Err(DeploymentPlanError::DuplicateResource {
                kind,
                resource_id: resource_id.to_string(),
            });
        }
    }
    Ok(indexed)
}
