use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    ArtifactTemplate, BUILD_WATCH_REVISION_ANNOTATION, Build, BuildId, BuildPhase, BuildSource,
    BuildSpec, BuildStatus, Deployment, DeploymentGoal, DeploymentId, DeploymentPhase,
    DeploymentSpec, DeploymentStatus, Generation, InvalidIdentifier, Object, ObjectMeta,
    OwnerReference, Ownership, ResourceId, ResourceKind, ResourceName, Service, ServiceId,
    Timestamp,
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
    let watched_revision = watched_revision(service);
    let generation = service.meta.generation.0.to_string();
    let mut identity = vec![cluster_id.as_str(), service.meta.id.as_str(), &generation];
    if let Some(revision) = watched_revision {
        identity.push(revision);
    }
    let deployment_id = DeploymentId::new(stable_id("deployment", &identity))?;
    let build_id = matches!(service.spec.artifact, ArtifactTemplate::Build { .. })
        .then(|| {
            BuildId::new(stable_id(
                "build",
                &[cluster_id.as_str(), deployment_id.as_str()],
            ))
        })
        .transpose()?;
    let mut captured_service = service.spec.clone();
    if let (
        Some(revision),
        ArtifactTemplate::Build {
            template:
                kernel_api::BuildTemplate {
                    source:
                        BuildSource::Git {
                            revision: desired, ..
                        },
                    ..
                },
        },
    ) = (watched_revision, &mut captured_service.artifact)
    {
        *desired = revision.to_string();
    }
    Ok(Object {
        meta: child_metadata(deployment_id, SERVICE_KIND, service.meta.id.clone().into())?,
        spec: DeploymentSpec {
            service_id: service.meta.id.clone(),
            service_generation: service.meta.generation,
            restart_generation: Generation(1),
            service: captured_service,
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

fn watched_revision(service: &Service) -> Option<&str> {
    let ArtifactTemplate::Build { template } = &service.spec.artifact else {
        return None;
    };
    if !template.watch || !matches!(template.source, BuildSource::Git { .. }) {
        return None;
    }
    service
        .meta
        .annotations
        .get(&kernel_api::AnnotationKey(
            BUILD_WATCH_REVISION_ANNOTATION.to_string(),
        ))
        .map(String::as_str)
        .filter(|revision| !revision.trim().is_empty())
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
