use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    AnnotationKey, ArtifactTemplate, BuildSource, Generation, IngressRoute, IngressRouteId, Object,
    ObjectMeta, OwnerReference, Ownership, Preview, ResourceId, ResourceKind, ResourceName,
    Service, ServiceStatus,
};
use sha2::{Digest, Sha256};

use crate::PreviewError;

pub(crate) fn desired_service(
    preview: &Preview,
    base: &Service,
    current: Option<&Service>,
) -> Result<Service, PreviewError> {
    if preview.spec.service_id == base.meta.id {
        return Err(PreviewError::InvalidDefinition {
            message: "preview service identity must differ from its base service".to_string(),
        });
    }
    if current.is_some_and(|current| !owned_by_preview(preview, &current.meta)) {
        return Err(PreviewError::InvalidDefinition {
            message: format!(
                "preview service identity `{}` is already owned by another resource",
                preview.spec.service_id
            ),
        });
    }
    let Some(policy) = base.spec.preview.as_ref() else {
        return Err(PreviewError::InvalidDefinition {
            message: format!("base service `{}` has previews disabled", base.meta.id),
        });
    };
    let mut spec = base.spec.clone();
    let ArtifactTemplate::Build { template } = &mut spec.artifact else {
        return Err(PreviewError::InvalidDefinition {
            message: format!(
                "preview-enabled service `{}` must use a build artifact",
                base.meta.id
            ),
        });
    };
    let BuildSource::Git { revision, .. } = &mut template.source else {
        return Err(PreviewError::InvalidDefinition {
            message: format!(
                "preview-enabled service `{}` must use a Git build source",
                base.meta.id
            ),
        });
    };
    if preview.spec.head_revision.trim().is_empty() {
        return Err(PreviewError::InvalidDefinition {
            message: "preview head revision cannot be empty".to_string(),
        });
    }
    *revision = preview.spec.head_revision.clone();
    template.watch = false;
    if template.registry.is_some() && template.registry_repository.is_none() {
        template.registry_repository = Some(base.meta.id.clone());
    }
    spec.name = preview.spec.service_id.to_string();
    spec.preview = None;
    spec.replicas = policy.replicas;
    spec.environment_sources
        .extend(policy.environment_source.clone());
    spec.environment.extend(policy.environment.clone());
    spec.volumes.clear();

    let mut desired = match current {
        Some(current) => current.clone(),
        None => Object {
            meta: child_metadata(preview, preview.spec.service_id.clone())?,
            spec: spec.clone(),
            status: ServiceStatus {
                active_deployment_id: None,
                replica_override: None,
                rollout: base.status.rollout,
                rollout_bypass_generation: None,
                conditions: Vec::new(),
            },
        },
    };
    if desired.spec != spec {
        desired.meta.generation = Generation(desired.meta.generation.0.saturating_add(1));
        desired.spec = spec;
        desired.status.active_deployment_id = None;
    }
    desired.status.replica_override = None;
    desired.status.rollout = base.status.rollout;
    desired.meta.owner_refs = vec![preview_owner(preview)?];
    Ok(desired)
}

pub(crate) fn desired_route(
    preview: &Preview,
    base: &IngressRoute,
    current: Option<&IngressRoute>,
    preview_domain: &str,
) -> Result<IngressRoute, PreviewError> {
    let id = route_id(preview, &base.meta.id)?;
    if current.is_some_and(|current| !owned_by_preview(preview, &current.meta)) {
        return Err(PreviewError::InvalidDefinition {
            message: format!("preview route identity `{id}` is already owned by another resource"),
        });
    }
    let mut spec = base.spec.clone();
    spec.service_id = preview.spec.service_id.clone();
    spec.hosts = vec![format!(
        "{}.{}",
        preview.spec.service_id,
        preview_domain.trim_end_matches('.')
    )];
    let mut desired = match current {
        Some(current) => current.clone(),
        None => Object {
            meta: child_metadata(preview, id.clone())?,
            spec: spec.clone(),
            status: kernel_api::IngressRouteStatus {
                applied_generation: Generation::default(),
                conditions: Vec::new(),
            },
        },
    };
    if desired.spec != spec {
        desired.meta.generation = Generation(desired.meta.generation.0.saturating_add(1));
        desired.spec = spec;
    }
    desired.meta.annotations.insert(
        AnnotationKey("preview.maestro.dev/base-route".to_string()),
        base.meta.id.to_string(),
    );
    desired.meta.owner_refs = vec![preview_owner(preview)?];
    Ok(desired)
}

pub(crate) fn owned_by_preview<ResourceId>(
    preview: &Preview,
    metadata: &ObjectMeta<ResourceId>,
) -> bool {
    metadata.owner_refs.iter().any(|owner| {
        owner.ownership == Ownership::Controller
            && owner.resource.kind.as_str() == "Preview"
            && owner.resource.id.as_str() == preview.meta.id.as_str()
    })
}

fn child_metadata<Id>(
    preview: &Preview,
    id: Id,
) -> Result<ObjectMeta<Id>, kernel_api::InvalidIdentifier> {
    Ok(ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: Default::default(),
        generation: Generation(1),
        owner_refs: vec![preview_owner(preview)?],
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    })
}

fn preview_owner(preview: &Preview) -> Result<OwnerReference, kernel_api::InvalidIdentifier> {
    Ok(OwnerReference {
        resource: ResourceId::new(
            ResourceKind::new("Preview")?,
            ResourceName::from(preview.meta.id.clone()),
        ),
        ownership: Ownership::Controller,
    })
}

pub(crate) fn route_id(
    preview: &Preview,
    base_route_id: &IngressRouteId,
) -> Result<IngressRouteId, PreviewError> {
    let mut digest = Sha256::new();
    digest.update(preview.meta.id.as_str());
    digest.update([0]);
    digest.update(base_route_id.as_str());
    let suffix = digest
        .finalize()
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    IngressRouteId::new(format!("preview-route-{suffix}")).map_err(Into::into)
}
