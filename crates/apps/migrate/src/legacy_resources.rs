use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    FirewallPolicy, FirewallPolicyId, FirewallPolicySpec, FirewallPolicyStatus, Generation,
    IngressRoute, IngressRouteId, IngressRouteSpec, IngressRouteStatus, Object, ObjectMeta,
    Preview, PreviewId, PreviewPhase, PreviewSpec, PreviewStatus, ResourceRevision, Service,
    ServiceId,
};

use crate::legacy_convert::{
    LegacyPlanError, add_seconds, annotations, invalid_generated, optional_timestamp, owner,
    parse_service_id, stable_id, timestamp,
};
use crate::legacy_schema::LegacyPreviewSource;
use crate::legacy_services::LegacyServiceState;

pub(crate) fn convert_route(
    service: &Service,
    spec: IngressRouteSpec,
) -> Result<IngressRoute, LegacyPlanError> {
    Ok(Object {
        meta: managed_metadata(
            IngressRouteId::new(managed_id(&service.meta.id, "ingress"))
                .map_err(|error| invalid_generated("IngressRoute", error))?,
            service,
        )?,
        spec,
        status: IngressRouteStatus {
            applied_generation: Generation(0),
            conditions: Vec::new(),
        },
    })
}

pub(crate) fn convert_policy(
    service: &Service,
    spec: FirewallPolicySpec,
) -> Result<FirewallPolicy, LegacyPlanError> {
    Ok(Object {
        meta: managed_metadata(
            FirewallPolicyId::new(managed_id(&service.meta.id, "egress"))
                .map_err(|error| invalid_generated("FirewallPolicy", error))?,
            service,
        )?,
        spec,
        status: FirewallPolicyStatus {
            applied_generation: Generation(0),
            ruleset_digest: None,
            conditions: Vec::new(),
        },
    })
}

pub(crate) fn convert_preview(
    state: &LegacyServiceState,
    source: &LegacyPreviewSource,
    services: &BTreeMap<ServiceId, Service>,
) -> Result<Preview, LegacyPlanError> {
    let service_id = parse_service_id(&state.info.config.id, "preview service id")?;
    let base_service_id = parse_service_id(&source.base_service_id, "preview base service id")?;
    let expected_id = format!("{base_service_id}-pr-{}", source.pr_number);
    if service_id.as_str() != expected_id {
        return Err(LegacyPlanError::InvalidPreview {
            service_id: service_id.to_string(),
            message: format!("expected derived identity `{expected_id}`"),
        });
    }
    let base = services
        .get(&base_service_id)
        .ok_or_else(|| LegacyPlanError::InvalidPreview {
            service_id: service_id.to_string(),
            message: format!("base service `{base_service_id}` is missing"),
        })?;
    let policy = base
        .spec
        .preview
        .as_ref()
        .ok_or_else(|| LegacyPlanError::InvalidPreview {
            service_id: service_id.to_string(),
            message: format!("base service `{base_service_id}` has previews disabled"),
        })?;
    let repository = match &state.info.config.build {
        Some(build) => build
            .repo
            .as_deref()
            .ok_or_else(|| LegacyPlanError::InvalidPreview {
                service_id: service_id.to_string(),
                message: "derived preview has no Git repository".to_owned(),
            })?,
        None => {
            return Err(LegacyPlanError::InvalidPreview {
                service_id: service_id.to_string(),
                message: "derived preview has no build config".to_owned(),
            });
        }
    };
    let repository = normalize_github_repository(&service_id, repository)?;
    let created_at = timestamp("preview.createdAt", source.created_at)?;
    let expires_at = add_seconds(created_at, policy.lifetime_secs, "preview expiry")?;
    let closed_at = optional_timestamp("preview.closedAt", source.closed_at)?;
    let teardown_at = closed_at
        .map(|closed| add_seconds(closed, policy.close_grace_period_secs, "preview teardown"))
        .transpose()?;
    let annotations = annotations([
        (
            "migration.maestro.dev/preview-head-ref",
            source.head_ref.clone(),
        ),
        ("migration.maestro.dev/preview-title", source.title.clone()),
        (
            "migration.maestro.dev/preview-volumes-stripped",
            source.volumes_stripped.to_string(),
        ),
    ]);
    Ok(Object {
        meta: ObjectMeta {
            id: PreviewId::new(service_id.to_string())
                .map_err(|error| invalid_generated("Preview", error))?,
            labels: BTreeMap::new(),
            annotations,
            revision: ResourceRevision(0),
            generation: state_generation(state),
            owner_refs: vec![owner("Service", base_service_id.clone().into())?],
            finalizers: BTreeSet::new(),
            deletion_timestamp: closed_at,
        },
        spec: PreviewSpec {
            base_service_id,
            repository,
            pull_request_number: source.pr_number,
            head_revision: source.head_sha.clone(),
            service_id,
            close_grace_period_secs: policy.close_grace_period_secs,
            expires_at,
        },
        status: PreviewStatus {
            phase: if closed_at.is_some() {
                PreviewPhase::Closing
            } else {
                PreviewPhase::Active
            },
            teardown_at,
            conditions: Vec::new(),
        },
    })
}

fn managed_metadata<Id>(id: Id, service: &Service) -> Result<ObjectMeta<Id>, LegacyPlanError> {
    Ok(ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision(0),
        generation: service.meta.generation,
        owner_refs: vec![owner("Service", service.meta.id.clone().into())?],
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    })
}

fn managed_id(service_id: &ServiceId, suffix: &str) -> String {
    let candidate = format!("{service_id}-{suffix}");
    if candidate.len() <= 253 {
        candidate
    } else {
        stable_id(&format!("managed-{suffix}"), service_id.as_str())
    }
}

fn normalize_github_repository(
    service_id: &ServiceId,
    repository: &str,
) -> Result<String, LegacyPlanError> {
    let trimmed = repository
        .trim()
        .trim_end_matches('/')
        .trim_end_matches(".git");
    let path = if let Some(path) = trimmed.strip_prefix("git@github.com:") {
        path
    } else {
        let without_scheme = trimmed
            .strip_prefix("https://")
            .or_else(|| trimmed.strip_prefix("http://"))
            .or_else(|| trimmed.strip_prefix("ssh://git@"))
            .ok_or_else(|| {
                invalid_preview(service_id, "GitHub repository must use HTTPS or SSH")
            })?;
        without_scheme.strip_prefix("github.com/").ok_or_else(|| {
            invalid_preview(service_id, "preview repository is not hosted on github.com")
        })?
    };
    let mut components = path.split('/');
    let owner = components.next().unwrap_or_default();
    let name = components.next().unwrap_or_default();
    if owner.is_empty() || name.is_empty() || components.next().is_some() {
        return Err(invalid_preview(
            service_id,
            "GitHub repository must identify one owner and repository",
        ));
    }
    Ok(format!(
        "{}/{}",
        owner.to_ascii_lowercase(),
        name.to_ascii_lowercase()
    ))
}

fn state_generation(state: &LegacyServiceState) -> Generation {
    Generation(state.next_history_index.max(1))
}

fn invalid_preview(service_id: &ServiceId, message: impl Into<String>) -> LegacyPlanError {
    LegacyPlanError::InvalidPreview {
        service_id: service_id.to_string(),
        message: message.into(),
    }
}
