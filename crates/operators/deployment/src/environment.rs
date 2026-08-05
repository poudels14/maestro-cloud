use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    EnvironmentTemplateContext, EnvironmentTemplateError, IngressRoute, MAESTRO_PREVIEW_HOST,
    Ownership, Service,
};
use sha2::{Digest, Sha256};

use crate::DeploymentPlanError;

const FINGERPRINT_DOMAIN: &[u8] = b"maestro-dynamic-environment-v1\0";

#[derive(Debug)]
pub(crate) struct EnvironmentResolution {
    pub(crate) context: EnvironmentTemplateContext,
    pub(crate) fingerprint: Option<String>,
}

pub(crate) fn resolve(
    service: &Service,
    routes: &[IngressRoute],
    environment: &mut BTreeMap<String, String>,
) -> Result<EnvironmentResolution, DeploymentPlanError> {
    let context = template_context(service, routes)?;
    let mut expanded = false;
    for (key, value) in environment.iter_mut() {
        let resolved = context
            .resolve(value)
            .map_err(|error| template_error(service, key, error))?;
        let changed = resolved != *value;
        *value = resolved;
        expanded |= changed;
    }
    Ok(EnvironmentResolution {
        context,
        fingerprint: expanded.then(|| fingerprint(environment)),
    })
}

fn template_context(
    service: &Service,
    routes: &[IngressRoute],
) -> Result<EnvironmentTemplateContext, DeploymentPlanError> {
    if !is_preview_service(service) {
        return Ok(EnvironmentTemplateContext::default());
    }
    Ok(EnvironmentTemplateContext {
        preview_host: Some(canonical_ingress_host(
            service,
            routes,
            MAESTRO_PREVIEW_HOST,
            MAESTRO_PREVIEW_HOST,
        )?),
    })
}

fn template_error(
    service: &Service,
    key: &str,
    error: EnvironmentTemplateError,
) -> DeploymentPlanError {
    match error {
        EnvironmentTemplateError::Invalid { message } => invalid_template(service, key, message),
        EnvironmentTemplateError::Unavailable { variable, message } => unavailable_variable(
            service,
            key,
            &variable,
            if variable == MAESTRO_PREVIEW_HOST && !is_preview_service(service) {
                "the service is not owned by a Preview".to_owned()
            } else {
                message
            },
        ),
    }
}

fn canonical_ingress_host(
    service: &Service,
    routes: &[IngressRoute],
    key: &str,
    variable: &str,
) -> Result<String, DeploymentPlanError> {
    let hosts = routes
        .iter()
        .filter(|route| {
            route.spec.service_id == service.meta.id && route.meta.deletion_timestamp.is_none()
        })
        .flat_map(|route| route.spec.hosts.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    match hosts.len() {
        0 => Err(unavailable_variable(
            service,
            key,
            variable,
            "the service has no active ingress host",
        )),
        1 => {
            let host = hosts.into_iter().next().ok_or_else(|| {
                unavailable_variable(
                    service,
                    key,
                    variable,
                    "the service has no active ingress host",
                )
            })?;
            if host.starts_with("*.") {
                Err(unavailable_variable(
                    service,
                    key,
                    variable,
                    format!("the only ingress host `{host}` is a wildcard"),
                ))
            } else {
                Ok(host)
            }
        }
        _ => Err(unavailable_variable(
            service,
            key,
            variable,
            format!(
                "the service has multiple ingress hosts: {}",
                hosts.into_iter().collect::<Vec<_>>().join(", ")
            ),
        )),
    }
}

fn is_preview_service(service: &Service) -> bool {
    service.meta.owner_refs.iter().any(|owner| {
        owner.ownership == Ownership::Controller && owner.resource.kind.as_str() == "Preview"
    })
}

fn fingerprint(environment: &BTreeMap<String, String>) -> String {
    let mut digest = Sha256::new();
    digest.update(FINGERPRINT_DOMAIN);
    for (key, value) in environment {
        digest.update(key.as_bytes());
        digest.update([0]);
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    digest
        .finalize()
        .iter()
        .take(16)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn invalid_template(
    service: &Service,
    key: &str,
    message: impl Into<String>,
) -> DeploymentPlanError {
    DeploymentPlanError::InvalidEnvironmentTemplate {
        service_id: service.meta.id.clone(),
        key: key.to_owned(),
        message: message.into(),
    }
}

fn unavailable_variable(
    service: &Service,
    key: &str,
    variable: &str,
    message: impl Into<String>,
) -> DeploymentPlanError {
    DeploymentPlanError::UnavailableEnvironmentVariable {
        service_id: service.meta.id.clone(),
        key: key.to_owned(),
        variable: variable.to_owned(),
        message: message.into(),
    }
}
