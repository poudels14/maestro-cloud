use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{IngressRoute, Ownership, Service};
use sha2::{Digest, Sha256};

use crate::DeploymentPlanError;

const TEMPLATE_OPEN: &str = "${{";
const TEMPLATE_CLOSE: &str = "}}";
const PREVIEW_HOST: &str = "MAESTRO_PREVIEW_HOST";
const FINGERPRINT_DOMAIN: &[u8] = b"maestro-dynamic-environment-v1\0";

pub(crate) fn resolve(
    service: &Service,
    routes: &[IngressRoute],
    environment: &mut BTreeMap<String, String>,
) -> Result<Option<String>, DeploymentPlanError> {
    let mut expanded = false;
    for (key, value) in environment.iter_mut() {
        let (resolved, changed) = resolve_value(service, routes, key, value)?;
        *value = resolved;
        expanded |= changed;
    }
    Ok(expanded.then(|| fingerprint(environment)))
}

fn resolve_value(
    service: &Service,
    routes: &[IngressRoute],
    key: &str,
    value: &str,
) -> Result<(String, bool), DeploymentPlanError> {
    let mut remaining = value;
    let mut output = String::with_capacity(value.len());
    let mut expanded = false;
    while let Some(open) = remaining.find(TEMPLATE_OPEN) {
        output.push_str(&remaining[..open]);
        let expression = &remaining[open + TEMPLATE_OPEN.len()..];
        let Some(close) = expression.find(TEMPLATE_CLOSE) else {
            return Err(invalid_template(
                service,
                key,
                "template is missing its closing `}}`",
            ));
        };
        let variable = expression[..close].trim();
        if variable.is_empty()
            || !variable
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(invalid_template(
                service,
                key,
                format!("`{variable}` is not a valid Maestro variable name"),
            ));
        }
        output.push_str(resolve_variable(service, routes, key, variable)?.as_str());
        remaining = &expression[close + TEMPLATE_CLOSE.len()..];
        expanded = true;
    }
    output.push_str(remaining);
    Ok((output, expanded))
}

fn resolve_variable(
    service: &Service,
    routes: &[IngressRoute],
    key: &str,
    variable: &str,
) -> Result<String, DeploymentPlanError> {
    match variable {
        PREVIEW_HOST if is_preview_service(service) => {
            canonical_ingress_host(service, routes, key, variable)
        }
        PREVIEW_HOST => Err(unavailable_variable(
            service,
            key,
            variable,
            "the service is not owned by a Preview",
        )),
        _ => Err(unavailable_variable(
            service,
            key,
            variable,
            "the variable is not supported",
        )),
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
