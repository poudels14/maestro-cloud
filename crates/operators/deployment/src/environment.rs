use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    EnvironmentTemplateContext, EnvironmentTemplateError, IngressRoute, MAESTRO_INGRESS_HOST,
    MAESTRO_INGRESS_PORT, Service,
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
    let context = template_context(service, routes);
    let mut expanded = false;
    for (key, value) in environment.iter_mut() {
        let resolved = context
            .resolve(value)
            .map_err(|error| template_error(service, routes, key, error))?;
        let changed = resolved != *value;
        *value = resolved;
        expanded |= changed;
    }
    Ok(EnvironmentResolution {
        context,
        fingerprint: expanded.then(|| fingerprint(environment)),
    })
}

fn template_context(service: &Service, routes: &[IngressRoute]) -> EnvironmentTemplateContext {
    EnvironmentTemplateContext {
        ingress_host: canonical_ingress_host(service, routes).ok(),
        ingress_port: canonical_ingress_port(service, routes).ok(),
    }
}

fn template_error(
    service: &Service,
    routes: &[IngressRoute],
    key: &str,
    error: EnvironmentTemplateError,
) -> DeploymentPlanError {
    match error {
        EnvironmentTemplateError::Invalid { message } => invalid_template(service, key, message),
        EnvironmentTemplateError::Unavailable { variable, message } => {
            let message = match variable.as_str() {
                MAESTRO_INGRESS_HOST => canonical_ingress_host(service, routes)
                    .err()
                    .unwrap_or(message),
                MAESTRO_INGRESS_PORT => canonical_ingress_port(service, routes)
                    .err()
                    .unwrap_or(message),
                _ => message,
            };
            unavailable_variable(service, key, &variable, message)
        }
    }
}

fn canonical_ingress_host(service: &Service, routes: &[IngressRoute]) -> Result<String, String> {
    let hosts = routes
        .iter()
        .filter(|route| {
            route.spec.service_id == service.meta.id && route.meta.deletion_timestamp.is_none()
        })
        .flat_map(|route| route.spec.hosts.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    match hosts.len() {
        0 => Err("the service has no active ingress host".to_owned()),
        1 => {
            let host = hosts
                .into_iter()
                .next()
                .ok_or_else(|| "the service has no active ingress host".to_owned())?;
            if host.starts_with("*.") {
                Err(format!("the only ingress host `{host}` is a wildcard"))
            } else {
                Ok(host)
            }
        }
        _ => Err(format!(
            "the service has multiple ingress hosts: {}",
            hosts.into_iter().collect::<Vec<_>>().join(", ")
        )),
    }
}

fn canonical_ingress_port(service: &Service, routes: &[IngressRoute]) -> Result<u16, String> {
    let ports = routes
        .iter()
        .filter(|route| {
            route.spec.service_id == service.meta.id && route.meta.deletion_timestamp.is_none()
        })
        .map(|route| route.spec.target_port)
        .collect::<BTreeSet<_>>();
    match ports.len() {
        0 => Err("the service has no active ingress port".to_owned()),
        1 => ports
            .into_iter()
            .next()
            .ok_or_else(|| "the service has no active ingress port".to_owned()),
        _ => Err(format!(
            "the service has multiple ingress ports: {}",
            ports
                .into_iter()
                .map(|port| port.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
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
