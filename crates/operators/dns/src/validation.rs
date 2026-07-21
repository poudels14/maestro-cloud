use kernel_api::{ClusterId, ServiceId, workload_hostname};

use crate::DnsPlanError;

const ZONE: &str = "maestro.internal.";

pub(crate) fn service_name(
    service_id: &ServiceId,
    cluster_id: &ClusterId,
) -> Result<String, DnsPlanError> {
    fqdn(service_id.as_str(), cluster_id)
}

pub(crate) fn replica_name(
    service_id: &ServiceId,
    replica_index: u32,
    cluster_id: &ClusterId,
) -> Result<String, DnsPlanError> {
    fqdn(&workload_hostname(service_id, replica_index), cluster_id)
}

fn fqdn(host: &str, cluster_id: &ClusterId) -> Result<String, DnsPlanError> {
    let name = format!("{host}.{}.{ZONE}", cluster_id.as_str());
    let relative = name.trim_end_matches('.');
    if relative.len() > 253 {
        return Err(invalid(name, "name exceeds 253 bytes"));
    }
    for label in relative.split('.') {
        validate_label(label).map_err(|message| invalid(name.clone(), message))?;
    }
    Ok(name)
}

fn validate_label(label: &str) -> Result<(), &'static str> {
    if label.is_empty() {
        Err("contains an empty label")
    } else if label.len() > 63 {
        Err("contains a label longer than 63 bytes")
    } else if !label
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        Err("labels may contain only lowercase ASCII letters, digits, and hyphens")
    } else if !label
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        || !label
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
    {
        Err("labels must start and end with an ASCII letter or digit")
    } else {
        Ok(())
    }
}

fn invalid(name: String, message: impl Into<String>) -> DnsPlanError {
    DnsPlanError::InvalidDnsName {
        name,
        message: message.into(),
    }
}
