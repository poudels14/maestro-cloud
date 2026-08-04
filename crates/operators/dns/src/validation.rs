use kernel_api::{ClusterId, DnsName, ServiceId, workload_hostname};

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

pub(crate) fn alias_name(alias: &str, cluster_id: &ClusterId) -> Result<String, DnsPlanError> {
    fqdn(alias, cluster_id)
}

fn fqdn(host: &str, cluster_id: &ClusterId) -> Result<String, DnsPlanError> {
    let name = format!("{host}.{}.{ZONE}", cluster_id.as_str());
    let relative = name.trim_end_matches('.');
    DnsName::parse(relative).map_err(|error| invalid(name.clone(), error.to_string()))?;
    Ok(name)
}

fn invalid(name: String, message: impl Into<String>) -> DnsPlanError {
    DnsPlanError::InvalidDnsName {
        name,
        message: message.into(),
    }
}
