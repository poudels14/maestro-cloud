use std::collections::{BTreeMap, BTreeSet};

use kernel_api::ServiceId;
use runtime::{
    HostPortPublication, NetworkAddressing, RuntimeCapability, RuntimeError, WorkloadRuntime,
};

const SYSTEM_RESOURCE_PREFIX: &str = "maestro-system-";

pub(crate) fn validate_system_host_ports(
    runtime: &dyn WorkloadRuntime,
    addressing: NetworkAddressing,
    grants: &BTreeMap<ServiceId, Vec<HostPortPublication>>,
) -> Result<(), RuntimeError> {
    if grants.is_empty() {
        return Ok(());
    }
    if matches!(addressing, NetworkAddressing::Delegated)
        && !runtime
            .capabilities()
            .supports(RuntimeCapability::HostPortPublishing)
    {
        return Err(RuntimeError::Unsupported {
            capability: RuntimeCapability::HostPortPublishing,
        });
    }
    let mut host_endpoints = BTreeSet::new();
    for (service_id, publications) in grants {
        if !service_id.as_str().starts_with(SYSTEM_RESOURCE_PREFIX) {
            return Err(RuntimeError::InvalidSpec {
                message: format!(
                    "host ports may be granted only to `{SYSTEM_RESOURCE_PREFIX}` services"
                ),
            });
        }
        for publication in publications {
            if publication.container_port == 0 || publication.host_port == 0 {
                return Err(RuntimeError::InvalidSpec {
                    message: "published container and host ports must be nonzero".to_owned(),
                });
            }
            let endpoint = (
                publication.host_address,
                publication.host_port,
                publication.protocol,
            );
            if !host_endpoints.insert(endpoint) {
                return Err(RuntimeError::InvalidSpec {
                    message: format!(
                        "host endpoint `{}:{}/{:?}` is granted more than once",
                        publication.host_address, publication.host_port, publication.protocol
                    ),
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn runtime_host_ports(
    addressing: NetworkAddressing,
    publications: Vec<HostPortPublication>,
) -> Vec<HostPortPublication> {
    match addressing {
        NetworkAddressing::Managed { .. } => Vec::new(),
        NetworkAddressing::Delegated => publications,
    }
}
