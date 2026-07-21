use std::collections::HashMap;
use std::net::IpAddr;

use async_trait::async_trait;
use docker::errors::Error as DockerError;
use docker::models::{
    EndpointIpamConfig, EndpointSettings, Ipam, IpamConfig, NetworkConnectRequest,
    NetworkCreateRequest, NetworkDisconnectRequest, NetworkInspect,
};
use kernel_api::WorkloadId;

use crate::docker::DockerRuntime;
use crate::docker_network_ipam::{
    cidr_text, gateway_is_usable, lease_is_reserved, parse_cidr, reconcile_network,
    release_address, reserve_address,
};
use crate::docker_support::{container_id, is_conflict, is_not_found};
use crate::{
    AddressLease, AddressRequest, NetworkAttachment, NetworkHandle, NetworkProvider,
    NetworkProviderError, NetworkSpec, WorkloadHandle, WorkloadNetworkStatus,
};

const NETWORK_MANAGED_LABEL: &str = "com.maestro.network";
const NETWORK_SUBNET_LABEL: &str = "com.maestro.network-subnet";
const NETWORK_GATEWAY_LABEL: &str = "com.maestro.network-gateway";
const NETWORK_MTU_LABEL: &str = "com.maestro.network-mtu";
const DOCKER_MTU_OPTION: &str = "com.docker.network.driver.mtu";

#[async_trait]
impl NetworkProvider for DockerRuntime {
    async fn ensure_network(
        &self,
        spec: &NetworkSpec,
    ) -> Result<NetworkHandle, NetworkProviderError> {
        validate_spec(spec)?;
        let inspect = match self.client.inspect_network(&spec.name, None).await {
            Ok(inspect) => inspect,
            Err(error) if is_not_found(&error) => {
                let request = network_create_request(spec);
                match self.client.create_network(request).await {
                    Ok(_) => self
                        .client
                        .inspect_network(&spec.name, None)
                        .await
                        .map_err(|error| network_error(error, &spec.name))?,
                    Err(error) if is_conflict(&error) => self
                        .client
                        .inspect_network(&spec.name, None)
                        .await
                        .map_err(|error| network_error(error, &spec.name))?,
                    Err(error) => return Err(network_error(error, &spec.name)),
                }
            }
            Err(error) => return Err(network_error(error, &spec.name)),
        };
        validate_existing_network(&inspect, spec)?;
        let mut state = self.network_state.lock().await;
        reconcile_network(&mut state, spec, &inspect)?;
        NetworkHandle::new(spec.name.clone())
    }

    async fn allocate_address(
        &self,
        network: &NetworkHandle,
        workload_id: &WorkloadId,
        request: AddressRequest,
    ) -> Result<AddressLease, NetworkProviderError> {
        let inspect = self
            .client
            .inspect_network(network.name(), None)
            .await
            .map_err(|error| network_error(error, network.name()))?;
        let spec = inspected_network_spec(&inspect)?;
        let mut state = self.network_state.lock().await;
        reconcile_network(&mut state, &spec, &inspect)?;
        reserve_address(&mut state, network.name(), workload_id, request)
    }

    async fn attach(
        &self,
        workload: &WorkloadHandle,
        network: &NetworkHandle,
        lease: &AddressLease,
    ) -> Result<NetworkAttachment, NetworkProviderError> {
        validate_lease(self, network, lease).await?;
        let inspect = self
            .inspect_container(workload)
            .await
            .map_err(runtime_network_error)?;
        if let Some(attachment) = container_attachment(&inspect, network)? {
            return matching_attachment(attachment, lease);
        }
        let endpoint_config = match lease.address {
            IpAddr::V4(_) => EndpointSettings {
                ipam_config: Some(EndpointIpamConfig {
                    ipv4_address: Some(lease.address.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            IpAddr::V6(_) => EndpointSettings {
                ipam_config: Some(EndpointIpamConfig {
                    ipv6_address: Some(lease.address.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        };
        let request = NetworkConnectRequest {
            container: container_id(workload)
                .map_err(runtime_network_error)?
                .to_owned(),
            endpoint_config: Some(endpoint_config),
        };
        match self.client.connect_network(network.name(), request).await {
            Ok(()) => Ok(NetworkAttachment {
                network: network.clone(),
                address: lease.address,
                interface_name: None,
            }),
            Err(error) if is_conflict(&error) => {
                let inspect = self
                    .inspect_container(workload)
                    .await
                    .map_err(runtime_network_error)?;
                let attachment = container_attachment(&inspect, network)?.ok_or(
                    NetworkProviderError::AddressConflict {
                        address: lease.address,
                    },
                )?;
                matching_attachment(attachment, lease)
            }
            Err(error) => Err(network_error(error, network.name())),
        }
    }

    async fn detach(
        &self,
        workload: &WorkloadHandle,
        network: &NetworkHandle,
    ) -> Result<(), NetworkProviderError> {
        let inspect = self
            .inspect_container(workload)
            .await
            .map_err(runtime_network_error)?;
        if container_attachment(&inspect, network)?.is_some() {
            self.client
                .disconnect_network(
                    network.name(),
                    NetworkDisconnectRequest {
                        container: container_id(workload)
                            .map_err(runtime_network_error)?
                            .to_owned(),
                        force: Some(false),
                    },
                )
                .await
                .map_err(|error| network_error(error, network.name()))
        } else {
            Ok(())
        }
    }

    async fn inspect(
        &self,
        workload: &WorkloadHandle,
    ) -> Result<WorkloadNetworkStatus, NetworkProviderError> {
        let inspect = self
            .inspect_container(workload)
            .await
            .map_err(runtime_network_error)?;
        let mut attachments = inspect
            .network_settings
            .and_then(|settings| settings.networks)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(name, settings)| endpoint_attachment(name, settings).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        attachments.sort_by(|left, right| left.network.name().cmp(right.network.name()));
        Ok(WorkloadNetworkStatus { attachments })
    }

    async fn release_address(
        &self,
        network: &NetworkHandle,
        lease: &AddressLease,
    ) -> Result<(), NetworkProviderError> {
        let mut state = self.network_state.lock().await;
        release_address(&mut state, network.name(), lease)
    }
}

pub(crate) fn network_create_request(spec: &NetworkSpec) -> NetworkCreateRequest {
    let subnet = cidr_text(spec.range);
    NetworkCreateRequest {
        name: spec.name.clone(),
        driver: Some("bridge".to_owned()),
        scope: Some("local".to_owned()),
        attachable: Some(true),
        ipam: Some(Ipam {
            config: Some(vec![IpamConfig {
                subnet: Some(subnet.clone()),
                gateway: Some(spec.gateway.to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        enable_ipv4: Some(spec.gateway.is_ipv4()),
        enable_ipv6: Some(spec.gateway.is_ipv6()),
        labels: Some(HashMap::from([
            (NETWORK_MANAGED_LABEL.to_owned(), "true".to_owned()),
            (NETWORK_SUBNET_LABEL.to_owned(), subnet),
            (NETWORK_GATEWAY_LABEL.to_owned(), spec.gateway.to_string()),
            (NETWORK_MTU_LABEL.to_owned(), spec.mtu_bytes.to_string()),
        ])),
        options: Some(HashMap::from([(
            DOCKER_MTU_OPTION.to_owned(),
            spec.mtu_bytes.to_string(),
        )])),
        ..Default::default()
    }
}

fn validate_spec(spec: &NetworkSpec) -> Result<(), NetworkProviderError> {
    let gateway_valid = gateway_is_usable(spec.range, spec.gateway);
    if spec.name.is_empty() || !gateway_valid || spec.mtu_bytes == 0 {
        Err(NetworkProviderError::InvalidRange {
            message: format!(
                "network `{}` gateway `{}` is not a usable address in `{}`",
                spec.name,
                spec.gateway,
                cidr_text(spec.range)
            ),
        })
    } else {
        Ok(())
    }
}

fn validate_existing_network(
    inspect: &NetworkInspect,
    desired: &NetworkSpec,
) -> Result<(), NetworkProviderError> {
    let actual = inspected_network_spec(inspect)?;
    let managed = inspect
        .labels
        .as_ref()
        .and_then(|labels| labels.get(NETWORK_MANAGED_LABEL))
        .is_some_and(|value| value == "true");
    if managed && actual == *desired {
        Ok(())
    } else {
        Err(NetworkProviderError::Rejected {
            message: format!(
                "docker network `{}` already exists with different ownership or IPAM",
                desired.name
            ),
        })
    }
}

fn inspected_network_spec(inspect: &NetworkInspect) -> Result<NetworkSpec, NetworkProviderError> {
    let name = inspect
        .name
        .clone()
        .ok_or_else(|| malformed_network("name"))?;
    let config = inspect
        .ipam
        .as_ref()
        .and_then(|ipam| ipam.config.as_ref())
        .and_then(|configs| configs.first())
        .ok_or_else(|| malformed_network("IPAM configuration"))?;
    let subnet = config
        .subnet
        .as_deref()
        .ok_or_else(|| malformed_network("subnet"))?;
    let gateway = config
        .gateway
        .as_deref()
        .ok_or_else(|| malformed_network("gateway"))?
        .parse()
        .map_err(|error| NetworkProviderError::Rejected {
            message: format!("docker network `{name}` has an invalid gateway: {error}"),
        })?;
    let mtu_bytes = inspect
        .labels
        .as_ref()
        .and_then(|labels| labels.get(NETWORK_MTU_LABEL))
        .ok_or_else(|| malformed_network("MTU label"))?
        .parse()
        .map_err(|error| NetworkProviderError::Rejected {
            message: format!("docker network `{name}` has an invalid MTU: {error}"),
        })?;
    Ok(NetworkSpec {
        name,
        range: parse_cidr(subnet)?,
        gateway,
        mtu_bytes,
    })
}

async fn validate_lease(
    runtime: &DockerRuntime,
    network: &NetworkHandle,
    lease: &AddressLease,
) -> Result<(), NetworkProviderError> {
    let state = runtime.network_state.lock().await;
    if lease_is_reserved(&state, network.name(), lease) {
        Ok(())
    } else {
        Err(NetworkProviderError::AddressConflict {
            address: lease.address,
        })
    }
}

fn container_attachment(
    inspect: &docker::models::ContainerInspectResponse,
    network: &NetworkHandle,
) -> Result<Option<NetworkAttachment>, NetworkProviderError> {
    inspect
        .network_settings
        .as_ref()
        .and_then(|settings| settings.networks.as_ref())
        .and_then(|networks| networks.get(network.name()))
        .cloned()
        .map(|settings| endpoint_attachment(network.name().to_owned(), settings))
        .transpose()
        .map(Option::flatten)
}

fn endpoint_attachment(
    name: String,
    settings: EndpointSettings,
) -> Result<Option<NetworkAttachment>, NetworkProviderError> {
    let address = settings
        .ip_address
        .filter(|value| !value.is_empty())
        .or_else(|| {
            settings
                .global_ipv6_address
                .filter(|value| !value.is_empty())
        });
    address
        .map(|address| {
            Ok(NetworkAttachment {
                network: NetworkHandle::new(name)?,
                address: address
                    .parse()
                    .map_err(|error| NetworkProviderError::Rejected {
                        message: format!("docker endpoint has an invalid address: {error}"),
                    })?,
                interface_name: None,
            })
        })
        .transpose()
}

fn matching_attachment(
    attachment: NetworkAttachment,
    lease: &AddressLease,
) -> Result<NetworkAttachment, NetworkProviderError> {
    if attachment.address == lease.address {
        Ok(attachment)
    } else {
        Err(NetworkProviderError::AddressConflict {
            address: lease.address,
        })
    }
}

fn malformed_network(field: &str) -> NetworkProviderError {
    NetworkProviderError::Rejected {
        message: format!("docker network inspection omitted its {field}"),
    }
}

fn network_error(error: DockerError, network: &str) -> NetworkProviderError {
    let message = error.to_string();
    match error {
        DockerError::DockerResponseServerError {
            status_code: 404, ..
        } => NetworkProviderError::NetworkNotFound {
            name: network.to_owned(),
        },
        DockerError::DockerResponseServerError {
            status_code: 400 | 403 | 409 | 422,
            ..
        } => NetworkProviderError::Rejected { message },
        _ => NetworkProviderError::Unavailable { message },
    }
}

fn runtime_network_error(error: crate::RuntimeError) -> NetworkProviderError {
    match error {
        crate::RuntimeError::Unavailable { message } => {
            NetworkProviderError::Unavailable { message }
        }
        other => NetworkProviderError::Rejected {
            message: other.to_string(),
        },
    }
}
