use std::collections::{BTreeSet, HashMap};

use async_trait::async_trait;
use docker::errors::Error as DockerError;
use docker::models::{
    EndpointSettings, NetworkConnectRequest, NetworkCreateRequest, NetworkDisconnectRequest,
    NetworkInspect,
};
use kernel_api::WorkloadId;

use crate::docker::DockerRuntime;
use crate::docker_support::{container_id, is_conflict, is_not_found};
use crate::{
    AddressRequest, AddressReservation, NetworkAddressing, NetworkAttachment, NetworkHandle,
    NetworkProvider, NetworkProviderError, NetworkSpec, WorkloadHandle, WorkloadNetworkStatus,
    WorkloadRuntime,
};

const NETWORK_MANAGED_LABEL: &str = "com.maestro.network";
const NETWORK_MTU_LABEL: &str = "com.maestro.network-mtu";
const LEGACY_NETWORK_SUBNET_LABEL: &str = "com.maestro.network-subnet";
const LEGACY_NETWORK_GATEWAY_LABEL: &str = "com.maestro.network-gateway";
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
        NetworkHandle::new(spec.name.clone())
    }

    async fn reconcile_address_owners(
        &self,
        network: &NetworkHandle,
        _active_workload_ids: &BTreeSet<WorkloadId>,
    ) -> Result<usize, NetworkProviderError> {
        self.client
            .inspect_network(network.name(), None)
            .await
            .map(|_| 0)
            .map_err(|error| network_error(error, network.name()))
    }

    async fn allocate_address(
        &self,
        network: &NetworkHandle,
        workload_id: &WorkloadId,
        request: AddressRequest,
    ) -> Result<AddressReservation, NetworkProviderError> {
        self.client
            .inspect_network(network.name(), None)
            .await
            .map_err(|error| network_error(error, network.name()))?;
        match request {
            AddressRequest::Any => Ok(AddressReservation::Delegated {
                workload_id: workload_id.clone(),
            }),
            AddressRequest::Exact(address) => Err(NetworkProviderError::Rejected {
                message: format!(
                    "docker network `{}` owns IPAM and cannot reserve `{address}`",
                    network.name()
                ),
            }),
        }
    }

    async fn attach(
        &self,
        workload: &WorkloadHandle,
        network: &NetworkHandle,
        reservation: &AddressReservation,
    ) -> Result<NetworkAttachment, NetworkProviderError> {
        let AddressReservation::Delegated { workload_id } = reservation else {
            return Err(NetworkProviderError::Rejected {
                message: "docker networking requires a delegated address reservation".to_owned(),
            });
        };
        if workload_id != workload.workload_id() {
            return Err(NetworkProviderError::Rejected {
                message: "docker network reservation belongs to a different workload".to_owned(),
            });
        }
        let inspect = self
            .inspect_container(workload)
            .await
            .map_err(runtime_network_error)?;
        if container_attachment(&inspect, network)?.is_some() {
            return attached_container(self, workload, network).await;
        }
        let request = NetworkConnectRequest {
            container: container_id(workload)
                .map_err(runtime_network_error)?
                .to_owned(),
            endpoint_config: Some(EndpointSettings::default()),
        };
        match self.client.connect_network(network.name(), request).await {
            Ok(()) => attached_container(self, workload, network).await,
            Err(error) if is_conflict(&error) => attached_container(self, workload, network).await,
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
        _workload_id: &WorkloadId,
    ) -> Result<(), NetworkProviderError> {
        self.client
            .inspect_network(network.name(), None)
            .await
            .map(|_| ())
            .map_err(|error| network_error(error, network.name()))
    }
}

async fn attached_container(
    runtime: &DockerRuntime,
    workload: &WorkloadHandle,
    network: &NetworkHandle,
) -> Result<NetworkAttachment, NetworkProviderError> {
    let mut inspect = runtime
        .inspect_container(workload)
        .await
        .map_err(runtime_network_error)?;
    if network.name() != "bridge" && has_network(&inspect, "bridge") {
        runtime
            .client
            .disconnect_network(
                "bridge",
                NetworkDisconnectRequest {
                    container: container_id(workload)
                        .map_err(runtime_network_error)?
                        .to_owned(),
                    force: Some(false),
                },
            )
            .await
            .map_err(|error| network_error(error, "bridge"))?;
        inspect = runtime
            .inspect_container(workload)
            .await
            .map_err(runtime_network_error)?;
    }
    if let Some(attachment) = container_attachment(&inspect, network)? {
        return Ok(attachment);
    }

    // Docker with the containerd image store defers delegated IPAM until the
    // container first starts. The assignment agent starts newly attached
    // workloads immediately after this call, so activate it here only when
    // needed to obtain the address required by the network contract.
    WorkloadRuntime::start(runtime, workload)
        .await
        .map_err(runtime_network_error)?;
    let inspect = runtime
        .inspect_container(workload)
        .await
        .map_err(runtime_network_error)?;
    container_attachment(&inspect, network)?.ok_or_else(|| NetworkProviderError::Unavailable {
        message: format!(
            "docker network `{}` attached without reporting an address after activation",
            network.name()
        ),
    })
}

fn has_network(inspect: &docker::models::ContainerInspectResponse, name: &str) -> bool {
    inspect
        .network_settings
        .as_ref()
        .and_then(|settings| settings.networks.as_ref())
        .is_some_and(|networks| networks.contains_key(name))
}

pub(crate) fn network_create_request(spec: &NetworkSpec) -> NetworkCreateRequest {
    NetworkCreateRequest {
        name: spec.name.clone(),
        driver: Some("bridge".to_owned()),
        scope: Some("local".to_owned()),
        attachable: Some(true),
        labels: Some(HashMap::from([
            (NETWORK_MANAGED_LABEL.to_owned(), "true".to_owned()),
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
    if spec.name.is_empty()
        || spec.mtu_bytes == 0
        || spec.addressing != NetworkAddressing::Delegated
    {
        Err(NetworkProviderError::InvalidRange {
            message: format!(
                "docker network `{}` requires delegated IPAM and a nonzero MTU",
                spec.name
            ),
        })
    } else {
        Ok(())
    }
}

pub(crate) fn validate_existing_network(
    inspect: &NetworkInspect,
    desired: &NetworkSpec,
) -> Result<(), NetworkProviderError> {
    let managed = inspect
        .labels
        .as_ref()
        .and_then(|labels| labels.get(NETWORK_MANAGED_LABEL))
        .is_some_and(|value| value == "true");
    let name_matches = inspect.name.as_deref() == Some(desired.name.as_str());
    let driver_matches = inspect.driver.as_deref() == Some("bridge");
    let mtu_matches = inspect
        .labels
        .as_ref()
        .and_then(|labels| labels.get(NETWORK_MTU_LABEL))
        .is_some_and(|value| value == &desired.mtu_bytes.to_string());
    let delegates_ipam = inspect.labels.as_ref().is_some_and(|labels| {
        !labels.contains_key(LEGACY_NETWORK_SUBNET_LABEL)
            && !labels.contains_key(LEGACY_NETWORK_GATEWAY_LABEL)
    });
    if managed && name_matches && driver_matches && mtu_matches && delegates_ipam {
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
