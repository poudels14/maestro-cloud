use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};

use async_trait::async_trait;
use kernel_api::WorkloadId;
use sha2::{Digest, Sha256};

use crate::containerd::ContainerdRuntime;
use crate::containerd_network_linux::LinuxContainerdNetwork;
use crate::containerd_support::container_id;
use crate::{
    AddressLease, AddressRequest, NetworkAttachment, NetworkHandle, NetworkProvider,
    NetworkProviderError, NetworkSpec, RuntimeError, WorkloadHandle, WorkloadNetworkStatus,
};

const CONTAINER_INTERFACE_NAME: &str = "eth0";

#[derive(Default)]
pub(crate) struct ContainerdNetworkState {
    spec: Option<NetworkSpec>,
    owners: BTreeMap<IpAddr, WorkloadId>,
}

pub(crate) struct AttachmentPlan {
    pub(crate) bridge_name: String,
    pub(crate) host_name: String,
    pub(crate) peer_name: String,
    pub(crate) container_name: String,
    pub(crate) address: Ipv4Addr,
    pub(crate) gateway: Ipv4Addr,
    pub(crate) prefix_length: u8,
    pub(crate) mtu_bytes: u16,
}

#[async_trait]
impl NetworkProvider for ContainerdRuntime {
    async fn ensure_network(
        &self,
        spec: &NetworkSpec,
    ) -> Result<NetworkHandle, NetworkProviderError> {
        validate_spec(spec)?;
        LinuxContainerdNetwork::verify_bridge(spec).await?;
        let mut state = self.network_state.lock().await;
        match &state.spec {
            Some(current) if current != spec => Err(rejected(format!(
                "containerd network `{}` changed configuration",
                spec.name
            ))),
            Some(_) => NetworkHandle::new(&spec.name),
            None => {
                state.spec = Some(spec.clone());
                NetworkHandle::new(&spec.name)
            }
        }
    }

    async fn allocate_address(
        &self,
        network: &NetworkHandle,
        workload_id: &WorkloadId,
        request: AddressRequest,
    ) -> Result<AddressLease, NetworkProviderError> {
        let mut state = self.network_state.lock().await;
        let spec = matching_spec(&state, network)?.clone();
        if let Some((address, _)) = state.owners.iter().find(|(_, owner)| *owner == workload_id) {
            if matches!(request, AddressRequest::Any)
                || matches!(request, AddressRequest::Exact(expected) if expected == *address)
            {
                return Ok(AddressLease {
                    workload_id: workload_id.clone(),
                    address: *address,
                });
            }
            return Err(rejected(format!(
                "workload `{workload_id}` already reserved `{address}`"
            )));
        }
        let address = match request {
            AddressRequest::Exact(address) => {
                validate_address(&spec, address)?;
                address
            }
            AddressRequest::Any => first_available(&spec, &state.owners)?,
        };
        if state.owners.contains_key(&address) {
            return Err(NetworkProviderError::AddressConflict { address });
        }
        state.owners.insert(address, workload_id.clone());
        Ok(AddressLease {
            workload_id: workload_id.clone(),
            address,
        })
    }

    async fn attach(
        &self,
        workload: &WorkloadHandle,
        network: &NetworkHandle,
        lease: &AddressLease,
    ) -> Result<NetworkAttachment, NetworkProviderError> {
        let spec = {
            let state = self.network_state.lock().await;
            let spec = matching_spec(&state, network)?.clone();
            if state.owners.get(&lease.address) != Some(&lease.workload_id) {
                return Err(NetworkProviderError::AddressConflict {
                    address: lease.address,
                });
            }
            spec
        };
        let plan = attachment_plan(&spec, workload.workload_id(), lease)?;
        let container = container_id(workload, &self.settings.namespace)
            .map_err(runtime_network_error)?
            .to_owned();
        let pid = match self
            .task(&container, workload.workload_id())
            .await
            .map_err(runtime_network_error)?
        {
            Some(process) if process.pid != 0 => process.pid,
            Some(_) => {
                return Err(unavailable(format!(
                    "containerd task for workload `{}` has no process id",
                    workload.workload_id()
                )));
            }
            None => self
                .create_task(workload, &container)
                .await
                .map_err(runtime_network_error)?,
        };
        LinuxContainerdNetwork::attach(pid, &plan).await?;
        Ok(NetworkAttachment {
            network: network.clone(),
            address: lease.address,
            interface_name: Some(plan.host_name),
        })
    }

    async fn detach(
        &self,
        workload: &WorkloadHandle,
        network: &NetworkHandle,
    ) -> Result<(), NetworkProviderError> {
        let state = self.network_state.lock().await;
        matching_spec(&state, network)?;
        drop(state);
        LinuxContainerdNetwork::detach(workload.workload_id()).await
    }

    async fn inspect(
        &self,
        workload: &WorkloadHandle,
    ) -> Result<WorkloadNetworkStatus, NetworkProviderError> {
        let spec = self
            .network_state
            .lock()
            .await
            .spec
            .clone()
            .ok_or_else(|| NetworkProviderError::NetworkNotFound {
                name: "containerd workload bridge".to_owned(),
            })?;
        let container =
            container_id(workload, &self.settings.namespace).map_err(runtime_network_error)?;
        let Some(process) = self
            .task(container, workload.workload_id())
            .await
            .map_err(runtime_network_error)?
        else {
            return Ok(WorkloadNetworkStatus {
                attachments: Vec::new(),
            });
        };
        let attachment =
            LinuxContainerdNetwork::inspect(process.pid, workload.workload_id(), &spec).await?;
        Ok(WorkloadNetworkStatus {
            attachments: attachment.into_iter().collect(),
        })
    }

    async fn release_address(
        &self,
        network: &NetworkHandle,
        lease: &AddressLease,
    ) -> Result<(), NetworkProviderError> {
        let mut state = self.network_state.lock().await;
        matching_spec(&state, network)?;
        match state.owners.get(&lease.address) {
            Some(owner) if owner == &lease.workload_id => {
                state.owners.remove(&lease.address);
                Ok(())
            }
            None => Ok(()),
            Some(_) => Err(NetworkProviderError::AddressConflict {
                address: lease.address,
            }),
        }
    }
}

pub(crate) fn attachment_plan(
    spec: &NetworkSpec,
    workload_id: &WorkloadId,
    lease: &AddressLease,
) -> Result<AttachmentPlan, NetworkProviderError> {
    if lease.workload_id != *workload_id {
        return Err(NetworkProviderError::AddressConflict {
            address: lease.address,
        });
    }
    validate_address(spec, lease.address)?;
    let (IpAddr::V4(address), IpAddr::V4(gateway)) = (lease.address, spec.gateway) else {
        return Err(rejected("containerd networking requires IPv4"));
    };
    Ok(AttachmentPlan {
        bridge_name: spec.name.clone(),
        host_name: host_interface_name(workload_id),
        peer_name: peer_interface_name(workload_id),
        container_name: CONTAINER_INTERFACE_NAME.to_owned(),
        address,
        gateway,
        prefix_length: spec.range.prefix_length(),
        mtu_bytes: spec.mtu_bytes,
    })
}

pub(crate) fn host_interface_name(workload_id: &WorkloadId) -> String {
    interface_name("mh", workload_id)
}

pub(crate) fn peer_interface_name(workload_id: &WorkloadId) -> String {
    interface_name("mp", workload_id)
}

fn interface_name(prefix: &str, workload_id: &WorkloadId) -> String {
    let suffix: String = hex::encode(Sha256::digest(workload_id.as_str().as_bytes()))
        .chars()
        .take(12)
        .collect();
    format!("{prefix}{suffix}")
}

fn matching_spec<'a>(
    state: &'a ContainerdNetworkState,
    network: &NetworkHandle,
) -> Result<&'a NetworkSpec, NetworkProviderError> {
    state
        .spec
        .as_ref()
        .filter(|spec| spec.name == network.name())
        .ok_or_else(|| NetworkProviderError::NetworkNotFound {
            name: network.name().to_owned(),
        })
}

fn validate_spec(spec: &NetworkSpec) -> Result<(), NetworkProviderError> {
    if spec.name.is_empty() || spec.name.len() > 15 || spec.mtu_bytes == 0 {
        return Err(NetworkProviderError::InvalidRange {
            message: "containerd bridge name must fit Linux IFNAMSIZ and MTU must be nonzero"
                .to_owned(),
        });
    }
    let (IpAddr::V4(network), IpAddr::V4(gateway)) = (spec.range.network_address(), spec.gateway)
    else {
        return Err(rejected("containerd networking requires IPv4"));
    };
    let broadcast = broadcast(network, spec.range.prefix_length());
    if !spec.range.contains(spec.gateway) || gateway == network || gateway == broadcast {
        Err(NetworkProviderError::InvalidRange {
            message: format!("gateway `{gateway}` is not usable in `{}`", spec.name),
        })
    } else {
        Ok(())
    }
}

fn validate_address(spec: &NetworkSpec, address: IpAddr) -> Result<(), NetworkProviderError> {
    validate_spec(spec)?;
    let (IpAddr::V4(network), IpAddr::V4(gateway), IpAddr::V4(address_v4)) =
        (spec.range.network_address(), spec.gateway, address)
    else {
        return Err(rejected("containerd networking requires IPv4"));
    };
    if spec.range.contains(address)
        && address_v4 != network
        && address_v4 != gateway
        && address_v4 != broadcast(network, spec.range.prefix_length())
    {
        Ok(())
    } else {
        Err(NetworkProviderError::InvalidRange {
            message: format!("address `{address}` is not usable in `{}`", spec.name),
        })
    }
}

fn first_available(
    spec: &NetworkSpec,
    owners: &BTreeMap<IpAddr, WorkloadId>,
) -> Result<IpAddr, NetworkProviderError> {
    let IpAddr::V4(network) = spec.range.network_address() else {
        return Err(rejected("containerd networking requires IPv4"));
    };
    let mut candidate = u32::from(network).saturating_add(1);
    for _attempt in 0..owners.len().saturating_add(4) {
        let address = IpAddr::V4(Ipv4Addr::from(candidate));
        if validate_address(spec, address).is_ok() && !owners.contains_key(&address) {
            return Ok(address);
        }
        candidate = candidate.saturating_add(1);
    }
    Err(rejected(format!(
        "containerd network `{}` has no free address",
        spec.name
    )))
}

fn broadcast(network: Ipv4Addr, prefix_length: u8) -> Ipv4Addr {
    let host_bits = u32::BITS.saturating_sub(u32::from(prefix_length));
    let host_mask = u32::MAX.checked_shr(u32::BITS - host_bits).unwrap_or(0);
    Ipv4Addr::from(u32::from(network) | host_mask)
}

fn runtime_network_error(error: RuntimeError) -> NetworkProviderError {
    match error {
        RuntimeError::Unavailable { message } => unavailable(message),
        other => rejected(other.to_string()),
    }
}

fn unavailable(message: impl Into<String>) -> NetworkProviderError {
    NetworkProviderError::Unavailable {
        message: message.into(),
    }
}

fn rejected(message: impl Into<String>) -> NetworkProviderError {
    NetworkProviderError::Rejected {
        message: message.into(),
    }
}
