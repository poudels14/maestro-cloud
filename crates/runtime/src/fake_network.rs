use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use kernel_api::WorkloadId;

use crate::{
    AddressLease, AddressRequest, NetworkAttachment, NetworkHandle, NetworkProvider,
    NetworkProviderError, NetworkSpec, WorkloadHandle, WorkloadNetworkStatus,
};

/// Deterministic in-memory network provider for reconciler and composition tests.
#[derive(Default)]
pub struct FakeNetworkProvider {
    state: Mutex<FakeNetworkState>,
}

#[derive(Default)]
struct FakeNetworkState {
    network: Option<NetworkSpec>,
    leases: BTreeMap<WorkloadId, IpAddr>,
    attachments: BTreeMap<WorkloadId, NetworkAttachment>,
}

impl FakeNetworkProvider {
    /// Returns the number of currently owned address reservations.
    pub fn lease_count(&self) -> usize {
        self.state
            .lock()
            .map(|state| state.leases.len())
            .unwrap_or_default()
    }

    /// Returns the number of currently attached workloads.
    pub fn attachment_count(&self) -> usize {
        self.state
            .lock()
            .map(|state| state.attachments.len())
            .unwrap_or_default()
    }

    fn lock(&self) -> Result<MutexGuard<'_, FakeNetworkState>, NetworkProviderError> {
        self.state
            .lock()
            .map_err(|_| NetworkProviderError::Unavailable {
                message: "fake network lock was poisoned".to_owned(),
            })
    }
}

#[async_trait]
impl NetworkProvider for FakeNetworkProvider {
    async fn ensure_network(
        &self,
        spec: &NetworkSpec,
    ) -> Result<NetworkHandle, NetworkProviderError> {
        let mut state = self.lock()?;
        match &state.network {
            Some(existing) if existing != spec => {
                return Err(NetworkProviderError::Rejected {
                    message: "fake network already has different desired state".to_owned(),
                });
            }
            Some(_) => {}
            None => state.network = Some(spec.clone()),
        }
        NetworkHandle::new(&spec.name)
    }

    async fn allocate_address(
        &self,
        network: &NetworkHandle,
        workload_id: &WorkloadId,
        request: AddressRequest,
    ) -> Result<AddressLease, NetworkProviderError> {
        let mut state = self.lock()?;
        let spec = validate_network(&state, network)?.clone();
        if let Some(address) = state.leases.get(workload_id).copied() {
            if matches!(request, AddressRequest::Any)
                || matches!(request, AddressRequest::Exact(expected) if expected == address)
            {
                return Ok(AddressLease {
                    workload_id: workload_id.clone(),
                    address,
                });
            }
            return Err(NetworkProviderError::Rejected {
                message: format!("workload `{workload_id}` already reserved `{address}`"),
            });
        }
        let address = match request {
            AddressRequest::Exact(address) => {
                validate_address(&spec, address)?;
                address
            }
            AddressRequest::Any => first_available(&spec, &state.leases)?,
        };
        if state.leases.values().any(|reserved| *reserved == address) {
            return Err(NetworkProviderError::AddressConflict { address });
        }
        state.leases.insert(workload_id.clone(), address);
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
        let mut state = self.lock()?;
        validate_network(&state, network)?;
        if state.leases.get(workload.workload_id()) != Some(&lease.address)
            || lease.workload_id != *workload.workload_id()
        {
            return Err(NetworkProviderError::AddressConflict {
                address: lease.address,
            });
        }
        let attachment = NetworkAttachment {
            network: network.clone(),
            address: lease.address,
            interface_name: Some("eth0".to_owned()),
        };
        state
            .attachments
            .insert(workload.workload_id().clone(), attachment.clone());
        Ok(attachment)
    }

    async fn detach(
        &self,
        workload: &WorkloadHandle,
        network: &NetworkHandle,
    ) -> Result<(), NetworkProviderError> {
        let mut state = self.lock()?;
        validate_network(&state, network)?;
        state.attachments.remove(workload.workload_id());
        Ok(())
    }

    async fn inspect(
        &self,
        workload: &WorkloadHandle,
    ) -> Result<WorkloadNetworkStatus, NetworkProviderError> {
        let state = self.lock()?;
        Ok(WorkloadNetworkStatus {
            attachments: state
                .attachments
                .get(workload.workload_id())
                .cloned()
                .into_iter()
                .collect(),
        })
    }

    async fn release_address(
        &self,
        network: &NetworkHandle,
        lease: &AddressLease,
    ) -> Result<(), NetworkProviderError> {
        let mut state = self.lock()?;
        validate_network(&state, network)?;
        match state.leases.get(&lease.workload_id) {
            Some(address) if *address == lease.address => {
                state.leases.remove(&lease.workload_id);
                Ok(())
            }
            None => Ok(()),
            Some(_) => Err(NetworkProviderError::AddressConflict {
                address: lease.address,
            }),
        }
    }
}

fn validate_network<'a>(
    state: &'a FakeNetworkState,
    network: &NetworkHandle,
) -> Result<&'a NetworkSpec, NetworkProviderError> {
    state
        .network
        .as_ref()
        .filter(|configured| configured.name == network.name())
        .ok_or_else(|| NetworkProviderError::NetworkNotFound {
            name: network.name().to_owned(),
        })
}

fn validate_address(spec: &NetworkSpec, address: IpAddr) -> Result<(), NetworkProviderError> {
    if spec.range.contains(address)
        && address != spec.range.network_address()
        && address != spec.gateway
        && !is_broadcast(spec, address)
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
    leases: &BTreeMap<WorkloadId, IpAddr>,
) -> Result<IpAddr, NetworkProviderError> {
    let mut candidate = next_address(spec.range.network_address());
    for _attempt in 0..leases.len().saturating_add(4) {
        if let Some(address) = candidate {
            if validate_address(spec, address).is_ok()
                && !leases.values().any(|reserved| *reserved == address)
            {
                return Ok(address);
            }
            candidate = next_address(address);
        }
    }
    Err(NetworkProviderError::Rejected {
        message: format!("fake network `{}` has no free address", spec.name),
    })
}

fn next_address(address: IpAddr) -> Option<IpAddr> {
    match address {
        IpAddr::V4(address) => u32::from(address)
            .checked_add(1)
            .map(Ipv4Addr::from)
            .map(IpAddr::V4),
        IpAddr::V6(address) => u128::from(address)
            .checked_add(1)
            .map(Ipv6Addr::from)
            .map(IpAddr::V6),
    }
}

fn is_broadcast(spec: &NetworkSpec, address: IpAddr) -> bool {
    match (spec.range.network_address(), address) {
        (IpAddr::V4(network), IpAddr::V4(address)) => {
            let host_bits = u32::BITS.saturating_sub(u32::from(spec.range.prefix_length()));
            let host_mask = u32::MAX.checked_shr(u32::BITS - host_bits).unwrap_or(0);
            u32::from(address) == u32::from(network) | host_mask
        }
        _ => false,
    }
}
