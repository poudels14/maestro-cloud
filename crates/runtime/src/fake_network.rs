use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use kernel_api::WorkloadId;

use crate::{
    AddressLease, AddressRequest, AddressReservation, NetworkAddressing, NetworkAttachment,
    NetworkCidr, NetworkHandle, NetworkProvider, NetworkProviderError, NetworkSpec, WorkloadHandle,
    WorkloadNetworkStatus,
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

    async fn reconcile_address_owners(
        &self,
        network: &NetworkHandle,
        active_workload_ids: &BTreeSet<WorkloadId>,
    ) -> Result<usize, NetworkProviderError> {
        let mut state = self.lock()?;
        validate_network(&state, network)?;
        let previous = state.leases.len();
        state
            .leases
            .retain(|owner, _| active_workload_ids.contains(owner));
        Ok(previous.saturating_sub(state.leases.len()))
    }

    async fn allocate_address(
        &self,
        network: &NetworkHandle,
        workload_id: &WorkloadId,
        request: AddressRequest,
    ) -> Result<AddressReservation, NetworkProviderError> {
        let mut state = self.lock()?;
        let spec = validate_network(&state, network)?.clone();
        if spec.addressing == NetworkAddressing::Delegated {
            return match request {
                AddressRequest::Any => Ok(AddressReservation::Delegated {
                    workload_id: workload_id.clone(),
                }),
                AddressRequest::Exact(address) => Err(NetworkProviderError::Rejected {
                    message: format!(
                        "delegated fake network `{}` cannot reserve `{address}`",
                        spec.name
                    ),
                }),
            };
        }
        if let Some(address) = state.leases.get(workload_id).copied() {
            if matches!(request, AddressRequest::Any)
                || matches!(request, AddressRequest::Exact(expected) if expected == address)
            {
                return Ok(AddressReservation::Exact(AddressLease {
                    workload_id: workload_id.clone(),
                    address,
                }));
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
        Ok(AddressReservation::Exact(AddressLease {
            workload_id: workload_id.clone(),
            address,
        }))
    }

    async fn attach(
        &self,
        workload: &WorkloadHandle,
        network: &NetworkHandle,
        reservation: &AddressReservation,
    ) -> Result<NetworkAttachment, NetworkProviderError> {
        let mut state = self.lock()?;
        let spec = validate_network(&state, network)?.clone();
        if reservation.workload_id() != workload.workload_id() {
            return Err(NetworkProviderError::Rejected {
                message: "fake network reservation belongs to another workload".to_owned(),
            });
        }
        let address = match reservation {
            AddressReservation::Exact(lease) => {
                if state.leases.get(workload.workload_id()) != Some(&lease.address) {
                    return Err(NetworkProviderError::AddressConflict {
                        address: lease.address,
                    });
                }
                lease.address
            }
            AddressReservation::Delegated { .. }
                if spec.addressing == NetworkAddressing::Delegated =>
            {
                if let Some(attachment) = state.attachments.get(workload.workload_id()) {
                    return Ok(attachment.clone());
                }
                let address = first_delegated(&state.leases)?;
                state.leases.insert(workload.workload_id().clone(), address);
                address
            }
            AddressReservation::Delegated { .. } => {
                return Err(NetworkProviderError::Rejected {
                    message: "managed fake network requires an exact reservation".to_owned(),
                });
            }
        };
        let attachment = NetworkAttachment {
            network: network.clone(),
            address,
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
        workload_id: &WorkloadId,
    ) -> Result<(), NetworkProviderError> {
        let mut state = self.lock()?;
        validate_network(&state, network)?;
        state.leases.remove(workload_id);
        Ok(())
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
    let (range, gateway) = managed_addressing(spec)?;
    if range.contains(address)
        && address != range.network_address()
        && address != gateway
        && !is_broadcast(range, address)
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
    let (range, _gateway) = managed_addressing(spec)?;
    let mut candidate = next_address(range.network_address());
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

fn first_delegated(leases: &BTreeMap<WorkloadId, IpAddr>) -> Result<IpAddr, NetworkProviderError> {
    (2_u8..=254)
        .map(|suffix| IpAddr::V4(Ipv4Addr::new(192, 0, 2, suffix)))
        .find(|address| !leases.values().any(|leased| leased == address))
        .ok_or_else(|| NetworkProviderError::Rejected {
            message: "delegated fake network has no free address".to_owned(),
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

fn is_broadcast(range: NetworkCidr, address: IpAddr) -> bool {
    match (range.network_address(), address) {
        (IpAddr::V4(network), IpAddr::V4(address)) => {
            let host_bits = u32::BITS.saturating_sub(u32::from(range.prefix_length()));
            let host_mask = u32::MAX.checked_shr(u32::BITS - host_bits).unwrap_or(0);
            u32::from(address) == u32::from(network) | host_mask
        }
        _ => false,
    }
}

fn managed_addressing(spec: &NetworkSpec) -> Result<(NetworkCidr, IpAddr), NetworkProviderError> {
    match spec.addressing {
        NetworkAddressing::Managed { range, gateway } => Ok((range, gateway)),
        NetworkAddressing::Delegated => Err(NetworkProviderError::Rejected {
            message: "delegated fake network does not expose managed IPAM state".to_owned(),
        }),
    }
}
