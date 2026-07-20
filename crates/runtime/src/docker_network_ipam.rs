use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use docker::models::NetworkInspect;
use kernel_api::WorkloadId;

use crate::{AddressLease, AddressRequest, NetworkCidr, NetworkProviderError, NetworkSpec};

/// Node-local reservations shared by clones and reconstructed from Docker attachments on restart.
#[derive(Default)]
pub(crate) struct DockerNetworkState {
    networks: BTreeMap<String, NetworkReservations>,
}

struct NetworkReservations {
    spec: NetworkSpec,
    owners: BTreeMap<IpAddr, ReservationOwner>,
}

#[derive(Clone, PartialEq, Eq)]
enum ReservationOwner {
    Workload(WorkloadId),
    External,
}

pub(crate) fn reconcile_network(
    state: &mut DockerNetworkState,
    spec: &NetworkSpec,
    inspect: &NetworkInspect,
) -> Result<(), NetworkProviderError> {
    let observed = observed_reservations(inspect)?;
    let reservations =
        state
            .networks
            .entry(spec.name.clone())
            .or_insert_with(|| NetworkReservations {
                spec: spec.clone(),
                owners: BTreeMap::new(),
            });
    if reservations.spec != *spec {
        return Err(NetworkProviderError::Rejected {
            message: format!("docker network `{}` changed IPAM configuration", spec.name),
        });
    }
    for (address, owner) in observed {
        match reservations.owners.get(&address) {
            Some(existing) if existing != &owner => {
                return Err(NetworkProviderError::AddressConflict { address });
            }
            _ => {
                reservations.owners.insert(address, owner);
            }
        }
    }
    Ok(())
}

pub(crate) fn reserve_address(
    state: &mut DockerNetworkState,
    network_name: &str,
    workload_id: &WorkloadId,
    request: AddressRequest,
) -> Result<AddressLease, NetworkProviderError> {
    let reservations = state.networks.get_mut(network_name).ok_or_else(|| {
        NetworkProviderError::NetworkNotFound {
            name: network_name.to_owned(),
        }
    })?;
    if let Some(address) = reservations.owners.iter().find_map(|(address, owner)| {
        (owner == &ReservationOwner::Workload(workload_id.clone())).then_some(*address)
    }) {
        let matches_existing = match request {
            AddressRequest::Any => true,
            AddressRequest::Exact(expected) => expected == address,
        };
        if matches_existing {
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
            validate_candidate(&reservations.spec, address)?;
            address
        }
        AddressRequest::Any => first_available(reservations)?,
    };
    if reservations.owners.contains_key(&address) {
        return Err(NetworkProviderError::AddressConflict { address });
    }
    reservations
        .owners
        .insert(address, ReservationOwner::Workload(workload_id.clone()));
    Ok(AddressLease {
        workload_id: workload_id.clone(),
        address,
    })
}

pub(crate) fn release_address(
    state: &mut DockerNetworkState,
    network_name: &str,
    lease: &AddressLease,
) -> Result<(), NetworkProviderError> {
    let reservations = state.networks.get_mut(network_name).ok_or_else(|| {
        NetworkProviderError::NetworkNotFound {
            name: network_name.to_owned(),
        }
    })?;
    match reservations.owners.get(&lease.address) {
        Some(ReservationOwner::Workload(owner)) if owner == &lease.workload_id => {
            reservations.owners.remove(&lease.address);
            Ok(())
        }
        None => Ok(()),
        Some(_) => Err(NetworkProviderError::AddressConflict {
            address: lease.address,
        }),
    }
}

pub(crate) fn lease_is_reserved(
    state: &DockerNetworkState,
    network_name: &str,
    lease: &AddressLease,
) -> bool {
    state
        .networks
        .get(network_name)
        .and_then(|reservations| reservations.owners.get(&lease.address))
        == Some(&ReservationOwner::Workload(lease.workload_id.clone()))
}

pub(crate) fn parse_cidr(value: &str) -> Result<NetworkCidr, NetworkProviderError> {
    let (address, prefix) =
        value
            .split_once('/')
            .ok_or_else(|| NetworkProviderError::Rejected {
                message: format!("docker network subnet `{value}` is not CIDR notation"),
            })?;
    let address = address
        .parse()
        .map_err(|error| NetworkProviderError::Rejected {
            message: format!("docker network subnet `{value}` has an invalid address: {error}"),
        })?;
    let prefix = prefix
        .parse()
        .map_err(|error| NetworkProviderError::Rejected {
            message: format!("docker network subnet `{value}` has an invalid prefix: {error}"),
        })?;
    NetworkCidr::new(address, prefix)
}

pub(crate) fn cidr_text(range: NetworkCidr) -> String {
    format!("{}/{}", range.network_address(), range.prefix_length())
}

pub(crate) fn gateway_is_usable(range: NetworkCidr, gateway: IpAddr) -> bool {
    range.contains(gateway) && gateway != range.network_address() && !is_broadcast(range, gateway)
}

fn observed_reservations(
    inspect: &NetworkInspect,
) -> Result<BTreeMap<IpAddr, ReservationOwner>, NetworkProviderError> {
    let mut reservations = BTreeMap::new();
    for endpoint in inspect.containers.clone().unwrap_or_default().into_values() {
        let owner = endpoint
            .name
            .as_deref()
            .and_then(|name| name.strip_prefix("maestro-"))
            .and_then(|value| WorkloadId::new(value.to_owned()).ok())
            .map_or(ReservationOwner::External, ReservationOwner::Workload);
        for address in [endpoint.ipv4_address, endpoint.ipv6_address]
            .into_iter()
            .flatten()
        {
            let address = address_without_prefix(&address)?;
            if reservations.insert(address, owner.clone()).is_some() {
                return Err(NetworkProviderError::AddressConflict { address });
            }
        }
    }
    Ok(reservations)
}

fn first_available(reservations: &NetworkReservations) -> Result<IpAddr, NetworkProviderError> {
    let mut candidate = next_address(reservations.spec.range.network_address());
    let attempts = reservations.owners.len().saturating_add(4);
    for _attempt in 0..attempts {
        if let Some(address) = candidate {
            if validate_candidate(&reservations.spec, address).is_ok()
                && !reservations.owners.contains_key(&address)
            {
                return Ok(address);
            }
            candidate = next_address(address);
        }
    }
    Err(NetworkProviderError::Rejected {
        message: format!(
            "docker network `{}` has no free addresses",
            reservations.spec.name
        ),
    })
}

fn validate_candidate(spec: &NetworkSpec, address: IpAddr) -> Result<(), NetworkProviderError> {
    if spec.range.contains(address)
        && address != spec.range.network_address()
        && address != spec.gateway
        && !is_broadcast(spec.range, address)
    {
        Ok(())
    } else {
        Err(NetworkProviderError::InvalidRange {
            message: format!("address `{address}` is not usable in `{}`", spec.name),
        })
    }
}

fn address_without_prefix(value: &str) -> Result<IpAddr, NetworkProviderError> {
    value
        .split_once('/')
        .map_or(value, |(address, _prefix)| address)
        .parse()
        .map_err(|error| NetworkProviderError::Rejected {
            message: format!("docker endpoint address `{value}` is invalid: {error}"),
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
