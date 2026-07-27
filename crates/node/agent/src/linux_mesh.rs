use std::collections::{BTreeSet, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use async_trait::async_trait;
use futures_util::TryStreamExt;
use netlink_packet_route::AddressFamily;
use netlink_packet_route::route::{
    RouteAddress, RouteAttribute, RouteHeader, RouteMessage, RouteProtocol, RouteScope, RouteType,
};
use rtnetlink::{Handle, LinkUnspec, RouteMessageBuilder};
use tokio::task::JoinHandle;
use wireguard_control::{Backend, Device, DeviceUpdate, InterfaceName, Key, PeerConfigBuilder};

use crate::{
    MESH_INTERFACE_NAME, MESH_MTU_BYTES, MeshBackend, MeshBackendError, MeshConfiguration,
    MeshPeer, MeshSubnet,
};

const IPV4_SUBNET_PREFIX_LENGTH: u8 = 24;

/// Linux kernel adapter for the node's exact WireGuard peer and route state.
#[derive(Debug, Clone, Copy, Default)]
pub struct LinuxMeshBackend;

impl LinuxMeshBackend {
    /// Creates a backend that programs the Linux kernel WireGuard interface.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl MeshBackend for LinuxMeshBackend {
    async fn apply(&self, desired: &MeshConfiguration) -> Result<(), MeshBackendError> {
        validate_desired(desired)?;

        // WireGuard's control library is synchronous. The awaited blocking task
        // may finish its one kernel transaction after caller cancellation; a
        // subsequent complete replacement remains safe and convergent.
        apply_wireguard(desired).await?;

        // Routes follow peers so traffic is never directed to a peer that has
        // not yet been admitted. Failure or cancellation can leave only an
        // intermediate subset, which the next full reconciliation repairs.
        apply_link_and_routes(desired).await
    }
}

async fn apply_wireguard(desired: &MeshConfiguration) -> Result<(), MeshBackendError> {
    let interface = desired
        .interface
        .name
        .parse::<InterfaceName>()
        .map_err(|error| backend_error("validate WireGuard interface name", error))?;
    let private_key = desired.interface.private_key.clone();
    let listen_port = desired.interface.listen_port;
    let peers = desired.peers.clone();

    tokio::task::spawn_blocking(move || {
        let backend = Backend::Kernel;
        let existing = match Device::get(&interface, backend) {
            Ok(device) => Some(device),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let desired_private_key = Key(*private_key.expose_bytes());
        let mut update = DeviceUpdate::new();
        if existing
            .as_ref()
            .and_then(|device| device.private_key.as_ref())
            != Some(&desired_private_key)
        {
            update = update.set_private_key(desired_private_key);
        }
        if existing.as_ref().and_then(|device| device.listen_port) != Some(listen_port) {
            update = update.set_listen_port(listen_port);
        }
        let existing_keys = existing
            .as_ref()
            .into_iter()
            .flat_map(|device| device.peers.iter())
            .map(|peer| peer.config.public_key.clone());
        for stale in stale_peer_keys(existing_keys, &peers) {
            update = update.remove_peer_by_key(&stale);
        }
        for peer in &peers {
            update = update.add_peer(peer_update(peer));
        }
        update.apply(&interface, backend)
    })
    .await
    .map_err(|error| backend_error("join WireGuard kernel update", error))?
    .map_err(|error| backend_error("apply WireGuard kernel update", error))
}

pub(crate) fn stale_peer_keys(
    existing: impl IntoIterator<Item = Key>,
    desired: &[MeshPeer],
) -> Vec<Key> {
    let desired_keys = desired
        .iter()
        .map(|peer| *peer.public_key.as_bytes())
        .collect::<HashSet<_>>();
    existing
        .into_iter()
        .filter(|key| !desired_keys.contains(key.as_bytes()))
        .collect()
}

fn peer_update(peer: &MeshPeer) -> PeerConfigBuilder {
    PeerConfigBuilder::new(&Key(*peer.public_key.as_bytes()))
        .set_endpoint(SocketAddr::V4(peer.endpoint))
        .replace_allowed_ips()
        .add_allowed_ip(
            IpAddr::V4(peer.allowed_subnet.network_address()),
            IPV4_SUBNET_PREFIX_LENGTH,
        )
}

async fn apply_link_and_routes(desired: &MeshConfiguration) -> Result<(), MeshBackendError> {
    let (connection, handle, _) = rtnetlink::new_connection()
        .map_err(|error| backend_error("open route netlink connection", error))?;
    let mut connection_task = AbortOnDrop::new(tokio::spawn(connection));

    let result = reconcile_link_and_routes(&handle, desired).await;
    drop(handle);
    connection_task.abort_and_wait().await;
    result
}

async fn reconcile_link_and_routes(
    handle: &Handle,
    desired: &MeshConfiguration,
) -> Result<(), MeshBackendError> {
    let mut links = handle
        .link()
        .get()
        .match_name(desired.interface.name.clone())
        .execute();
    let link = links
        .try_next()
        .await
        .map_err(|error| backend_error("find WireGuard link", error))?
        .ok_or_else(|| MeshBackendError::new("WireGuard link disappeared after configuration"))?;
    let interface_index = link.header.index;

    handle
        .link()
        .set(
            LinkUnspec::new_with_index(interface_index)
                .mtu(u32::from(desired.interface.mtu_bytes))
                .up()
                .build(),
        )
        .execute()
        .await
        .map_err(|error| backend_error("set WireGuard link MTU and state", error))?;

    let query = RouteMessageBuilder::<Ipv4Addr>::new().build();
    let existing = handle
        .route()
        .get(query)
        .execute()
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| backend_error("list workload mesh routes", error))?;
    let desired_subnets = desired
        .routes
        .iter()
        .map(|route| route.destination)
        .collect::<BTreeSet<_>>();
    let delta = route_delta(existing, interface_index, &desired_subnets);

    for stale in delta.stale {
        handle
            .route()
            .del(stale)
            .execute()
            .await
            .map_err(|error| backend_error("remove stale workload mesh route", error))?;
    }
    for missing in delta.missing {
        handle
            .route()
            .add(route_message(missing, interface_index))
            .execute()
            .await
            .map_err(|error| backend_error("add workload mesh route", error))?;
    }
    Ok(())
}

fn validate_desired(desired: &MeshConfiguration) -> Result<(), MeshBackendError> {
    if desired.interface.name != MESH_INTERFACE_NAME {
        return Err(MeshBackendError::new(format!(
            "refusing unmanaged mesh interface `{}`",
            desired.interface.name
        )));
    }
    if desired.interface.mtu_bytes != MESH_MTU_BYTES {
        return Err(MeshBackendError::new(format!(
            "mesh MTU {} does not match required {MESH_MTU_BYTES}",
            desired.interface.mtu_bytes
        )));
    }

    let peer_subnets = desired
        .peers
        .iter()
        .map(|peer| peer.allowed_subnet)
        .collect::<BTreeSet<_>>();
    if peer_subnets.len() != desired.peers.len() {
        return Err(MeshBackendError::new(
            "desired WireGuard peers contain duplicate allowed subnets",
        ));
    }
    if peer_subnets.contains(&desired.interface.local_subnet) {
        return Err(MeshBackendError::new(
            "a WireGuard peer claims the local workload subnet",
        ));
    }

    if desired
        .routes
        .iter()
        .any(|route| route.interface != MESH_INTERFACE_NAME)
    {
        return Err(MeshBackendError::new(
            "desired workload route targets an unmanaged interface",
        ));
    }
    let route_subnets = desired
        .routes
        .iter()
        .map(|route| route.destination)
        .collect::<BTreeSet<_>>();
    if route_subnets.len() != desired.routes.len() || route_subnets != peer_subnets {
        return Err(MeshBackendError::new(
            "desired workload routes do not match WireGuard allowed subnets",
        ));
    }
    Ok(())
}

pub(crate) struct RouteDelta {
    pub(crate) stale: Vec<RouteMessage>,
    pub(crate) missing: Vec<MeshSubnet>,
}

pub(crate) fn route_delta(
    existing: Vec<RouteMessage>,
    interface_index: u32,
    desired: &BTreeSet<MeshSubnet>,
) -> RouteDelta {
    let mut present = BTreeSet::new();
    let mut stale = Vec::new();
    for message in existing {
        let Some(subnet) = managed_subnet(&message, interface_index) else {
            continue;
        };
        if !desired.contains(&subnet) || !present.insert(subnet) {
            stale.push(message);
        }
    }
    let missing = desired.difference(&present).copied().collect();
    RouteDelta { stale, missing }
}

pub(crate) fn managed_subnet(message: &RouteMessage, interface_index: u32) -> Option<MeshSubnet> {
    let header = &message.header;
    if header.address_family != AddressFamily::Inet
        || header.destination_prefix_length != IPV4_SUBNET_PREFIX_LENGTH
        || header.source_prefix_length != 0
        || header.table != RouteHeader::RT_TABLE_MAIN
        || header.protocol != RouteProtocol::Static
        || header.scope != RouteScope::Link
        || header.kind != RouteType::Unicast
    {
        return None;
    }

    let on_interface = message.attributes.iter().any(
        |attribute| matches!(attribute, RouteAttribute::Oif(index) if *index == interface_index),
    );
    if !on_interface {
        return None;
    }
    message.attributes.iter().find_map(|attribute| {
        let RouteAttribute::Destination(RouteAddress::Inet(network)) = attribute else {
            return None;
        };
        format!("{network}/{IPV4_SUBNET_PREFIX_LENGTH}")
            .parse()
            .ok()
    })
}

pub(crate) fn route_message(subnet: MeshSubnet, interface_index: u32) -> RouteMessage {
    RouteMessageBuilder::<Ipv4Addr>::new()
        .destination_prefix(subnet.network_address(), IPV4_SUBNET_PREFIX_LENGTH)
        .output_interface(interface_index)
        .scope(RouteScope::Link)
        .build()
}

fn backend_error(action: &str, error: impl std::fmt::Display) -> MeshBackendError {
    MeshBackendError::new(format!("failed to {action}: {error}"))
}

pub(crate) struct AbortOnDrop<Output> {
    handle: JoinHandle<Output>,
}

impl<Output> AbortOnDrop<Output> {
    pub(crate) fn new(handle: JoinHandle<Output>) -> Self {
        Self { handle }
    }

    pub(crate) async fn abort_and_wait(&mut self) {
        self.handle.abort();
        let _result = (&mut self.handle).await;
    }
}

impl<Output> Drop for AbortOnDrop<Output> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
