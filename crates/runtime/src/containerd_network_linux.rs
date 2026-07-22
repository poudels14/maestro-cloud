use std::fs::File;
use std::net::{IpAddr, Ipv4Addr};

use futures_util::TryStreamExt;
use netlink_packet_route::address::{AddressAttribute, AddressMessage};
use netlink_packet_route::link::{InfoKind, LinkAttribute, LinkInfo, LinkMessage};
use netlink_packet_route::route::{RouteAddress, RouteAttribute, RouteMessage};
use nix::sched::{CloneFlags, setns};
use rtnetlink::{Handle, LinkUnspec, LinkVeth, RouteMessageBuilder};
use tokio::task::JoinHandle;

use kernel_api::WorkloadId;

use crate::containerd_network::{AttachmentPlan, host_interface_name};
use crate::{NetworkAttachment, NetworkHandle, NetworkProviderError, NetworkSpec};

pub(crate) struct LinuxContainerdNetwork;

impl LinuxContainerdNetwork {
    pub(crate) async fn verify_bridge(spec: &NetworkSpec) -> Result<(), NetworkProviderError> {
        let connection = RouteConnection::host()?;
        let result = async {
            let bridge = find_link(&connection.handle, &spec.name)
                .await?
                .ok_or_else(|| NetworkProviderError::NetworkNotFound {
                    name: spec.name.clone(),
                })?;
            if is_link_kind(&bridge, InfoKind::Bridge) {
                Ok(())
            } else {
                Err(rejected(format!(
                    "managed network `{}` is not a Linux bridge",
                    spec.name
                )))
            }
        }
        .await;
        finish(connection, result).await
    }

    pub(crate) async fn attach(
        pid: u32,
        plan: &AttachmentPlan,
    ) -> Result<(), NetworkProviderError> {
        if pid == 0 {
            return Err(rejected("cannot attach a task without a process id"));
        }
        // Every mutation below is level-triggered: cancellation may leave a
        // partial veth, and the next attachment pass resumes from that state.
        let host = RouteConnection::host()?;
        let host_result = prepare_host(&host.handle, pid, plan).await;
        finish(host, host_result).await?;

        let namespace = RouteConnection::namespace(pid).await?;
        let namespace_result = configure_namespace(&namespace.handle, plan).await;
        finish(namespace, namespace_result).await
    }

    pub(crate) async fn inspect(
        pid: u32,
        workload_id: &WorkloadId,
        spec: &NetworkSpec,
    ) -> Result<Option<NetworkAttachment>, NetworkProviderError> {
        let host_name = host_interface_name(workload_id);
        let host = RouteConnection::host()?;
        let exists = find_link(&host.handle, &host_name).await?.is_some();
        finish(host, Ok(())).await?;
        if !exists || pid == 0 {
            return Ok(None);
        }
        let namespace = RouteConnection::namespace(pid).await?;
        let result = inspect_namespace(&namespace.handle, "eth0").await;
        let address = finish(namespace, result).await?;
        address
            .map(|address| {
                Ok(NetworkAttachment {
                    network: NetworkHandle::new(&spec.name)?,
                    address: IpAddr::V4(address),
                    interface_name: Some(host_name),
                })
            })
            .transpose()
    }

    pub(crate) async fn detach(workload_id: &WorkloadId) -> Result<(), NetworkProviderError> {
        let connection = RouteConnection::host()?;
        let result = async {
            if let Some(link) =
                find_link(&connection.handle, &host_interface_name(workload_id)).await?
            {
                connection
                    .handle
                    .link()
                    .del(link.header.index)
                    .execute()
                    .await
                    .map_err(|error| network_error("delete workload veth", error))?;
            }
            Ok(())
        }
        .await;
        finish(connection, result).await
    }
}

async fn prepare_host(
    handle: &Handle,
    pid: u32,
    plan: &AttachmentPlan,
) -> Result<(), NetworkProviderError> {
    let bridge = find_link(handle, &plan.bridge_name).await?.ok_or_else(|| {
        NetworkProviderError::NetworkNotFound {
            name: plan.bridge_name.clone(),
        }
    })?;
    if find_link(handle, &plan.host_name).await?.is_none() {
        handle
            .link()
            .add(LinkVeth::new(&plan.host_name, &plan.peer_name).build())
            .execute()
            .await
            .map_err(|error| network_error("create workload veth pair", error))?;
    }
    let host = find_link(handle, &plan.host_name)
        .await?
        .ok_or_else(|| unavailable("workload veth disappeared after creation"))?;
    if !is_link_kind(&host, InfoKind::Veth) {
        return Err(rejected(format!(
            "managed interface `{}` exists but is not a veth",
            plan.host_name
        )));
    }
    handle
        .link()
        .set(
            LinkUnspec::new_with_index(host.header.index)
                .controller(bridge.header.index)
                .mtu(u32::from(plan.mtu_bytes))
                .up()
                .build(),
        )
        .execute()
        .await
        .map_err(|error| network_error("attach host veth to workload bridge", error))?;
    if let Some(peer) = find_link(handle, &plan.peer_name).await? {
        handle
            .link()
            .set(
                LinkUnspec::new_with_index(peer.header.index)
                    .mtu(u32::from(plan.mtu_bytes))
                    .setns_by_pid(pid)
                    .build(),
            )
            .execute()
            .await
            .map_err(|error| network_error("move workload veth into task namespace", error))?;
    }
    Ok(())
}

async fn configure_namespace(
    handle: &Handle,
    plan: &AttachmentPlan,
) -> Result<(), NetworkProviderError> {
    let link = match find_link(handle, &plan.container_name).await? {
        Some(link) => link,
        None => find_link(handle, &plan.peer_name)
            .await?
            .ok_or_else(|| unavailable("task namespace is missing its workload veth"))?,
    };
    handle
        .link()
        .set(
            LinkUnspec::new_with_index(link.header.index)
                .name(plan.container_name.clone())
                .mtu(u32::from(plan.mtu_bytes))
                .up()
                .build(),
        )
        .execute()
        .await
        .map_err(|error| network_error("configure workload namespace veth", error))?;
    if let Some(loopback) = find_link(handle, "lo").await? {
        handle
            .link()
            .set(
                LinkUnspec::new_with_index(loopback.header.index)
                    .up()
                    .build(),
            )
            .execute()
            .await
            .map_err(|error| network_error("enable workload loopback", error))?;
    }
    reconcile_addresses(handle, link.header.index, plan).await?;
    reconcile_default_route(handle, link.header.index, plan.gateway).await
}

async fn reconcile_addresses(
    handle: &Handle,
    interface_index: u32,
    plan: &AttachmentPlan,
) -> Result<(), NetworkProviderError> {
    let addresses = handle
        .address()
        .get()
        .set_link_index_filter(interface_index)
        .execute()
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| network_error("list workload interface addresses", error))?;
    let mut desired_present = false;
    for address in addresses {
        let ipv4 = address_ipv4(&address);
        if ipv4 == Some(plan.address)
            && address.header.prefix_len == plan.prefix_length
            && !desired_present
        {
            desired_present = true;
        } else if ipv4.is_some() {
            handle
                .address()
                .del(address)
                .execute()
                .await
                .map_err(|error| network_error("remove stale workload address", error))?;
        }
    }
    if !desired_present {
        handle
            .address()
            .add(
                interface_index,
                IpAddr::V4(plan.address),
                plan.prefix_length,
            )
            .execute()
            .await
            .map_err(|error| network_error("add workload address", error))?;
    }
    Ok(())
}

async fn reconcile_default_route(
    handle: &Handle,
    interface_index: u32,
    gateway: Ipv4Addr,
) -> Result<(), NetworkProviderError> {
    let routes = handle
        .route()
        .get(RouteMessageBuilder::<Ipv4Addr>::new().build())
        .execute()
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| network_error("list workload routes", error))?;
    let mut desired_present = false;
    for route in routes.into_iter().filter(is_default_route) {
        if route_gateway(&route) == Some(gateway)
            && route_output_interface(&route) == Some(interface_index)
            && !desired_present
        {
            desired_present = true;
        } else {
            handle
                .route()
                .del(route)
                .execute()
                .await
                .map_err(|error| network_error("remove stale workload default route", error))?;
        }
    }
    if !desired_present {
        handle
            .route()
            .add(
                RouteMessageBuilder::<Ipv4Addr>::new()
                    .gateway(gateway)
                    .output_interface(interface_index)
                    .build(),
            )
            .execute()
            .await
            .map_err(|error| network_error("add workload default route", error))?;
    }
    Ok(())
}

async fn inspect_namespace(
    handle: &Handle,
    interface_name: &str,
) -> Result<Option<Ipv4Addr>, NetworkProviderError> {
    let Some(link) = find_link(handle, interface_name).await? else {
        return Ok(None);
    };
    let addresses = handle
        .address()
        .get()
        .set_link_index_filter(link.header.index)
        .execute()
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| network_error("inspect workload address", error))?;
    Ok(addresses.iter().find_map(address_ipv4))
}

async fn find_link(
    handle: &Handle,
    name: &str,
) -> Result<Option<LinkMessage>, NetworkProviderError> {
    let links = handle
        .link()
        .get()
        .execute()
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| network_error("list network interfaces", error))?;
    Ok(links.into_iter().find(|link| {
        link.attributes
            .iter()
            .any(|attribute| matches!(attribute, LinkAttribute::IfName(found) if found == name))
    }))
}

fn address_ipv4(address: &AddressMessage) -> Option<Ipv4Addr> {
    address
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            AddressAttribute::Address(IpAddr::V4(address))
            | AddressAttribute::Local(IpAddr::V4(address)) => Some(*address),
            _ => None,
        })
}

fn is_link_kind(link: &LinkMessage, kind: InfoKind) -> bool {
    link.attributes.iter().any(|attribute| {
        matches!(
            attribute,
            LinkAttribute::LinkInfo(info)
                if info.iter().any(|entry| matches!(entry, LinkInfo::Kind(actual) if *actual == kind))
        )
    })
}

fn is_default_route(route: &RouteMessage) -> bool {
    route.header.destination_prefix_length == 0
}

fn route_gateway(route: &RouteMessage) -> Option<Ipv4Addr> {
    route
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            RouteAttribute::Gateway(RouteAddress::Inet(address)) => Some(*address),
            _ => None,
        })
}

fn route_output_interface(route: &RouteMessage) -> Option<u32> {
    route
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            RouteAttribute::Oif(index) => Some(*index),
            _ => None,
        })
}

struct RouteConnection {
    handle: Handle,
    task: Option<JoinHandle<()>>,
}

impl RouteConnection {
    fn host() -> Result<Self, NetworkProviderError> {
        let (connection, handle, _) = rtnetlink::new_connection()
            .map_err(|error| network_error("open host route netlink", error))?;
        Ok(Self {
            handle,
            task: Some(tokio::spawn(connection)),
        })
    }

    async fn namespace(pid: u32) -> Result<Self, NetworkProviderError> {
        let runtime = tokio::runtime::Handle::current();
        let thread = std::thread::Builder::new()
            .name(format!("maestro-netns-{pid}"))
            .spawn(move || {
                let _runtime_context = runtime.enter();
                let target = File::open(format!("/proc/{pid}/ns/net"))
                    .map_err(|error| network_error("open task network namespace", error))?;
                setns(&target, CloneFlags::CLONE_NEWNET)
                    .map_err(|error| network_error("enter task network namespace", error))?;
                let (connection, handle, _) = rtnetlink::new_connection()
                    .map_err(|error| network_error("open task route netlink", error))?;
                Ok(Self {
                    handle,
                    task: Some(runtime.spawn(connection)),
                })
            })
            .map_err(|error| unavailable(format!("start namespace setup thread: {error}")))?;
        tokio::task::spawn_blocking(move || thread.join())
            .await
            .map_err(|error| unavailable(format!("namespace join task failed: {error}")))?
            .map_err(|_| unavailable("namespace setup thread panicked"))?
    }

    async fn shutdown(mut self) -> Result<(), NetworkProviderError> {
        if let Some(task) = self.task.take() {
            task.abort();
            match task.await {
                Err(error) if error.is_cancelled() => Ok(()),
                Ok(()) => Ok(()),
                Err(error) => Err(unavailable(format!("route netlink task failed: {error}"))),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for RouteConnection {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn finish<T>(
    connection: RouteConnection,
    result: Result<T, NetworkProviderError>,
) -> Result<T, NetworkProviderError> {
    let shutdown = connection.shutdown().await;
    match (result, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn network_error(action: &str, error: impl std::fmt::Display) -> NetworkProviderError {
    unavailable(format!("failed to {action}: {error}"))
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
