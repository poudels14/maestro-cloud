use std::net::{IpAddr, Ipv4Addr};

use async_trait::async_trait;
use futures_util::TryStreamExt;
use netlink_packet_route::address::{AddressAttribute, AddressMessage};
use netlink_packet_route::link::{InfoKind, LinkAttribute, LinkInfo, LinkMessage};
use rtnetlink::{Handle, LinkBridge, LinkUnspec};

use crate::linux_mesh::AbortOnDrop;
use crate::{
    WORKLOAD_BRIDGE_NAME, WorkloadBridge, WorkloadBridgeBackend, WorkloadBridgeBackendError,
};

/// Linux route-netlink adapter for the Maestro-owned workload bridge.
#[derive(Debug, Clone, Copy, Default)]
pub struct LinuxWorkloadBridgeBackend;

impl LinuxWorkloadBridgeBackend {
    /// Creates a backend that programs the host network namespace.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl WorkloadBridgeBackend for LinuxWorkloadBridgeBackend {
    async fn apply(&self, desired: &WorkloadBridge) -> Result<(), WorkloadBridgeBackendError> {
        if desired.name != WORKLOAD_BRIDGE_NAME {
            return Err(WorkloadBridgeBackendError::new(format!(
                "refusing unmanaged workload bridge `{}`",
                desired.name
            )));
        }
        let (connection, handle, _) = rtnetlink::new_connection()
            .map_err(|error| backend_error("open route netlink connection", error))?;
        let mut connection_task = AbortOnDrop::new(tokio::spawn(connection));
        let result = reconcile(&handle, desired).await;
        drop(handle);
        connection_task.abort_and_wait().await;
        result
    }
}

async fn reconcile(
    handle: &Handle,
    desired: &WorkloadBridge,
) -> Result<(), WorkloadBridgeBackendError> {
    let link = match find_link(handle, &desired.name).await? {
        Some(link) => link,
        None => {
            let create = handle
                .link()
                .add(LinkBridge::new(&desired.name).build())
                .execute()
                .await;
            match find_link(handle, &desired.name).await? {
                Some(link) => link,
                None => {
                    return Err(match create {
                        Ok(()) => WorkloadBridgeBackendError::new(
                            "workload bridge disappeared immediately after creation",
                        ),
                        Err(error) => backend_error("create workload bridge", error),
                    });
                }
            }
        }
    };
    validate_bridge(&link)?;
    let index = link.header.index;
    handle
        .link()
        .set(
            LinkUnspec::new_with_index(index)
                .mtu(u32::from(desired.mtu_bytes))
                .up()
                .build(),
        )
        .execute()
        .await
        .map_err(|error| backend_error("set workload bridge MTU and state", error))?;

    let addresses = handle
        .address()
        .get()
        .set_link_index_filter(index)
        .execute()
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| backend_error("list workload bridge addresses", error))?;
    let delta = address_delta(addresses, desired.gateway, desired.prefix_length);
    if !delta.present {
        handle
            .address()
            .add(index, IpAddr::V4(desired.gateway), desired.prefix_length)
            .execute()
            .await
            .map_err(|error| backend_error("add workload bridge gateway", error))?;
    }
    for stale in delta.stale {
        handle
            .address()
            .del(stale)
            .execute()
            .await
            .map_err(|error| backend_error("remove stale workload bridge address", error))?;
    }
    Ok(())
}

async fn find_link(
    handle: &Handle,
    name: &str,
) -> Result<Option<LinkMessage>, WorkloadBridgeBackendError> {
    handle
        .link()
        .get()
        .match_name(name.to_string())
        .execute()
        .try_next()
        .await
        .map_err(|error| backend_error("find workload bridge", error))
}

fn validate_bridge(link: &LinkMessage) -> Result<(), WorkloadBridgeBackendError> {
    let bridge = link.attributes.iter().any(|attribute| {
        matches!(
            attribute,
            LinkAttribute::LinkInfo(info)
                if info.iter().any(|entry| matches!(entry, LinkInfo::Kind(InfoKind::Bridge)))
        )
    });
    if bridge {
        Ok(())
    } else {
        Err(WorkloadBridgeBackendError::new(
            "the managed workload interface exists but is not a Linux bridge",
        ))
    }
}

pub(crate) struct AddressDelta {
    pub(crate) present: bool,
    pub(crate) stale: Vec<AddressMessage>,
}

pub(crate) fn address_delta(
    addresses: Vec<AddressMessage>,
    gateway: Ipv4Addr,
    prefix_length: u8,
) -> AddressDelta {
    let mut present = false;
    let mut stale = Vec::new();
    for address in addresses {
        let ipv4 = address
            .attributes
            .iter()
            .find_map(|attribute| match attribute {
                AddressAttribute::Address(IpAddr::V4(value))
                | AddressAttribute::Local(IpAddr::V4(value)) => Some(*value),
                _ => None,
            });
        let Some(ipv4) = ipv4 else {
            continue;
        };
        if ipv4 == gateway && address.header.prefix_len == prefix_length && !present {
            present = true;
        } else {
            stale.push(address);
        }
    }
    AddressDelta { present, stale }
}

fn backend_error(action: &str, error: impl std::fmt::Display) -> WorkloadBridgeBackendError {
    WorkloadBridgeBackendError::new(format!("failed to {action}: {error}"))
}
