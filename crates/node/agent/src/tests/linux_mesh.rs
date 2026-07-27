use std::collections::BTreeSet;
use std::net::{Ipv4Addr, SocketAddrV4};

use kernel_api::NodeId;
use netlink_packet_route::route::{RouteMessage, RouteScope};
use wireguard_control::Key;

use crate::{
    MeshPeer, MeshSubnet, WireGuardPrivateKey,
    linux_mesh::{managed_subnet, route_delta, route_message, stale_peer_keys},
};

#[test]
fn wireguard_peer_delta_removes_only_stale_keys() -> Result<(), Box<dyn std::error::Error>> {
    let retained = WireGuardPrivateKey::from_bytes([1; 32]).public_key();
    let peer = MeshPeer {
        node_id: NodeId::new("node-2")?,
        public_key: retained.clone(),
        endpoint: SocketAddrV4::new(Ipv4Addr::new(10, 20, 0, 12), 51_820),
        allowed_subnet: "172.22.2.0/24".parse()?,
    };
    let retained = Key(*retained.as_bytes());
    let stale = Key([9; 32]);

    assert_eq!(
        stale_peer_keys(vec![retained, stale.clone()], &[peer]),
        vec![stale]
    );
    Ok(())
}

#[test]
fn route_delta_removes_only_owned_stale_routes_and_adds_missing_routes()
-> Result<(), Box<dyn std::error::Error>> {
    let interface_index = 12;
    let retained = "172.22.2.0/24".parse::<MeshSubnet>()?;
    let stale = "172.22.3.0/24".parse::<MeshSubnet>()?;
    let missing = "172.22.4.0/24".parse::<MeshSubnet>()?;
    let foreign = route_message("172.22.9.0/24".parse()?, 99);
    let desired = BTreeSet::from([retained, missing]);
    let delta = route_delta(
        vec![
            route_message(retained, interface_index),
            route_message(stale, interface_index),
            foreign,
        ],
        interface_index,
        &desired,
    );

    assert_eq!(delta.stale.len(), 1);
    assert_eq!(
        delta
            .stale
            .first()
            .and_then(|route| managed_subnet(route, interface_index)),
        Some(stale)
    );
    assert_eq!(delta.missing, vec![missing]);
    Ok(())
}

#[test]
fn route_delta_removes_duplicate_owned_routes() -> Result<(), Box<dyn std::error::Error>> {
    let interface_index = 12;
    let subnet = "172.22.2.0/24".parse::<MeshSubnet>()?;
    let route = route_message(subnet, interface_index);
    let delta = route_delta(
        vec![route.clone(), route],
        interface_index,
        &BTreeSet::from([subnet]),
    );
    assert_eq!(delta.stale.len(), 1);
    assert!(delta.missing.is_empty());
    Ok(())
}

#[test]
fn parser_ignores_non_maestro_route_shape() -> Result<(), Box<dyn std::error::Error>> {
    let subnet = "172.22.2.0/24".parse::<MeshSubnet>()?;
    let mut route: RouteMessage = route_message(subnet, 12);
    route.header.scope = RouteScope::Universe;
    assert_eq!(managed_subnet(&route, 12), None);
    Ok(())
}
