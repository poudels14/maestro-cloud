use std::net::Ipv4Addr;

use kernel_api::{NodeId, NodeNetworkSpec};

use crate::{
    MESH_INTERFACE_NAME, MESH_MTU_BYTES, MeshError, MeshIdentity, MeshPlanner, MeshReconciler,
    MeshSubnet, WireGuardPrivateKey,
};

use super::fake_mesh::FakeMeshBackend;

#[test]
fn planner_builds_sorted_peers_allowed_subnets_and_routes() -> Result<(), Box<dyn std::error::Error>>
{
    let planner = planner("node-2", 2)?;
    let publications = vec![
        publication("node-3", 3, 13, "172.22.3.0/24")?,
        planner.publication(Ipv4Addr::new(10, 20, 0, 12), "172.22.2.0/24".parse()?)?,
        publication("node-1", 1, 11, "172.22.1.0/24")?,
    ];
    let desired = planner.plan(&publications)?;

    assert_eq!(desired.interface.name, MESH_INTERFACE_NAME);
    assert_eq!(desired.interface.listen_port, 51_820);
    assert_eq!(desired.interface.mtu_bytes, MESH_MTU_BYTES);
    assert_eq!(desired.interface.local_subnet.to_string(), "172.22.2.0/24");
    assert_eq!(
        desired
            .peers
            .iter()
            .map(|peer| peer.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["node-1", "node-3"]
    );
    assert_eq!(
        desired
            .peers
            .iter()
            .map(|peer| peer.allowed_subnet.to_string())
            .collect::<Vec<_>>(),
        vec!["172.22.1.0/24", "172.22.3.0/24"]
    );
    assert_eq!(
        desired
            .routes
            .iter()
            .map(|route| (route.destination.to_string(), route.interface.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("172.22.1.0/24".to_owned(), MESH_INTERFACE_NAME),
            ("172.22.3.0/24".to_owned(), MESH_INTERFACE_NAME),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn reconciler_replaces_stale_peers_from_each_complete_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let planner = planner("node-1", 1)?;
    let local = planner.publication(Ipv4Addr::new(10, 20, 0, 11), "172.22.1.0/24".parse()?)?;
    let peer_2 = publication("node-2", 2, 12, "172.22.2.0/24")?;
    let peer_3 = publication("node-3", 3, 13, "172.22.3.0/24")?;
    let backend = FakeMeshBackend::default();
    let reconciler = MeshReconciler::new(planner, backend);

    reconciler
        .reconcile(&[local.clone(), peer_2.clone(), peer_3])
        .await?;
    let final_desired = reconciler.reconcile(&[peer_2, local]).await?;
    assert_eq!(final_desired.peers.len(), 1);
    assert_eq!(
        final_desired
            .peers
            .first()
            .map(|peer| peer.node_id.as_str()),
        Some("node-2")
    );
    let applied = reconciler.backend().applied();
    assert_eq!(applied.len(), 2);
    assert_eq!(applied.last(), Some(&final_desired));
    Ok(())
}

#[test]
fn planner_rejects_key_subnet_endpoint_and_mtu_conflicts() -> Result<(), Box<dyn std::error::Error>>
{
    let planner = planner("node-1", 1)?;
    let local = planner.publication(Ipv4Addr::new(10, 20, 0, 11), "172.22.1.0/24".parse()?)?;

    let mut wrong_local_key = local.clone();
    wrong_local_key.public_key = private_key(9).public_key().to_string();
    assert!(planner.plan(&[wrong_local_key]).is_err());

    let mut wrong_port = publication("node-2", 2, 12, "172.22.2.0/24")?;
    wrong_port.endpoint.set_port(51_821);
    assert!(planner.plan(&[local.clone(), wrong_port]).is_err());

    let mut wrong_mtu = publication("node-2", 2, 12, "172.22.2.0/24")?;
    wrong_mtu.mtu_bytes = 1_500;
    assert!(planner.plan(&[local.clone(), wrong_mtu]).is_err());

    let duplicate_subnet = publication("node-2", 2, 12, "172.22.1.0/24")?;
    assert!(matches!(
        planner.plan(&[local.clone(), duplicate_subnet]),
        Err(MeshError::DuplicateSubnet { .. })
    ));

    assert!(matches!(
        planner.plan(&[local.clone(), local]),
        Err(MeshError::DuplicateNode { .. })
    ));
    Ok(())
}

#[test]
fn single_node_topology_has_no_mesh_peers_or_routes() -> Result<(), Box<dyn std::error::Error>> {
    let planner = planner("node-1", 1)?;
    let local = planner.publication(Ipv4Addr::new(10, 20, 0, 11), "172.22.1.0/24".parse()?)?;
    let desired = planner.plan(&[local])?;
    assert!(desired.peers.is_empty());
    assert!(desired.routes.is_empty());
    Ok(())
}

fn planner(node_id: &str, key_byte: u8) -> Result<MeshPlanner, Box<dyn std::error::Error>> {
    MeshPlanner::new(
        NodeId::new(node_id)?,
        MeshIdentity::from_private_key(private_key(key_byte)),
        51_820,
    )
    .map_err(Into::into)
}

fn publication(
    node_id: &str,
    key_byte: u8,
    address_suffix: u8,
    subnet: &str,
) -> Result<NodeNetworkSpec, Box<dyn std::error::Error>> {
    planner(node_id, key_byte)?
        .publication(
            Ipv4Addr::new(10, 20, 0, address_suffix),
            subnet.parse::<MeshSubnet>()?,
        )
        .map_err(Into::into)
}

fn private_key(key_byte: u8) -> WireGuardPrivateKey {
    WireGuardPrivateKey::from_bytes([key_byte; 32])
}
