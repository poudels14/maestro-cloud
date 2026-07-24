use std::sync::Arc;

use cluster::WIREGUARD_MTU_BYTES;
use kernel_api::WorkloadNetworkMode;
use kernel_store::Store;
use node_agent::{
    AUTHORITATIVE_DNS_PORT, AuthoritativeDnsResolver, DnsResourceAgent, DnsServerRuntime,
    DnsServerSettings, FirewallBackend, MeshBackend, MeshPlanner, MeshResourceAgent,
    NodeFirewallAgent, WorkloadBridge, WorkloadBridgeAgent, WorkloadBridgeBackend,
};

use crate::control_plane::{DaemonRoleFactory, role_error};
use crate::{DaemonPlan, RoleError, RoleSpec};

/// Node-local network agents present only for cluster-routed Linux networking.
pub(crate) enum AgentNetworkAgents<MeshBackendType, FirewallBackendType, BridgeBackendType> {
    ClusterRouted {
        bridge: WorkloadBridgeAgent<BridgeBackendType>,
        mesh: MeshResourceAgent<MeshBackendType>,
        dns: DnsResourceAgent,
        firewall: NodeFirewallAgent<FirewallBackendType>,
        dns_server: Box<dyn DnsServerRuntime>,
    },
    RuntimeDelegated,
}

pub(crate) async fn prepare_agent_network<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
    mesh_backend: MeshBackendType,
    firewall_backend: FirewallBackendType,
    bridge_backend: BridgeBackendType,
) -> Result<AgentNetworkAgents<MeshBackendType, FirewallBackendType, BridgeBackendType>, RoleError>
where
    MeshBackendType: MeshBackend,
    FirewallBackendType: FirewallBackend,
    BridgeBackendType: WorkloadBridgeBackend,
{
    match factory.workload_network_mode {
        WorkloadNetworkMode::RuntimeDelegated => Ok(AgentNetworkAgents::RuntimeDelegated),
        WorkloadNetworkMode::ClusterRouted => {
            let bridge = build_bridge_agent(factory, plan, spec, bridge_backend)?;
            let mesh = build_mesh_agent(factory, plan, spec, store.clone(), mesh_backend)?;
            let firewall =
                build_firewall_agent(factory, plan, spec, store.clone(), firewall_backend)?;
            let resolver = AuthoritativeDnsResolver::new()
                .map_err(|error| role_error("construct authoritative DNS resolver", error))?;
            let dns = DnsResourceAgent::new(
                store,
                &plan.cluster().cluster_id,
                spec.node_id.clone(),
                resolver.clone(),
                factory.monotonic_clock.clone(),
                factory.settings.dns_resync_interval,
            )
            .map_err(|error| role_error("construct DNS resource agent", error))?;
            bridge
                .reconcile_once()
                .await
                .map_err(|error| role_error("establish workload bridge", error))?;
            mesh.reconcile_once()
                .await
                .map_err(|error| role_error("establish initial mesh snapshot", error))?;
            dns.reconcile_once()
                .await
                .map_err(|error| role_error("establish initial DNS snapshot", error))?;
            firewall
                .reconcile_once()
                .await
                .map_err(|error| role_error("establish initial firewall snapshot", error))?;
            let settings = DnsServerSettings::new(std::net::SocketAddr::new(
                std::net::IpAddr::V4(bridge.desired().gateway),
                AUTHORITATIVE_DNS_PORT,
            ))
            .map_err(|error| role_error("validate authoritative DNS listener", error))?;
            let dns_server = factory
                .dns_server_binder
                .bind(settings, resolver)
                .await
                .map_err(|error| role_error("bind authoritative DNS listener", error))?;
            Ok(AgentNetworkAgents::ClusterRouted {
                bridge,
                mesh,
                dns,
                firewall,
                dns_server,
            })
        }
    }
}

pub(crate) fn build_bridge_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    backend: BridgeBackendType,
) -> Result<WorkloadBridgeAgent<BridgeBackendType>, RoleError>
where
    BridgeBackendType: WorkloadBridgeBackend,
{
    let node = plan
        .cluster()
        .nodes
        .get(&spec.node_id)
        .ok_or_else(|| RoleError::new("local node disappeared from validated topology"))?;
    let gateway = node.workload_subnet.gateway_address().ok_or_else(|| {
        RoleError::new("local workload subnet has no usable workload bridge gateway")
    })?;
    let desired = WorkloadBridge::new(gateway, node.workload_subnet.prefix(), WIREGUARD_MTU_BYTES)
        .map_err(|error| role_error("build workload bridge state", error))?;
    WorkloadBridgeAgent::new(
        desired,
        backend,
        factory.monotonic_clock.clone(),
        factory.settings.bridge_resync_interval,
    )
    .map_err(|error| role_error("construct workload bridge agent", error))
}

pub(crate) fn build_mesh_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
    backend: MeshBackendType,
) -> Result<MeshResourceAgent<MeshBackendType>, RoleError>
where
    MeshBackendType: MeshBackend,
{
    let node = plan
        .cluster()
        .nodes
        .get(&spec.node_id)
        .ok_or_else(|| RoleError::new("local node disappeared from validated topology"))?;
    let planner = MeshPlanner::new(
        spec.node_id.clone(),
        factory.mesh_identity.clone(),
        plan.cluster().ports.wireguard,
    )
    .map_err(|error| role_error("build local mesh planner", error))?;
    let publication = planner
        .publication(
            node.endpoint.host_address,
            node.workload_subnet
                .to_string()
                .parse()
                .map_err(|error| role_error("convert local workload subnet", error))?,
        )
        .map_err(|error| role_error("build local mesh publication", error))?;
    MeshResourceAgent::new(
        store,
        &plan.cluster().cluster_id,
        planner,
        publication,
        backend,
        factory.monotonic_clock.clone(),
        factory.status_clock.clone(),
        factory.settings.mesh_resync_interval,
    )
    .map_err(|error| role_error("construct mesh resource agent", error))
}

pub(crate) fn build_firewall_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
    backend: FirewallBackendType,
) -> Result<NodeFirewallAgent<FirewallBackendType>, RoleError>
where
    FirewallBackendType: FirewallBackend,
{
    NodeFirewallAgent::new(
        store,
        &plan.cluster().cluster_id,
        spec.node_id.clone(),
        backend,
        factory.monotonic_clock.clone(),
        factory.status_clock.clone(),
        factory.settings.firewall_resync_interval,
    )
    .map_err(|error| role_error("construct firewall resource agent", error))
}
