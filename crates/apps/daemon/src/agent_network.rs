use std::sync::Arc;
use std::time::Duration;

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

const INITIAL_STORE_RECONCILE_ATTEMPTS: u32 = 60;
const INITIAL_STORE_RECONCILE_RETRY: Duration = Duration::from_secs(1);

macro_rules! establish_store_snapshot {
    ($factory:expr, $agent:expr, $action:literal) => {{
        let mut attempt = 1;
        loop {
            match $agent.reconcile_once().await {
                Ok(_) => break Ok(()),
                Err(error) if attempt < INITIAL_STORE_RECONCILE_ATTEMPTS => {
                    if attempt == 1 || attempt % 10 == 0 {
                        tracing::warn!(
                            action = $action,
                            attempt,
                            error = %error,
                            "initial store-backed network snapshot is not ready; retrying"
                        );
                    }
                    let deadline = $factory
                        .monotonic_clock
                        .now()
                        .saturating_add(INITIAL_STORE_RECONCILE_RETRY);
                    $factory.monotonic_clock.sleep_until(deadline).await;
                    attempt += 1;
                }
                Err(error) => break Err(role_error($action, error)),
            }
        }
    }};
}

/// Node-local network agents present only for cluster-routed Linux networking.
#[allow(clippy::large_enum_variant)]
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
            let resolver = match &factory.dns_plugin_settings {
                Some(settings) => settings.attach(resolver),
                None => resolver,
            };
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
            establish_store_snapshot!(factory, mesh, "establish initial mesh snapshot")?;
            establish_store_snapshot!(factory, dns, "establish initial DNS snapshot")?;
            establish_store_snapshot!(factory, firewall, "establish initial firewall snapshot")?;
            let settings = DnsServerSettings::bridge(std::net::SocketAddr::new(
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
    let admin_address = node
        .workload_subnet
        .admin_address()
        .ok_or_else(|| RoleError::new("local workload subnet has no fixed Admin address"))?;
    let desired = WorkloadBridge::new(gateway, node.workload_subnet.prefix(), WIREGUARD_MTU_BYTES)
        .and_then(|bridge| bridge.with_admin_address(admin_address))
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
