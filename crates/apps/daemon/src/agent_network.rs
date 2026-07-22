use std::sync::Arc;

use cluster::WIREGUARD_MTU_BYTES;
use kernel_store::Store;
use node_agent::{
    FirewallBackend, MeshBackend, MeshPlanner, MeshResourceAgent, NodeFirewallAgent,
    WorkloadBridge, WorkloadBridgeAgent, WorkloadBridgeBackend,
};

use crate::control_plane::{DaemonRoleFactory, role_error};
use crate::{DaemonPlan, RoleError, RoleSpec};

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
