use std::net::IpAddr;
use std::sync::Arc;

use kernel_store::Store;
use logs::{LogStore, RuntimeLogPipeline};
use metrics::{HostMetricPipeline, HostMetricStore, MetricStore, WorkloadMetricPipeline};
use node_agent::{
    AssignmentAgent, AssignmentAgentSettings, FileLogCheckpointStore, HealthAgent,
    HealthAgentSettings, HostTelemetryAgent, HostTelemetrySettings, RuntimeLogAgent,
    RuntimeLogAgentSettings, WORKLOAD_BRIDGE_NAME, WorkloadStatsAgent, WorkloadStatsSettings,
};
use runtime::{NetworkCidr, NetworkSpec};

use crate::control_plane::{DaemonRoleFactory, role_error};
use crate::{DaemonPlan, RoleError, RoleSpec};

pub(crate) fn build_assignment_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
) -> Result<AssignmentAgent, RoleError> {
    let node = plan
        .cluster()
        .nodes
        .get(&spec.node_id)
        .ok_or_else(|| RoleError::new("local node disappeared from validated topology"))?;
    let gateway = node.workload_subnet.gateway_address().ok_or_else(|| {
        RoleError::new("local workload subnet has no usable workload bridge gateway")
    })?;
    let network = NetworkSpec {
        name: WORKLOAD_BRIDGE_NAME.to_owned(),
        range: NetworkCidr::new(
            IpAddr::V4(node.workload_subnet.network_address()),
            node.workload_subnet.prefix(),
        )
        .map_err(|error| role_error("build workload network range", error))?,
        gateway: IpAddr::V4(gateway),
        mtu_bytes: cluster::WIREGUARD_MTU_BYTES,
    };
    AssignmentAgent::new(
        store,
        factory.workload_runtime.clone(),
        factory.network_provider.clone(),
        AssignmentAgentSettings {
            cluster_id: plan.cluster().cluster_id.clone(),
            node_id: spec.node_id.clone(),
            network,
            stop_timeout: factory.settings.workload_stop_timeout,
            resync_interval: factory.settings.assignment_resync_interval,
            restart_backoff_base: factory.settings.restart_backoff_base,
            restart_backoff_max: factory.settings.restart_backoff_max,
            secrets_root: factory.volatile_root.join("secrets"),
            node_api_root: factory.volatile_root.join("node-api"),
        },
        #[cfg(unix)]
        None,
        factory.monotonic_clock.clone(),
        factory.status_clock.clone(),
    )
    .map_err(|error| role_error("construct assignment agent", error))
}

pub(crate) fn build_health_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
) -> Result<HealthAgent, RoleError> {
    HealthAgent::new(
        store,
        factory.health_prober.clone(),
        HealthAgentSettings {
            cluster_id: plan.cluster().cluster_id.clone(),
            node_id: spec.node_id.clone(),
            poll_interval: factory.settings.health_poll_interval,
        },
        factory.monotonic_clock.clone(),
        factory.status_clock.clone(),
    )
    .map_err(|error| role_error("construct workload health agent", error))
}

pub(crate) fn build_log_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn LogStore>,
) -> Result<RuntimeLogAgent, RoleError> {
    let checkpoints = FileLogCheckpointStore::new(spec.data_directory.join("log-checkpoints"))
        .map_err(|error| role_error("construct runtime log checkpoint store", error))?;
    RuntimeLogAgent::new(
        factory.workload_runtime.clone(),
        Arc::new(checkpoints),
        Arc::new(RuntimeLogPipeline::standard(store)),
        RuntimeLogAgentSettings {
            cluster_id: plan.cluster().cluster_id.clone(),
            node_id: spec.node_id.clone(),
            max_frames_per_workload: factory.settings.max_log_frames_per_workload,
            poll_interval: factory.settings.log_poll_interval,
        },
        factory.status_clock.clone(),
        factory.monotonic_clock.clone(),
    )
    .map_err(|error| role_error("construct runtime log agent", error))
}

pub(crate) fn build_stats_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn MetricStore>,
) -> Result<WorkloadStatsAgent, RoleError> {
    WorkloadStatsAgent::new(
        factory.workload_runtime.clone(),
        factory.stats_reader.clone(),
        factory.network_stats_reader.clone(),
        Arc::new(WorkloadMetricPipeline::new(store)),
        WorkloadStatsSettings {
            cluster_id: plan.cluster().cluster_id.clone(),
            node_id: spec.node_id.clone(),
            poll_interval: factory.settings.stats_poll_interval,
        },
        factory.status_clock.clone(),
        factory.monotonic_clock.clone(),
    )
    .map_err(|error| role_error("construct workload stats agent", error))
}

pub(crate) fn build_host_telemetry_agent<
    MeshBackendType,
    FirewallBackendType,
    BridgeBackendType,
>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn HostMetricStore>,
) -> Result<HostTelemetryAgent, RoleError> {
    HostTelemetryAgent::new(
        factory.host_stats_reader.clone(),
        factory.host_disk_reader.clone(),
        Arc::new(HostMetricPipeline::new(store)),
        HostTelemetrySettings {
            cluster_id: plan.cluster().cluster_id.clone(),
            node_id: spec.node_id.clone(),
            poll_interval: factory.settings.host_telemetry_poll_interval,
        },
        factory.status_clock.clone(),
        factory.monotonic_clock.clone(),
    )
    .map_err(|error| role_error("construct host telemetry agent", error))
}
