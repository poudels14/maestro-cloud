use std::net::IpAddr;
use std::sync::Arc;

use kernel_api::{NodeSpec, WorkloadNetworkMode};
use kernel_store::Store;
use logs::{LogStore, OtlpEnvelopeStore, OtlpLogHandler, OtlpSignalHandler, RuntimeLogPipeline};
use metrics::{HostMetricPipeline, HostMetricStore, MetricStore, WorkloadMetricPipeline};
use node_agent::{
    AssignmentAgent, AssignmentAgentSettings, FileLogCheckpointStore, HealthAgent,
    HealthAgentSettings, HostTelemetryAgent, HostTelemetrySettings, NodeApiServices,
    NodeRegistryAgent, NodeRegistrySettings, RuntimeLogAgent, RuntimeLogAgentSettings,
    StoreNodeControlHandler, WORKLOAD_BRIDGE_NAME, WorkloadStatsAgent, WorkloadStatsSettings,
};
use runtime::{NetworkAddressing, NetworkCidr, NetworkSpec};
use upgrade::{NodeUpgradeAgent, NodeUpgradeAgentSettings};

use crate::control_plane::{DaemonRoleFactory, HostTelemetryDependencies, role_error};
use crate::{DaemonPlan, RoleError, RoleSpec};

pub(crate) fn build_node_registry_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
) -> Result<NodeRegistryAgent, RoleError> {
    let node = plan
        .cluster()
        .nodes
        .get(&spec.node_id)
        .ok_or_else(|| RoleError::new("local node disappeared from validated topology"))?;
    NodeRegistryAgent::new(
        store,
        NodeRegistrySettings {
            cluster_id: plan.cluster().cluster_id.clone(),
            node_id: spec.node_id.clone(),
            node_spec: NodeSpec {
                hostname: node.hostname.clone(),
                host_address: node.endpoint.host_address.into(),
                role: node.role,
                workload_network_mode: factory.workload_network_mode,
                scheduling_labels: Default::default(),
            },
            instance_id: factory.instance_id().clone(),
            running_version: factory.running_version.clone(),
            session_ttl: factory.settings.node_liveness_ttl,
            keepalive_interval: factory.settings.node_liveness_keepalive_interval,
        },
        factory.monotonic_clock.clone(),
        factory.status_clock.clone(),
    )
    .map_err(|error| role_error("construct node registry agent", error))
}

pub(crate) fn build_node_upgrade_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
) -> Result<Option<NodeUpgradeAgent>, RoleError> {
    let Some(dependencies) = factory.node_upgrade.as_ref() else {
        return Ok(None);
    };
    NodeUpgradeAgent::new(
        store,
        NodeUpgradeAgentSettings {
            cluster_id: plan.cluster().cluster_id.clone(),
            node_id: spec.node_id.clone(),
            instance_id: factory.instance_id().clone(),
            running_version: factory.running_version.clone(),
            resync_interval: factory.settings.upgrade_resync_interval,
        },
        dependencies.stager.clone(),
        dependencies.rebooter.clone(),
        factory.monotonic_clock.clone(),
    )
    .map(Some)
    .map_err(|error| role_error("construct node upgrade agent", error))
}

pub(crate) fn build_assignment_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
    log_store: Arc<dyn LogStore>,
    otlp_store: Arc<dyn OtlpEnvelopeStore>,
) -> Result<AssignmentAgent, RoleError> {
    let node = plan
        .cluster()
        .nodes
        .get(&spec.node_id)
        .ok_or_else(|| RoleError::new("local node disappeared from validated topology"))?;
    let network = assignment_network(node, factory.workload_network_mode)?;
    AssignmentAgent::new(
        store.clone(),
        factory.workload_runtime.clone(),
        factory.network_provider.clone(),
        AssignmentAgentSettings {
            cluster_id: plan.cluster().cluster_id.clone(),
            node_id: spec.node_id.clone(),
            network: network.spec,
            dns_server: network.dns_server,
            system_host_ports: factory.system_host_ports.clone(),
            stop_timeout: factory.settings.workload_stop_timeout,
            resync_interval: factory.settings.assignment_resync_interval,
            restart_backoff_base: factory.settings.restart_backoff_base,
            restart_backoff_max: factory.settings.restart_backoff_max,
            reconcile_timeout: factory.settings.assignment_reconcile_timeout,
            secrets_root: factory.volatile_root.join("secrets"),
            node_api_root: factory.volatile_root.join("node-api"),
        },
        #[cfg(unix)]
        Some({
            let signals = Arc::new(OtlpSignalHandler::new(
                plan.cluster().cluster_id.clone(),
                otlp_store,
                factory.status_clock.clone(),
            ));
            NodeApiServices::new(
                Arc::new(StoreNodeControlHandler::new(
                    store.clone(),
                    &plan.cluster().cluster_id,
                    factory.status_clock.clone(),
                )),
                Arc::new(OtlpLogHandler::new(
                    plan.cluster().cluster_id.clone(),
                    log_store,
                    factory.status_clock.clone(),
                )),
                signals.clone(),
                signals,
            )
        }),
        factory.monotonic_clock.clone(),
        factory.status_clock.clone(),
    )
    .map_err(|error| role_error("construct assignment agent", error))
}

struct AssignmentNetwork {
    spec: NetworkSpec,
    dns_server: Option<IpAddr>,
}

fn assignment_network(
    node: &cluster::NodeDefinition,
    mode: WorkloadNetworkMode,
) -> Result<AssignmentNetwork, RoleError> {
    match mode {
        WorkloadNetworkMode::ClusterRouted => {
            let gateway = node.workload_subnet.gateway_address().ok_or_else(|| {
                RoleError::new("local workload subnet has no usable workload bridge gateway")
            })?;
            Ok(AssignmentNetwork {
                spec: NetworkSpec {
                    name: WORKLOAD_BRIDGE_NAME.to_owned(),
                    addressing: NetworkAddressing::Managed {
                        range: NetworkCidr::new(
                            IpAddr::V4(node.workload_subnet.network_address()),
                            node.workload_subnet.prefix(),
                        )
                        .map_err(|error| role_error("build workload network range", error))?,
                        gateway: IpAddr::V4(gateway),
                    },
                    mtu_bytes: cluster::WIREGUARD_MTU_BYTES,
                },
                dns_server: Some(IpAddr::V4(gateway)),
            })
        }
        WorkloadNetworkMode::RuntimeDelegated => Ok(AssignmentNetwork {
            spec: NetworkSpec {
                name: WORKLOAD_BRIDGE_NAME.to_owned(),
                addressing: NetworkAddressing::Delegated,
                mtu_bytes: cluster::WIREGUARD_MTU_BYTES,
            },
            dns_server: None,
        }),
    }
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
) -> Result<Option<HostTelemetryAgent>, RoleError> {
    let HostTelemetryDependencies::Available {
        resource_reader,
        disk_reader,
    } = &factory.host_telemetry
    else {
        return Ok(None);
    };
    HostTelemetryAgent::new(
        resource_reader.clone(),
        disk_reader.clone(),
        Arc::new(HostMetricPipeline::new(store)),
        HostTelemetrySettings {
            cluster_id: plan.cluster().cluster_id.clone(),
            node_id: spec.node_id.clone(),
            poll_interval: factory.settings.host_telemetry_poll_interval,
        },
        factory.status_clock.clone(),
        factory.monotonic_clock.clone(),
    )
    .map(Some)
    .map_err(|error| role_error("construct host telemetry agent", error))
}
