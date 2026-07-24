use std::sync::Arc;

use logs::SinkWorker;
use metrics::{HostMetricSinkWorker, MetricSinkWorker};
use node_agent::{
    ArtifactReplicationAgent, AssignmentAgent, DnsResourceAgent, DnsServerRuntime, FirewallBackend,
    HealthAgent, HostTelemetryAgent, MeshBackend, MeshResourceAgent, NodeFirewallAgent,
    NodeRegistration, NodeRegistryAgent, RuntimeLogAgent, WorkloadBridgeAgent,
    WorkloadBridgeBackend, WorkloadStatsAgent,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use upgrade::NodeUpgradeAgent;

use crate::control_plane::role_error;
use crate::stats_metric_sampler::StatsMetricSampler;
use crate::{LogMaintenanceWorker, RoleError};

pub(crate) struct AgentTaskInputs<MeshBackendType, FirewallBackendType, BridgeBackendType> {
    pub(crate) api_server: server::BoundApiServer,
    pub(crate) node_registry_agent: NodeRegistryAgent,
    pub(crate) node_registration: NodeRegistration,
    pub(crate) artifact_replication_agent: Arc<ArtifactReplicationAgent>,
    pub(crate) bridge_agent: WorkloadBridgeAgent<BridgeBackendType>,
    pub(crate) mesh_agent: MeshResourceAgent<MeshBackendType>,
    pub(crate) dns_agent: DnsResourceAgent,
    pub(crate) firewall_agent: NodeFirewallAgent<FirewallBackendType>,
    pub(crate) dns_server: Box<dyn DnsServerRuntime>,
    pub(crate) stats_metric_sampler: StatsMetricSampler,
    pub(crate) assignment_agent: Option<AssignmentAgent>,
    pub(crate) health_agent: Option<HealthAgent>,
    pub(crate) log_agent: Option<RuntimeLogAgent>,
    pub(crate) stats_agent: Option<WorkloadStatsAgent>,
    pub(crate) host_telemetry_agent: HostTelemetryAgent,
    pub(crate) node_upgrade_agent: Option<NodeUpgradeAgent>,
    pub(crate) sink_workers: Vec<SinkWorker>,
    pub(crate) metric_sink_workers: Vec<MetricSinkWorker>,
    pub(crate) host_metric_sink_workers: Vec<HostMetricSinkWorker>,
    pub(crate) log_maintenance: Option<LogMaintenanceWorker>,
}

pub(crate) struct AgentTasks {
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<Result<(), RoleError>>>,
}

impl AgentTasks {
    pub(crate) fn into_parts(
        self,
    ) -> (watch::Sender<bool>, Vec<JoinHandle<Result<(), RoleError>>>) {
        (self.shutdown, self.tasks)
    }
}

pub(crate) fn spawn_agent_tasks<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    inputs: AgentTaskInputs<MeshBackendType, FirewallBackendType, BridgeBackendType>,
) -> AgentTasks
where
    MeshBackendType: MeshBackend + 'static,
    FirewallBackendType: FirewallBackend + 'static,
    BridgeBackendType: WorkloadBridgeBackend + 'static,
{
    let AgentTaskInputs {
        api_server,
        node_registry_agent,
        node_registration,
        artifact_replication_agent,
        bridge_agent,
        mesh_agent,
        dns_agent,
        firewall_agent,
        dns_server,
        stats_metric_sampler,
        assignment_agent,
        health_agent,
        log_agent,
        stats_agent,
        host_telemetry_agent,
        node_upgrade_agent,
        sink_workers,
        metric_sink_workers,
        host_metric_sink_workers,
        log_maintenance,
    } = inputs;
    let (shutdown, bridge_shutdown) = watch::channel(false);
    let mesh_shutdown = bridge_shutdown.clone();
    let dns_resource_shutdown = bridge_shutdown.clone();
    let firewall_shutdown = bridge_shutdown.clone();
    let dns_server_shutdown = bridge_shutdown.clone();
    let assignment_shutdown = bridge_shutdown.clone();
    let artifact_replication_shutdown = bridge_shutdown.clone();
    let health_shutdown = bridge_shutdown.clone();
    let log_shutdown = bridge_shutdown.clone();
    let stats_shutdown = bridge_shutdown.clone();
    let host_telemetry_shutdown = bridge_shutdown.clone();
    let node_upgrade_shutdown = bridge_shutdown.clone();
    let node_registry_shutdown = bridge_shutdown.clone();
    let api_shutdown = bridge_shutdown.clone();
    let stats_metric_shutdown = bridge_shutdown.clone();
    let api_task = tokio::spawn(async move {
        api_server
            .serve(api_shutdown)
            .await
            .map_err(|error| role_error("serve operator API", error))
    });
    let node_registry_task = tokio::spawn(async move {
        node_registry_agent
            .run_registered(node_registration, node_registry_shutdown)
            .await
            .map_err(|error| role_error("run node registry agent", error))
    });
    let artifact_replication_task = tokio::spawn(async move {
        artifact_replication_agent
            .run(artifact_replication_shutdown)
            .await
            .map_err(|error| role_error("run artifact replication agent", error))
    });
    let bridge_task = tokio::spawn(async move {
        bridge_agent
            .run(bridge_shutdown)
            .await
            .map_err(|error| role_error("run workload bridge agent", error))
    });
    let mesh_task = tokio::spawn(async move {
        mesh_agent
            .run(mesh_shutdown)
            .await
            .map_err(|error| role_error("run mesh agent", error))
    });
    let dns_resource_task = tokio::spawn(async move {
        dns_agent
            .run(dns_resource_shutdown)
            .await
            .map_err(|error| role_error("run DNS resource agent", error))
    });
    let firewall_task = tokio::spawn(async move {
        firewall_agent
            .run(firewall_shutdown)
            .await
            .map_err(|error| role_error("run firewall agent", error))
    });
    let dns_server_task = tokio::spawn(async move {
        dns_server
            .serve(dns_server_shutdown)
            .await
            .map_err(|error| role_error("serve authoritative DNS", error))
    });
    let mut tasks = vec![
        bridge_task,
        mesh_task,
        dns_resource_task,
        firewall_task,
        dns_server_task,
        artifact_replication_task,
        node_registry_task,
        api_task,
        tokio::spawn(async move {
            stats_metric_sampler.run(stats_metric_shutdown).await;
            Ok(())
        }),
    ];
    if let Some(agent) = assignment_agent {
        tasks.push(tokio::spawn(async move {
            agent
                .run(assignment_shutdown)
                .await
                .map_err(|error| role_error("run assignment agent", error))
        }));
    }
    if let Some(agent) = health_agent {
        tasks.push(tokio::spawn(async move {
            agent
                .run(health_shutdown)
                .await
                .map_err(|error| role_error("run workload health agent", error))
        }));
    }
    if let Some(agent) = log_agent {
        tasks.push(tokio::spawn(async move {
            agent
                .run(log_shutdown)
                .await
                .map_err(|error| role_error("run runtime log agent", error))
        }));
    }
    if let Some(agent) = stats_agent {
        tasks.push(tokio::spawn(async move {
            agent
                .run(stats_shutdown)
                .await
                .map_err(|error| role_error("run workload stats agent", error))
        }));
    }
    tasks.push(tokio::spawn(async move {
        host_telemetry_agent
            .run(host_telemetry_shutdown)
            .await
            .map_err(|error| role_error("run host telemetry agent", error))
    }));
    if let Some(agent) = node_upgrade_agent {
        tasks.push(tokio::spawn(async move {
            agent
                .run(node_upgrade_shutdown)
                .await
                .map_err(|error| role_error("run node upgrade agent", error))
        }));
    }
    for worker in sink_workers {
        let sink_shutdown = shutdown.subscribe();
        tasks.push(tokio::spawn(async move {
            worker.run(sink_shutdown).await;
            Ok(())
        }));
    }
    for worker in metric_sink_workers {
        let sink_shutdown = shutdown.subscribe();
        tasks.push(tokio::spawn(async move {
            worker.run(sink_shutdown).await;
            Ok(())
        }));
    }
    for worker in host_metric_sink_workers {
        let sink_shutdown = shutdown.subscribe();
        tasks.push(tokio::spawn(async move {
            worker.run(sink_shutdown).await;
            Ok(())
        }));
    }
    if let Some(worker) = log_maintenance {
        let maintenance_shutdown = shutdown.subscribe();
        tasks.push(tokio::spawn(async move {
            worker.run(maintenance_shutdown).await;
            Ok(())
        }));
    }
    AgentTasks { shutdown, tasks }
}
