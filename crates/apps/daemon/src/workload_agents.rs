use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::{NodeSpec, WorkloadNetworkMode};
use kernel_store::Store;
use logs::{
    IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStore, LogStream,
    OriginCursor, OtlpEnvelopeStore, OtlpLogHandler, OtlpSignalHandler, RuntimeLogPipeline,
};
use metrics::{HostMetricPipeline, HostMetricStore, MetricStore, WorkloadMetricPipeline};
use node_agent::{
    AssignmentAgent, AssignmentAgentSettings, AssignmentPublishingEvent, AssignmentPublishingSink,
    AssignmentPublishingState, FileLogCheckpointStore, HealthAgent, HealthAgentSettings,
    HostTelemetryAgent, HostTelemetrySettings, NodeApiServices, NodeRegistryAgent,
    NodeRegistrySettings, RuntimeLogAgent, RuntimeLogAgentSettings, StoreNodeControlHandler,
    WORKLOAD_BRIDGE_NAME, WorkloadDns, WorkloadStatsAgent, WorkloadStatsSettings,
};
use runtime::{NetworkAddressing, NetworkCidr, NetworkSpec};
use sha2::{Digest, Sha256};
use upgrade::{NodeUpgradeAgent, NodeUpgradeAgentSettings};

use crate::control_plane::{DaemonRoleFactory, HostTelemetryDependencies, role_error};
use crate::dns_resources::DNS_RESOLVER_SERVICE_ID;
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
    let boot_id = dependencies
        .recovery_marker
        .current_boot_id()
        .map_err(|error| role_error("read current host boot identity", error))?;
    NodeUpgradeAgent::new(
        store,
        NodeUpgradeAgentSettings {
            cluster_id: plan.cluster().cluster_id.clone(),
            node_id: spec.node_id.clone(),
            instance_id: factory.instance_id().clone(),
            boot_id,
            running_version: factory.running_version.clone(),
            resync_interval: factory.settings.upgrade_resync_interval,
        },
        dependencies.stager.clone(),
        dependencies.rebooter.clone(),
        factory.monotonic_clock.clone(),
    )
    .map(|agent| agent.with_store_recovery_marker(dependencies.recovery_marker.clone()))
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
    let publishing_sink = Arc::new(BuildPublishingLogSink {
        cluster_id: plan.cluster().cluster_id.clone(),
        logs: log_store.clone(),
    });
    let agent = AssignmentAgent::new(
        store.clone(),
        factory.workload_runtime.clone(),
        factory.network_provider.clone(),
        AssignmentAgentSettings {
            cluster_id: plan.cluster().cluster_id.clone(),
            node_id: spec.node_id.clone(),
            network: network.spec,
            dns: network.dns,
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
                    log_store.clone(),
                    factory.status_clock.clone(),
                )),
                signals.clone(),
                signals,
            )
        }),
        factory.monotonic_clock.clone(),
        factory.status_clock.clone(),
    )
    .map_err(|error| role_error("construct assignment agent", error))?
    .with_publishing_sink(publishing_sink);
    Ok(match &factory.value_sources {
        Some(resolver) => agent.with_value_source_resolver(resolver.clone()),
        None => agent,
    })
}

struct BuildPublishingLogSink {
    cluster_id: kernel_api::ClusterId,
    logs: Arc<dyn LogStore>,
}

#[async_trait]
impl AssignmentPublishingSink for BuildPublishingLogSink {
    async fn record(&self, event: AssignmentPublishingEvent) {
        let (state, severity, stream, reason, body) = match &event.state {
            AssignmentPublishingState::Started => (
                "started",
                "info",
                LogStream::Stdout,
                None,
                format!(
                    "Publishing replica {} to node {}",
                    event.replica_index, event.node_id
                ),
            ),
            AssignmentPublishingState::Waiting {
                reason,
                message,
                failed,
            } => (
                if *failed { "failed" } else { "waiting" },
                if *failed { "error" } else { "warn" },
                LogStream::Stderr,
                Some(reason.as_str()),
                format!(
                    "Publishing replica {} to node {} is {}: {message}",
                    event.replica_index,
                    event.node_id,
                    if *failed { "failed" } else { "waiting" },
                ),
            ),
            AssignmentPublishingState::Completed => (
                "completed",
                "info",
                LogStream::Stdout,
                None,
                format!(
                    "Published replica {} to node {}",
                    event.replica_index, event.node_id
                ),
            ),
        };
        let fingerprint = format!("{:x}", Sha256::digest(body.as_bytes()));
        let mut attributes = BTreeMap::from([
            ("maestro.build.phase".to_owned(), "publishing".to_owned()),
            ("maestro.build.output".to_owned(), "maestro".to_owned()),
            (
                "maestro.deployment.id".to_owned(),
                event.deployment_id.to_string(),
            ),
            (
                "maestro.assignment.id".to_owned(),
                event.assignment_id.to_string(),
            ),
            (
                "maestro.replica.index".to_owned(),
                event.replica_index.to_string(),
            ),
            ("maestro.publish.state".to_owned(), state.to_owned()),
        ]);
        if let Some(reason) = reason {
            attributes.insert("maestro.publish.reason".to_owned(), reason.to_owned());
        }
        let entry = IngestLogEntry {
            id: LogRecordId {
                node_id: event.node_id.clone(),
                producer: LogProducer::Build(event.build_id.clone()),
                cursor: OriginCursor::new(format!(
                    "assignment-publishing:{}:{state}:{fingerprint}",
                    event.assignment_id
                )),
            },
            observed_at: event.occurred_at,
            event_at: event.occurred_at,
            severity: severity.to_owned(),
            stream,
            origin: LogOrigin::Build {
                cluster_id: self.cluster_id.clone(),
                node_id: event.node_id,
                build_id: event.build_id,
            },
            body: LogBody::Text(body),
            attributes,
        };
        let _ignored = self.logs.append(&[entry]).await;
    }
}

#[cfg(test)]
mod publishing_log_tests {
    use std::sync::Arc;

    use kernel_api::{AssignmentId, BuildId, DeploymentId, NodeId, Timestamp};
    use logs::{InMemoryLogStore, LogBody, LogStream};
    use node_agent::{
        AssignmentPublishingEvent, AssignmentPublishingSink, AssignmentPublishingState,
    };

    use super::BuildPublishingLogSink;

    #[tokio::test]
    async fn assignment_wait_reason_is_written_to_the_build_log()
    -> Result<(), Box<dyn std::error::Error>> {
        let logs = Arc::new(InMemoryLogStore::new());
        let sink = BuildPublishingLogSink {
            cluster_id: kernel_api::ClusterId::new("cluster-1")?,
            logs: logs.clone(),
        };

        sink.record(AssignmentPublishingEvent {
            build_id: BuildId::new("build-1")?,
            deployment_id: DeploymentId::new("deployment-1")?,
            assignment_id: AssignmentId::new("assignment-1")?,
            node_id: NodeId::new("node-1")?,
            replica_index: 2,
            occurred_at: Timestamp(1_750_000_000_000),
            state: AssignmentPublishingState::Waiting {
                reason: "ArtifactReplicationUnavailable".to_owned(),
                message: "artifact has no live holder".to_owned(),
                failed: false,
            },
        })
        .await;

        let entries = logs.entries()?;
        let [entry] = entries.as_slice() else {
            return Err(format!("expected one publishing entry, found {}", entries.len()).into());
        };
        assert_eq!(entry.stream, LogStream::Stderr);
        assert_eq!(entry.severity, "warn");
        assert_eq!(
            entry.body,
            LogBody::Text(
                "Publishing replica 2 to node node-1 is waiting: artifact has no live holder"
                    .to_owned()
            )
        );
        assert_eq!(
            entry
                .attributes
                .get("maestro.publish.reason")
                .map(String::as_str),
            Some("ArtifactReplicationUnavailable")
        );
        Ok(())
    }
}

struct AssignmentNetwork {
    spec: NetworkSpec,
    dns: WorkloadDns,
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
                dns: WorkloadDns::Static(IpAddr::V4(gateway)),
            })
        }
        WorkloadNetworkMode::RuntimeDelegated => {
            let service_id = kernel_api::ServiceId::new(DNS_RESOLVER_SERVICE_ID)
                .map_err(|error| role_error("build delegated DNS service identity", error))?;
            Ok(AssignmentNetwork {
                spec: NetworkSpec {
                    name: WORKLOAD_BRIDGE_NAME.to_owned(),
                    addressing: NetworkAddressing::Delegated,
                    mtu_bytes: cluster::WIREGUARD_MTU_BYTES,
                },
                dns: WorkloadDns::DelegatedService(service_id),
            })
        }
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
