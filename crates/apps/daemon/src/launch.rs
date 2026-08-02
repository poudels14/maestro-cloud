use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use build::LocalBuildSourceProvider;
use cluster::{
    ClusterConfig, EmbeddedEtcdProvider, EmbeddedEtcdSettings, NodeCertificateBundle, StoreMember,
    StoreProviderConfig,
};
use kernel_api::{NodeId, NodeInstanceId, SecretValue};
use kernel_controller::SystemTimestampClock;
use kernel_store::{EtcdStore, EtcdTlsConfig, Store, TokioClock, derive_key};
use logstore::{DuckLogStoreRuntime, DuckMetricStoreRuntime, DuckStoreSettings};
use node_agent::{
    CgroupV2StatsReader, HickoryDnsServerBinder, MeshIdentity, NetworkHealthProber,
    SystemDnsPluginSettings, SystemStatusClock, TailscaleDnsPluginSettings, TailscaleDnsRoute,
};
#[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
use node_agent::{
    HostNetworkStatsReader, LinuxHostDiskReader, LinuxHostStatsReader, LinuxMeshBackend,
    LinuxWorkloadBridgeBackend, NftablesFirewallBackend,
};
#[cfg(any(target_os = "macos", feature = "macos-platform"))]
use runtime::DockerRuntime;
#[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
use runtime::{ContainerdRuntime, ContainerdRuntimeSettings, TokioRuntimeClock};
use server::{ServerSettings, TlsIdentity};
use upgrade::StoreNodeUpgradeBackendSettings;
#[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
use upgrade::UpgradeSettings;
use webhook::HttpWebhookBackend;

#[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
use crate::NodeUpgradeDependencies;
use crate::cloudflare_resources::CloudflareSystemResources;
use crate::datadog::{build_datadog_sinks, configure_datadog};
use crate::dns_resources::DnsResolverSystemResources;
use crate::launch_error::{DaemonLaunchError, invalid};
use crate::log_backup_config::configure_log_maintenance;
#[cfg(any(target_os = "macos", feature = "macos-platform"))]
use crate::platform::{AbsentHostNetworkBackend, RuntimeDelegatedNetworkStatsReader};
#[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
use crate::tailscale_resources::TAILSCALE_IPV4_CIDR;
use crate::tailscale_resources::TailscaleSystemResources;
use crate::traefik_resources::TraefikSystemResources;
use crate::value_source::AwsValueSourceResolver;
use crate::{
    AdmissionDependencies, AgentStore, BuildOperatorBackends, Daemon, DaemonPlan,
    DaemonRoleDependencies, DaemonRoleFactory, DaemonRoleSettings, HostTelemetryDependencies,
    OperatorLeaderWorkload, OperatorSettings, RunningDaemon,
};

mod config;

pub use config::{
    DaemonLaunchConfig, DaemonLaunchDocument, StoreLaunchMode, load_launch_config,
    load_launch_config_with_fallbacks, load_launch_document,
};

/// Builds production adapters and starts one daemon instance for its declared node role.
pub async fn launch_daemon(config: DaemonLaunchConfig) -> Result<RunningDaemon, DaemonLaunchError> {
    launch_daemon_inner(config, None).await
}

async fn launch_daemon_inner(
    config: DaemonLaunchConfig,
    launch_config_admin: Option<Arc<dyn server::LaunchConfigAdmin>>,
) -> Result<RunningDaemon, DaemonLaunchError> {
    config.validate()?;
    let DaemonLaunchConfig {
        cluster,
        node_id,
        data_directory,
        containerd_socket,
        etcd_binary,
        store_mode,
        security,
        certificate_issuer,
        jwt_secret_key,
        store_encryption_secret,
        instance_id,
        datadog,
        depot,
        log_backup,
        preview,
        nixos_upgrade,
    } = config;
    let known_members = control_plane_members(&cluster);
    let dns_plugin_settings = dns_plugin_settings(&cluster)?;
    let dns_upstream_settings = SystemDnsPluginSettings::from_resolv_conf_file(
        Path::new("/etc/resolv.conf"),
        Duration::from_secs(5),
    )
    .map_err(|error| invalid(format!("invalid upstream DNS settings: {error}")))?;
    let clock = Arc::new(TokioClock::new());
    let local_node = cluster
        .nodes
        .get(&node_id)
        .ok_or_else(|| invalid("local node disappeared from validated topology"))?;
    let configured_datadog =
        configure_datadog(datadog.as_ref(), &cluster.name, &local_node.hostname)?;
    let api_settings = api_settings(&cluster, local_node, &security, jwt_secret_key.clone());
    let admin_api_settings = admin_api_settings(&cluster, local_node, jwt_secret_key.clone())?;
    let admission = local_node
        .role
        .is_control_plane()
        .then(|| AdmissionDependencies {
            authority_seed: certificate_issuer,
        });
    let agent_store = if local_node.role.is_control_plane() {
        let local_member = known_members
            .get(&node_id)
            .cloned()
            .ok_or_else(|| invalid("local node is absent from control-plane membership"))?;
        let provider_config = StoreProviderConfig::new(
            cluster.cluster_id.clone(),
            local_member,
            known_members.clone(),
            cluster.ports,
            data_directory.join("store"),
            store_encryption_secret.clone(),
            security.clone(),
        )?;
        let provider = Arc::new(EmbeddedEtcdProvider::new(
            provider_config,
            etcd_binary
                .ok_or_else(|| invalid("control-plane nodes require an embedded etcd binary"))?,
            clock.clone(),
            EmbeddedEtcdSettings::default(),
        )?);
        AgentStore::Managed {
            provider,
            start_mode: store_mode.provider_mode().ok_or_else(|| {
                invalid("control-plane nodes cannot use client-only store access")
            })?,
        }
    } else {
        AgentStore::Remote(
            connect_worker_store(
                &cluster,
                &known_members,
                &security,
                &store_encryption_secret,
            )
            .await?,
        )
    };
    let mesh_identity = MeshIdentity::load_or_generate(&data_directory.join("agent").join("mesh"))?;
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    let runtime = Arc::new(
        ContainerdRuntime::connect(
            ContainerdRuntimeSettings {
                socket: containerd_socket,
                namespace: format!("maestro-{}", cluster.cluster_id),
                state_root: data_directory.join("runtime").join("containerd"),
                ..ContainerdRuntimeSettings::default()
            },
            Arc::new(TokioRuntimeClock::new()),
        )
        .await?,
    );
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    let runtime = {
        let _containerd_socket = containerd_socket;
        Arc::new(DockerRuntime::connect_with_defaults()?)
    };
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    let volatile_root = PathBuf::from("/run/maestro")
        .join(cluster.cluster_id.as_str())
        .join(node_id.as_str());
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    let volatile_root = data_directory.join("runtime").join("volatile");
    let instance_id = match instance_id {
        Some(instance_id) => instance_id,
        None => generate_instance_id()?,
    };
    let timestamp_clock = Arc::new(SystemTimestampClock);
    let build_root = data_directory.join("build");
    let build_source = Arc::new(LocalBuildSourceProvider::new(
        build_root.join("workspaces"),
        build_root.join("archives"),
    )?);
    let depot_backend = depot
        .as_ref()
        .map(|depot| {
            crate::depot_config::configure_depot(depot, build_root.join("depot"), runtime.clone())
        })
        .transpose()
        .map_err(|error| invalid(error.to_string()))?;
    let configured_preview = preview
        .as_ref()
        .map(crate::preview_config::configure_preview)
        .transpose()?;
    let configured_upgrade =
        crate::upgrade_config::configure_nixos_upgrade(nixos_upgrade.as_ref())?;
    let running_version = configured_upgrade.running_version.clone();
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    let node_upgrade = Some(NodeUpgradeDependencies {
        stager: Some(configured_upgrade.stager),
        rebooter: configured_upgrade.rebooter,
    });
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    let node_upgrade = None;
    let tailscale_resources = TailscaleSystemResources::from_cluster(&cluster)
        .map_err(|error| invalid(format!("invalid Tailscale system resources: {error}")))?;
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    let dns_resolver_resources = Some(
        DnsResolverSystemResources::for_docker_node(
            &cluster,
            &node_id,
            &security,
            &store_encryption_secret,
            &running_version,
        )
        .map_err(|error| invalid(format!("invalid DNS resolver system resources: {error}")))?,
    );
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    let dns_resolver_resources: Option<DnsResolverSystemResources> = None;
    let traefik_resources = Some(
        TraefikSystemResources::for_cluster(&cluster, &security)
            .map_err(|error| invalid(format!("invalid Traefik system resources: {error}")))?,
    );
    let cloudflare_resources =
        CloudflareSystemResources::from_cluster(&cluster).map_err(|error| {
            invalid(format!(
                "invalid Cloudflare Tunnel system resources: {error}"
            ))
        })?;
    let system_host_ports = traefik_resources
        .as_ref()
        .map(TraefikSystemResources::host_port_grants)
        .unwrap_or_default();
    let mut operator_settings = OperatorSettings::production(&cluster)?;
    if let Some(resources) = &traefik_resources {
        operator_settings
            .firewall
            .system_services
            .insert(resources.service.meta.id.clone());
        #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
        {
            operator_settings.firewall.host_port_routes = resources.firewall_routes();
        }
    }
    if let Some(resources) = &cloudflare_resources {
        operator_settings
            .firewall
            .system_services
            .insert(resources.service.meta.id.clone());
    }
    if let Some(resources) = &tailscale_resources {
        let service_id = resources.service.meta.id.clone();
        operator_settings
            .firewall
            .system_services
            .insert(service_id);
        operator_settings
            .firewall
            .system_host_access
            .push(resources.system_host_access.clone());
    }
    operator_settings.preview = configured_preview
        .as_ref()
        .map(|preview| preview.settings.clone());
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    {
        operator_settings.upgrade = Some(
            UpgradeSettings::new(Duration::from_secs(30), Duration::from_secs(5), 3)
                .map_err(|error| invalid(error.to_string()))?,
        );
    }
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    {
        operator_settings.upgrade = None;
    }
    let api_firewall_settings = operator_settings.firewall.clone();
    let store_upgrades = Some(
        StoreNodeUpgradeBackendSettings::new(Duration::from_secs(60 * 60), Duration::from_secs(2))
            .map_err(|error| invalid(error.to_string()))?,
    );
    let webhook_backend = Arc::new(
        HttpWebhookBackend::new(Duration::from_secs(10))
            .map_err(|error| invalid(error.to_string()))?,
    );
    let aws_sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let value_sources = Arc::new(AwsValueSourceResolver::new(&aws_sdk));
    let operator_workload = Arc::new(
        OperatorLeaderWorkload::new(
            cluster.cluster_id.clone(),
            clock.clone(),
            timestamp_clock.clone(),
            operator_settings,
            BuildOperatorBackends {
                source: build_source.clone(),
                revisions: build_source.clone(),
                artifacts: runtime.clone(),
                depot: depot_backend,
                value_sources: Some(value_sources.clone()),
                pull_requests: configured_preview.map(|preview| preview.pull_requests),
                upgrades: None,
                store_upgrades,
                webhooks: webhook_backend.clone(),
            },
        )
        .with_cloudflare_resources(cloudflare_resources)
        .with_tailscale_resources(tailscale_resources)
        .with_dns_resolver_resources(dns_resolver_resources)
        .with_traefik_resources(traefik_resources),
    );
    let plan = DaemonPlan::new(cluster, node_id, data_directory)?;
    let health_prober = Arc::new(NetworkHealthProber::new(Duration::from_secs(5))?);
    let (log_store_runtime, metric_store_runtime) =
        open_observability_stores(plan.data_directory()).await?;
    let datadog_sinks = build_datadog_sinks(configured_datadog, &log_store_runtime);
    let log_maintenance = configure_log_maintenance(
        log_backup.as_ref(),
        &plan.cluster().name,
        plan.node_id(),
        log_store_runtime.store(),
        datadog_sinks
            .logs
            .iter()
            .map(|sink| sink.id().clone())
            .collect(),
        clock.clone(),
        timestamp_clock,
    )
    .await?;
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    let network_stats_reader = Arc::new(HostNetworkStatsReader::production(runtime.clone()));
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    let network_stats_reader = Arc::new(RuntimeDelegatedNetworkStatsReader);
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    let mesh_backend = LinuxMeshBackend::new();
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    let mesh_backend = AbsentHostNetworkBackend;
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    let firewall_backend = NftablesFirewallBackend::new();
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    let firewall_backend = AbsentHostNetworkBackend;
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    let bridge_backend = LinuxWorkloadBridgeBackend::new();
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    let bridge_backend = AbsentHostNetworkBackend;
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    let workload_network_mode = kernel_api::WorkloadNetworkMode::ClusterRouted;
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    let workload_network_mode = kernel_api::WorkloadNetworkMode::RuntimeDelegated;
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    let host_telemetry = HostTelemetryDependencies::Available {
        resource_reader: Arc::new(LinuxHostStatsReader::production()),
        disk_reader: Arc::new(LinuxHostDiskReader::production()),
    };
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    let host_telemetry = HostTelemetryDependencies::Unavailable;
    let mut factory = DaemonRoleFactory::new(
        DaemonRoleDependencies {
            agent_store,
            mesh_backend,
            firewall_backend,
            bridge_backend,
            workload_network_mode,
            system_host_ports,
            dns_server_binder: Arc::new(HickoryDnsServerBinder),
            dns_plugin_settings,
            dns_upstream_settings: Some(dns_upstream_settings),
            workload_runtime: runtime.clone(),
            artifact_store: runtime.clone(),
            artifact_archives: build_source,
            log_store_runtime: Box::new(log_store_runtime),
            log_sinks: datadog_sinks.logs,
            metric_sinks: datadog_sinks.metrics,
            host_metric_sinks: datadog_sinks.host_metrics,
            metric_store_runtime: Box::new(metric_store_runtime),
            stats_reader: Arc::new(CgroupV2StatsReader),
            network_stats_reader,
            host_telemetry,
            network_provider: runtime,
            health_prober,
            volatile_root,
            mesh_identity,
            instance_id,
            running_version,
            monotonic_clock: clock,
            status_clock: Arc::new(SystemStatusClock),
            node_upgrade,
            api_settings,
            admin_api_settings,
            firewall_settings: api_firewall_settings,
        },
        DaemonRoleSettings::default(),
    )
    .with_log_maintenance(log_maintenance)
    .with_value_source_resolver(value_sources)
    .with_webhook_backend(webhook_backend)
    .with_leader_workload(operator_workload);
    if let Some(admin) = launch_config_admin {
        factory = factory.with_launch_config_admin(admin);
    }
    if let Some(admission) = admission {
        factory = factory.with_admission_dependencies(admission);
    }
    Daemon::new(plan, factory).start().await.map_err(Into::into)
}

fn dns_plugin_settings(
    cluster: &ClusterConfig,
) -> Result<Option<TailscaleDnsPluginSettings>, DaemonLaunchError> {
    let routes = cluster
        .tailscale
        .iter()
        .flat_map(|tailscale| &tailscale.cross_cluster_dns)
        .map(|route| TailscaleDnsRoute::new(route.cluster_id.clone(), route.nameservers.clone()))
        .collect::<Vec<_>>();
    if routes.is_empty() {
        Ok(None)
    } else {
        TailscaleDnsPluginSettings::new(cluster.cluster_id.clone(), routes, Duration::from_secs(5))
            .map(Some)
            .map_err(|error| invalid(format!("invalid cross-cluster DNS settings: {error}")))
    }
}

pub(crate) fn api_settings(
    cluster: &ClusterConfig,
    node: &cluster::NodeDefinition,
    security: &NodeCertificateBundle,
    jwt_secret_key: SecretValue,
) -> ServerSettings {
    let identity = TlsIdentity::new(
        security.identity.certificate_pem.clone(),
        security.identity.private_key_pem.clone(),
    );
    let settings = ServerSettings::new(
        SocketAddr::new(
            IpAddr::V4(node.endpoint.host_address),
            node.endpoint.api_port,
        ),
        Some(jwt_secret_key),
    )
    .with_tls_identity(identity.clone())
    .with_cluster_trust_root(security.trust_root_pem.clone())
    .with_cluster_client_identity(identity)
    .with_operator_proxy_cidrs(cluster.nodes.values().map(|node| node.workload_subnet));
    match packaged_panel_directory() {
        Some(directory) => settings.with_panel_directory(directory),
        None => settings,
    }
}

pub(crate) fn admin_api_settings(
    cluster: &ClusterConfig,
    node: &cluster::NodeDefinition,
    jwt_secret_key: SecretValue,
) -> Result<Option<ServerSettings>, DaemonLaunchError> {
    if cluster.tailscale.is_none() {
        return Ok(None);
    }
    #[cfg(any(target_os = "macos", feature = "macos-platform"))]
    {
        let _ = (cluster, node, jwt_secret_key);
        Ok(None)
    }
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    {
        let address = node.workload_subnet.admin_address().ok_or_else(|| {
            invalid(format!(
                "node workload subnet `{}` has no reserved Admin address",
                node.workload_subnet
            ))
        })?;
        let tailnet = TAILSCALE_IPV4_CIDR.parse().map_err(|error| {
            invalid(format!(
                "invalid built-in Tailscale operator CIDR `{TAILSCALE_IPV4_CIDR}`: {error}"
            ))
        })?;
        let settings = ServerSettings::new(
            SocketAddr::new(IpAddr::V4(address), node.endpoint.api_port),
            Some(jwt_secret_key),
        )
        .with_managed_operator_plaintext()
        .with_operator_proxy_cidrs(
            cluster
                .nodes
                .values()
                .map(|node| node.workload_subnet)
                .chain([tailnet]),
        );
        Ok(Some(match packaged_panel_directory() {
            Some(directory) => settings.with_panel_directory(directory),
            None => settings,
        }))
    }
}

fn packaged_panel_directory() -> Option<PathBuf> {
    let executable = std::env::current_exe().ok()?;
    let package_root = executable.parent()?.parent()?;
    panel_directory(package_root)
}

pub(crate) fn panel_directory(package_root: &Path) -> Option<PathBuf> {
    let directory = package_root.join("share").join("maestro-panel");
    directory.join("index.html").is_file().then_some(directory)
}

async fn open_observability_stores(
    data_directory: &Path,
) -> Result<(DuckLogStoreRuntime, DuckMetricStoreRuntime), DaemonLaunchError> {
    let agent_directory = data_directory.join("agent");
    let log_settings = DuckStoreSettings::new(agent_directory.join("logs.duckdb"), 1_024)?;
    let metric_settings = DuckStoreSettings::new(agent_directory.join("metrics.duckdb"), 1_024)?;
    let logs = DuckLogStoreRuntime::open(log_settings).await?;
    match DuckMetricStoreRuntime::open(metric_settings).await {
        Ok(metrics) => Ok((logs, metrics)),
        Err(error) => match logs.shutdown().await {
            Ok(()) => Err(error.into()),
            Err(rollback_error) => Err(DaemonLaunchError::ObservabilityStoreRollback {
                startup: error.to_string(),
                rollback: rollback_error.to_string(),
            }),
        },
    }
}

async fn connect_worker_store(
    cluster: &ClusterConfig,
    members: &BTreeMap<NodeId, StoreMember>,
    security: &NodeCertificateBundle,
    store_encryption_secret: &SecretValue,
) -> Result<Arc<dyn Store>, DaemonLaunchError> {
    let endpoints = members
        .values()
        .map(|member| {
            format!(
                "https://{}:{}",
                member.host_address, cluster.ports.store_client
            )
        })
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return Err(DaemonLaunchError::RemoteStore {
            detail: "topology declares no control-plane store endpoints".to_owned(),
        });
    }
    let tls = EtcdTlsConfig::for_endpoints(
        security.trust_root_pem.as_bytes().to_vec(),
        security.identity.certificate_pem.as_bytes().to_vec(),
        security
            .identity
            .private_key_pem
            .expose()
            .as_bytes()
            .to_vec(),
    );
    let encryption_key =
        derive_key(store_encryption_secret.expose()).map_err(|error| invalid(error.to_string()))?;
    EtcdStore::connect_with_tls_and_encryption(endpoints, tls, encryption_key)
        .await
        .map(|store| Arc::new(store) as Arc<dyn Store>)
        .map_err(|error| DaemonLaunchError::RemoteStore {
            detail: error.to_string(),
        })
}

fn control_plane_members(config: &ClusterConfig) -> BTreeMap<NodeId, StoreMember> {
    config
        .nodes
        .iter()
        .filter(|(_, node)| node.role.is_control_plane())
        .map(|(node_id, node)| {
            (
                node_id.clone(),
                StoreMember {
                    node_id: node_id.clone(),
                    host_address: node.endpoint.host_address,
                },
            )
        })
        .collect()
}

fn generate_instance_id() -> Result<NodeInstanceId, DaemonLaunchError> {
    let entropy = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    NodeInstanceId::new(format!("{}-{entropy}", std::process::id())).map_err(Into::into)
}
