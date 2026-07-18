use std::sync::Arc;
use std::time::Duration;

use base64::Engine;

use crate::cluster::bootstrap::BootstrapAction;
use crate::deployment::dns::DnsManager;
use crate::logs::{LogConfig, LogEntry, LogOrigin, Logger};
use crate::runtime::{BuildSpec, RunSpec, RuntimeProvider};
use crate::supervisor::{
    ContainerRef, SupervisedJobConfig, SupervisedJobStatus, controller::JobSupervisor,
};
use crate::utils::certs::EtcdCerts;

pub mod controller;
pub mod dns;
pub mod etcd;
pub mod ingress_blocklist;
pub mod keys;
pub mod provider;
pub mod store;
pub mod types;

pub use types::ControllerConfig;

pub const ETCD_IMAGE_TAG: &str = "quay.io/coreos/etcd:v3.6.8";
pub const INGRESS_IMAGE_TAG: &str = "traefik:v3.6";
pub const DNS_IMAGE_TAG: &str = "coredns/coredns:1.12.1";
const PROBE_IMAGE_NAME: &str = "maestro-probe";
const ADMIN_IMAGE_NAME: &str = "maestro-admin";
const TAILSCALE_IMAGE_NAME: &str = "maestro-tailscale";
pub const PROBE_IMAGE_TAG: &str = concat!("maestro-probe:", env!("CARGO_PKG_VERSION"));
pub const ADMIN_IMAGE_TAG: &str = concat!("maestro-admin:", env!("CARGO_PKG_VERSION"));
pub const TAILSCALE_IMAGE_TAG: &str = concat!("maestro-tailscale:", env!("CARGO_PKG_VERSION"));
pub const CLOUDFLARED_IMAGE_TAG: &str = "cloudflare/cloudflared:1852-21ca2e225ea5";
pub const MAX_CLOUDFLARED_REPLICAS: u32 = 25;

pub(crate) fn container_etcd_endpoints(
    cluster: Option<&crate::cluster::ClusterRuntime>,
    host_endpoints: &[String],
    disable_etcd_cert: bool,
) -> Vec<String> {
    let scheme = if disable_etcd_cert { "http" } else { "https" };
    let Some(cluster) = cluster else {
        return vec![format!("{scheme}://maestro-etcd:2379")];
    };
    if !cluster.role.is_voter() {
        return host_endpoints.to_vec();
    }

    let local_host_address = format!("{}:{}", cluster.host_ip, cluster.etcd_client_port);
    let mut endpoints = vec![format!("{scheme}://maestro-etcd:2379")];
    endpoints.extend(
        host_endpoints
            .iter()
            .filter(|endpoint| {
                endpoint
                    .trim_start_matches("https://")
                    .trim_start_matches("http://")
                    != local_host_address
            })
            .cloned(),
    );
    endpoints
}

pub(crate) fn ingress_access_log_args() -> Vec<String> {
    vec![
        "--accesslog=true".into(),
        "--accesslog.format=json".into(),
        "--accesslog.fields.defaultmode=keep".into(),
        "--accesslog.fields.headers.defaultMode=drop".into(),
        "--accesslog.fields.headers.names.X-Forwarded-For=keep".into(),
        "--accesslog.fields.headers.names.X-Real-IP=keep".into(),
        "--accesslog.fields.headers.names.CF-Connecting-IP=keep".into(),
    ]
}

pub struct SystemService {
    pub id: &'static str,
    pub name: &'static str,
    pub image: &'static str,
}

pub const SYSTEM_SERVICES: &[SystemService] = &[
    SystemService {
        id: "maestro-etcd",
        name: "etcd",
        image: ETCD_IMAGE_TAG,
    },
    SystemService {
        id: "maestro-ingress",
        name: "ingress",
        image: INGRESS_IMAGE_TAG,
    },
    SystemService {
        id: "maestro-gateway",
        name: "gateway",
        image: INGRESS_IMAGE_TAG,
    },
    SystemService {
        id: "maestro-dns",
        name: "dns",
        image: DNS_IMAGE_TAG,
    },
    SystemService {
        id: "maestro-probe",
        name: "controller",
        image: PROBE_IMAGE_TAG,
    },
    SystemService {
        id: "maestro-admin",
        name: "admin",
        image: ADMIN_IMAGE_TAG,
    },
    SystemService {
        id: "maestro-tailscale",
        name: "tailscale",
        image: TAILSCALE_IMAGE_TAG,
    },
    SystemService {
        id: "maestro-cloudflared",
        name: "cloudflared",
        image: CLOUDFLARED_IMAGE_TAG,
    },
];

fn write_private_file(path: &std::path::Path, content: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, content)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

pub struct SystemStartupInfo {
    pub dns_manager: Arc<DnsManager>,
    pub nameserver_ip: Option<String>,
    pub ingress_ip: Option<String>,
    pub leader_elector: Option<Arc<crate::cluster::elector::EtcdLeaderElector>>,
    pub cluster_handles: Vec<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SystemJobCapabilities {
    local_etcd: bool,
    ingress: bool,
    gateway: bool,
    admin: bool,
}

fn system_job_capabilities(role: Option<crate::cluster::NodeRole>) -> SystemJobCapabilities {
    match role {
        None | Some(crate::cluster::NodeRole::Hybrid) | Some(crate::cluster::NodeRole::Master) => {
            SystemJobCapabilities {
                local_etcd: true,
                ingress: true,
                gateway: role.is_some(),
                admin: true,
            }
        }
        Some(crate::cluster::NodeRole::Voter) => SystemJobCapabilities {
            local_etcd: true,
            ingress: false,
            gateway: false,
            admin: true,
        },
        Some(crate::cluster::NodeRole::Worker) => SystemJobCapabilities {
            local_etcd: false,
            ingress: true,
            gateway: true,
            admin: false,
        },
    }
}

fn system_log_tags(config: &ControllerConfig) -> Vec<String> {
    let mut tags = config.tags.clone();
    tags.push(format!("cluster:{}", config.cluster_name));
    if let Some(cluster) = &config.cluster {
        tags.push(format!("node:{}", cluster.node_id));
    }
    tags
}

pub async fn start_system_jobs(
    config: &ControllerConfig,
    dns_upstreams: &[std::net::Ipv4Addr],
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    logger: &Logger,
    supervisor: &mut JobSupervisor,
    shutdown: tokio::sync::broadcast::Receiver<crate::signal::ShutdownEvent>,
) -> SystemStartupInfo {
    let capabilities = system_job_capabilities(config.cluster.as_ref().map(|cluster| cluster.role));
    let mut leader_elector = None;
    let mut cluster_handles = Vec::new();
    let secrets_dir = config.data_dir.join("secrets");
    if secrets_dir.exists() {
        let _ = std::fs::remove_dir_all(&secrets_dir);
    }

    if let Err(err) = runtime.prune_containers().await {
        logger.emit(
            "warn",
            &format!("failed to prune stopped containers on startup: {err}"),
        );
    }
    if let Err(err) = runtime.prune_images().await {
        logger.emit(
            "warn",
            &format!("failed to prune unused images on startup: {err}"),
        );
    }

    let etcd_certs = if config.disable_etcd_cert {
        logger.emit(
            "warn",
            "etcd mTLS disabled (--disable-etcd-cert); etcd traffic is unencrypted",
        );
        None
    } else if config.cluster.is_some() {
        Some(
            crate::utils::certs::read_etcd_certs(&config.certs_dir())
                .expect("failed to read cluster node certificates"),
        )
    } else {
        let certs_dir = config.certs_dir();
        if certs_dir.exists() {
            let _ = std::fs::remove_dir_all(&certs_dir);
        }
        let certs = crate::utils::certs::generate_etcd_certs()
            .expect("failed to generate etcd TLS certificates");
        crate::utils::certs::write_etcd_certs(&certs_dir, &certs)
            .expect("failed to write etcd TLS certificates");
        logger.emit("info", "generated etcd mTLS certificates");
        Some(certs)
    };

    let mut bootstrap_action =
        crate::cluster::bootstrap::decide(config.cluster.as_ref(), &config.data_dir)
            .expect("failed to determine etcd bootstrap action");
    if let (Some(cluster), BootstrapAction::WaitForAdmission, Some(certs)) =
        (&config.cluster, &bootstrap_action, &etcd_certs)
    {
        let tls = build_etcd_tls_options(Some(certs)).expect("cluster TLS options");
        let join_info =
            crate::cluster::bootstrap::wait_for_admission(cluster, &config.data_dir, tls, logger)
                .await
                .expect("failed while waiting for etcd learner admission");
        bootstrap_action = BootstrapAction::JoinExisting(join_info);
    }
    if matches!(bootstrap_action, BootstrapAction::BootstrapSeed) {
        let tls = build_etcd_tls_options(etcd_certs.as_ref()).expect("cluster TLS options");
        crate::cluster::bootstrap::ensure_seed_is_fresh(
            config.cluster.as_ref().expect("cluster runtime"),
            tls,
        )
        .await
        .expect("fresh cluster safety check failed");
        crate::cluster::bootstrap::mark_seed_starting(&config.data_dir)
            .expect("failed to consume bootstrap permit");
    }

    let suffix = config.system_name();
    let dns_domain = format!("{}.maestro.internal", config.cluster_name);
    let etcd_container = format!("maestro-etcd-{suffix}");
    let probe_container = format!("maestro-probe-{suffix}");
    let ingress_container = format!("maestro-ingress-{suffix}");
    let gateway_container = format!("maestro-gateway-{suffix}");
    let dns_container = format!("maestro-dns-{suffix}");
    let admin_container = format!("maestro-admin-{suffix}");
    let tailscale_container = format!("maestro-tailscale-{suffix}");
    let cloudflared_container_prefix = format!("maestro-cloudflared-{suffix}");
    let _ = runtime.remove_container(&etcd_container).await;
    let _ = runtime.remove_container(&probe_container).await;
    let _ = runtime.remove_container(&ingress_container).await;
    let _ = runtime.remove_container(&gateway_container).await;
    let _ = runtime.remove_container(&dns_container).await;
    let _ = runtime.remove_container(&admin_container).await;
    let _ = runtime.remove_container(&tailscale_container).await;
    let _ = runtime
        .remove_container(&cloudflared_container_prefix)
        .await;
    for replica in 1..=config.cloudflare_tunnel_replicas {
        let _ = runtime
            .remove_container(&format!("{cloudflared_container_prefix}-{replica}"))
            .await;
    }
    if config.force {
        let static_ips = config
            .subnet
            .as_deref()
            .and_then(|cidr| {
                let ips = system_ips_from_cidr(cidr)?;
                let mut addresses = vec![
                    ips.etcd,
                    ips.probe,
                    ips.ingress,
                    ips.gateway,
                    ips.admin,
                    ips.dns,
                ];
                addresses.extend(
                    (1..=config.cloudflare_tunnel_replicas)
                        .filter_map(|replica| cloudflared_ip_from_cidr(cidr, replica)),
                );
                Some(addresses)
            })
            .unwrap_or_default();
        let no_names: Vec<String> = Vec::new();
        let _ = runtime
            .remove_conflicting_containers(&config.network, &no_names, &static_ips)
            .await;
        if let Err(error) = runtime.remove_network(&config.network).await {
            panic!(
                "[maestro]: failed to replace container network `{}`: {error}",
                config.network
            );
        }
    }
    if let Err(err) = runtime
        .ensure_network(&config.network, config.subnet.as_deref())
        .await
    {
        panic!("[maestro]: {err}");
    }

    let dns_dir = config.data_dir.join("system/dns");
    let dns_manager = Arc::new(DnsManager::new(dns_dir.clone()));
    DnsManager::write_corefile(
        &dns_dir,
        if config.tailscale_authkey.is_some() {
            5353
        } else {
            53
        },
        dns_upstreams,
    );

    let network_cidr = runtime.inspect_network_cidr(&config.network).await;
    if crate::cluster::migration::network_reconfiguration_required(&config.data_dir) {
        let expected = config
            .subnet
            .as_deref()
            .expect("cluster migration requires a configured subnet");
        if network_cidr.as_deref() != Some(expected) {
            panic!(
                "[maestro]: migrated cluster network `{}` has subnet {:?}, expected `{expected}`; the legacy network was not safely replaced",
                config.network, network_cidr
            );
        }
        crate::cluster::migration::complete_network_reconfiguration(&config.data_dir)
            .expect("failed to finalize legacy network migration");
    }
    let system_ips = network_cidr.as_deref().and_then(system_ips_from_cidr);

    if let Some(ips) = &system_ips {
        if capabilities.local_etcd {
            dns_manager.set_record("maestro-etcd", &dns_domain, &ips.etcd);
        }
        if capabilities.ingress {
            dns_manager.set_record("web", &dns_domain, &ips.ingress);
        }
        dns_manager.set_record("maestro-probe", &dns_domain, &ips.probe);
        if capabilities.admin {
            dns_manager.set_record("admin", &dns_domain, &ips.admin);
        }
        if capabilities.admin && config.tailscale_authkey.is_some() {
            dns_manager.set_record(
                "admin",
                &format!("{}.maestro.internal", config.cluster_alias),
                &ips.dns,
            );
        }
        let _ = dns_manager.flush();
    }

    let ip_flag = |ip: &str| vec!["--ip".to_string(), ip.to_string()];

    if capabilities.local_etcd {
        init_etcd(
            &etcd_container,
            &dns_domain,
            system_ips
                .as_ref()
                .map(|ips| ip_flag(&ips.etcd))
                .unwrap_or_default(),
            etcd_certs.as_ref(),
            &bootstrap_action,
            config,
            runtime,
            log_sender,
            supervisor,
        )
        .await;
    }

    if let (Some(cluster), Some(certs), BootstrapAction::Restart) =
        (&config.cluster, &etcd_certs, &bootstrap_action)
    {
        crate::cluster::bootstrap::reconcile_legacy_migration(
            cluster,
            &config.data_dir,
            build_etcd_tls_options(Some(certs)).expect("cluster TLS options"),
        )
        .await
        .expect("failed to reconcile legacy single-member cluster migration");
    }

    if let (Some(cluster), Some(certs)) = (&config.cluster, &etcd_certs)
        && cluster.role.is_voter()
        && cluster.is_seed()
    {
        let elector = Arc::new(
            crate::cluster::elector::EtcdLeaderElector::connect(
                &config.etcd_endpoints,
                build_etcd_tls_options(Some(certs)),
                cluster.node_id.clone(),
                true,
            )
            .await
            .expect("failed to connect cluster leader elector"),
        );
        cluster_handles.extend(
            elector
                .clone()
                .spawn(shutdown.resubscribe(), logger.clone()),
        );
        if matches!(bootstrap_action, BootstrapAction::BootstrapSeed) {
            elector
                .wait_until_leading(std::time::Duration::from_secs(30))
                .await
                .expect("bootstrap seed failed to acquire initial Maestro leadership");
        }
        leader_elector = Some(elector);
    }

    if let (Some(cluster), Some(certs)) = (&config.cluster, &etcd_certs) {
        let tls = build_etcd_tls_options(Some(certs)).expect("cluster TLS options");
        if let BootstrapAction::JoinExisting(join_info) = &bootstrap_action {
            crate::cluster::bootstrap::promote_when_ready(
                cluster,
                join_info.member_id,
                tls.clone(),
                logger,
            )
            .await
            .expect("failed to promote etcd learner");
        }
        if cluster.role.is_voter() {
            crate::cluster::bootstrap::write_cluster_meta(
                cluster,
                &config.cluster_alias,
                tls.clone(),
            )
            .await
            .expect("failed to initialize or validate cluster metadata");
            if cluster.is_seed() {
                crate::cluster::auth::bootstrap_initial(
                    cluster,
                    build_etcd_tls_options(Some(certs)).expect("cluster TLS options"),
                )
                .await
                .expect("failed to initialize etcd RBAC");
            }
            crate::cluster::auth::provision_local_voter_users(
                &config.etcd_endpoints,
                build_etcd_tls_options(Some(certs)).expect("cluster TLS options"),
                cluster.host_ip,
                cluster.identity_api_port,
                &cluster.node_id,
            )
            .await
            .expect("failed to provision local least-privilege etcd users");
            if matches!(bootstrap_action, BootstrapAction::ForceNewCluster) {
                crate::cluster::bootstrap::complete_force_new_cluster(&config.data_dir)
                    .expect("failed to finalize automatic etcd quorum recovery");
                logger.emit(
                    "info",
                    "recovered etcd quorum from the surviving voter state",
                );
            }
        } else {
            crate::cluster::bootstrap::validate_cluster_meta(cluster, &config.cluster_alias, tls)
                .await
                .expect("failed to validate cluster metadata");
        }
        if matches!(bootstrap_action, BootstrapAction::BootstrapSeed) {
            crate::cluster::bootstrap::mark_seed_joined(&config.data_dir)
                .expect("failed to finalize bootstrap permit");
        }
        if leader_elector.is_none() {
            let elector = Arc::new(
                crate::cluster::elector::EtcdLeaderElector::connect(
                    &config.etcd_endpoints,
                    build_etcd_tls_options(Some(certs)),
                    cluster.node_id.clone(),
                    cluster.role.is_voter(),
                )
                .await
                .expect("failed to connect cluster leader observer"),
            );
            cluster_handles.extend(
                elector
                    .clone()
                    .spawn(shutdown.resubscribe(), logger.clone()),
            );
            leader_elector = Some(elector);
        }
    }

    if config.tailscale_authkey.is_some() {
        init_tailnet(
            &tailscale_container,
            &dns_domain,
            logger,
            system_ips.as_ref().map(|ips| ips.dns.as_str()),
            config,
            runtime,
            log_sender,
            supervisor,
        )
        .await;
    } else if config.cluster.is_some() {
        init_dns(
            &dns_container,
            &dns_domain,
            system_ips.as_ref().map(|ips| ips.dns.as_str()),
            config,
            runtime,
            log_sender,
            supervisor,
        )
        .await;
    }

    let nameserver_ip = if config.tailscale_authkey.is_some() || config.cluster.is_some() {
        system_ips.as_ref().map(|ips| ips.dns.clone())
    } else {
        None
    };
    let dns_flag = dns_flag_for_runtime(runtime.as_ref(), nameserver_ip.as_deref());

    if capabilities.ingress {
        init_ingress(
            &ingress_container,
            &dns_domain,
            &dns_flag,
            system_ips
                .as_ref()
                .map(|ips| ip_flag(&ips.ingress))
                .unwrap_or_default(),
            network_cidr.as_deref(),
            etcd_certs.as_ref(),
            logger,
            config,
            runtime,
            log_sender,
            supervisor,
        )
        .await;
    }

    if capabilities.gateway {
        init_gateway(
            &gateway_container,
            &dns_domain,
            &dns_flag,
            system_ips
                .as_ref()
                .map(|ips| ip_flag(&ips.gateway))
                .unwrap_or_default(),
            etcd_certs
                .as_ref()
                .expect("cluster gateway requires node certificates"),
            logger,
            config,
            runtime,
            log_sender,
            supervisor,
        )
        .await;
    }

    init_probe(
        &probe_container,
        &dns_domain,
        &dns_flag,
        system_ips
            .as_ref()
            .map(|ips| ip_flag(&ips.probe))
            .unwrap_or_default(),
        etcd_certs.as_ref(),
        config,
        runtime,
        log_sender,
        supervisor,
    )
    .await;

    if capabilities.admin {
        init_admin(
            &admin_container,
            system_ips.as_ref().map(|ips| ips.probe.as_str()),
            &dns_domain,
            &dns_flag,
            system_ips
                .as_ref()
                .map(|ips| ip_flag(&ips.admin))
                .unwrap_or_default(),
            logger,
            config,
            runtime,
            log_sender,
            supervisor,
        )
        .await;
    }

    if capabilities.ingress && config.cloudflare_tunnel_token.is_some() {
        init_cloudflared(
            &cloudflared_container_prefix,
            &dns_domain,
            &dns_flag,
            network_cidr.as_deref(),
            logger,
            config,
            runtime,
            log_sender,
            supervisor,
        )
        .await;
    }

    SystemStartupInfo {
        dns_manager,
        nameserver_ip,
        ingress_ip: if capabilities.ingress {
            system_ips.map(|ips| ips.ingress)
        } else {
            None
        },
        leader_elector,
        cluster_handles,
    }
}

fn dns_flag_for_runtime(runtime: &dyn RuntimeProvider, coredns_ip: Option<&str>) -> Vec<String> {
    if let (true, Some(ip)) = (runtime.requires_explicit_dns(), coredns_ip) {
        vec!["--dns".to_string(), ip.to_string()]
    } else {
        Vec::new()
    }
}

async fn init_etcd(
    container_name: &str,
    dns_domain: &str,
    ip_flags: Vec<String>,
    etcd_certs: Option<&EtcdCerts>,
    bootstrap_action: &BootstrapAction,
    config: &ControllerConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let static_ip = static_ip_from_flags(&ip_flags);
    let etcd_data_dir = config.etcd_dir().join("data");
    std::fs::create_dir_all(&etcd_data_dir).expect("Failed to create etcd data dir");
    let etcd_data_path =
        std::fs::canonicalize(&etcd_data_dir).expect("error canonicalizing etcd data dir");
    let mut extra_flags = vec!["-v".into(), format!("{}:/data", etcd_data_path.display())];
    if let Some(cluster) = &config.cluster {
        extra_flags.extend([
            "-p".into(),
            format!("{}:{}:2379", cluster.host_ip, cluster.etcd_client_port),
            "-p".into(),
            format!("{}:{}:2380", cluster.host_ip, cluster.etcd_peer_port),
        ]);
    } else {
        extra_flags.extend(["-p".into(), format!("127.0.0.1:{}:2379", config.etcd_port)]);
    }
    if etcd_certs.is_some() {
        let certs_abs =
            std::fs::canonicalize(config.certs_dir()).expect("failed to canonicalize certs dir");
        extra_flags.extend(["-v".into(), format!("{}:/certs:ro", certs_abs.display())]);
    }
    extra_flags.extend(ip_flags);

    let scheme = if etcd_certs.is_some() {
        "https"
    } else {
        "http"
    };
    let mut image_and_args = vec![
        ETCD_IMAGE_TAG.into(),
        "etcd".into(),
        "--data-dir=/data".into(),
    ];
    if let Some(cluster) = &config.cluster {
        let member_name =
            crate::cluster::bootstrap::member_name_for_start(cluster, &config.data_dir)
                .expect("failed to resolve clustered etcd member name");
        let initial_cluster = clustered_initial_cluster(cluster, &member_name, bootstrap_action);
        let state = if matches!(bootstrap_action, BootstrapAction::BootstrapSeed) {
            "new"
        } else {
            "existing"
        };
        image_and_args.extend([
            format!("--name={member_name}"),
            "--listen-client-urls=https://0.0.0.0:2379".into(),
            "--listen-peer-urls=https://0.0.0.0:2380".into(),
            format!(
                "--advertise-client-urls=https://{}:{}",
                cluster.host_ip, cluster.etcd_client_port
            ),
            format!("--initial-advertise-peer-urls={}", cluster.peer_url()),
            format!("--initial-cluster={initial_cluster}"),
            format!("--initial-cluster-state={state}"),
            format!("--initial-cluster-token=maestro-{}", cluster.cluster_id),
            "--strict-reconfig-check=true".into(),
            "--auto-compaction-mode=periodic".into(),
            "--auto-compaction-retention=1h".into(),
            "--quota-backend-bytes=8589934592".into(),
        ]);
        if matches!(bootstrap_action, BootstrapAction::ForceNewCluster) {
            image_and_args.push("--force-new-cluster=true".into());
        }
    } else {
        image_and_args.extend([
            format!("--name=maestro-{}", config.cluster_name),
            format!("--listen-client-urls={scheme}://0.0.0.0:2379"),
            format!("--advertise-client-urls={scheme}://127.0.0.1:6479"),
        ]);
    }
    if etcd_certs.is_some() {
        image_and_args.extend([
            "--cert-file=/certs/server.pem".into(),
            "--key-file=/certs/server-key.pem".into(),
            "--trusted-ca-file=/certs/ca.pem".into(),
            "--client-cert-auth=true".into(),
        ]);
        if config.cluster.is_some() {
            image_and_args.extend([
                "--peer-cert-file=/certs/peer.pem".into(),
                "--peer-key-file=/certs/peer-key.pem".into(),
                "--peer-trusted-ca-file=/certs/ca.pem".into(),
                "--peer-client-cert-auth=true".into(),
            ]);
        }
    }

    let etcd_job_config = SupervisedJobConfig {
        id: "maestro-etcd".to_string(),
        command: runtime.run_command(&RunSpec {
            container_name: container_name.to_string(),
            hostname: "maestro-etcd".to_string(),
            dns_domain: Some(dns_domain.to_string()),
            network: config.network.clone(),
            extra_flags,
            image_and_args,
        }),
        name: "maestro-etcd".to_string(),
        max_restarts: None,
        restart_delay_ms: 100,
        max_restart_delay_ms: Some(15_000),
        shutdown_grace_period_ms: 10_000,
        container: Some(ContainerRef {
            name: container_name.to_string(),
            runtime_cli: runtime.cli_name().to_string(),
        }),
        secrets_mount: None,
        log_config: Some(LogConfig {
            sender: log_sender.clone(),
            tags: system_log_tags(config),
            origin: LogOrigin::System,
        }),
    };
    await_job_running(supervisor, etcd_job_config).await;
    let readiness_address = config.cluster.as_ref().map_or_else(
        || format!("127.0.0.1:{}", config.etcd_port),
        |cluster| format!("{}:{}", cluster.host_ip, cluster.etcd_client_port),
    );
    await_container_ready(
        runtime.as_ref(),
        container_name,
        static_ip.as_deref(),
        Some(&readiness_address),
    )
    .await;
}

fn clustered_initial_cluster(
    cluster: &crate::cluster::ClusterRuntime,
    member_name: &str,
    bootstrap_action: &BootstrapAction,
) -> String {
    match bootstrap_action {
        BootstrapAction::BootstrapSeed => format!("{member_name}={}", cluster.peer_url()),
        BootstrapAction::JoinExisting(join_info) => join_info.initial_cluster.clone(),
        BootstrapAction::Restart | BootstrapAction::ForceNewCluster => cluster
            .initial_voters
            .iter()
            .map(|node| {
                let name = if *node == cluster.local_endpoint() {
                    member_name.to_string()
                } else {
                    node.member_name()
                };
                format!("{name}={}", node.peer_url())
            })
            .collect::<Vec<_>>()
            .join(","),
        _ => unreachable!("invalid clustered voter bootstrap action"),
    }
}

async fn init_ingress(
    container_name: &str,
    dns_domain: &str,
    dns_flag: &[String],
    ip_flags: Vec<String>,
    network_cidr: Option<&str>,
    etcd_certs: Option<&EtcdCerts>,
    logger: &Logger,
    config: &ControllerConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let static_ip = static_ip_from_flags(&ip_flags);
    let tls = build_etcd_tls_options(etcd_certs);
    let connect_options = tls.map(|tls_opts| etcd_client::ConnectOptions::new().with_tls(tls_opts));
    if let Ok(mut client) =
        etcd_client::Client::connect(config.etcd_endpoints.iter(), connect_options).await
    {
        let _ = client.put("traefik", "", None).await;
    }

    let mut extra_flags: Vec<String> = Vec::new();
    for port in &config.ingress_ports {
        extra_flags.extend(["-p".into(), format!("0.0.0.0:{port}:8888")]);
    }
    if etcd_certs.is_some() {
        let certs_abs =
            std::fs::canonicalize(config.certs_dir()).expect("failed to canonicalize certs dir");
        extra_flags.extend(["-v".into(), format!("{}:/certs:ro", certs_abs.display())]);
    }
    if config.cluster.is_some() {
        let gateway_config =
            write_gateway_dynamic_config(config, etcd_certs.expect("cluster certs"))
                .expect("failed to write cluster gateway TLS configuration");
        let gateway_dir = gateway_config
            .parent()
            .expect("gateway config parent directory");
        extra_flags.extend([
            "-v".into(),
            format!("{}:/gateway:ro", gateway_dir.display()),
        ]);
    }
    extra_flags.extend_from_slice(dns_flag);
    extra_flags.extend(ip_flags);

    let provider_endpoints = if config.container_etcd_endpoints.is_empty() {
        "maestro-etcd:2379".to_string()
    } else {
        config
            .container_etcd_endpoints
            .iter()
            .map(|endpoint| {
                endpoint
                    .trim_start_matches("https://")
                    .trim_start_matches("http://")
            })
            .collect::<Vec<_>>()
            .join(",")
    };
    let mut image_and_args = vec![
        INGRESS_IMAGE_TAG.into(),
        "--providers.etcd=true".into(),
        "--providers.etcd.rootKey=traefik".into(),
        format!("--providers.etcd.endpoints={provider_endpoints}"),
        "--entrypoints.web.address=:8888".into(),
        "--entrypoints.metrics.address=:9100".into(),
        "--metrics.prometheus=true".into(),
        "--metrics.prometheus.entryPoint=metrics".into(),
        "--metrics.prometheus.addEntryPointsLabels=true".into(),
        "--metrics.prometheus.addRoutersLabels=true".into(),
        "--metrics.prometheus.addServicesLabels=true".into(),
        "--metrics.prometheus.buckets=1.0,5.0,10.0".into(),
    ];
    if config.cluster.is_some() {
        image_and_args.extend([
            "--providers.file.filename=/gateway/dynamic.yml".into(),
            "--entrypoints.internal.address=:80".into(),
            "--entrypoints.gateway.address=:8443".into(),
            "--entrypoints.gateway.http.tls=true".into(),
            "--entrypoints.gateway.http.tls.options=cluster-gateway@file".into(),
            "--ping=true".into(),
            "--ping.manualrouting=true".into(),
        ]);
    }
    if let Some(cidr) = network_cidr {
        image_and_args.push(format!(
            "--entrypoints.web.forwardedHeaders.trustedIPs={cidr}"
        ));
    }
    if config.enable_ingress_access_logs {
        image_and_args.extend(ingress_access_log_args());
    }
    if etcd_certs.is_some() {
        image_and_args.extend([
            "--providers.etcd.tls.cert=/certs/traefik-client.pem".into(),
            "--providers.etcd.tls.key=/certs/traefik-client-key.pem".into(),
            "--providers.etcd.tls.ca=/certs/ca.pem".into(),
        ]);
    }

    let ingress_job_config = SupervisedJobConfig {
        id: "maestro-ingress".to_string(),
        command: runtime.run_command(&RunSpec {
            container_name: container_name.to_string(),
            hostname: "web".to_string(),
            dns_domain: Some(dns_domain.to_string()),
            network: config.network.clone(),
            extra_flags,
            image_and_args,
        }),
        name: "maestro-ingress".to_string(),
        max_restarts: None,
        restart_delay_ms: 1_000,
        max_restart_delay_ms: Some(15_000),
        shutdown_grace_period_ms: 10_000,
        container: Some(ContainerRef {
            name: container_name.to_string(),
            runtime_cli: runtime.cli_name().to_string(),
        }),
        secrets_mount: None,
        log_config: Some(LogConfig {
            sender: log_sender.clone(),
            tags: system_log_tags(config),
            origin: LogOrigin::System,
        }),
    };
    await_job_running(supervisor, ingress_job_config).await;
    let readiness_address = format!("127.0.0.1:{}", config.ingress_ports[0]);
    await_container_ready(
        runtime.as_ref(),
        container_name,
        static_ip.as_deref(),
        Some(&readiness_address),
    )
    .await;
    for port in &config.ingress_ports {
        logger.emit(
            "info",
            &format!("ingress listening on http://0.0.0.0:{port}"),
        );
    }
}

fn write_gateway_dynamic_config(
    config: &ControllerConfig,
    certs: &EtcdCerts,
) -> std::io::Result<std::path::PathBuf> {
    let directory = config.data_dir.join("system/gateway");
    std::fs::create_dir_all(&directory)?;
    let identity_path = directory.join("traefik-client-identity.pem");
    write_private_file(
        &identity_path,
        &format!(
            "{}\n{}",
            certs.traefik_client_cert_pem, certs.traefik_client_key_pem
        ),
    )?;
    let dynamic_path = directory.join("dynamic.yml");
    write_private_file(&dynamic_path, &gateway_dynamic_config())?;
    Ok(dynamic_path)
}

fn gateway_dynamic_config() -> String {
    format!(
        r#"http:
  routers:
    maestro-gateway-health:
      entryPoints:
        - gateway
      rule: Path(`{}`)
      service: ping@internal
      priority: 10000
      tls:
        options: cluster-gateway
  serversTransports:
    cluster-gateway:
      rootCAs:
        - /certs/ca.pem
      certificates:
        - /gateway/traefik-client-identity.pem
tls:
  certificates:
    - certFile: /certs/api.pem
      keyFile: /certs/api-key.pem
  stores:
    default:
      defaultCertificate:
        certFile: /certs/api.pem
        keyFile: /certs/api-key.pem
  options:
    cluster-gateway:
      minVersion: VersionTLS13
      clientAuth:
        caFiles:
          - /certs/ca.pem
        clientAuthType: RequireAndVerifyClientCert
"#,
        crate::cluster::traefik::GATEWAY_HEALTH_PATH
    )
}

async fn init_gateway(
    container_name: &str,
    dns_domain: &str,
    dns_flag: &[String],
    ip_flags: Vec<String>,
    etcd_certs: &EtcdCerts,
    logger: &Logger,
    config: &ControllerConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let static_ip = static_ip_from_flags(&ip_flags);
    let cluster = config.cluster.as_ref().expect("cluster gateway runtime");
    let config_path = write_gateway_dynamic_config(config, etcd_certs)
        .expect("failed to write cluster gateway TLS configuration");
    let gateway_dir = config_path.parent().expect("gateway config directory");
    let certs_abs =
        std::fs::canonicalize(config.certs_dir()).expect("failed to canonicalize certs dir");
    let mut extra_flags = vec![
        "-p".to_string(),
        format!("{}:{}:8443", cluster.host_ip, cluster.gateway_port),
        "-v".to_string(),
        format!("{}:/certs:ro", certs_abs.display()),
        "-v".to_string(),
        format!("{}:/gateway:ro", gateway_dir.display()),
    ];
    extra_flags.extend_from_slice(dns_flag);
    extra_flags.extend(ip_flags);

    let provider_endpoints = config
        .container_etcd_endpoints
        .iter()
        .map(|endpoint| {
            endpoint
                .trim_start_matches("https://")
                .trim_start_matches("http://")
        })
        .collect::<Vec<_>>()
        .join(",");
    let root_key = format!("maestro-gateway/{}", cluster.node_id);
    let image_and_args = vec![
        INGRESS_IMAGE_TAG.into(),
        "--providers.etcd=true".into(),
        format!("--providers.etcd.rootKey={root_key}"),
        format!("--providers.etcd.endpoints={provider_endpoints}"),
        "--providers.etcd.tls.cert=/certs/traefik-client.pem".into(),
        "--providers.etcd.tls.key=/certs/traefik-client-key.pem".into(),
        "--providers.etcd.tls.ca=/certs/ca.pem".into(),
        "--providers.file.filename=/gateway/dynamic.yml".into(),
        "--entrypoints.gateway.address=:8443".into(),
        "--entrypoints.gateway.http.tls=true".into(),
        "--entrypoints.gateway.http.tls.options=cluster-gateway@file".into(),
        "--ping=true".into(),
        "--ping.manualrouting=true".into(),
    ];
    await_job_running(
        supervisor,
        SupervisedJobConfig {
            id: "maestro-gateway".to_string(),
            command: runtime.run_command(&RunSpec {
                container_name: container_name.to_string(),
                hostname: "maestro-gateway".to_string(),
                dns_domain: Some(dns_domain.to_string()),
                network: config.network.clone(),
                extra_flags,
                image_and_args,
            }),
            name: "maestro-gateway".to_string(),
            max_restarts: None,
            restart_delay_ms: 1_000,
            max_restart_delay_ms: Some(15_000),
            shutdown_grace_period_ms: 10_000,
            container: Some(ContainerRef {
                name: container_name.to_string(),
                runtime_cli: runtime.cli_name().to_string(),
            }),
            secrets_mount: None,
            log_config: Some(LogConfig {
                sender: log_sender.clone(),
                tags: system_log_tags(config),
                origin: LogOrigin::System,
            }),
        },
    )
    .await;
    let readiness_address = format!("{}:{}", cluster.host_ip, cluster.gateway_port);
    await_container_ready(
        runtime.as_ref(),
        container_name,
        static_ip.as_deref(),
        Some(&readiness_address),
    )
    .await;
    logger.emit(
        "info",
        &format!(
            "cluster node gateway listening on https://{}:{}",
            cluster.host_ip, cluster.gateway_port
        ),
    );
}

async fn init_admin(
    container_name: &str,
    probe_ip: Option<&str>,
    dns_domain: &str,
    dns_flag: &[String],
    ip_flags: Vec<String>,
    logger: &Logger,
    config: &ControllerConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let static_ip = static_ip_from_flags(&ip_flags);
    runtime
        .build_image(
            &BuildSpec {
                context_dir: config.project_dir.clone(),
                tag: ADMIN_IMAGE_TAG.to_string(),
                dockerfile: Some("Dockerfile.admin".to_string()),
                labels: Default::default(),
                build_args: Default::default(),
                secrets: Default::default(),
                command_env: Default::default(),
                builder: crate::config::BuilderType::Default,
                depot_project: None,
                push_to_registry: false,
            },
            None,
            None,
        )
        .await
        .expect("failed to build admin-ui image");

    let port_flag = config.admin_port.map(|p| format!("127.0.0.1:{}:80", p));
    let mut admin_flags: Vec<String> = Vec::new();
    if let Some(pf) = &port_flag {
        admin_flags.extend(["-p".to_string(), pf.clone()]);
    }
    let api_host = probe_ip
        .map(|ip| format!("http://{ip}:3001"))
        .unwrap_or_else(|| "http://maestro-probe:3001".to_string());
    admin_flags.extend(["-e".to_string(), format!("MAESTRO_API_HOST={api_host}")]);
    let service_jwt_key_path = config.data_dir.join("system/admin/service-jwt-key");
    if config.jwt_secret_key.is_some() {
        admin_flags.extend([
            "-v".to_string(),
            format!(
                "{}:/run/secrets/service-jwt-key:ro",
                service_jwt_key_path.display()
            ),
            "-e".to_string(),
            "MAESTRO_SERVICE_JWT_KEY_FILE=/run/secrets/service-jwt-key".to_string(),
        ]);
    }
    admin_flags.extend_from_slice(dns_flag);
    admin_flags.extend(ip_flags);

    let admin_job_config = SupervisedJobConfig {
        id: "maestro-admin".to_string(),
        command: runtime.run_command(&RunSpec {
            container_name: container_name.to_string(),
            hostname: "admin".to_string(),
            dns_domain: Some(dns_domain.to_string()),
            network: config.network.clone(),
            extra_flags: admin_flags,
            image_and_args: vec![ADMIN_IMAGE_TAG.into()],
        }),
        name: "maestro-admin".to_string(),
        max_restarts: None,
        restart_delay_ms: 1_000,
        max_restart_delay_ms: Some(15_000),
        shutdown_grace_period_ms: 10_000,
        container: Some(ContainerRef {
            name: container_name.to_string(),
            runtime_cli: runtime.cli_name().to_string(),
        }),
        secrets_mount: config.jwt_secret_key.as_ref().map(|secret| {
            crate::supervisor::SecretsMount {
                host_path: service_jwt_key_path,
                container_path: "/run/secrets/service-jwt-key".to_string(),
                content: secret.clone(),
            }
        }),
        log_config: Some(LogConfig {
            sender: log_sender.clone(),
            tags: system_log_tags(config),
            origin: LogOrigin::System,
        }),
    };
    await_job_running(supervisor, admin_job_config).await;
    let readiness_address = config.admin_port.map(|port| format!("127.0.0.1:{port}"));
    await_container_ready(
        runtime.as_ref(),
        container_name,
        static_ip.as_deref(),
        readiness_address.as_deref(),
    )
    .await;
    if let Some(port) = config.probe_port {
        logger.emit(
            "info",
            &format!("probe api listening on http://127.0.0.1:{port}"),
        );
    }
    if let Some(port) = config.admin_port {
        logger.emit(
            "info",
            &format!("admin ui listening on http://127.0.0.1:{port}"),
        );
    }
    logger.emit(
        "info",
        &format!("admin ui running at http://admin.{dns_domain}"),
    );
}

async fn init_probe(
    container_name: &str,
    dns_domain: &str,
    dns_flag: &[String],
    ip_flags: Vec<String>,
    etcd_certs: Option<&EtcdCerts>,
    config: &ControllerConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let static_ip = static_ip_from_flags(&ip_flags);
    runtime
        .build_image(
            &BuildSpec {
                context_dir: config.project_dir.clone(),
                tag: PROBE_IMAGE_TAG.to_string(),
                dockerfile: Some("Dockerfile.probe".to_string()),
                labels: Default::default(),
                build_args: Default::default(),
                secrets: Default::default(),
                command_env: Default::default(),
                builder: crate::config::BuilderType::Default,
                depot_project: None,
                push_to_registry: false,
            },
            None,
            None,
        )
        .await
        .expect("failed to build probe image");
    let probe_dir = config.probe_dir();
    let probe_data_dir = probe_dir.join("data");
    std::fs::create_dir_all(&probe_data_dir).expect("Failed to create probe data dir");
    std::fs::create_dir_all(probe_data_dir.join("uploads"))
        .expect("Failed to create probe uploads dir");
    let probe_data_abs =
        std::fs::canonicalize(&probe_data_dir).expect("failed to canonicalize probe data dir");
    let encryption_key_path = probe_dir.join("encryption-key");
    let encryption_key_abs = std::fs::canonicalize(&probe_dir)
        .expect("failed to canonicalize probe dir")
        .join("encryption-key");
    let ingestion_token_abs = std::fs::canonicalize(&probe_dir)
        .expect("failed to canonicalize probe dir")
        .join("ingestion-token");
    let jwt_key_path = probe_dir.join("jwt-key");
    if let Some(secret) = &config.jwt_secret_key {
        write_private_file(&jwt_key_path, secret).expect("failed to write probe JWT key");
    }
    let control_dir = config.data_dir.join("system/control");
    if config.cluster.is_some() {
        std::fs::create_dir_all(&control_dir).expect("failed to create control directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&control_dir, std::fs::Permissions::from_mode(0o700))
                .expect("failed to protect control directory");
        }
        write_private_file(
            &control_dir.join("control-token"),
            config.internal_control_token.as_str(),
        )
        .expect("failed to write internal control token");
    }
    let probe_host_port = config.probe_port.expect("probe_port should be resolved");
    let etcd_scheme = if etcd_certs.is_some() {
        "https"
    } else {
        "http"
    };
    let probe_job_config = SupervisedJobConfig {
        id: "maestro-probe".to_string(),
        command: {
            let etcd_endpoint = config
                .container_etcd_endpoints
                .first()
                .cloned()
                .unwrap_or_else(|| format!("{etcd_scheme}://maestro-etcd:2379"));
            let mut probe_flags: Vec<String> = vec![
                "-p".into(),
                format!("127.0.0.1:{probe_host_port}:3001"),
                "-v".into(),
                format!("{}:/data", probe_data_abs.display()),
                "-v".into(),
                format!(
                    "{}:/run/secrets/encryption-key:ro",
                    encryption_key_abs.display()
                ),
                "-v".into(),
                format!(
                    "{}:/run/secrets/ingestion-token:ro",
                    ingestion_token_abs.display()
                ),
                "-e".into(),
                format!("ETCD_ENDPOINT={etcd_endpoint}"),
                "-e".into(),
                format!(
                    "ETCD_ENDPOINTS={}",
                    config.container_etcd_endpoints.join(",")
                ),
                "-e".into(),
                "MAESTRO_ENCRYPTION_KEY_FILE=/run/secrets/encryption-key".into(),
                "-e".into(),
                "MAESTRO_INGESTION_TOKEN_FILE=/run/secrets/ingestion-token".into(),
                "-e".into(),
                "PORT=3001".into(),
                "-e".into(),
                format!("MAESTRO_DNS_DOMAIN={dns_domain}"),
                "-e".into(),
                format!("MAESTRO_CLUSTER_NAME={}", config.cluster_name),
                "-e".into(),
                format!("MAESTRO_CLUSTER_ALIAS={}", config.cluster_alias),
                "-e".into(),
                format!(
                    "MAESTRO_CONFIG={}",
                    base64::engine::general_purpose::STANDARD.encode(&config.maestro_config)
                ),
            ];
            if config.cluster.is_none() {
                probe_flags.extend([
                    "-v".into(),
                    "/:/host/root:ro".into(),
                    "-e".into(),
                    "MAESTRO_HOST_ROOT=/host/root".into(),
                ]);
            }
            if let Some(cluster) = &config.cluster {
                probe_flags.extend([
                    "-v".into(),
                    format!("{}:/run/maestro-control", control_dir.display()),
                    "-e".into(),
                    "MAESTRO_CONTROL_SOCKET=/run/maestro-control/control.sock".into(),
                    "-e".into(),
                    "MAESTRO_CONTROL_TOKEN_FILE=/run/maestro-control/control-token".into(),
                    "-p".into(),
                    format!("{}:{probe_host_port}:3002", cluster.host_ip),
                    "-e".into(),
                    "MAESTRO_TLS_PORT=3002".into(),
                    "-e".into(),
                    "MAESTRO_API_CERT_FILE=/certs/api.pem".into(),
                    "-e".into(),
                    "MAESTRO_API_KEY_FILE=/certs/api-key.pem".into(),
                    "-e".into(),
                    format!("MAESTRO_NODE_ID={}", cluster.node_id),
                ]);
            }
            if config.jwt_secret_key.is_some() {
                probe_flags.extend([
                    "-v".into(),
                    format!("{}:/run/secrets/jwt-key:ro", jwt_key_path.display()),
                    "-e".into(),
                    "MAESTRO_JWT_SECRET_KEY_FILE=/run/secrets/jwt-key".into(),
                ]);
            }
            if etcd_certs.is_some() {
                let certs_abs = std::fs::canonicalize(config.certs_dir())
                    .expect("failed to canonicalize certs dir");
                probe_flags.extend([
                    "-v".into(),
                    format!("{}:/certs:ro", certs_abs.display()),
                    "-e".into(),
                    "ETCD_CA_FILE=/certs/ca.pem".into(),
                    "-e".into(),
                    "ETCD_CERT_FILE=/certs/probe-client.pem".into(),
                    "-e".into(),
                    "ETCD_KEY_FILE=/certs/probe-client-key.pem".into(),
                ]);
            }
            if let Some(system_type) = &config.system_type {
                probe_flags.extend(["-e".into(), format!("MAESTRO_SYSTEM_TYPE={system_type}")]);
            }
            if let Some(slack_url) = &config.slack_webhook_url {
                probe_flags.extend([
                    "-e".into(),
                    format!("MAESTRO_SLACK_WEBHOOK_URL={}", slack_url.as_str()),
                ]);
            }
            for name in [
                "AWS_REGION",
                "AWS_DEFAULT_REGION",
                "AWS_ENDPOINT_URL_S3",
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            ] {
                if let Ok(value) = std::env::var(name) {
                    probe_flags.extend(["-e".into(), format!("{name}={value}")]);
                }
            }
            probe_flags.extend_from_slice(dns_flag);
            probe_flags.extend(ip_flags);
            runtime.run_command(&RunSpec {
                container_name: container_name.to_string(),
                hostname: "maestro-probe".to_string(),
                dns_domain: Some(dns_domain.to_string()),
                network: config.network.clone(),
                extra_flags: probe_flags,
                image_and_args: vec![PROBE_IMAGE_TAG.into()],
            })
        },
        name: "maestro-probe".to_string(),
        max_restarts: None,
        restart_delay_ms: 1_000,
        max_restart_delay_ms: Some(15_000),
        shutdown_grace_period_ms: 60_000,
        container: Some(ContainerRef {
            name: container_name.to_string(),
            runtime_cli: runtime.cli_name().to_string(),
        }),
        secrets_mount: Some(crate::supervisor::SecretsMount {
            host_path: encryption_key_path,
            container_path: "/run/secrets/encryption-key".to_string(),
            content: config.encryption_key.as_str().to_string(),
        }),
        log_config: Some(LogConfig {
            sender: log_sender.clone(),
            tags: system_log_tags(config),
            origin: LogOrigin::System,
        }),
    };
    await_job_running(supervisor, probe_job_config).await;
    let readiness_address = format!("127.0.0.1:{probe_host_port}");
    await_container_ready(
        runtime.as_ref(),
        container_name,
        static_ip.as_deref(),
        Some(&readiness_address),
    )
    .await;
    let port_path = probe_dir.join("api-port");
    let temp_port_path = probe_dir.join(format!("api-port.tmp-{}", std::process::id()));
    std::fs::write(&temp_port_path, probe_host_port.to_string())
        .expect("failed to write probe API port");
    std::fs::File::open(&temp_port_path)
        .and_then(|file| file.sync_all())
        .expect("failed to sync probe API port");
    std::fs::rename(&temp_port_path, &port_path).expect("failed to persist probe API port");
    if let Ok(directory) = std::fs::File::open(&probe_dir) {
        let _ = directory.sync_all();
    }
}

async fn init_dns(
    container_name: &str,
    dns_domain: &str,
    static_ip: Option<&str>,
    config: &ControllerConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let dns_dir = std::fs::canonicalize(config.data_dir.join("system/dns"))
        .expect("failed to canonicalize dns dir");
    let mut extra_flags = vec![
        "-v".to_string(),
        format!("{}:/data/dns:ro", dns_dir.display()),
    ];
    if let Some(ip) = static_ip {
        extra_flags.extend(["--ip".to_string(), ip.to_string()]);
    }
    await_job_running(
        supervisor,
        SupervisedJobConfig {
            id: "maestro-dns".to_string(),
            command: runtime.run_command(&RunSpec {
                container_name: container_name.to_string(),
                hostname: "maestro-dns".to_string(),
                dns_domain: Some(dns_domain.to_string()),
                network: config.network.clone(),
                extra_flags,
                image_and_args: vec![
                    DNS_IMAGE_TAG.to_string(),
                    "-conf".to_string(),
                    "/data/dns/Corefile".to_string(),
                ],
            }),
            name: "maestro-dns".to_string(),
            max_restarts: None,
            restart_delay_ms: 1_000,
            max_restart_delay_ms: Some(15_000),
            shutdown_grace_period_ms: 10_000,
            container: Some(ContainerRef {
                name: container_name.to_string(),
                runtime_cli: runtime.cli_name().to_string(),
            }),
            secrets_mount: None,
            log_config: Some(LogConfig {
                sender: log_sender.clone(),
                tags: system_log_tags(config),
                origin: LogOrigin::System,
            }),
        },
    )
    .await;
}

async fn init_tailnet(
    container_name: &str,
    dns_domain: &str,
    logger: &Logger,
    static_ip: Option<&str>,
    config: &ControllerConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let Some(authkey) = &config.tailscale_authkey else {
        return;
    };
    let network_cidr = runtime.inspect_network_cidr(&config.network).await;
    let Some(routes) =
        tailscale_advertise_routes(network_cidr.as_deref(), &config.tailscale_advertise_routes)
    else {
        logger.emit(
            "warn",
            "failed to discover network CIDR; skipping tailscale setup",
        );
        return;
    };
    let advertise_routes = routes.join(",");

    runtime
        .build_image(
            &BuildSpec {
                context_dir: config.project_dir.clone(),
                tag: TAILSCALE_IMAGE_TAG.to_string(),
                dockerfile: Some("dns/Dockerfile.tailscale".to_string()),
                labels: Default::default(),
                build_args: Default::default(),
                secrets: Default::default(),
                command_env: Default::default(),
                builder: crate::config::BuilderType::Default,
                depot_project: None,
                push_to_registry: false,
            },
            None,
            None,
        )
        .await
        .expect("failed to build tailscale image");

    let state_dir = config.data_dir.join("system/tailscale/state");
    std::fs::create_dir_all(&state_dir).expect("Failed to create tailscale state dir");
    let state_dir =
        std::fs::canonicalize(&state_dir).expect("failed to canonicalize tailscale state dir");
    let ts_authkey_path = config.data_dir.join("system/tailscale/authkey");
    let ts_authkey_abs = std::fs::canonicalize(config.data_dir.join("system/tailscale"))
        .expect("failed to canonicalize tailscale dir")
        .join("authkey");
    await_job_running(
        supervisor,
        SupervisedJobConfig {
            id: "maestro-tailscale".to_string(),
            command: {
                let ts_hostname = config
                    .cluster
                    .as_ref()
                    .map(|cluster| {
                        format!(
                            "maestro-tailscale-{}-{}",
                            config.cluster_name,
                            &cluster.node_id[..12]
                        )
                    })
                    .unwrap_or_else(|| format!("maestro-tailscale-{}", config.cluster_name));
                let dns_dir_abs = std::fs::canonicalize(config.data_dir.join("system/dns"))
                    .expect("failed to canonicalize dns dir");
                let mut flags: Vec<String> = vec![
                    "-v".to_string(),
                    format!("{}:/var/lib/tailscale", state_dir.display()),
                    "-v".to_string(),
                    format!("{}:/run/secrets/ts-authkey:ro", ts_authkey_abs.display()),
                    "-v".to_string(),
                    format!("{}:/data/dns", dns_dir_abs.display()),
                    "-e".to_string(),
                    "TS_STATE_DIR=/var/lib/tailscale".to_string(),
                    "-e".to_string(),
                    format!("TS_HOSTNAME={ts_hostname}"),
                    "-e".to_string(),
                    "TS_EXTRA_ARGS=--accept-dns=false".to_string(),
                    "-e".to_string(),
                    "MAESTRO_DNS_UPSTREAM=coredns".to_string(),
                ];
                flags.extend(tailscale_container_network_flags(&advertise_routes));
                if let Some(ip) = static_ip {
                    flags.extend(["--ip".to_string(), ip.to_string()]);
                }
                runtime.run_command(&RunSpec {
                    container_name: container_name.to_string(),
                    hostname: ts_hostname,
                    dns_domain: Some(dns_domain.to_string()),
                    network: config.network.clone(),
                    extra_flags: flags,
                    image_and_args: vec![
                        TAILSCALE_IMAGE_TAG.into(),
                        config.cluster_name.clone(),
                        config.cluster_alias.clone(),
                    ],
                })
            },
            name: "maestro-tailscale".to_string(),
            max_restarts: None,
            restart_delay_ms: 1_000,
            max_restart_delay_ms: Some(15_000),
            shutdown_grace_period_ms: 10_000,
            container: Some(ContainerRef {
                name: container_name.to_string(),
                runtime_cli: runtime.cli_name().to_string(),
            }),
            secrets_mount: Some(crate::supervisor::SecretsMount {
                host_path: ts_authkey_path,
                container_path: "/run/secrets/ts-authkey".to_string(),
                content: authkey.clone(),
            }),
            log_config: Some(LogConfig {
                sender: log_sender.clone(),
                tags: system_log_tags(config),
                origin: LogOrigin::System,
            }),
        },
    )
    .await;

    let routes_arg = format!("--advertise-routes={advertise_routes}");
    let set_args = vec!["tailscale", "set", &routes_arg];
    let _ = runtime.exec_in_container(container_name, &set_args).await;

    if advertise_routes.is_empty() {
        logger.emit(
            "info",
            "tailscale userspace peer started without subnet routes",
        );
    } else {
        logger.emit(
            "info",
            &format!("tailscale subnet router started, advertising routes {advertise_routes}"),
        );
    }
    let resolved_ip = static_ip
        .map(String::from)
        .or(runtime.inspect_container_ip(container_name).await);
    if let Some(ip) = &resolved_ip {
        logger.emit("info", &format!("dns server running at {ip}"));
        logger.emit(
            "info",
            &format!(
                "configure split DNS in Tailscale admin: nameserver {ip} for \"maestro.internal\""
            ),
        );
    }
    logger.emit(
        "info",
        &format!(
            "canonical: {}.maestro.internal (active)",
            config.cluster_name
        ),
    );
    logger.emit(
        "info",
        &format!(
            "alias: {}.maestro.internal (pending peer conflict check)",
            config.cluster_alias
        ),
    );
}

fn tailscale_advertise_routes(
    network_cidr: Option<&str>,
    configured_routes: &[String],
) -> Option<Vec<String>> {
    let mut routes = vec![network_cidr?.to_string()];
    for route in configured_routes {
        if !routes.contains(route) {
            routes.push(route.clone());
        }
    }
    Some(routes)
}

fn tailscale_container_network_flags(advertise_routes: &str) -> Vec<String> {
    vec![
        "-e".to_string(),
        format!("TS_ROUTES={advertise_routes}"),
        "-e".to_string(),
        "TS_USERSPACE=true".to_string(),
    ]
}

async fn init_cloudflared(
    container_name_prefix: &str,
    dns_domain: &str,
    dns_flag: &[String],
    network_cidr: Option<&str>,
    logger: &Logger,
    config: &ControllerConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let token = config
        .cloudflare_tunnel_token
        .as_ref()
        .expect("init_cloudflared called without cloudflare tunnel token");

    let mut flags: Vec<String> = vec!["-e".to_string(), format!("TUNNEL_TOKEN={}", token.as_str())];
    flags.extend_from_slice(dns_flag);
    let gate_on_data_plane = config.cluster.is_some();
    let initial_network = if gate_on_data_plane && runtime.supports_dynamic_network_attachment() {
        "none".to_string()
    } else {
        config.network.clone()
    };

    for replica in 1..=config.cloudflare_tunnel_replicas {
        let container_name = format!("{container_name_prefix}-{replica}");
        let hostname = format!("maestro-cloudflared-{replica}");
        let job_id = format!("maestro-cloudflared-{replica}");
        let static_ip = network_cidr
            .and_then(|cidr| cloudflared_ip_from_cidr(cidr, replica))
            .unwrap_or_else(|| {
                panic!(
                    "[maestro]: cloudflared replica {replica} exceeds the reserved system address capacity"
                )
            });
        let mut replica_flags = flags.clone();
        if initial_network != "none" {
            replica_flags.extend(["--ip".to_string(), static_ip.clone()]);
        }

        await_job_running(
            supervisor,
            SupervisedJobConfig {
                id: job_id.clone(),
                command: runtime.run_command(&RunSpec {
                    container_name: container_name.clone(),
                    hostname,
                    dns_domain: Some(dns_domain.to_string()),
                    network: initial_network.clone(),
                    extra_flags: replica_flags,
                    image_and_args: vec![
                        CLOUDFLARED_IMAGE_TAG.to_string(),
                        "tunnel".to_string(),
                        "--no-autoupdate".to_string(),
                        "run".to_string(),
                    ],
                }),
                name: job_id,
                max_restarts: None,
                restart_delay_ms: 1_000,
                max_restart_delay_ms: Some(15_000),
                shutdown_grace_period_ms: 10_000,
                container: Some(ContainerRef {
                    name: container_name.clone(),
                    runtime_cli: runtime.cli_name().to_string(),
                }),
                secrets_mount: None,
                log_config: Some(LogConfig {
                    sender: log_sender.clone(),
                    tags: system_log_tags(config),
                    origin: LogOrigin::System,
                }),
            },
        )
        .await;
        if gate_on_data_plane {
            let gate_result = runtime
                .set_container_network_access(
                    &container_name,
                    &config.network,
                    false,
                    Some(&static_ip),
                )
                .await;
            if let Some(warning) = initial_cloudflared_gate_warning(&container_name, gate_result) {
                logger.emit("warn", &warning);
            }
        }
    }

    logger.emit(
        "info",
        &format!(
            "cloudflared tunnel started ({} replica{})",
            config.cloudflare_tunnel_replicas,
            if config.cloudflare_tunnel_replicas == 1 {
                ""
            } else {
                "s"
            }
        ),
    );
}

fn initial_cloudflared_gate_warning(
    container_name: &str,
    result: anyhow::Result<()>,
) -> Option<String> {
    result.err().map(|error| {
        format!(
            "could not apply the initial data-plane gate to cloudflared container `{container_name}`; the connector remains unready and reconciliation will retry: {error}"
        )
    })
}

struct SystemIps {
    etcd: String,
    ingress: String,
    gateway: String,
    probe: String,
    admin: String,
    dns: String,
}

fn system_ips_from_cidr(network_cidr: &str) -> Option<SystemIps> {
    let subnet = crate::cluster::network::Ipv4Cidr::parse(network_cidr).ok()?;
    Some(SystemIps {
        dns: subnet.system_address_from_end(1)?.to_string(),
        etcd: subnet.system_address_from_end(2)?.to_string(),
        ingress: subnet.system_address_from_end(3)?.to_string(),
        probe: subnet.system_address_from_end(4)?.to_string(),
        admin: subnet.system_address_from_end(5)?.to_string(),
        gateway: subnet.system_address_from_end(6)?.to_string(),
    })
}

pub(crate) fn cloudflared_ip_from_cidr(network_cidr: &str, replica: u32) -> Option<String> {
    let subnet = crate::cluster::network::Ipv4Cidr::parse(network_cidr).ok()?;
    let offset = 6_u32.checked_add(replica)?;
    (replica <= MAX_CLOUDFLARED_REPLICAS)
        .then(|| subnet.system_address_from_end(offset))
        .flatten()
        .map(|address| address.to_string())
}

pub fn build_etcd_tls_options(certs: Option<&EtcdCerts>) -> Option<etcd_client::TlsOptions> {
    certs.map(|certs| {
        let ca = etcd_client::Certificate::from_pem(certs.ca_pem.clone());
        let identity = etcd_client::Identity::from_pem(
            certs.client_cert_pem.clone(),
            certs.client_key_pem.clone(),
        );
        etcd_client::TlsOptions::new()
            .ca_certificate(ca)
            .identity(identity)
    })
}

pub fn build_etcd_tls_from_files(
    ca_path: &str,
    cert_path: &str,
    key_path: &str,
) -> Option<etcd_client::TlsOptions> {
    let ca_pem = std::fs::read_to_string(ca_path).ok()?;
    let cert_pem = std::fs::read_to_string(cert_path).ok()?;
    let key_pem = std::fs::read_to_string(key_path).ok()?;
    let ca = etcd_client::Certificate::from_pem(ca_pem);
    let identity = etcd_client::Identity::from_pem(cert_pem, key_pem);
    Some(
        etcd_client::TlsOptions::new()
            .ca_certificate(ca)
            .identity(identity),
    )
}

async fn await_job_running(supervisor: &mut JobSupervisor, config: SupervisedJobConfig) {
    let name = config.name.clone();
    let job_id = supervisor.start_job(config);
    if let Some(job_id) = job_id {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let status = supervisor.job_status(&job_id).await;
            match status {
                Some(SupervisedJobStatus::Running) => break,
                Some(SupervisedJobStatus::Crashed | SupervisedJobStatus::Completed) | None => {
                    panic!("[maestro]: system job `{name}` stopped during startup")
                }
                Some(SupervisedJobStatus::Pending | SupervisedJobStatus::Stopped) => {}
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("[maestro]: system job `{name}` did not start within 30 seconds");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn static_ip_from_flags(flags: &[String]) -> Option<String> {
    flags
        .windows(2)
        .find(|pair| pair[0] == "--ip")
        .map(|pair| pair[1].clone())
}

async fn await_container_ready(
    runtime: &dyn RuntimeProvider,
    container_name: &str,
    expected_ip: Option<&str>,
    readiness_address: Option<&str>,
) {
    let discovered_ip = runtime.inspect_container_ip(container_name).await;
    match (expected_ip, discovered_ip.as_deref()) {
        (Some(expected), Some(actual)) if expected != actual => panic!(
            "[maestro]: system container `{container_name}` received `{actual}`, expected `{expected}`"
        ),
        (Some(_), Some(_)) | (None, Some(_)) => {}
        (None, None) => panic!(
            "[maestro]: system container `{container_name}` has no network address after startup"
        ),
        (Some(expected), None) => panic!(
            "[maestro]: system container `{container_name}` did not acquire expected address `{expected}`"
        ),
    }
    let Some(address) = readiness_address else {
        return;
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::net::TcpStream::connect(&address).await.is_ok() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "[maestro]: system container `{container_name}` did not accept TCP connections at `{address}` within 30 seconds"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::NodeRole;

    fn cluster_runtime(role: NodeRole) -> crate::cluster::ClusterRuntime {
        crate::cluster::ClusterRuntime {
            cluster_id: "cluster-id".to_string(),
            node_id: "node-a".to_string(),
            instance_id: "instance-a".to_string(),
            host_ip: "10.20.0.11".parse().unwrap(),
            role,
            initial_voters: vec![
                "10.20.0.11:3001".parse().unwrap(),
                "10.20.0.12:3101".parse().unwrap(),
                "10.20.0.13:3201".parse().unwrap(),
            ],
            voter_endpoints: vec![
                "10.20.0.11:3001".parse().unwrap(),
                "10.20.0.12:3101".parse().unwrap(),
                "10.20.0.13:3201".parse().unwrap(),
            ],
            subnet: "172.22.1.0/24".to_string(),
            control_allow_cidrs: vec!["10.20.0.0/24".to_string()],
            api_port: 3001,
            gateway_port: 3002,
            etcd_client_port: 3003,
            etcd_peer_port: 3004,
            shared_registry: Some("registry.example.com/maestro".to_string()),
            labels: Default::default(),
            identity_api_port: Some(3001),
        }
    }

    #[test]
    fn containers_never_receive_the_host_loopback_etcd_endpoint() {
        assert_eq!(
            container_etcd_endpoints(None, &["https://127.0.0.1:35487".to_string()], false),
            vec!["https://maestro-etcd:2379"]
        );

        let hybrid = cluster_runtime(NodeRole::Hybrid);
        assert_eq!(
            container_etcd_endpoints(
                Some(&hybrid),
                &[
                    "https://10.20.0.11:3003".to_string(),
                    "https://10.20.0.12:3103".to_string(),
                ],
                false,
            ),
            vec!["https://maestro-etcd:2379", "https://10.20.0.12:3103"]
        );

        let worker = cluster_runtime(NodeRole::Worker);
        assert_eq!(
            container_etcd_endpoints(
                Some(&worker),
                &["https://10.20.0.11:3003".to_string()],
                false,
            ),
            vec!["https://10.20.0.11:3003"]
        );
    }

    #[test]
    fn bootstrap_seed_initial_cluster_contains_only_the_master() {
        let master = cluster_runtime(NodeRole::Master);
        let member_name = master.member_name();
        let initial_cluster =
            clustered_initial_cluster(&master, &member_name, &BootstrapAction::BootstrapSeed);

        assert_eq!(
            initial_cluster,
            format!("{member_name}={}", master.peer_url())
        );
        assert!(!initial_cluster.contains("10.20.0.12"));
        assert!(!initial_cluster.contains("10.20.0.13"));
    }

    #[test]
    fn clustered_tailscale_stays_in_userspace_and_advertises_its_workload_subnet() {
        let routes =
            tailscale_advertise_routes(Some("172.22.1.0/24"), &["10.40.0.0/16".to_string()])
                .unwrap();
        let flags = tailscale_container_network_flags(&routes.join(","));

        assert_eq!(routes, ["172.22.1.0/24", "10.40.0.0/16"]);
        assert_eq!(
            flags,
            [
                "-e",
                "TS_ROUTES=172.22.1.0/24,10.40.0.0/16",
                "-e",
                "TS_USERSPACE=true"
            ]
        );
    }

    #[test]
    fn standalone_tailscale_keeps_its_existing_route_behavior() {
        let routes = tailscale_advertise_routes(
            Some("172.22.1.0/24"),
            &["10.40.0.0/16".to_string(), "172.22.1.0/24".to_string()],
        )
        .unwrap();

        assert_eq!(routes, ["172.22.1.0/24", "10.40.0.0/16"]);
        assert!(tailscale_advertise_routes(None, &[]).is_none());
    }

    #[test]
    fn gateway_tls_version_is_configured_only_on_tls_options() {
        let config = gateway_dynamic_config();

        assert_eq!(config.matches("minVersion: VersionTLS13").count(), 1);
        assert!(config.contains("options:\n    cluster-gateway:\n      minVersion: VersionTLS13"));
        let server_transport = config
            .split_once("serversTransports:")
            .unwrap()
            .1
            .split_once("tls:")
            .unwrap()
            .0;
        assert!(!server_transport.contains("minVersion"));
    }

    #[test]
    fn an_unavailable_cloudflared_container_does_not_abort_startup() {
        let warning = initial_cloudflared_gate_warning(
            "maestro-cloudflared-cluster-node-1",
            Err(anyhow::anyhow!("no such object")),
        )
        .unwrap();

        assert!(warning.contains("connector remains unready"));
        assert!(warning.contains("reconciliation will retry"));
        assert!(initial_cloudflared_gate_warning("cloudflared", Ok(())).is_none());
    }

    #[test]
    fn system_addresses_preserve_the_first_24_address_block() {
        let legacy = system_ips_from_cidr("10.100.0.0/16").unwrap();
        assert_eq!(legacy.dns, "10.100.0.254");
        assert_eq!(legacy.etcd, "10.100.0.253");
        assert_eq!(legacy.ingress, "10.100.0.252");
        assert_eq!(legacy.probe, "10.100.0.251");
        assert_eq!(legacy.admin, "10.100.0.250");
        assert_eq!(legacy.gateway, "10.100.0.249");
        assert_eq!(
            cloudflared_ip_from_cidr("10.100.0.0/16", 1).as_deref(),
            Some("10.100.0.248")
        );

        let cluster = system_ips_from_cidr("172.22.1.0/24").unwrap();
        assert_eq!(cluster.dns, "172.22.1.254");
        assert_eq!(cluster.etcd, "172.22.1.253");
        assert_eq!(cluster.ingress, "172.22.1.252");
        assert_eq!(cluster.probe, "172.22.1.251");
        assert_eq!(cluster.admin, "172.22.1.250");
        assert_eq!(cluster.gateway, "172.22.1.249");
        assert_eq!(
            cloudflared_ip_from_cidr("172.22.1.0/24", 2).as_deref(),
            Some("172.22.1.247")
        );
        assert!(cloudflared_ip_from_cidr("172.22.1.0/24", 26).is_none());
    }

    #[test]
    fn tailscale_watchdog_never_discards_the_persisted_node_identity() {
        let entrypoint = include_str!("../../../dns/tailscale-entrypoint.sh");
        assert!(!entrypoint.contains("rm -rf /var/lib/tailscale"));
        assert!(entrypoint.contains("kill 1"));
    }

    #[test]
    fn production_access_log_flags_are_supported_by_the_pinned_config_shape() {
        let flags = ingress_access_log_args();
        assert!(flags.iter().any(|flag| flag == "--accesslog.format=json"));
        assert!(!flags.iter().any(|flag| flag.contains("queryParameters")));
    }

    #[test]
    fn node_roles_select_only_required_system_jobs() {
        assert_eq!(
            system_job_capabilities(Some(NodeRole::Hybrid)),
            SystemJobCapabilities {
                local_etcd: true,
                ingress: true,
                gateway: true,
                admin: true,
            }
        );
        assert_eq!(
            system_job_capabilities(Some(NodeRole::Master)),
            SystemJobCapabilities {
                local_etcd: true,
                ingress: true,
                gateway: true,
                admin: true,
            }
        );
        assert_eq!(
            system_job_capabilities(Some(NodeRole::Voter)),
            SystemJobCapabilities {
                local_etcd: true,
                ingress: false,
                gateway: false,
                admin: true,
            }
        );
        assert_eq!(
            system_job_capabilities(Some(NodeRole::Worker)),
            SystemJobCapabilities {
                local_etcd: false,
                ingress: true,
                gateway: true,
                admin: false,
            }
        );
    }
}
