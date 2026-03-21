use std::sync::Arc;
use std::time::Duration;

use crate::deployment::dns::DnsManager;
use crate::logs::{LogConfig, LogEntry, LogOrigin};
use crate::runtime::{BuildSpec, RunSpec, RuntimeProvider};
use crate::supervisor::{
    ContainerRef, SupervisedJobConfig, SupervisedJobStatus, controller::JobSupervisor,
};

pub mod controller;
pub mod dns;
pub mod etcd;
pub mod keys;
pub mod provider;
pub mod store;
pub mod types;

pub use types::DeploymentConfig;

const PROBE_IMAGE_TAG: &str = "maestro-probe";
const ADMIN_IMAGE_TAG: &str = "maestro-admin";
const TAILSCALE_IMAGE_TAG: &str = "maestro-tailscale";
pub struct SystemStartupInfo {
    pub dns_manager: Arc<DnsManager>,
    pub nameserver_ip: Option<String>,
}

pub async fn start_system_jobs(
    config: &DeploymentConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    logger: &crate::logs::SystemLogger,
    supervisor: &mut JobSupervisor,
) -> SystemStartupInfo {
    let secrets_dir = config.data_dir.join("secrets");
    if secrets_dir.exists() {
        let _ = std::fs::remove_dir_all(&secrets_dir);
    }

    let suffix = &config.cluster_name;
    let dns_domain = format!("{suffix}.maestro.internal");
    let etcd_container = format!("maestro-etcd-{suffix}");
    let probe_container = format!("maestro-probe-{suffix}");
    let ingress_container = format!("maestro-ingress-{suffix}");
    let admin_container = format!("maestro-admin-{suffix}");
    let tailscale_container = format!("maestro-tailscale-{suffix}");
    let _ = runtime.remove_container(&etcd_container).await;
    let _ = runtime.remove_container(&probe_container).await;
    let _ = runtime.remove_container(&ingress_container).await;
    let _ = runtime.remove_container(&admin_container).await;
    let _ = runtime.remove_container(&tailscale_container).await;
    if config.force {
        let static_ips = config
            .subnet
            .as_deref()
            .and_then(system_ips_from_cidr)
            .map(|ips| vec![ips.etcd, ips.probe, ips.ingress, ips.admin, ips.tailscale])
            .unwrap_or_default();
        let no_names: Vec<String> = Vec::new();
        let _ = runtime
            .remove_conflicting_containers(&config.network, &no_names, &static_ips)
            .await;
        let _ = runtime.remove_network(&config.network).await;
    }
    if let Err(err) = runtime
        .ensure_network(&config.network, config.subnet.as_deref())
        .await
    {
        panic!("[maestro]: {err}");
    }

    let dns_dir = config.data_dir.join("system/dns");
    let dns_manager = Arc::new(DnsManager::new(dns_dir.clone()));
    DnsManager::write_corefile(&dns_dir);

    let network_cidr = runtime.inspect_network_cidr(&config.network).await;
    let system_ips = network_cidr.as_deref().and_then(system_ips_from_cidr);

    if let Some(ips) = &system_ips {
        dns_manager.set_record("maestro-etcd", &dns_domain, &ips.etcd);
        dns_manager.set_record("web", &dns_domain, &ips.ingress);
        dns_manager.set_record("maestro-probe", &dns_domain, &ips.probe);
        dns_manager.set_record("admin", &dns_domain, &ips.admin);
        let _ = dns_manager.flush();
    }

    let ip_flag = |ip: &str| vec!["--ip".to_string(), ip.to_string()];

    init_etcd(
        &etcd_container,
        &dns_domain,
        system_ips
            .as_ref()
            .map(|ips| ip_flag(&ips.etcd))
            .unwrap_or_default(),
        config,
        runtime,
        log_sender,
        supervisor,
    )
    .await;

    if config.tailscale_authkey.is_some() {
        init_tailnet(
            &tailscale_container,
            &dns_domain,
            logger,
            system_ips.as_ref().map(|ips| ips.tailscale.as_str()),
            config,
            runtime,
            log_sender,
            supervisor,
        )
        .await;
    }

    let nameserver_ip = if config.tailscale_authkey.is_some() {
        system_ips.as_ref().map(|ips| ips.tailscale.clone())
    } else {
        None
    };
    let dns_flag = dns_flag_for_runtime(runtime.as_ref(), nameserver_ip.as_deref());

    init_ingress(
        &ingress_container,
        &etcd_container,
        &dns_domain,
        &dns_flag,
        system_ips
            .as_ref()
            .map(|ips| ip_flag(&ips.ingress))
            .unwrap_or_default(),
        logger,
        config,
        runtime,
        log_sender,
        supervisor,
    )
    .await;

    init_probe(
        &probe_container,
        &etcd_container,
        &dns_domain,
        &dns_flag,
        system_ips
            .as_ref()
            .map(|ips| ip_flag(&ips.probe))
            .unwrap_or_default(),
        config,
        runtime,
        log_sender,
        supervisor,
    )
    .await;

    init_admin(
        &admin_container,
        &probe_container,
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

    SystemStartupInfo {
        dns_manager,
        nameserver_ip,
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
    config: &DeploymentConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let etcd_data_dir = config.etcd_dir().join("data");
    std::fs::create_dir_all(&etcd_data_dir).expect("Failed to create etcd data dir");
    let etcd_data_path =
        std::fs::canonicalize(&etcd_data_dir).expect("error canonicalizing etcd data dir");
    let mut extra_flags = vec![
        "-p".into(),
        format!("127.0.0.1:{}:2379", config.etcd_port),
        "-v".into(),
        format!("{}:/data", etcd_data_path.display()),
    ];
    extra_flags.extend(ip_flags);
    let etcd_job_config = SupervisedJobConfig {
        id: "maestro-etcd".to_string(),
        command: runtime.run_command(&RunSpec {
            container_name: container_name.to_string(),
            hostname: "maestro-etcd".to_string(),
            dns_domain: Some(dns_domain.to_string()),
            network: config.network.clone(),
            extra_flags,
            image_and_args: vec![
                "quay.io/coreos/etcd:v3.6.8".into(),
                "etcd".into(),
                format!("--name=maestro-{}", config.cluster_name),
                "--data-dir=/data".into(),
                "--listen-client-urls=http://0.0.0.0:2379".into(),
                "--advertise-client-urls=http://127.0.0.1:6479".into(),
            ],
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
            tags: Default::default(),
            origin: LogOrigin::System,
        }),
    };
    await_job_running(supervisor, etcd_job_config).await;
}

async fn init_ingress(
    container_name: &str,
    etcd_container: &str,
    dns_domain: &str,
    dns_flag: &[String],
    ip_flags: Vec<String>,
    logger: &crate::logs::SystemLogger,
    config: &DeploymentConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let endpoint = format!("http://127.0.0.1:{}", config.etcd_port);
    if let Ok(mut client) = etcd_client::Client::connect([&endpoint], None).await {
        let _ = client.put("traefik", "", None).await;
    }

    let mut extra_flags: Vec<String> = Vec::new();
    for port in &config.ingress_ports {
        extra_flags.extend(["-p".into(), format!("0.0.0.0:{port}:8888")]);
    }
    extra_flags.extend_from_slice(dns_flag);
    extra_flags.extend(ip_flags);

    let ingress_job_config = SupervisedJobConfig {
        id: "maestro-ingress".to_string(),
        command: runtime.run_command(&RunSpec {
            container_name: container_name.to_string(),
            hostname: "web".to_string(),
            dns_domain: Some(dns_domain.to_string()),
            network: config.network.clone(),
            extra_flags,
            image_and_args: vec![
                "traefik:v3.6".into(),
                "--providers.etcd=true".into(),
                "--providers.etcd.rootKey=traefik".into(),
                format!("--providers.etcd.endpoints={etcd_container}:2379"),
                "--entrypoints.web.address=:8888".into(),
            ],
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
            tags: Default::default(),
            origin: LogOrigin::System,
        }),
    };
    await_job_running(supervisor, ingress_job_config).await;
    for port in &config.ingress_ports {
        logger.emit(
            "info",
            &format!("ingress listening on http://0.0.0.0:{port}"),
        );
    }
}

async fn init_admin(
    container_name: &str,
    probe_container: &str,
    dns_domain: &str,
    dns_flag: &[String],
    ip_flags: Vec<String>,
    logger: &crate::logs::SystemLogger,
    config: &DeploymentConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    runtime
        .build_image(
            &BuildSpec {
                context_dir: config.project_dir.clone(),
                tag: ADMIN_IMAGE_TAG.to_string(),
                dockerfile: Some("Dockerfile.admin".to_string()),
                build_args: Default::default(),
            },
            None,
            None,
        )
        .await
        .expect("failed to build admin-ui image");

    let port_flag = config.admin_port.map(|p| format!("127.0.0.1:{}:3000", p));
    let mut admin_flags: Vec<String> = Vec::new();
    if let Some(pf) = &port_flag {
        admin_flags.extend(["-p".to_string(), pf.clone()]);
    }
    admin_flags.extend([
        "-e".to_string(),
        format!("MAESTRO_API_HOST=http://{probe_container}:3001"),
    ]);
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
        secrets_mount: None,
        log_config: Some(LogConfig {
            sender: log_sender.clone(),
            tags: Default::default(),
            origin: LogOrigin::System,
        }),
    };
    await_job_running(supervisor, admin_job_config).await;
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
        &format!("admin ui running at http://admin.{dns_domain}:3000"),
    );
}

async fn init_probe(
    container_name: &str,
    etcd_container: &str,
    dns_domain: &str,
    dns_flag: &[String],
    ip_flags: Vec<String>,
    config: &DeploymentConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    runtime
        .build_image(
            &BuildSpec {
                context_dir: config.project_dir.clone(),
                tag: PROBE_IMAGE_TAG.to_string(),
                dockerfile: Some("Dockerfile.probe".to_string()),
                build_args: Default::default(),
            },
            None,
            None,
        )
        .await
        .expect("failed to build probe image");
    let probe_dir = config.probe_dir();
    let probe_data_dir = probe_dir.join("data");
    std::fs::create_dir_all(&probe_data_dir).expect("Failed to create probe data dir");
    let probe_data_abs =
        std::fs::canonicalize(&probe_data_dir).expect("failed to canonicalize probe data dir");
    let encryption_key_path = probe_dir.join("encryption-key");
    let encryption_key_abs = std::fs::canonicalize(&probe_dir)
        .expect("failed to canonicalize probe dir")
        .join("encryption-key");
    let probe_host_port = config.probe_port.expect("probe_port should be resolved");
    let probe_job_config = SupervisedJobConfig {
        id: "maestro-probe".to_string(),
        command: {
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
                "-e".into(),
                format!("ETCD_ENDPOINT=http://{etcd_container}:2379"),
                "-e".into(),
                "MAESTRO_ENCRYPTION_KEY_FILE=/run/secrets/encryption-key".into(),
                "-e".into(),
                "PORT=3001".into(),
                "-e".into(),
                format!("MAESTRO_DNS_DOMAIN={dns_domain}"),
                "-e".into(),
                format!("MAESTRO_CLUSTER_NAME={}", config.cluster_name),
                "-e".into(),
                format!("MAESTRO_CLUSTER_ALIAS={}", config.cluster_alias),
            ];
            if let Some(secret) = &config.jwt_secret {
                probe_flags.extend(["-e".into(), format!("MAESTRO_JWT_SECRET={secret}")]);
            }
            if let Some(system_type) = &config.system_type {
                probe_flags.extend(["-e".into(), format!("MAESTRO_SYSTEM_TYPE={system_type}")]);
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
            tags: Default::default(),
            origin: LogOrigin::System,
        }),
    };
    await_job_running(supervisor, probe_job_config).await;
}

async fn init_tailnet(
    container_name: &str,
    dns_domain: &str,
    logger: &crate::logs::SystemLogger,
    static_ip: Option<&str>,
    config: &DeploymentConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let Some(authkey) = &config.tailscale_authkey else {
        return;
    };
    let network_cidr = match runtime.inspect_network_cidr(&config.network).await {
        Some(cidr) => cidr,
        None => {
            logger.emit(
                "warn",
                "failed to discover network CIDR; skipping tailscale setup",
            );
            return;
        }
    };

    runtime
        .build_image(
            &BuildSpec {
                context_dir: config.project_dir.clone(),
                tag: TAILSCALE_IMAGE_TAG.to_string(),
                dockerfile: Some("Dockerfile.tailscale".to_string()),
                build_args: Default::default(),
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
                let ts_hostname = format!("maestro-tailscale-{}", config.cluster_name);
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
                    format!("TS_ROUTES={network_cidr}"),
                    "-e".to_string(),
                    "TS_USERSPACE=true".to_string(),
                    "-e".to_string(),
                    format!("TS_HOSTNAME=maestro-tailscale-{}", config.cluster_name),
                    "-e".to_string(),
                    "TS_EXTRA_ARGS=--accept-dns=false".to_string(),
                    "-e".to_string(),
                    "MAESTRO_DNS_UPSTREAM=coredns".to_string(),
                ];
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
                tags: Default::default(),
                origin: LogOrigin::System,
            }),
        },
    )
    .await;

    let routes_arg = format!("--advertise-routes={network_cidr}");
    let _ = runtime
        .exec_in_container(container_name, &["tailscale", "set", &routes_arg])
        .await;

    logger.emit(
        "info",
        &format!("tailscale subnet router started, advertising route {network_cidr}"),
    );
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

struct SystemIps {
    etcd: String,
    ingress: String,
    probe: String,
    admin: String,
    tailscale: String,
}

fn system_ips_from_cidr(network_cidr: &str) -> Option<SystemIps> {
    let base = network_cidr.split('/').next()?;
    let octets: Vec<u8> = base.split('.').filter_map(|o| o.parse().ok()).collect();
    if octets.len() == 4 {
        let prefix = format!("{}.{}.{}", octets[0], octets[1], octets[2]);
        Some(SystemIps {
            admin: format!("{prefix}.250"),
            etcd: format!("{prefix}.251"),
            ingress: format!("{prefix}.252"),
            probe: format!("{prefix}.253"),
            tailscale: format!("{prefix}.254"),
        })
    } else {
        None
    }
}

async fn await_job_running(supervisor: &mut JobSupervisor, config: SupervisedJobConfig) {
    let job_id = supervisor.start_job(config);
    if let Some(job_id) = job_id {
        loop {
            let status = supervisor.job_status(&job_id).await;
            match status {
                Some(s) => {
                    if s.finished() || s == SupervisedJobStatus::Running {
                        break;
                    }
                }
                None => break,
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
