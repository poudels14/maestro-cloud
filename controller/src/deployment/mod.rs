use std::sync::Arc;
use std::time::Duration;

use base64::Engine;

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
pub mod keys;
pub mod provider;
pub mod store;
pub mod types;

pub use types::ControllerConfig;

pub const ETCD_IMAGE_TAG: &str = "quay.io/coreos/etcd:v3.6.8";
pub const INGRESS_IMAGE_TAG: &str = "traefik:v3.6";
pub const PROBE_IMAGE_TAG: &str = "maestro-probe";
pub const ADMIN_IMAGE_TAG: &str = "maestro-admin";
pub const TAILSCALE_IMAGE_TAG: &str = "maestro-tailscale";
pub const CLOUDFLARED_IMAGE_TAG: &str = "cloudflare/cloudflared:1852-21ca2e225ea5";
pub struct SystemStartupInfo {
    pub dns_manager: Arc<DnsManager>,
    pub nameserver_ip: Option<String>,
}

pub async fn start_system_jobs(
    config: &ControllerConfig,
    runtime: &Arc<dyn RuntimeProvider>,
    log_sender: &flume::Sender<LogEntry>,
    logger: &Logger,
    supervisor: &mut JobSupervisor,
) -> SystemStartupInfo {
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

    let suffix = &config.cluster_name;
    let dns_domain = format!("{suffix}.maestro.internal");
    let etcd_container = format!("maestro-etcd-{suffix}");
    let probe_container = format!("maestro-probe-{suffix}");
    let ingress_container = format!("maestro-ingress-{suffix}");
    let admin_container = format!("maestro-admin-{suffix}");
    let tailscale_container = format!("maestro-tailscale-{suffix}");
    let cloudflared_container_prefix = format!("maestro-cloudflared-{suffix}");
    let _ = runtime.remove_container(&etcd_container).await;
    let _ = runtime.remove_container(&probe_container).await;
    let _ = runtime.remove_container(&ingress_container).await;
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
        if config.tailscale_authkey.is_some() {
            dns_manager.set_record(
                "admin",
                &format!("{}.maestro.internal", config.cluster_alias),
                &ips.tailscale,
            );
        }
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
        etcd_certs.as_ref(),
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

    if config.cloudflare_tunnel_token.is_some() {
        init_cloudflared(
            &cloudflared_container_prefix,
            &dns_domain,
            &dns_flag,
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
    config: &ControllerConfig,
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
    let cluster_bootstrap = &config.cluster_bootstrap;
    if cluster_bootstrap.mode != crate::deployment::types::ClusterBootstrapMode::Single {
        let port = cluster_bootstrap.etcd_peer_port;
        // Bind ONLY to the advertise address + loopback. Avoids accidental
        // exposure on other interfaces (e.g., public IPs on a multi-NIC host).
        // If no advertise host is known we fall back to 0.0.0.0 with a warning;
        // resolve_advertise_host in main.rs makes this rare.
        match cluster_bootstrap.advertise_host.as_deref() {
            Some(host) => {
                extra_flags.extend([
                    "-p".into(),
                    format!("{host}:{port}:{port}"),
                    "-p".into(),
                    format!("127.0.0.1:{port}:{port}"),
                ]);
            }
            None => {
                eprintln!(
                    "[maestro]: WARNING: no advertise_host resolved; binding etcd peer port to 0.0.0.0"
                );
                extra_flags.extend(["-p".into(), format!("0.0.0.0:{port}:{port}")]);
            }
        }
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
    let etcd_name = format!("maestro-{}", config.node_id);
    let mut image_and_args = vec![
        ETCD_IMAGE_TAG.into(),
        "etcd".into(),
        format!("--name={etcd_name}"),
        "--data-dir=/data".into(),
        format!("--listen-client-urls={scheme}://0.0.0.0:2379"),
        format!("--advertise-client-urls={scheme}://127.0.0.1:6479"),
        "--auto-compaction-mode=periodic".into(),
        "--auto-compaction-retention=1h".into(),
        "--quota-backend-bytes=8589934592".into(),
    ];
    if etcd_certs.is_some() {
        image_and_args.extend([
            "--cert-file=/certs/server.pem".into(),
            "--key-file=/certs/server-key.pem".into(),
            "--trusted-ca-file=/certs/ca.pem".into(),
            "--client-cert-auth=true".into(),
        ]);
    }
    apply_cluster_flags(&mut image_and_args, config, etcd_certs.is_some());

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
        secrets_mounts: Vec::new(),
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
    let tls = build_etcd_tls_options(etcd_certs);
    let scheme = if etcd_certs.is_some() {
        "https"
    } else {
        "http"
    };
    let endpoint = format!("{scheme}://127.0.0.1:{}", config.etcd_port);
    let connect_options = tls.map(|tls_opts| etcd_client::ConnectOptions::new().with_tls(tls_opts));
    if let Ok(mut client) = etcd_client::Client::connect([&endpoint], connect_options).await {
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
    extra_flags.extend_from_slice(dns_flag);
    extra_flags.extend(ip_flags);

    let mut image_and_args = vec![
        INGRESS_IMAGE_TAG.into(),
        "--providers.etcd=true".into(),
        "--providers.etcd.rootKey=traefik".into(),
        "--providers.etcd.endpoints=maestro-etcd:2379".into(),
        "--entrypoints.web.address=:8888".into(),
        "--entrypoints.metrics.address=:9100".into(),
        "--metrics.prometheus=true".into(),
        "--metrics.prometheus.entryPoint=metrics".into(),
        "--metrics.prometheus.addEntryPointsLabels=true".into(),
        "--metrics.prometheus.addRoutersLabels=true".into(),
        "--metrics.prometheus.addServicesLabels=true".into(),
        "--metrics.prometheus.buckets=1.0,5.0,10.0".into(),
    ];
    if let Some(cidr) = network_cidr {
        image_and_args.push(format!(
            "--entrypoints.web.forwardedHeaders.trustedIPs={cidr}"
        ));
    }
    if config.enable_ingress_access_logs {
        image_and_args.extend([
            "--accesslog=true".into(),
            "--accesslog.format=json".into(),
            "--accesslog.fields.defaultmode=keep".into(),
            "--accesslog.fields.headers.names.X-Forwarded-For=keep".into(),
            "--accesslog.fields.headers.names.CF-Connecting-IP=keep".into(),
        ]);
    }
    if etcd_certs.is_some() {
        image_and_args.extend([
            "--providers.etcd.tls.cert=/certs/client.pem".into(),
            "--providers.etcd.tls.key=/certs/client-key.pem".into(),
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
        secrets_mounts: Vec::new(),
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
        secrets_mounts: Vec::new(),
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
    let probe_host_port = config.probe_port.expect("probe_port should be resolved");
    let etcd_scheme = if etcd_certs.is_some() {
        "https"
    } else {
        "http"
    };
    let probe_dir_abs =
        std::fs::canonicalize(&probe_dir).expect("failed to canonicalize probe dir");
    let jwt_secret_path = probe_dir.join("jwt-secret");
    let jwt_secret_abs = probe_dir_abs.join("jwt-secret");
    let slack_webhook_path = probe_dir.join("slack-webhook");
    let slack_webhook_abs = probe_dir_abs.join("slack-webhook");

    let mut secrets_mounts: Vec<crate::supervisor::SecretsMount> =
        vec![crate::supervisor::SecretsMount {
            host_path: encryption_key_path,
            container_path: "/run/secrets/encryption-key".to_string(),
            content: config.encryption_key.as_str().to_string(),
        }];
    if let Some(secret) = &config.jwt_secret_key {
        secrets_mounts.push(crate::supervisor::SecretsMount {
            host_path: jwt_secret_path.clone(),
            container_path: "/run/secrets/jwt-secret".to_string(),
            content: secret.clone(),
        });
    }
    if let Some(slack_url) = &config.slack_webhook_url {
        secrets_mounts.push(crate::supervisor::SecretsMount {
            host_path: slack_webhook_path.clone(),
            container_path: "/run/secrets/slack-webhook".to_string(),
            content: slack_url.as_str().to_string(),
        });
    }

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
                format!("ETCD_ENDPOINT={etcd_scheme}://maestro-etcd:2379"),
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
                "-e".into(),
                format!("MAESTRO_NODE_ID={}", config.node_id),
                "-e".into(),
                format!(
                    "MAESTRO_CONFIG={}",
                    base64::engine::general_purpose::STANDARD.encode(&config.maestro_config)
                ),
            ];
            if etcd_certs.is_some() {
                let certs_abs = std::fs::canonicalize(config.certs_dir())
                    .expect("failed to canonicalize certs dir");
                probe_flags.extend([
                    "-v".into(),
                    format!("{}:/certs:ro", certs_abs.display()),
                    "-e".into(),
                    "ETCD_CA_FILE=/certs/ca.pem".into(),
                    "-e".into(),
                    "ETCD_CERT_FILE=/certs/client.pem".into(),
                    "-e".into(),
                    "ETCD_KEY_FILE=/certs/client-key.pem".into(),
                ]);
            }
            if config.jwt_secret_key.is_some() {
                probe_flags.extend([
                    "-v".into(),
                    format!("{}:/run/secrets/jwt-secret:ro", jwt_secret_abs.display()),
                    "-e".into(),
                    "MAESTRO_JWT_SECRET_KEY_FILE=/run/secrets/jwt-secret".into(),
                ]);
            }
            if let Some(system_type) = &config.system_type {
                probe_flags.extend(["-e".into(), format!("MAESTRO_SYSTEM_TYPE={system_type}")]);
            }
            if config.slack_webhook_url.is_some() {
                probe_flags.extend([
                    "-v".into(),
                    format!(
                        "{}:/run/secrets/slack-webhook:ro",
                        slack_webhook_abs.display()
                    ),
                    "-e".into(),
                    "MAESTRO_SLACK_WEBHOOK_URL_FILE=/run/secrets/slack-webhook".into(),
                ]);
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
        secrets_mounts,
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
                    "TS_STATE_DIR=/var/lib/tailscale".to_string(),
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
            secrets_mounts: vec![crate::supervisor::SecretsMount {
                host_path: ts_authkey_path,
                container_path: "/run/secrets/ts-authkey".to_string(),
                content: authkey.clone(),
            }],
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

async fn init_cloudflared(
    container_name_prefix: &str,
    dns_domain: &str,
    dns_flag: &[String],
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

    for replica in 1..=config.cloudflare_tunnel_replicas {
        let container_name = format!("{container_name_prefix}-{replica}");
        let hostname = format!("maestro-cloudflared-{replica}");
        let job_id = format!("maestro-cloudflared-{replica}");

        await_job_running(
            supervisor,
            SupervisedJobConfig {
                id: job_id.clone(),
                command: runtime.run_command(&RunSpec {
                    container_name: container_name.clone(),
                    hostname,
                    dns_domain: Some(dns_domain.to_string()),
                    network: config.network.clone(),
                    extra_flags: flags.clone(),
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
                    name: container_name,
                    runtime_cli: runtime.cli_name().to_string(),
                }),
                secrets_mounts: Vec::new(),
                log_config: Some(LogConfig {
                    sender: log_sender.clone(),
                    tags: Default::default(),
                    origin: LogOrigin::System,
                }),
            },
        )
        .await;
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

fn apply_cluster_flags(args: &mut Vec<String>, config: &ControllerConfig, secure: bool) {
    use crate::deployment::types::{ClusterBootstrapMode, sanitize_member_name};

    let bootstrap = &config.cluster_bootstrap;
    if bootstrap.mode == ClusterBootstrapMode::Single {
        return;
    }
    let peer_scheme = if secure { "https" } else { "http" };
    let listen_peer = format!("{peer_scheme}://0.0.0.0:{}", bootstrap.etcd_peer_port);
    let advertise_host = bootstrap
        .advertise_host
        .clone()
        .unwrap_or_else(|| format!("maestro-{}", config.node_id));
    let advertise_peer = format!(
        "{peer_scheme}://{advertise_host}:{}",
        bootstrap.etcd_peer_port
    );
    let local_member_name = sanitize_member_name(&advertise_host);
    // Replace the earlier `--name=maestro-{node_id}` entry so the local etcd
    // member name matches what peers reference us by (our advertise host).
    if let Some(name_arg) = args.iter_mut().find(|arg| arg.starts_with("--name=")) {
        *name_arg = format!("--name={local_member_name}");
    }
    args.push(format!("--listen-peer-urls={listen_peer}"));
    args.push(format!("--initial-advertise-peer-urls={advertise_peer}"));

    let mut initial_cluster: Vec<String> = bootstrap
        .peers
        .iter()
        .map(|peer| {
            format!(
                "{}={}",
                peer.etcd_member_name(),
                peer.peer_url(peer_scheme, bootstrap.etcd_peer_port)
            )
        })
        .collect();
    let self_entry = format!("{local_member_name}={advertise_peer}");
    if !initial_cluster
        .iter()
        .any(|entry| entry.starts_with(&format!("{local_member_name}=")))
    {
        initial_cluster.push(self_entry);
    }
    initial_cluster.sort();
    args.push(format!("--initial-cluster={}", initial_cluster.join(",")));
    let state = match bootstrap.mode {
        ClusterBootstrapMode::NewCluster => "new",
        ClusterBootstrapMode::JoinExisting => "existing",
        ClusterBootstrapMode::Single => "new",
    };
    args.push(format!("--initial-cluster-state={state}"));
    args.push(format!(
        "--initial-cluster-token=maestro-{}",
        config.cluster_name
    ));
    if secure {
        args.extend([
            "--peer-cert-file=/certs/server.pem".into(),
            "--peer-key-file=/certs/server-key.pem".into(),
            "--peer-trusted-ca-file=/certs/ca.pem".into(),
            "--peer-client-cert-auth=true".into(),
        ]);
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
