use crate::deployment::provider::{DockerBuildConfig, DockerDeploymentProvider};
use crate::logs::{LogConfig, LogEntry, LogOrigin};
use crate::supervisor::{
    JobCommand, SupervisedJobConfig, SupervisedJobStatus, controller::JobSupervisor,
};
use crate::utils::cmd;

pub mod controller;
pub mod etcd;
pub mod keys;
pub mod provider;
pub mod store;
pub mod types;

pub use types::DeploymentConfig;

const PROBE_IMAGE_TAG: &str = "maestro-probe";
const ADMIN_IMAGE_TAG: &str = "maestro-admin";
const TAILSCALE_IMAGE_TAG: &str = "maestro-tailscale";

pub async fn start_system_jobs(
    config: &DeploymentConfig,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
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
    cleanup_container(&etcd_container).await;
    cleanup_container(&probe_container).await;
    cleanup_container(&ingress_container).await;
    cleanup_container(&admin_container).await;
    cleanup_container(&tailscale_container).await;
    ensure_docker_network(&config.network, config.subnet.as_deref()).await;
    init_etcd(&etcd_container, &dns_domain, config, log_sender, supervisor).await;
    if config.tailscale_authkey.is_some() {
        init_tailnet(
            &tailscale_container,
            &dns_domain,
            config,
            log_sender,
            supervisor,
        )
        .await;
    }
    init_ingress(
        &ingress_container,
        &etcd_container,
        &dns_domain,
        config,
        log_sender,
        supervisor,
    )
    .await;
    init_probe(
        &probe_container,
        &etcd_container,
        &dns_domain,
        config,
        log_sender,
        supervisor,
    )
    .await;
    init_admin(
        &admin_container,
        &probe_container,
        &dns_domain,
        config,
        log_sender,
        supervisor,
    )
    .await;
}

async fn cleanup_container(name: &str) {
    let _ = cmd::run("docker", &["rm", "-f", name]).await;
}

async fn ensure_docker_network(network: &str, subnet: Option<&str>) {
    let mut args = vec!["network", "create"];
    let subnet_flag;
    if let Some(s) = subnet {
        subnet_flag = format!("--subnet={s}");
        args.push(&subnet_flag);
    }
    args.push(network);
    let _ = cmd::run("docker", &args).await;
}

async fn init_etcd(
    container_name: &str,
    dns_domain: &str,
    config: &DeploymentConfig,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let etcd_data_dir = config.etcd_dir().join("data");
    std::fs::create_dir_all(&etcd_data_dir).expect("Failed to create etcd data dir");
    let etcd_data_path =
        std::fs::canonicalize(&etcd_data_dir).expect("error canonicalizing etcd data dir");
    let etcd_job_config = SupervisedJobConfig {
        id: "maestro-etcd".to_string(),
        command: docker_run_exec(
            container_name,
            "maestro-etcd",
            dns_domain,
            &config.network,
            &[
                "-p",
                &format!("127.0.0.1:{}:2379", config.etcd_port),
                "-v",
                &format!("{}:/data", etcd_data_path.display()),
            ],
            &[
                "quay.io/coreos/etcd:v3.6.8",
                "etcd",
                &format!("--name=maestro-{}", config.cluster_name),
                "--data-dir=/data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:6479",
            ],
        ),
        name: "maestro-etcd".to_string(),
        max_restarts: None,
        restart_delay_ms: 100,
        max_restart_delay_ms: Some(15_000),
        shutdown_grace_period_ms: 10_000,
        docker_container: Some(container_name.to_string()),
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
    config: &DeploymentConfig,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let endpoint = format!("http://127.0.0.1:{}", config.etcd_port);
    if let Ok(mut client) = etcd_client::Client::connect([&endpoint], None).await {
        let _ = client.put("traefik", "", None).await;
    }

    let web_port = config.web_port;
    let ingress_job_config = SupervisedJobConfig {
        id: "maestro-ingress".to_string(),
        command: docker_run_exec(
            container_name,
            "web",
            dns_domain,
            &config.network,
            &["--network-alias", "web", "-p", &format!("{web_port}:8888")],
            &[
                "traefik:v3.6",
                "--providers.etcd=true",
                "--providers.etcd.rootKey=traefik",
                &format!("--providers.etcd.endpoints={etcd_container}:2379"),
                "--entrypoints.web.address=:8888",
            ],
        ),
        name: "maestro-ingress".to_string(),
        max_restarts: None,
        restart_delay_ms: 1_000,
        max_restart_delay_ms: Some(15_000),
        shutdown_grace_period_ms: 10_000,
        docker_container: Some(container_name.to_string()),
        secrets_mount: None,
        log_config: Some(LogConfig {
            sender: log_sender.clone(),
            tags: Default::default(),
            origin: LogOrigin::System,
        }),
    };
    await_job_running(supervisor, ingress_job_config).await;
    eprintln!("[maestro]: ingress listening on http://127.0.0.1:{web_port}");
}

async fn init_admin(
    container_name: &str,
    probe_container: &str,
    dns_domain: &str,
    config: &DeploymentConfig,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    DockerDeploymentProvider::build(
        &DockerBuildConfig {
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
    let admin_flag_refs: Vec<&str> = admin_flags.iter().map(|s| s.as_str()).collect();
    let admin_job_config = SupervisedJobConfig {
        id: "maestro-admin".to_string(),
        command: docker_run_exec(
            container_name,
            "admin",
            dns_domain,
            &config.network,
            &admin_flag_refs,
            &[ADMIN_IMAGE_TAG],
        ),
        name: "maestro-admin".to_string(),
        max_restarts: None,
        restart_delay_ms: 1_000,
        max_restart_delay_ms: Some(15_000),
        shutdown_grace_period_ms: 10_000,
        docker_container: Some(container_name.to_string()),
        secrets_mount: None,
        log_config: Some(LogConfig {
            sender: log_sender.clone(),
            tags: Default::default(),
            origin: LogOrigin::System,
        }),
    };
    await_job_running(supervisor, admin_job_config).await;
    if let Some(port) = config.probe_port {
        eprintln!("[maestro]: admin ui listening on http://127.0.0.1:{port}");
    }
    eprintln!("[maestro]: admin ui running at http://admin.{dns_domain}:3000");
}

async fn init_probe(
    container_name: &str,
    etcd_container: &str,
    dns_domain: &str,
    config: &DeploymentConfig,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    DockerDeploymentProvider::build(
        &DockerBuildConfig {
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
            ];
            if let Some(secret) = &config.jwt_secret {
                probe_flags.extend(["-e".into(), format!("MAESTRO_JWT_SECRET={secret}")]);
            }
            let refs: Vec<&str> = probe_flags.iter().map(|s| s.as_str()).collect();
            docker_run_exec(
                container_name,
                "maestro-probe",
                dns_domain,
                &config.network,
                &refs,
                &[PROBE_IMAGE_TAG],
            )
        },
        name: "maestro-probe".to_string(),
        max_restarts: None,
        restart_delay_ms: 1_000,
        max_restart_delay_ms: Some(15_000),
        shutdown_grace_period_ms: 60_000,
        docker_container: Some(container_name.to_string()),
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
    config: &DeploymentConfig,
    log_sender: &flume::Sender<LogEntry>,
    supervisor: &mut JobSupervisor,
) {
    let Some(authkey) = &config.tailscale_authkey else {
        return;
    };
    let network_cidr = match get_network_cidr(&config.network).await {
        Some(cidr) => cidr,
        None => {
            eprintln!(
                "[maestro]: failed to discover Docker network CIDR; skipping tailscale setup"
            );
            return;
        }
    };

    DockerDeploymentProvider::build(
        &DockerBuildConfig {
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
    let nameserver_ip = default_nameserver_ip(&network_cidr);

    await_job_running(
        supervisor,
        SupervisedJobConfig {
            id: "maestro-tailscale".to_string(),
            command: {
                let ts_hostname = format!("maestro-tailscale-{}", config.cluster_name);
                let mut flags: Vec<String> = vec![
                    "-v".to_string(),
                    format!("{}:/var/lib/tailscale", state_dir.display()),
                    "-v".to_string(),
                    format!("{}:/run/secrets/ts-authkey:ro", ts_authkey_abs.display()),
                    "-e".to_string(),
                    format!("TS_ROUTES={network_cidr}"),
                    "-e".to_string(),
                    "TS_USERSPACE=true".to_string(),
                    "-e".to_string(),
                    format!("TS_HOSTNAME=maestro-tailscale-{}", config.cluster_name),
                    "-e".to_string(),
                    "TS_EXTRA_ARGS=--accept-dns=false".to_string(),
                ];
                if let Some(ip) = &nameserver_ip {
                    flags.extend(["--ip".to_string(), ip.clone()]);
                }
                let flag_refs: Vec<&str> = flags.iter().map(|s| s.as_str()).collect();
                docker_run_exec(
                    container_name,
                    &ts_hostname,
                    dns_domain,
                    &config.network,
                    &flag_refs,
                    &[TAILSCALE_IMAGE_TAG, &config.cluster_name],
                )
            },
            name: "maestro-tailscale".to_string(),
            max_restarts: None,
            restart_delay_ms: 1_000,
            max_restart_delay_ms: Some(15_000),
            shutdown_grace_period_ms: 10_000,
            docker_container: Some(container_name.to_string()),
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
    let _ = cmd::run(
        "docker",
        &["exec", container_name, "tailscale", "set", &routes_arg],
    )
    .await;

    eprintln!("[maestro]: tailscale subnet router started, advertising route {network_cidr}");
    let nameserver_ip = nameserver_ip.or(get_docker_ip(container_name).await);
    if let Some(nameserver_ip) = &nameserver_ip {
        eprintln!(
            "[maestro]: dns server running at {nameserver_ip}\n           configure split DNS in Tailscale admin: nameserver {nameserver_ip} for \"maestro.internal\""
        );
    }
}

async fn get_docker_ip(container_name: &str) -> Option<String> {
    for _ in 0..10 {
        if let Ok(stdout) = cmd::run(
            "docker",
            &[
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                container_name,
            ],
        )
        .await
        {
            let ip = stdout.trim().to_string();
            if !ip.is_empty() {
                return Some(ip);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    None
}

async fn get_network_cidr(network: &str) -> Option<String> {
    let stdout = cmd::run(
        "docker",
        &[
            "network",
            "inspect",
            network,
            "--format",
            "{{range .IPAM.Config}}{{.Subnet}}{{end}}",
        ],
    )
    .await
    .ok()?;
    let cidr = stdout.trim().to_string();
    if cidr.is_empty() { None } else { Some(cidr) }
}

fn default_nameserver_ip(network_cidr: &str) -> Option<String> {
    let base = network_cidr.split('/').next()?;
    let mut octets: Vec<u8> = base.split('.').filter_map(|o| o.parse().ok()).collect();
    if octets.len() != 4 {
        return None;
    }
    octets[3] = 254;
    Some(format!(
        "{}.{}.{}.{}",
        octets[0], octets[1], octets[2], octets[3]
    ))
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
        }
    }
}

fn docker_run_exec(
    container_name: &str,
    hostname: &str,
    dns_domain: &str,
    network: &str,
    extra_flags: &[&str],
    image_and_args: &[&str],
) -> JobCommand {
    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--name".to_string(),
        container_name.to_string(),
        "--hostname".to_string(),
        hostname.to_string(),
        "--domainname".to_string(),
        dns_domain.to_string(),
        "--network".to_string(),
        network.to_string(),
    ];
    for flag in extra_flags {
        args.push(flag.to_string());
    }
    for arg in image_and_args {
        args.push(arg.to_string());
    }
    JobCommand::Exec {
        program: "docker".to_string(),
        args,
    }
}
