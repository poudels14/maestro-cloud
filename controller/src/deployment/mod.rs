use crate::deployment::provider::{DockerBuildConfig, DockerDeploymentProvider};
use crate::supervisor::{SupervisedJobConfig, SupervisedJobStatus, controller::JobSupervisor};

pub mod controller;
pub mod etcd;
pub mod keys;
pub mod provider;
pub mod store;
pub mod types;

pub use types::DeploymentConfig;

const PROBE_IMAGE_TAG: &str = "maestro-probe";
const DNS_IMAGE_TAG: &str = "maestro-dns";

pub async fn start_system_jobs(config: &DeploymentConfig, supervisor: &mut JobSupervisor) {
    let suffix = &config.cluster_name;
    let dns_domain = format!("{suffix}.maestro.internal");
    let etcd_container = format!("maestro-etcd-{suffix}");
    let probe_container = format!("maestro-probe-{suffix}");
    let ingress_container = format!("maestro-ingress-{suffix}");
    let tailscale_container = format!("maestro-tailscale-{suffix}");
    let dns_container = format!("maestro-dns-{suffix}");
    cleanup_container(&etcd_container).await;
    cleanup_container(&probe_container).await;
    cleanup_container(&ingress_container).await;
    cleanup_container(&tailscale_container).await;
    cleanup_container(&dns_container).await;
    ensure_docker_network(&config.network).await;
    init_etcd(&etcd_container, &dns_domain, config, supervisor).await;
    if config.tailscale_authkey.is_some() {
        init_tailnet(
            &tailscale_container,
            &dns_container,
            &dns_domain,
            config,
            supervisor,
        )
        .await;
    }
    init_probe(
        &probe_container,
        &etcd_container,
        &dns_domain,
        config,
        supervisor,
    )
    .await;
    init_ingress(
        &ingress_container,
        &etcd_container,
        &dns_domain,
        config,
        supervisor,
    )
    .await;
}

async fn cleanup_container(name: &str) {
    let _ = tokio::process::Command::new("docker")
        .args(["rm", "-f", name])
        .output()
        .await;
}

async fn ensure_docker_network(network: &str) {
    let _ = tokio::process::Command::new("docker")
        .args(["network", "create", network])
        .output()
        .await;
}

async fn init_etcd(
    container_name: &str,
    dns_domain: &str,
    config: &DeploymentConfig,
    supervisor: &mut JobSupervisor,
) {
    let etcd_data_dir = config.etcd_dir().join("data");
    std::fs::create_dir_all(&etcd_data_dir).expect("Failed to create etcd data dir");
    let etcd_job_config = SupervisedJobConfig {
        id: "maestro-etcd".to_string(),
        command: format!(
            "docker run --name {container_name} --hostname maestro-etcd --domainname {dns_domain} --network {} -p {}:2379 -v {}:/data --rm quay.io/coreos/etcd:v3.6.8 etcd {} {} {}",
            config.network,
            config.etcd_port,
            std::fs::canonicalize(etcd_data_dir)
                .expect("error canonicalizing etcd data dir")
                .to_str()
                .expect("error getting etcd data dir"),
            "--data-dir=/data",
            " --listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://127.0.0.1:6479"
        ),
        name: "maestro-etcd".to_string(),
        max_restarts: None,
        restart_delay_ms: 100,
        shutdown_grace_period_ms: 10_000,
        logs_dir: Some(config.etcd_dir().join("logs/")),
    };
    await_job_running(supervisor, etcd_job_config).await;
}

async fn init_ingress(
    container_name: &str,
    etcd_container: &str,
    dns_domain: &str,
    config: &DeploymentConfig,
    supervisor: &mut JobSupervisor,
) {
    let endpoint = format!("http://127.0.0.1:{}", config.etcd_port);
    if let Ok(mut client) = etcd_client::Client::connect([&endpoint], None).await {
        let _ = client.put("traefik", "", None).await;
    }

    let logs_dir = config.data_dir.join("logs/system/maestro-ingress");
    std::fs::create_dir_all(&logs_dir).expect("Failed to create ingress logs dir");

    let ingress_job_config = SupervisedJobConfig {
        id: "maestro-ingress".to_string(),
        command: format!(
            "docker run --name {container_name} --hostname web --domainname {dns_domain} --network {} -p 8888:8888 -p 8080:8080 --rm traefik:v3.6 {} {} {} {} {}",
            config.network,
            "--api.insecure=true",
            "--providers.etcd=true",
            format_args!("--providers.etcd.rootKey=traefik"),
            format_args!("--providers.etcd.endpoints={etcd_container}:2379"),
            "--entrypoints.web.address=:8888",
        ),
        name: "maestro-ingress".to_string(),
        max_restarts: None,
        restart_delay_ms: 1_000,
        shutdown_grace_period_ms: 10_000,
        logs_dir: Some(logs_dir),
    };
    await_job_running(supervisor, ingress_job_config).await;
}

async fn init_probe(
    container_name: &str,
    etcd_container: &str,
    dns_domain: &str,
    config: &DeploymentConfig,
    supervisor: &mut JobSupervisor,
) {
    DockerDeploymentProvider::build(&DockerBuildConfig {
        context_dir: config.project_dir.clone(),
        tag: PROBE_IMAGE_TAG.to_string(),
        dockerfile: Some("Dockerfile.probe".to_string()),
    })
    .await
    .expect("failed to build probe image");
    let logs_dir = config.data_dir.join("logs/system/maestro-probe");
    std::fs::create_dir_all(&logs_dir).expect("Failed to create probe logs dir");
    let deployment_logs_dir = std::fs::canonicalize(config.deployment_logs_dir())
        .expect("failed to canonicalize deployment logs dir");

    let probe_job_config = SupervisedJobConfig {
        id: "maestro-probe".to_string(),
        command: format!(
            "docker run --name {container_name} --hostname maestro-probe --domainname {dns_domain} --network {} -p {}:6400 -v {}:/logs:ro --rm -e ETCD_ENDPOINT=http://{etcd_container}:2379 -e PORT=6400 {PROBE_IMAGE_TAG}",
            config.network,
            config.probe_port,
            deployment_logs_dir.display(),
        ),
        name: "maestro-probe".to_string(),
        max_restarts: None,
        restart_delay_ms: 1_000,
        shutdown_grace_period_ms: 60_000,
        logs_dir: Some(logs_dir),
    };
    await_job_running(supervisor, probe_job_config).await;
}

async fn init_tailnet(
    tailscale_container: &str,
    dns_container: &str,
    dns_domain: &str,
    config: &DeploymentConfig,
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

    DockerDeploymentProvider::build(&DockerBuildConfig {
        context_dir: config.project_dir.clone(),
        tag: DNS_IMAGE_TAG.to_string(),
        dockerfile: Some("Dockerfile.dns".to_string()),
    })
    .await
    .expect("failed to build dns image");

    let dns_logs_dir = config.data_dir.join("logs/system/maestro-dns");
    std::fs::create_dir_all(&dns_logs_dir).expect("Failed to create dns logs dir");
    await_job_running(
        supervisor,
        SupervisedJobConfig {
            id: "maestro-dns".to_string(),
            command: format!(
                "docker run --name {dns_container} --hostname maestro-dns --domainname {dns_domain} --network {} --rm {DNS_IMAGE_TAG} {dns_domain}",
                config.network,
            ),
            name: "maestro-dns".to_string(),
            max_restarts: None,
            restart_delay_ms: 1_000,
            shutdown_grace_period_ms: 10_000,
            logs_dir: Some(dns_logs_dir),
        },
    )
    .await;

    let ts_logs_dir = config.data_dir.join("logs/system/maestro-tailscale");
    std::fs::create_dir_all(&ts_logs_dir).expect("Failed to create tailscale logs dir");
    let state_dir = config.data_dir.join("system/tailscale/state");
    std::fs::create_dir_all(&state_dir).expect("Failed to create tailscale state dir");
    let state_dir =
        std::fs::canonicalize(&state_dir).expect("failed to canonicalize tailscale state dir");
    await_job_running(
        supervisor,
        SupervisedJobConfig {
            id: "maestro-tailscale".to_string(),
            command: format!(
                "docker run --name {tailscale_container} --hostname maestro-tailscale --domainname {dns_domain} --network {} --cap-add=NET_ADMIN --device=/dev/net/tun -v {}:/var/lib/tailscale --rm -e TS_AUTHKEY={authkey} -e TS_ROUTES={network_cidr} -e TS_USERSPACE=false ghcr.io/tailscale/tailscale:latest",
                config.network,
                state_dir.display(),
            ),
            name: "maestro-tailscale".to_string(),
            max_restarts: None,
            restart_delay_ms: 1_000,
            shutdown_grace_period_ms: 10_000,
            logs_dir: Some(ts_logs_dir),
        },
    )
    .await;

    let _ = tokio::process::Command::new("docker")
        .args([
            "exec",
            tailscale_container,
            "tailscale",
            "set",
            &format!("--advertise-routes={network_cidr}"),
        ])
        .output()
        .await;

    eprintln!("[maestro]: tailscale subnet router started, advertising route {network_cidr}");
    let nameserver_ip = config
        .nameserver_ip
        .clone()
        .or(get_docker_ip(dns_container).await);
    if let Some(nameserver_ip) = &nameserver_ip {
        eprintln!(
            "[maestro]: dns server running at {nameserver_ip}\n           configure split DNS in Tailscale admin: nameserver {nameserver_ip} for \"{dns_domain}\""
        );
    }
}

async fn get_docker_ip(container_name: &str) -> Option<String> {
    for _ in 0..10 {
        let output = tokio::process::Command::new("docker")
            .args([
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                container_name,
            ])
            .output()
            .await
            .ok()?;

        if output.status.success() {
            let ip = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !ip.is_empty() {
                return Some(ip);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    None
}

async fn get_network_cidr(network: &str) -> Option<String> {
    let output = tokio::process::Command::new("docker")
        .args([
            "network",
            "inspect",
            network,
            "--format",
            "{{range .IPAM.Config}}{{.Subnet}}{{end}}",
        ])
        .output()
        .await
        .ok()?;

    if output.status.success() {
        let cidr = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if cidr.is_empty() { None } else { Some(cidr) }
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
        }
    }
}
