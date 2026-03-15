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

pub async fn start_system_jobs(config: &DeploymentConfig, supervisor: &mut JobSupervisor) {
    let suffix = &config.cluster_name;
    let etcd_container = format!("maestro-etcd-{suffix}");
    let probe_container = format!("maestro-probe-{suffix}");
    let ingress_container = format!("maestro-ingress-{suffix}");
    cleanup_container(&etcd_container).await;
    cleanup_container(&probe_container).await;
    cleanup_container(&ingress_container).await;
    ensure_docker_network(&config.network).await;
    start_etcd(&etcd_container, config, supervisor).await;
    start_ingress(&ingress_container, &etcd_container, config, supervisor).await;
    init_probe(&probe_container, &etcd_container, config, supervisor).await;
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

async fn start_etcd(
    container_name: &str,
    config: &DeploymentConfig,
    supervisor: &mut JobSupervisor,
) {
    let etcd_data_dir = config.etcd_dir().join("data");
    std::fs::create_dir_all(&etcd_data_dir).expect("Failed to create etcd data dir");
    let etcd_job_config = SupervisedJobConfig {
        id: "maestro-etcd".to_string(),
        command: format!(
            "docker run --name {container_name} --network {} -p {}:2379 -v {}:/data --rm quay.io/coreos/etcd:v3.6.8 etcd {} {} {}",
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

async fn start_ingress(
    container_name: &str,
    etcd_container: &str,
    config: &DeploymentConfig,
    supervisor: &mut JobSupervisor,
) {
    let logs_dir = config.data_dir.join("logs/system/maestro-ingress");
    std::fs::create_dir_all(&logs_dir).expect("Failed to create ingress logs dir");

    let ingress_job_config = SupervisedJobConfig {
        id: "maestro-ingress".to_string(),
        command: format!(
            "docker run --name {container_name} --network {} -p 8888:8888 -p 8080:8080 --rm traefik:v3.6 {} {} {} {}",
            config.network,
            "--api.insecure=true",
            format_args!("--providers.etcd.rootKey=/traefik"),
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
            "docker run --name {container_name} --network {} -p {}:6400 -v {}:/logs:ro --rm -e ETCD_ENDPOINT=http://{etcd_container}:2379 -e PORT=6400 {PROBE_IMAGE_TAG}",
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
