use crate::deployment::provider::{DockerBuildConfig, DockerDeploymentProvider};
use crate::supervisor::{SupervisedJobConfig, SupervisedJobStatus, controller::JobSupervisor};

pub mod controller;
pub mod etcd;
pub mod keys;
pub mod provider;
pub mod store;
pub mod types;

pub use types::DeploymentConfig;

const MAESTRO_NETWORK: &str = "maestro";
const SIDECAR_IMAGE_TAG: &str = "maestro-sidecar";

pub async fn start_system_jobs(config: &DeploymentConfig, supervisor: &mut JobSupervisor) {
    ensure_docker_network().await;
    start_etcd(config, supervisor).await;
    init_sidecar(config, supervisor).await;
}

async fn ensure_docker_network() {
    let _ = tokio::process::Command::new("docker")
        .args(["network", "create", MAESTRO_NETWORK])
        .output()
        .await;
}

async fn start_etcd(config: &DeploymentConfig, supervisor: &mut JobSupervisor) {
    let etcd_data_dir = config.etcd_dir().join("data");
    std::fs::create_dir_all(&etcd_data_dir).expect("Failed to create etcd data dir");
    let etcd_job_config = SupervisedJobConfig {
        id: "maestro-etcd".to_string(),
        command: format!(
            "docker run --name maestro-etcd --network {MAESTRO_NETWORK} -p {}:2379 -v {}:/data --rm quay.io/coreos/etcd:v3.6.8 etcd {} {} {}",
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

async fn init_sidecar(config: &DeploymentConfig, supervisor: &mut JobSupervisor) {
    DockerDeploymentProvider::build(&DockerBuildConfig {
        context_dir: config.sidecar_dir.clone(),
        tag: SIDECAR_IMAGE_TAG.to_string(),
        dockerfile: None,
    })
    .await
    .expect("failed to build sidecar image");

    let logs_dir = config.data_dir.join("logs/system/sidecar");
    std::fs::create_dir_all(&logs_dir).expect("Failed to create sidecar logs dir");
    let sidecar_job_config = SupervisedJobConfig {
        id: "maestro-sidecar".to_string(),
        command: format!(
            "docker run --name maestro-sidecar --network {MAESTRO_NETWORK} --rm -e ETCD_ENDPOINT=http://maestro-etcd:2379 {SIDECAR_IMAGE_TAG}",
        ),
        name: "maestro-sidecar".to_string(),
        max_restarts: None,
        restart_delay_ms: 1_000,
        shutdown_grace_period_ms: 60_000,
        logs_dir: Some(logs_dir),
    };
    await_job_running(supervisor, sidecar_job_config).await;
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
