use crate::supervisor::{SupervisedJobConfig, SupervisedJobStatus, controller::JobSupervisor};

pub mod controller;
pub mod etcd;
pub mod keys;
pub mod provider;
pub mod store;
pub mod types;

pub use types::DeploymentConfig;

pub async fn start_system_jobs(config: &DeploymentConfig, supervisor: &mut JobSupervisor) {
    let etcd_data_dir = config.etcd_dir().join("data");
    std::fs::create_dir_all(&etcd_data_dir).expect("Failed to create etcd data dir");
    let etcd_job_config = SupervisedJobConfig {
        id: "maestro-etcd".to_string(),
        command: format!(
            "docker run --name maestro-etcd -p {}:2379 -v {}:/data --rm quay.io/coreos/etcd:v3.6.8 etcd {} {} {}",
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
    let job_id = supervisor.start_job(etcd_job_config);
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
