use std::path::PathBuf;

use crate::supervisor::{SupervisedJobConfig, SupervisedJobStatus, controller::JobSupervisor};

pub mod controller;
pub mod etcd;
pub mod events;
pub mod keys;
pub mod provider;
pub mod store;
pub mod types;

#[derive(Debug, Clone)]
pub struct DeploymentConfig {
    pub data_dir: PathBuf,
    pub etcd_port: u16,
}

impl DeploymentConfig {
    #[inline]
    pub fn logs_dir(&self) -> PathBuf {
        self.data_dir.join("logs")
    }
}

pub async fn start_system_jobs(config: &DeploymentConfig, supervisor: &mut JobSupervisor) {
    let job_id = supervisor.start_job(SupervisedJobConfig {
        id: "maestro-etcd".to_string(),
        command: format!(
            "docker run --name maestro-etcd -p {}:2379 --rm quay.io/coreos/etcd:v3.6.8 etcd {}",
            config.etcd_port,
            "--listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://127.0.0.1:6479"
        ),
        name: "maestro-etcd".to_string(),
        max_restarts: 100,
        restart_delay_ms: 100,
        shutdown_grace_period_ms: 10_000,
        logs_dir: Some(config.logs_dir().join("system/etcd/logs/")),
    });
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
