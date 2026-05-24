//! Replica supervision: trait + production impl + in-memory test impl.
//!
//! The engine doesn't need to know how processes are actually spawned —
//! production wires [`JobReplicaSupervisor`] around the real [`JobSupervisor`],
//! tests wire [`InMemoryReplicaSupervisor`].

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::supervisor::{
    ShutdownRequest, SupervisedJobConfig,
    controller::{FinishedJob, JobSupervisor},
};

#[async_trait]
pub trait ReplicaSupervisor: Send + Sync {
    async fn start_job(&self, config: SupervisedJobConfig) -> Option<String>;
    async fn shutdown_job(&self, job_id: &str, request: ShutdownRequest) -> bool;
    async fn reap_finished_jobs(&self) -> Vec<FinishedJob>;
    async fn has_jobs(&self) -> bool;
    async fn shutdown_all(&self, request: ShutdownRequest) -> Vec<FinishedJob>;
}

pub struct JobReplicaSupervisor {
    inner: Mutex<JobSupervisor>,
}

impl JobReplicaSupervisor {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(JobSupervisor::new()),
        }
    }
}

impl Default for JobReplicaSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReplicaSupervisor for JobReplicaSupervisor {
    async fn start_job(&self, config: SupervisedJobConfig) -> Option<String> {
        let mut inner = self.inner.lock().await;
        inner.start_job(config)
    }

    async fn shutdown_job(&self, job_id: &str, request: ShutdownRequest) -> bool {
        let inner = self.inner.lock().await;
        inner.shutdown_job(job_id, request)
    }

    async fn reap_finished_jobs(&self) -> Vec<FinishedJob> {
        let mut inner = self.inner.lock().await;
        inner.reap_finished_jobs().await
    }

    async fn has_jobs(&self) -> bool {
        let inner = self.inner.lock().await;
        inner.has_jobs()
    }

    async fn shutdown_all(&self, request: ShutdownRequest) -> Vec<FinishedJob> {
        let mut inner = self.inner.lock().await;
        inner.shutdown_all(request).await
    }
}

#[cfg(test)]
pub mod fake {
    use super::*;
    use crate::supervisor::SupervisedJobStatus;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    pub struct InMemoryReplicaSupervisor {
        state: StdMutex<State>,
    }

    #[derive(Default)]
    struct State {
        running: HashMap<String, RunningJob>,
        alive_hostnames: HashMap<String, String>,
        finished: Vec<FinishedJob>,
    }

    struct RunningJob {
        hostname: Option<String>,
    }

    impl InMemoryReplicaSupervisor {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn crash_job(&self, job_id: &str) {
            let mut state = self.state.lock().expect("supervisor state");
            if let Some(job) = state.running.remove(job_id) {
                if let Some(hostname) = job.hostname {
                    state.alive_hostnames.remove(&hostname);
                }
                state.finished.push(FinishedJob {
                    id: job_id.to_string(),
                    status: SupervisedJobStatus::Crashed,
                });
            }
        }

        pub fn is_hostname_alive(&self, hostname: &str) -> bool {
            self.state
                .lock()
                .expect("supervisor state")
                .alive_hostnames
                .contains_key(hostname)
        }

        pub fn running_count(&self) -> usize {
            self.state.lock().expect("supervisor state").running.len()
        }
    }

    #[async_trait]
    impl ReplicaSupervisor for InMemoryReplicaSupervisor {
        async fn start_job(&self, config: SupervisedJobConfig) -> Option<String> {
            let mut state = self.state.lock().expect("supervisor state");
            if state.running.contains_key(&config.id) {
                return None;
            }
            let hostname = config.container.as_ref().map(|c| c.name.clone());
            if let Some(host) = hostname.clone() {
                state.alive_hostnames.insert(host, config.id.clone());
            }
            state
                .running
                .insert(config.id.clone(), RunningJob { hostname });
            Some(config.id)
        }

        async fn shutdown_job(&self, job_id: &str, _request: ShutdownRequest) -> bool {
            let mut state = self.state.lock().expect("supervisor state");
            if let Some(job) = state.running.remove(job_id) {
                if let Some(hostname) = job.hostname {
                    state.alive_hostnames.remove(&hostname);
                }
                state.finished.push(FinishedJob {
                    id: job_id.to_string(),
                    status: SupervisedJobStatus::Stopped,
                });
                true
            } else {
                false
            }
        }

        async fn reap_finished_jobs(&self) -> Vec<FinishedJob> {
            let mut state = self.state.lock().expect("supervisor state");
            std::mem::take(&mut state.finished)
        }

        async fn has_jobs(&self) -> bool {
            let state = self.state.lock().expect("supervisor state");
            !state.running.is_empty()
        }

        async fn shutdown_all(&self, _request: ShutdownRequest) -> Vec<FinishedJob> {
            let mut state = self.state.lock().expect("supervisor state");
            let running = std::mem::take(&mut state.running);
            state.alive_hostnames.clear();
            let mut finished = Vec::with_capacity(running.len());
            for (id, _) in running {
                finished.push(FinishedJob {
                    id,
                    status: SupervisedJobStatus::Stopped,
                });
            }
            finished.append(&mut state.finished);
            finished
        }
    }
}
