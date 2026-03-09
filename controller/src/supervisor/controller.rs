use std::{collections::HashMap, time::Duration};

use tokio::time::timeout;

use crate::supervisor::{
    SupervisedJobConfig,
    worker::{ShutdownRequest, SupervisedJob, SupervisedJobRunner, SupervisedJobStatus},
};

const FORCE_JOIN_WAIT: Duration = Duration::from_secs(8);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishedJob {
    pub id: String,
    pub status: SupervisedJobStatus,
}

pub struct JobSupervisor {
    runner: SupervisedJobRunner,
    jobs: HashMap<String, SupervisedJob>,
}

impl JobSupervisor {
    pub fn new() -> Self {
        Self {
            runner: SupervisedJobRunner::new(),
            jobs: HashMap::new(),
        }
    }

    pub fn has_jobs(&self) -> bool {
        !self.jobs.is_empty()
    }

    pub fn contains_job(&self, job_id: &str) -> bool {
        self.jobs.contains_key(job_id)
    }

    pub fn start_job(&mut self, config: SupervisedJobConfig) -> Option<String> {
        if self.contains_job(&config.id) {
            return None;
        }

        let job = self.runner.spawn(config.clone());
        let job_id = config.id.clone();
        self.jobs.insert(config.id, job);
        Some(job_id)
    }

    pub async fn job_status(&self, job_id: &str) -> Option<SupervisedJobStatus> {
        match self.jobs.get(job_id) {
            Some(job) => Some(job.status().await),
            _ => None,
        }
    }

    pub async fn reap_finished_jobs(&mut self) -> Vec<FinishedJob> {
        let mut done = Vec::new();
        for (id, job) in &self.jobs {
            let status = job.status().await;
            if job.is_finished() && status.finished() {
                done.push(id.clone());
            }
        }

        let mut finished = Vec::with_capacity(done.len());
        for id in done {
            if let Some(mut job) = self.jobs.remove(&id) {
                let status = match job.join().await {
                    Ok(status) => status,
                    Err(err) => {
                        eprintln!(
                            "[maestro]: job `{id}` join error while reaping: {err}; marking crashed"
                        );
                        SupervisedJobStatus::Crashed
                    }
                };
                finished.push(FinishedJob { id, status });
            }
        }
        finished
    }

    pub fn shutdown_job(&self, job_id: &str, request: ShutdownRequest) -> bool {
        let Some(job) = self.jobs.get(job_id) else {
            return false;
        };
        job.shutdown(request);
        true
    }

    pub async fn shutdown_all(&mut self, request: ShutdownRequest) -> Vec<FinishedJob> {
        for job in self.jobs.values() {
            let _ = job.shutdown(request);
        }

        if request != ShutdownRequest::Force {
            return Vec::new();
        }

        let draining = self.jobs.drain().collect::<Vec<_>>();
        let mut finished = Vec::with_capacity(draining.len());

        for (id, mut job) in draining {
            let status = match timeout(FORCE_JOIN_WAIT, job.join()).await {
                Ok(result) => match result {
                    Ok(status) => status,
                    Err(err) => {
                        eprintln!(
                            "[maestro]: job `{id}` join error during shutdown: {err}; marking crashed"
                        );
                        SupervisedJobStatus::Crashed
                    }
                },
                Err(_) => {
                    eprintln!("[maestro]: job `{id}` did not stop in time; aborting worker task");
                    job.abort();
                    match timeout(Duration::from_secs(1), job.join()).await {
                        Ok(Ok(status)) => status,
                        Ok(Err(err)) => {
                            eprintln!(
                                "[maestro]: job `{id}` join error after abort: {err}; marking crashed"
                            );
                            SupervisedJobStatus::Crashed
                        }
                        Err(_) => {
                            eprintln!(
                                "[maestro]: job `{id}` still did not stop after abort; marking crashed"
                            );
                            SupervisedJobStatus::Crashed
                        }
                    }
                }
            };
            finished.push(FinishedJob { id, status });
        }
        finished
    }
}
