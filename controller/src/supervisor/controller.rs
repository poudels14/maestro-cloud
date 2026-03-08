use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::sleep,
};

use crate::supervisor::{
    config_source::ServiceConfigSource,
    model::{NamedServiceConfig, ServiceRuntimeConfig},
    worker::run_managed_service,
};

struct ManagedService {
    config: ServiceRuntimeConfig,
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ManagedService {
    fn spawn(name: String, config: ServiceRuntimeConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_managed_service(name, config.clone(), shutdown_rx));

        Self {
            config,
            shutdown_tx,
            task,
        }
    }

    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.task.await;
    }
}

pub struct ServiceController<S: ServiceConfigSource + Send> {
    source: S,
    services: HashMap<String, ManagedService>,
}

impl<S: ServiceConfigSource + Send> ServiceController<S> {
    pub fn new(source: S) -> Self {
        Self {
            source,
            services: HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        const CTRL_C_CONFIRM_WINDOW: Duration = Duration::from_secs(2);
        let mut first_ctrl_c_at: Option<Instant> = None;
        let (ctrl_c_tx, mut ctrl_c_rx) = mpsc::unbounded_channel();

        let signal_task = tokio::spawn(async move {
            loop {
                if tokio::signal::ctrl_c().await.is_err() {
                    break;
                }
                if ctrl_c_tx.send(()).is_err() {
                    break;
                }
            }
        });

        loop {
            tokio::select! {
                Some(()) = ctrl_c_rx.recv() => {
                    let now = Instant::now();
                    let should_shutdown = first_ctrl_c_at
                        .is_some_and(|first| now.duration_since(first) <= CTRL_C_CONFIRM_WINDOW);

                    if should_shutdown {
                        eprintln!("[maestro]: stopping all jobs and terminate cleanly.");
                        self.shutdown_all().await;
                        signal_task.abort();
                        break;
                    }

                    first_ctrl_c_at = Some(now);
                    eprintln!("[maestro]: press ctrl+c again to stop.");
                }
                snapshot = self.source.next_snapshot() => {
                    match snapshot {
                        Ok(snapshot) => {
                            self.reconcile(snapshot).await;
                        }
                        Err(err) => {
                            eprintln!("[maestro]: config update error: {err}");
                            sleep(std::time::Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        }
    }

    async fn reconcile(&mut self, snapshot: Vec<NamedServiceConfig>) {
        let next: HashMap<String, ServiceRuntimeConfig> = snapshot
            .into_iter()
            .map(|service| (service.name, service.config))
            .collect();

        let existing_names = self.services.keys().cloned().collect::<Vec<_>>();

        for name in existing_names {
            if !next.contains_key(&name)
                && let Some(existing) = self.services.remove(&name)
            {
                eprintln!("[maestro]: stopping removed service '{name}'");
                existing.shutdown().await;
            }
        }

        for (name, config) in next {
            match self.services.remove(&name) {
                None => {
                    self.services
                        .insert(name.clone(), ManagedService::spawn(name, config));
                }
                Some(existing) => {
                    if existing.config != config {
                        eprintln!("[maestro]: restarting changed service '{name}'");
                        existing.shutdown().await;
                        self.services
                            .insert(name.clone(), ManagedService::spawn(name, config));
                    } else {
                        self.services.insert(name, existing);
                    }
                }
            }
        }
    }

    async fn shutdown_all(&mut self) {
        let existing = self
            .services
            .drain()
            .map(|(_, service)| service)
            .collect::<Vec<_>>();
        for service in existing {
            service.shutdown().await;
        }
    }
}
