use std::collections::HashMap;
use std::time::Duration;

use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::sleep,
};

use crate::supervisor::{
    config_source::ServiceConfigSource,
    model::{NamedServiceConfig, ServiceRuntimeConfig},
    worker::{ShutdownRequest, WorkerExitStatus, run_managed_service},
};

struct ManagedService {
    config: ServiceRuntimeConfig,
    shutdown_tx: watch::Sender<ShutdownRequest>,
    task: JoinHandle<WorkerExitStatus>,
}

enum ShutdownSignal {
    CtrlC,
    Terminate(&'static str),
}

impl ManagedService {
    fn spawn(name: String, config: ServiceRuntimeConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownRequest::None);
        let task = tokio::spawn(run_managed_service(name, config.clone(), shutdown_rx));

        Self {
            config,
            shutdown_tx,
            task,
        }
    }

    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(ShutdownRequest::Graceful);
        let _ = self.task.await;
    }

    async fn force_shutdown(self) {
        let _ = self.shutdown_tx.send(ShutdownRequest::Force);
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
        let mut ctrl_c_count: u8 = 0;
        let mut shutdown_started = false;
        let (signal_tx, mut signal_rx) = mpsc::unbounded_channel();
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        let mut sigquit = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::quit())
            .expect("failed to install SIGQUIT handler");
        let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
            .expect("failed to install SIGHUP handler");

        let signal_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    ctrl_c = tokio::signal::ctrl_c() => {
                        if ctrl_c.is_err() {
                            break;
                        }
                        if signal_tx.send(ShutdownSignal::CtrlC).is_err() {
                            break;
                        }
                    }
                    _ = sigterm.recv() => {
                        if signal_tx.send(ShutdownSignal::Terminate("SIGTERM")).is_err() {
                            break;
                        }
                    }
                    _ = sigquit.recv() => {
                        if signal_tx.send(ShutdownSignal::Terminate("SIGQUIT")).is_err() {
                            break;
                        }
                    }
                    _ = sighup.recv() => {
                        if signal_tx.send(ShutdownSignal::Terminate("SIGHUP")).is_err() {
                            break;
                        }
                    }
                }
            }
        });

        loop {
            tokio::select! {
                Some(signal) = signal_rx.recv() => {
                    match signal {
                        ShutdownSignal::CtrlC => {
                            ctrl_c_count = ctrl_c_count.saturating_add(1);
                            match ctrl_c_count {
                                1 => {
                                    eprintln!("[maestro]: press ctrl+c again to stop gracefully (third ctrl+c will force shutdown).");
                                }
                                2 => {
                                    if !shutdown_started {
                                        eprintln!("[maestro]: stopping all jobs gracefully.");
                                        self.request_shutdown_all(ShutdownRequest::Graceful);
                                        shutdown_started = true;
                                    }
                                }
                                _ => {
                                    eprintln!("[maestro]: forcing shutdown.");
                                    self.shutdown_all(ShutdownRequest::Force).await;
                                    signal_task.abort();
                                    break;
                                }
                            }
                        }
                        ShutdownSignal::Terminate(signal_name) => {
                            eprintln!("[maestro]: received {signal_name}; stopping all jobs and terminate cleanly.");
                            self.shutdown_all(ShutdownRequest::Graceful).await;
                            signal_task.abort();
                            break;
                        }
                    }
                }
                _ = sleep(Duration::from_millis(100)), if shutdown_started => {
                    self.reap_finished_services().await;
                    if self.services.is_empty() {
                        signal_task.abort();
                        break;
                    }
                }
                snapshot = self.source.next_snapshot(), if !shutdown_started => {
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

    fn request_shutdown_all(&self, request: ShutdownRequest) {
        for service in self.services.values() {
            let _ = service.shutdown_tx.send(request);
        }
    }

    async fn reap_finished_services(&mut self) {
        let done = self
            .services
            .iter()
            .filter_map(|(name, service)| {
                if service.task.is_finished() {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for name in done {
            if let Some(service) = self.services.remove(&name) {
                let _ = service.task.await;
            }
        }
    }

    async fn shutdown_all(&mut self, request: ShutdownRequest) {
        let existing = self
            .services
            .drain()
            .map(|(_, service)| service)
            .collect::<Vec<_>>();
        for service in existing {
            match request {
                ShutdownRequest::Force => service.force_shutdown().await,
                ShutdownRequest::Graceful | ShutdownRequest::None => service.shutdown().await,
            }
        }
    }
}
