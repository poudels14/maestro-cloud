use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cluster::{StoreRuntime, StoreShutdown};
use kernel_store::Clock;
use logs::{LogDeliveryStore, LogQueryStore, LogStore, LogStoreRuntime};
use metrics::{
    HostMetricDeliveryStore, HostMetricStore, MetricDeliveryStore, MetricStore, MetricStoreRuntime,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::{RoleError, RoleRuntime};

pub(crate) struct AgentStartupRuntimes {
    store: Option<Box<dyn StoreRuntime>>,
    logs: Box<dyn LogStoreRuntime>,
    metrics: Box<dyn MetricStoreRuntime>,
}

impl AgentStartupRuntimes {
    pub(crate) fn new(
        store: Option<Box<dyn StoreRuntime>>,
        logs: Box<dyn LogStoreRuntime>,
        metrics: Box<dyn MetricStoreRuntime>,
    ) -> Self {
        Self {
            store,
            logs,
            metrics,
        }
    }

    pub(crate) fn log_store(&self) -> Arc<dyn LogStore> {
        self.logs.store()
    }

    pub(crate) fn log_delivery_store(&self) -> Arc<dyn LogDeliveryStore> {
        self.logs.delivery_store()
    }

    pub(crate) fn log_query_store(&self) -> Arc<dyn LogQueryStore> {
        self.logs.query_store()
    }

    pub(crate) fn metric_store(&self) -> Arc<dyn MetricStore> {
        self.metrics.store()
    }

    pub(crate) fn metric_delivery_store(&self) -> Arc<dyn MetricDeliveryStore> {
        self.metrics.delivery_store()
    }

    pub(crate) fn host_metric_store(&self) -> Arc<dyn HostMetricStore> {
        self.metrics.host_store()
    }

    pub(crate) fn host_metric_delivery_store(&self) -> Arc<dyn HostMetricDeliveryStore> {
        self.metrics.host_delivery_store()
    }

    pub(crate) fn into_owned(self) -> AgentOwnedRuntimes {
        AgentOwnedRuntimes {
            store: self.store,
            logs: self.logs,
            metrics: self.metrics,
        }
    }

    pub(crate) async fn fail<T>(self, error: RoleError) -> Result<T, RoleError> {
        let Self {
            store,
            logs,
            metrics,
        } = self;
        let mut failures = vec![error.detail().to_owned()];
        if let Err(shutdown_error) = metrics.shutdown().await {
            failures.push(format!(
                "failed to roll back metric-store runtime: {shutdown_error}"
            ));
        }
        if let Err(shutdown_error) = logs.shutdown().await {
            failures.push(format!(
                "failed to roll back log-store runtime: {shutdown_error}"
            ));
        }
        if let Some(runtime) = store
            && let Err(shutdown_error) = runtime.shutdown(StoreShutdown::Immediate).await
        {
            failures.push(format!("failed to roll back local store: {shutdown_error}"));
        }
        Err(RoleError::new(failures.join("; ")))
    }
}

pub(crate) struct AgentOwnedRuntimes {
    store: Option<Box<dyn StoreRuntime>>,
    logs: Box<dyn LogStoreRuntime>,
    metrics: Box<dyn MetricStoreRuntime>,
}

pub(crate) struct AgentRoleRuntime {
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<Result<(), RoleError>>>,
    store_runtime: Option<Box<dyn StoreRuntime>>,
    log_store_runtime: Option<Box<dyn LogStoreRuntime>>,
    metric_store_runtime: Option<Box<dyn MetricStoreRuntime>>,
    clock: Arc<dyn Clock>,
    shutdown_grace: Duration,
}

impl AgentRoleRuntime {
    pub(crate) fn new(
        shutdown: watch::Sender<bool>,
        tasks: Vec<JoinHandle<Result<(), RoleError>>>,
        runtimes: AgentOwnedRuntimes,
        clock: Arc<dyn Clock>,
        shutdown_grace: Duration,
    ) -> Self {
        Self {
            shutdown,
            tasks,
            store_runtime: runtimes.store,
            log_store_runtime: Some(runtimes.logs),
            metric_store_runtime: Some(runtimes.metrics),
            clock,
            shutdown_grace,
        }
    }
}

#[async_trait]
impl RoleRuntime for AgentRoleRuntime {
    async fn shutdown(mut self: Box<Self>) -> Result<(), RoleError> {
        let _ = self.shutdown.send(true);
        let mut failures = Vec::new();
        for task in self.tasks.drain(..) {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error.to_string()),
                Err(error) => failures.push(format!("node agent task failed: {error}")),
            }
        }
        if let Some(runtime) = self.metric_store_runtime.take()
            && let Err(error) = runtime.shutdown().await
        {
            failures.push(format!("metric-store shutdown failed: {error}"));
        }
        if let Some(runtime) = self.log_store_runtime.take()
            && let Err(error) = runtime.shutdown().await
        {
            failures.push(format!("log-store shutdown failed: {error}"));
        }
        if let Some(runtime) = self.store_runtime.take() {
            let deadline = self.clock.now().saturating_add(self.shutdown_grace);
            if let Err(error) = runtime.shutdown(StoreShutdown::Graceful { deadline }).await {
                failures.push(format!("store shutdown failed: {error}"));
            }
        }
        finish_shutdown(failures)
    }
}

impl Drop for AgentRoleRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn finish_shutdown(failures: Vec<String>) -> Result<(), RoleError> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(RoleError::new(failures.join("; ")))
    }
}
