use std::sync::Arc;
use std::thread::JoinHandle;

use async_trait::async_trait;
use metrics::{
    HostMetricPoint, HostMetricQuery, HostMetricQueryStore, HostMetricQueryStoreError,
    HostMetricStore, LatestHostMetricQuery, MetricAppendReport, MetricDeliveryStore,
    MetricDeliveryStoreError, MetricSequence, MetricSinkId, MetricStore, MetricStoreError,
    MetricStoreRuntime, MetricStoreRuntimeError, SequencedMetricPoint, WorkloadMetricHistoryPoint,
    WorkloadMetricPoint, WorkloadMetricQuery, WorkloadMetricQueryStore,
    WorkloadMetricQueryStoreError,
};
use tokio::sync::{mpsc, oneshot};

use crate::metric_schema;
use crate::{DuckStoreError, DuckStoreSettings};

enum Command {
    Append {
        points: Vec<WorkloadMetricPoint>,
        response: oneshot::Sender<Result<MetricAppendReport, MetricStoreError>>,
    },
    AppendHost {
        points: Vec<HostMetricPoint>,
        response: oneshot::Sender<Result<MetricAppendReport, MetricStoreError>>,
    },
    QueryHost {
        query: HostMetricQuery,
        response: oneshot::Sender<Result<Vec<HostMetricPoint>, HostMetricQueryStoreError>>,
    },
    LatestHost {
        query: LatestHostMetricQuery,
        response: oneshot::Sender<Result<Vec<HostMetricPoint>, HostMetricQueryStoreError>>,
    },
    QueryWorkloads {
        query: WorkloadMetricQuery,
        response:
            oneshot::Sender<Result<Vec<WorkloadMetricHistoryPoint>, WorkloadMetricQueryStoreError>>,
    },
    ReadAfter {
        cursor: Option<MetricSequence>,
        limit: usize,
        response: oneshot::Sender<Result<Vec<SequencedMetricPoint>, MetricDeliveryStoreError>>,
    },
    LoadCursor {
        sink_id: MetricSinkId,
        response: oneshot::Sender<Result<Option<MetricSequence>, MetricDeliveryStoreError>>,
    },
    CommitCursor {
        sink_id: MetricSinkId,
        sequence: MetricSequence,
        response: oneshot::Sender<Result<(), MetricDeliveryStoreError>>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

/// Async metric append handle applying bounded backpressure to one DuckDB owner thread.
pub struct DuckMetricStore {
    commands: mpsc::Sender<Command>,
}

/// Explicit lifetime owner for the blocking metric DuckDB writer thread.
pub struct DuckMetricStoreRuntime {
    store: Arc<DuckMetricStore>,
    worker: Option<JoinHandle<()>>,
}

impl DuckMetricStoreRuntime {
    /// Opens and migrates the metric database on its dedicated thread before returning.
    pub async fn open(settings: DuckStoreSettings) -> Result<Self, DuckStoreError> {
        let (commands, receiver) = mpsc::channel(settings.queue_capacity);
        let (initialized, initialization) = oneshot::channel();
        let path = settings.path.clone();
        let worker_path = path.clone();
        let worker = std::thread::Builder::new()
            .name("maestro-metricstore".to_owned())
            .spawn(move || run_worker(&worker_path, receiver, initialized))
            .map_err(|source| DuckStoreError::Spawn { path, source })?;
        match initialization.await {
            Ok(Ok(())) => Ok(Self {
                store: Arc::new(DuckMetricStore { commands }),
                worker: Some(worker),
            }),
            Ok(Err(message)) => {
                join(worker).await?;
                Err(DuckStoreError::Initialize {
                    path: settings.path,
                    message,
                })
            }
            Err(_) => {
                join(worker).await?;
                Err(DuckStoreError::WorkerStopped {
                    action: "reporting metric initialization",
                })
            }
        }
    }

    /// Returns the normalized metric append contract.
    pub fn store(&self) -> Arc<DuckMetricStore> {
        self.store.clone()
    }

    /// Drains accepted appends, closes DuckDB, and joins the owner thread.
    pub async fn shutdown(mut self) -> Result<(), DuckStoreError> {
        let (shutdown, stopped) = oneshot::channel();
        let lifecycle = match self
            .store
            .commands
            .send(Command::Shutdown { response: shutdown })
            .await
        {
            Ok(()) => stopped.await.map_err(|_| DuckStoreError::WorkerStopped {
                action: "confirming metric shutdown",
            }),
            Err(_) => Err(DuckStoreError::WorkerStopped {
                action: "requesting metric shutdown",
            }),
        };
        if let Some(worker) = self.worker.take() {
            join(worker).await?;
        }
        lifecycle
    }
}

impl Drop for DuckMetricStoreRuntime {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let (response, _stopped) = oneshot::channel();
            let _ignored = self.store.commands.try_send(Command::Shutdown { response });
        }
    }
}

#[async_trait]
impl MetricStore for DuckMetricStore {
    async fn append(
        &self,
        points: &[WorkloadMetricPoint],
    ) -> Result<MetricAppendReport, MetricStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::Append {
                points: points.to_vec(),
                response,
            })
            .await
            .map_err(|_| MetricStoreError::Unavailable {
                message: "DuckDB metric writer stopped before accepting append".to_owned(),
            })?;
        result.await.map_err(|_| MetricStoreError::Unavailable {
            message: "DuckDB metric writer stopped before completing append".to_owned(),
        })?
    }
}

#[async_trait]
impl HostMetricStore for DuckMetricStore {
    async fn append_host_metrics(
        &self,
        points: &[HostMetricPoint],
    ) -> Result<MetricAppendReport, MetricStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::AppendHost {
                points: points.to_vec(),
                response,
            })
            .await
            .map_err(|_| MetricStoreError::Unavailable {
                message: "DuckDB metric writer stopped before accepting host append".to_owned(),
            })?;
        result.await.map_err(|_| MetricStoreError::Unavailable {
            message: "DuckDB metric writer stopped before completing host append".to_owned(),
        })?
    }
}

#[async_trait]
impl HostMetricQueryStore for DuckMetricStore {
    async fn query_host_metrics(
        &self,
        query: &HostMetricQuery,
    ) -> Result<Vec<HostMetricPoint>, HostMetricQueryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::QueryHost {
                query: query.clone(),
                response,
            })
            .await
            .map_err(|_| host_query_worker_stopped("accepting host history query"))?;
        result
            .await
            .map_err(|_| host_query_worker_stopped("completing host history query"))?
    }

    async fn latest_host_metrics(
        &self,
        query: &LatestHostMetricQuery,
    ) -> Result<Vec<HostMetricPoint>, HostMetricQueryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::LatestHost {
                query: query.clone(),
                response,
            })
            .await
            .map_err(|_| host_query_worker_stopped("accepting latest host query"))?;
        result
            .await
            .map_err(|_| host_query_worker_stopped("completing latest host query"))?
    }
}

#[async_trait]
impl WorkloadMetricQueryStore for DuckMetricStore {
    async fn query_workload_metrics(
        &self,
        query: &WorkloadMetricQuery,
    ) -> Result<Vec<WorkloadMetricHistoryPoint>, WorkloadMetricQueryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::QueryWorkloads {
                query: query.clone(),
                response,
            })
            .await
            .map_err(|_| workload_query_worker_stopped("accepting workload history query"))?;
        result
            .await
            .map_err(|_| workload_query_worker_stopped("completing workload history query"))?
    }
}

#[async_trait]
impl MetricDeliveryStore for DuckMetricStore {
    async fn read_after(
        &self,
        cursor: Option<MetricSequence>,
        limit: usize,
    ) -> Result<Vec<SequencedMetricPoint>, MetricDeliveryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::ReadAfter {
                cursor,
                limit,
                response,
            })
            .await
            .map_err(|_| delivery_worker_stopped("accepting ordered metric read"))?;
        result
            .await
            .map_err(|_| delivery_worker_stopped("completing ordered metric read"))?
    }

    async fn load_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
    ) -> Result<Option<MetricSequence>, MetricDeliveryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::LoadCursor {
                sink_id: sink_id.clone(),
                response,
            })
            .await
            .map_err(|_| delivery_worker_stopped("accepting metric cursor read"))?;
        result
            .await
            .map_err(|_| delivery_worker_stopped("completing metric cursor read"))?
    }

    async fn commit_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
        sequence: MetricSequence,
    ) -> Result<(), MetricDeliveryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::CommitCursor {
                sink_id: sink_id.clone(),
                sequence,
                response,
            })
            .await
            .map_err(|_| delivery_worker_stopped("accepting metric cursor commit"))?;
        result
            .await
            .map_err(|_| delivery_worker_stopped("completing metric cursor commit"))?
    }
}

#[async_trait]
impl MetricStoreRuntime for DuckMetricStoreRuntime {
    fn store(&self) -> Arc<dyn MetricStore> {
        self.store.clone()
    }

    fn delivery_store(&self) -> Arc<dyn MetricDeliveryStore> {
        self.store.clone()
    }

    fn query_store(&self) -> Arc<dyn WorkloadMetricQueryStore> {
        self.store.clone()
    }

    fn host_store(&self) -> Arc<dyn HostMetricStore> {
        self.store.clone()
    }

    fn host_query_store(&self) -> Arc<dyn HostMetricQueryStore> {
        self.store.clone()
    }

    async fn shutdown(self: Box<Self>) -> Result<(), MetricStoreRuntimeError> {
        DuckMetricStoreRuntime::shutdown(*self)
            .await
            .map_err(|error| MetricStoreRuntimeError {
                message: error.to_string(),
            })
    }
}

fn run_worker(
    path: &std::path::Path,
    mut commands: mpsc::Receiver<Command>,
    initialized: oneshot::Sender<Result<(), String>>,
) {
    let mut connection = match metric_schema::open(path) {
        Ok(connection) => {
            if initialized.send(Ok(())).is_err() {
                return;
            }
            connection
        }
        Err(error) => {
            let _ignored = initialized.send(Err(error));
            return;
        }
    };
    while let Some(command) = commands.blocking_recv() {
        match command {
            Command::Append { points, response } => {
                let _ignored = response.send(metric_schema::append(&mut connection, &points));
            }
            Command::AppendHost { points, response } => {
                let _ignored =
                    response.send(crate::host_metric_schema::append(&mut connection, &points));
            }
            Command::QueryHost { query, response } => {
                let _ignored = response.send(crate::host_metric_schema::query(&connection, &query));
            }
            Command::LatestHost { query, response } => {
                let _ignored =
                    response.send(crate::host_metric_schema::latest(&connection, &query));
            }
            Command::QueryWorkloads { query, response } => {
                let _ignored =
                    response.send(crate::workload_metric_schema::query(&connection, &query));
            }
            Command::ReadAfter {
                cursor,
                limit,
                response,
            } => {
                let _ignored = response.send(crate::metric_delivery_schema::read_after(
                    &connection,
                    cursor,
                    limit,
                ));
            }
            Command::LoadCursor { sink_id, response } => {
                let _ignored = response.send(crate::metric_delivery_schema::load_cursor(
                    &connection,
                    &sink_id,
                ));
            }
            Command::CommitCursor {
                sink_id,
                sequence,
                response,
            } => {
                let _ignored = response.send(crate::metric_delivery_schema::commit_cursor(
                    &mut connection,
                    &sink_id,
                    sequence,
                ));
            }
            Command::Shutdown { response } => {
                drop(connection);
                let _ignored = response.send(());
                return;
            }
        }
    }
}

fn delivery_worker_stopped(action: &'static str) -> MetricDeliveryStoreError {
    MetricDeliveryStoreError::Unavailable {
        message: format!("DuckDB metric writer stopped before {action}"),
    }
}

fn host_query_worker_stopped(action: &'static str) -> HostMetricQueryStoreError {
    HostMetricQueryStoreError::Unavailable {
        message: format!("DuckDB metric writer stopped before {action}"),
    }
}

fn workload_query_worker_stopped(action: &'static str) -> WorkloadMetricQueryStoreError {
    WorkloadMetricQueryStoreError::Unavailable {
        message: format!("DuckDB metric writer stopped before {action}"),
    }
}

async fn join(worker: JoinHandle<()>) -> Result<(), DuckStoreError> {
    tokio::task::spawn_blocking(move || worker.join())
        .await
        .map_err(|_| DuckStoreError::WorkerPanicked)?
        .map_err(|_| DuckStoreError::WorkerPanicked)
}
