use async_trait::async_trait;
use logs::{
    LogHistogramBucket, LogHistogramQuery, LogQueryStore, LogQueryStoreError, LogReadQuery,
    SequencedLogEntry,
};
use tokio::sync::oneshot;

use crate::duck::{Command, DuckLogStore};
use crate::duck_worker::query_worker_stopped;

#[async_trait]
impl LogQueryStore for DuckLogStore {
    async fn query_logs(
        &self,
        query: &LogReadQuery,
    ) -> Result<Vec<SequencedLogEntry>, LogQueryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::QueryLogs {
                query: query.clone(),
                response,
            })
            .await
            .map_err(|_| query_worker_stopped("accepting log query"))?;
        result
            .await
            .map_err(|_| query_worker_stopped("completing log query"))?
    }

    async fn query_log_histogram(
        &self,
        query: &LogHistogramQuery,
    ) -> Result<Vec<LogHistogramBucket>, LogQueryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::QueryHistogram {
                query: query.clone(),
                response,
            })
            .await
            .map_err(|_| query_worker_stopped("accepting log histogram"))?;
        result
            .await
            .map_err(|_| query_worker_stopped("completing log histogram"))?
    }
}
