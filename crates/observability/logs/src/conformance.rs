use std::collections::BTreeMap;

use kernel_api::{ClusterId, NodeId, Timestamp};

use crate::{
    IngestLogEntry, LogAppendReport, LogBody, LogOrigin, LogProducer, LogRecordId, LogStore,
    LogStoreError, LogStream, OriginCursor,
};

/// Runs the reusable append, replay, collision, and atomicity battery on a fresh store.
pub async fn check_log_store(store: &dyn LogStore) -> Result<(), LogStoreConformanceError> {
    let first = entry("producer-1", "cursor-1", "first")?;
    require_report(
        "initial append",
        store.append(std::slice::from_ref(&first)).await?,
        LogAppendReport {
            committed: 1,
            deduplicated: 0,
        },
    )?;
    require_report(
        "exact replay",
        store.append(std::slice::from_ref(&first)).await?,
        LogAppendReport {
            committed: 0,
            deduplicated: 1,
        },
    )?;

    let mut collision = first;
    collision.body = LogBody::Text("collision".to_owned());
    if !matches!(
        store.append(&[collision.clone()]).await,
        Err(LogStoreError::Rejected { .. })
    ) {
        return Err(LogStoreConformanceError::CollisionAccepted);
    }

    let second = entry("producer-2", "cursor-1", "second")?;
    if !matches!(
        store.append(&[second.clone(), collision]).await,
        Err(LogStoreError::Rejected { .. })
    ) {
        return Err(LogStoreConformanceError::CollisionAccepted);
    }
    require_report(
        "append after rejected batch",
        store.append(&[second]).await?,
        LogAppendReport {
            committed: 1,
            deduplicated: 0,
        },
    )?;

    let repeated = entry("producer-3", "cursor-1", "third")?;
    require_report(
        "same-batch replay",
        store.append(&[repeated.clone(), repeated]).await?,
        LogAppendReport {
            committed: 1,
            deduplicated: 1,
        },
    )
}

fn require_report(
    stage: &'static str,
    actual: LogAppendReport,
    expected: LogAppendReport,
) -> Result<(), LogStoreConformanceError> {
    if actual == expected {
        Ok(())
    } else {
        Err(LogStoreConformanceError::UnexpectedReport {
            stage,
            expected,
            actual,
        })
    }
}

fn entry(
    producer_id: &str,
    cursor: &str,
    body: &str,
) -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    let cluster_id = ClusterId::new("log-store-conformance")?;
    let node_id = NodeId::new("node-1")?;
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::System(producer_id.to_owned()),
            cursor: OriginCursor::new(cursor),
        },
        observed_at: Timestamp(1),
        event_at: Timestamp(1),
        severity: "info".to_owned(),
        stream: LogStream::System,
        origin: LogOrigin::System {
            cluster_id,
            node_id: Some(node_id),
            component: "conformance".to_owned(),
        },
        body: LogBody::Text(body.to_owned()),
        attributes: BTreeMap::new(),
    })
}

/// A store violated behavior required by the normalized-log contract.
#[derive(Debug, thiserror::Error)]
pub enum LogStoreConformanceError {
    /// The store itself failed while processing valid conformance input.
    #[error(transparent)]
    Store(#[from] LogStoreError),
    /// Test identifiers unexpectedly violated kernel identifier rules.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// A replay identity was accepted with different immutable content.
    #[error("log store accepted a replay identity with different content")]
    CollisionAccepted,
    /// Append accounting did not describe the tested operation.
    #[error("{stage} returned {actual:?}, expected {expected:?}")]
    UnexpectedReport {
        /// Conformance stage that produced the mismatch.
        stage: &'static str,
        /// Required accounting.
        expected: LogAppendReport,
        /// Store-provided accounting.
        actual: LogAppendReport,
    },
}
