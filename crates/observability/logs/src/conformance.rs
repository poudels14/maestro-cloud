use std::collections::BTreeMap;

use kernel_api::{ClusterId, NodeId, Timestamp};

use crate::{
    DeadLetterStore, DeadLetterStoreError, IngestLogEntry, LogAppendReport, LogBody,
    LogDeliveryStore, LogDeliveryStoreError, LogOrigin, LogProducer, LogRecordId, LogSequence,
    LogSinkId, LogSinkIdError, LogStatsStore, LogStatsStoreError, LogStore, LogStoreError,
    LogStream, OriginCursor, SequencedLogEntry, SinkDeadLetter,
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

/// Runs ordered reads and monotonic cursor checks against a pre-seeded delivery store.
pub async fn check_log_delivery_store(
    store: &dyn LogDeliveryStore,
    expected: &[SequencedLogEntry],
) -> Result<(), LogDeliveryConformanceError> {
    if expected.len() < 2 {
        return Err(LogDeliveryConformanceError::InsufficientFixture);
    }
    let all = store.read_after(None, expected.len()).await?;
    if all != expected {
        return Err(LogDeliveryConformanceError::UnexpectedEntries);
    }
    let first = expected
        .first()
        .ok_or(LogDeliveryConformanceError::InsufficientFixture)?;
    let last = expected
        .last()
        .ok_or(LogDeliveryConformanceError::InsufficientFixture)?;
    let tail = store
        .read_after(Some(first.sequence), expected.len())
        .await?;
    if tail
        != expected
            .get(1..)
            .ok_or(LogDeliveryConformanceError::InsufficientFixture)?
    {
        return Err(LogDeliveryConformanceError::UnexpectedEntries);
    }

    let sink_id = LogSinkId::new("delivery-conformance")?;
    if store.load_sink_cursor(&sink_id).await?.is_some() {
        return Err(LogDeliveryConformanceError::UnexpectedCursor);
    }
    store.commit_sink_cursor(&sink_id, last.sequence).await?;
    if store.load_sink_cursor(&sink_id).await? != Some(last.sequence) {
        return Err(LogDeliveryConformanceError::UnexpectedCursor);
    }
    if !matches!(
        store.commit_sink_cursor(&sink_id, first.sequence).await,
        Err(LogDeliveryStoreError::Rejected { .. })
    ) {
        return Err(LogDeliveryConformanceError::CursorRegressionAccepted);
    }
    Ok(())
}

/// Runs dead-letter replay, collision, accounting, ordering, and purge checks on a fresh store.
pub async fn check_dead_letter_store(
    store: &dyn DeadLetterStore,
) -> Result<(), DeadLetterConformanceError> {
    let sink_id = LogSinkId::new("dead-letter-conformance")?;
    let first = SinkDeadLetter {
        sink_id: sink_id.clone(),
        source_sequence: LogSequence(1),
        status_code: Some(400),
        reason: "rejected".to_owned(),
        payload: b"first".to_vec(),
        recorded_at: Timestamp(1),
    };
    let second = SinkDeadLetter {
        source_sequence: LogSequence(2),
        payload: b"second".to_vec(),
        ..first.clone()
    };
    store.record(&first).await?;
    store.record(&first).await?;
    store.record(&second).await?;
    let stats = store.stats(&sink_id).await?;
    if stats.count != 2 || stats.payload_bytes != 11 {
        return Err(DeadLetterConformanceError::UnexpectedStats);
    }
    if store.list(&sink_id, None, 8).await? != vec![first.clone(), second.clone()]
        || store.list(&sink_id, Some(LogSequence(1)), 8).await? != vec![second]
    {
        return Err(DeadLetterConformanceError::UnexpectedEntries);
    }
    let mut collision = first;
    collision.payload = b"collision".to_vec();
    if !matches!(
        store.record(&collision).await,
        Err(DeadLetterStoreError::Rejected { .. })
    ) {
        return Err(DeadLetterConformanceError::CollisionAccepted);
    }
    if store.purge(&sink_id, Some(LogSequence(1))).await? != 1
        || store.purge(&sink_id, None).await? != 1
    {
        return Err(DeadLetterConformanceError::UnexpectedPurge);
    }
    Ok(())
}

/// Runs spool, pending-cursor, and aggregate dead-letter stats checks on a fresh shared store.
pub async fn check_log_stats_store(
    append: &dyn LogStore,
    delivery: &dyn LogDeliveryStore,
    dead_letters: &dyn DeadLetterStore,
    stats: &dyn LogStatsStore,
) -> Result<(), LogStatsConformanceError> {
    let mut first = entry("stats-producer-1", "cursor-1", "first")?;
    first.event_at = Timestamp(10);
    let mut second = entry("stats-producer-2", "cursor-1", "second")?;
    second.event_at = Timestamp(20);
    append.append(&[first, second]).await?;
    let entries = delivery.read_after(None, 8).await?;
    let first_sequence = entries
        .first()
        .map(|entry| entry.sequence)
        .ok_or(LogStatsConformanceError::UnexpectedStats)?;
    let first_sink = LogSinkId::new("a-first")?;
    let second_sink = LogSinkId::new("z-last")?;
    delivery
        .commit_sink_cursor(&first_sink, first_sequence)
        .await?;
    dead_letters
        .record(&SinkDeadLetter {
            sink_id: second_sink.clone(),
            source_sequence: first_sequence,
            status_code: Some(413),
            reason: "too large".to_owned(),
            payload: b"payload".to_vec(),
            recorded_at: Timestamp(30),
        })
        .await?;

    let snapshot = stats
        .stats_snapshot(&[second_sink.clone(), first_sink.clone(), second_sink])
        .await?;
    let expected_sinks = [
        (first_sink, Some(first_sequence), 1, Some(20)),
        (LogSinkId::new("z-last")?, None, 2, Some(10)),
    ];
    let sinks_match = snapshot.sinks.iter().zip(expected_sinks).all(
        |(actual, (sink_id, cursor, pending, oldest))| {
            actual.sink_id == sink_id
                && actual.cursor == cursor
                && actual.pending_entries == pending
                && actual.oldest_pending_at_ms == oldest
        },
    );
    if snapshot.row_count != 2
        || snapshot.high_watermark != LogSequence(2)
        || snapshot.oldest_entry_at_ms != Some(10)
        || snapshot.sinks.len() != 2
        || !sinks_match
        || snapshot.dead_letters.count != 1
        || snapshot.dead_letters.payload_bytes != 7
        || snapshot.dead_letters.latest_at_ms != Some(30)
        || snapshot.dead_letters.latest_status != Some(413)
        || snapshot.dead_letters.latest_error.as_deref() != Some("too large")
    {
        return Err(LogStatsConformanceError::UnexpectedStats);
    }
    Ok(())
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

/// A store violated ordered sink-delivery behavior.
#[derive(Debug, thiserror::Error)]
pub enum LogDeliveryConformanceError {
    /// Delivery storage failed valid conformance input.
    #[error(transparent)]
    Store(#[from] LogDeliveryStoreError),
    /// The conformance sink identifier unexpectedly failed validation.
    #[error(transparent)]
    InvalidSinkId(#[from] LogSinkIdError),
    /// The caller did not seed at least two ordered records.
    #[error("delivery conformance requires at least two seeded entries")]
    InsufficientFixture,
    /// Ordered reads differed from the pre-seeded records.
    #[error("delivery store returned unexpected ordered entries")]
    UnexpectedEntries,
    /// Cursor load or commit did not return the durable value.
    #[error("delivery store returned an unexpected sink cursor")]
    UnexpectedCursor,
    /// A cursor regression was accepted.
    #[error("delivery store accepted a cursor regression")]
    CursorRegressionAccepted,
}

/// A store violated dead-letter quarantine behavior.
#[derive(Debug, thiserror::Error)]
pub enum DeadLetterConformanceError {
    /// Dead-letter storage failed valid conformance input.
    #[error(transparent)]
    Store(#[from] DeadLetterStoreError),
    /// The conformance sink identifier unexpectedly failed validation.
    #[error(transparent)]
    InvalidSinkId(#[from] LogSinkIdError),
    /// Listing did not preserve source order and exact content.
    #[error("dead-letter store returned unexpected entries")]
    UnexpectedEntries,
    /// Retained row or byte counts were incorrect.
    #[error("dead-letter store returned unexpected stats")]
    UnexpectedStats,
    /// An identity collision overwrote or deduplicated different content.
    #[error("dead-letter store accepted an identity collision")]
    CollisionAccepted,
    /// Explicit purge did not report the exact removed rows.
    #[error("dead-letter store returned unexpected purge accounting")]
    UnexpectedPurge,
}

/// A store violated the operational log statistics contract.
#[derive(Debug, thiserror::Error)]
pub enum LogStatsConformanceError {
    /// Normalized append storage failed valid fixture input.
    #[error(transparent)]
    Append(#[from] LogStoreError),
    /// Ordered delivery storage failed valid fixture input.
    #[error(transparent)]
    Delivery(#[from] LogDeliveryStoreError),
    /// Dead-letter storage failed valid fixture input.
    #[error(transparent)]
    DeadLetter(#[from] DeadLetterStoreError),
    /// Statistics storage failed a valid snapshot query.
    #[error(transparent)]
    Stats(#[from] LogStatsStoreError),
    /// A fixture identifier unexpectedly failed validation.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// A fixture sink identifier unexpectedly failed validation.
    #[error(transparent)]
    InvalidSinkId(#[from] LogSinkIdError),
    /// Statistics did not describe the durable fixture state.
    #[error("log stats store returned an unexpected snapshot")]
    UnexpectedStats,
}
