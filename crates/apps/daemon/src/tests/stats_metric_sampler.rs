use std::sync::Arc;
use std::time::Duration;

use kernel_api::{NodeId, Timestamp};
use kernel_store::TokioClock;
use logs::{
    InMemoryLogStore, LiveControllerStats, SinkRuntimeRegistry, StatsMetricQuery, StatsMetricStore,
};
use node_agent::StatusClock;

use crate::stats_metric_sampler::StatsMetricSampler;

#[tokio::test]
async fn sampler_persists_controller_and_default_backup_series_with_node_labels()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogStore::new());
    let controller = Arc::new(LiveControllerStats::new(
        store.clone(),
        Vec::new(),
        SinkRuntimeRegistry::default(),
        "1.2.3",
    ));
    let sampler = StatsMetricSampler::new(
        NodeId::new("node-1")?,
        store.clone(),
        controller,
        None,
        Arc::new(TokioClock::new()),
        Arc::new(FixedClock),
        Duration::from_secs(5),
    )?;

    let report = sampler.collect_once().await?;
    assert_eq!(report.committed, 8);
    let points = store
        .query_stats_metrics(&StatsMetricQuery::new(None, 10_000, 10_000, 16)?)
        .await?;
    assert_eq!(points.len(), 8);
    assert!(
        points
            .iter()
            .all(|point| point.labels.get("node").map(String::as_str) == Some("node-1"))
    );
    assert!(
        points
            .iter()
            .any(|point| point.name == "logs.backup.pending_partitions")
    );
    Ok(())
}

#[test]
fn sampler_rejects_a_zero_interval() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogStore::new());
    let controller = Arc::new(LiveControllerStats::new(
        store.clone(),
        Vec::new(),
        SinkRuntimeRegistry::default(),
        "1.2.3",
    ));
    assert!(
        StatsMetricSampler::new(
            NodeId::new("node-1")?,
            store,
            controller,
            None,
            Arc::new(TokioClock::new()),
            Arc::new(FixedClock),
            Duration::ZERO,
        )
        .is_err()
    );
    Ok(())
}

struct FixedClock;

impl StatusClock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp(10_000)
    }
}
