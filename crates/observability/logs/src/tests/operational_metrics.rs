use std::collections::BTreeMap;

use crate::{
    InMemoryLogStore, StatsMetricPoint, StatsMetricQuery, StatsMetricStore, StatsMetricStoreError,
};

#[tokio::test]
async fn in_memory_stats_metrics_pass_shared_conformance() {
    let store = InMemoryLogStore::new();
    crate::conformance::check_stats_metric_store(&store)
        .await
        .expect("stats metric conformance");
}

#[tokio::test]
async fn stats_metric_contract_rejects_invalid_content_and_queries() {
    let store = InMemoryLogStore::new();
    for point in [
        point("", 1.0, BTreeMap::new()),
        point("valid", f64::NAN, BTreeMap::new()),
        point(
            "valid",
            1.0,
            BTreeMap::from([(String::new(), String::new())]),
        ),
    ] {
        assert!(matches!(
            store.append_stats_metrics(&[point]).await,
            Err(StatsMetricStoreError::Rejected { .. })
        ));
    }
    assert!(StatsMetricQuery::new(None, 2, 1, 1).is_err());
    assert!(StatsMetricQuery::new(None, 1, 2, 0).is_err());
    assert!(StatsMetricQuery::new(Some(String::new()), 1, 2, 1).is_err());
}

fn point(name: &str, value: f64, labels: BTreeMap<String, String>) -> StatsMetricPoint {
    StatsMetricPoint {
        ts: 1,
        name: name.to_owned(),
        value,
        labels,
    }
}
