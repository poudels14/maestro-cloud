use crate::{InMemoryHostMetricStore, InMemoryMetricStore};

#[tokio::test]
async fn in_memory_metric_store_passes_the_shared_conformance_battery()
-> Result<(), Box<dyn std::error::Error>> {
    crate::conformance::check_metric_store(&InMemoryMetricStore::new()).await?;
    let delivery = InMemoryMetricStore::new();
    crate::conformance::check_metric_delivery_store(&delivery, &delivery).await?;
    crate::conformance::check_host_metric_store(&InMemoryHostMetricStore::new()).await?;
    Ok(())
}
