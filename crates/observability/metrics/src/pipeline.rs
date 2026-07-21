use std::sync::Arc;

use async_trait::async_trait;
use node_agent::{WorkloadStatsSample, WorkloadStatsSink, WorkloadStatsSinkError};

use crate::{MetricRecordId, MetricStore, MetricStoreError, WorkloadMetricPoint};

/// Normalizes agent cgroup samples and commits one replay-safe metric representation.
pub struct WorkloadMetricPipeline {
    store: Arc<dyn MetricStore>,
}

impl WorkloadMetricPipeline {
    /// Creates a workload metric pipeline over a durable store.
    pub fn new(store: Arc<dyn MetricStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl WorkloadStatsSink for WorkloadMetricPipeline {
    async fn ingest(&self, samples: &[WorkloadStatsSample]) -> Result<(), WorkloadStatsSinkError> {
        let points = samples.iter().map(normalize).collect::<Vec<_>>();
        self.store
            .append(&points)
            .await
            .map(|_report| ())
            .map_err(map_store_error)
    }
}

fn normalize(sample: &WorkloadStatsSample) -> WorkloadMetricPoint {
    WorkloadMetricPoint {
        id: MetricRecordId {
            node_id: sample.metadata.node_id.clone(),
            workload_id: sample.metadata.workload_id.clone(),
            collected_at: sample.collected_at,
        },
        metadata: sample.metadata.clone(),
        cpu_usage_usec: sample.stats.cpu.usage_usec,
        cpu_user_usec: sample.stats.cpu.user_usec,
        cpu_system_usec: sample.stats.cpu.system_usec,
        cpu_periods: sample.stats.cpu.periods,
        cpu_throttled_periods: sample.stats.cpu.throttled_periods,
        cpu_throttled_usec: sample.stats.cpu.throttled_usec,
        memory_current_bytes: sample.stats.memory.current_bytes,
        memory_maximum_bytes: sample.stats.memory.maximum_bytes,
        memory_out_of_memory_kills: sample.stats.memory.events.out_of_memory_kills,
        memory_low_events: sample.stats.memory.events.low,
        memory_high_events: sample.stats.memory.events.high,
        memory_maximum_events: sample.stats.memory.events.maximum,
        memory_out_of_memory_events: sample.stats.memory.events.out_of_memory,
        memory_out_of_memory_group_kills: sample.stats.memory.events.out_of_memory_group_kills,
        io_read_bytes: sample.stats.io.read_bytes,
        io_write_bytes: sample.stats.io.write_bytes,
        io_read_operations: sample.stats.io.read_operations,
        io_write_operations: sample.stats.io.write_operations,
        io_discarded_bytes: sample.stats.io.discarded_bytes,
        io_discard_operations: sample.stats.io.discard_operations,
        network_receive_bytes: sample.network.map(|network| network.receive_bytes),
        network_transmit_bytes: sample.network.map(|network| network.transmit_bytes),
        processes_current: sample.stats.processes.current,
        processes_maximum: sample.stats.processes.maximum,
    }
}

fn map_store_error(error: MetricStoreError) -> WorkloadStatsSinkError {
    match error {
        MetricStoreError::Rejected { message } => WorkloadStatsSinkError::Rejected { message },
        MetricStoreError::Unavailable { message } => {
            WorkloadStatsSinkError::Unavailable { message }
        }
    }
}
