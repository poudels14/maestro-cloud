use std::sync::Arc;

use async_trait::async_trait;
use node_agent::{HostTelemetrySample, HostTelemetrySink, HostTelemetrySinkError};

use crate::{
    HostDiskMetricPoint, HostMetricPoint, HostMetricRecordId, HostMetricStore,
    HostResourceMetricPoint, MetricStoreError,
};

/// Normalizes node-agent host samples and commits one replay-safe representation.
pub struct HostMetricPipeline {
    store: Arc<dyn HostMetricStore>,
}

impl HostMetricPipeline {
    /// Creates a host metric pipeline over a durable store.
    pub fn new(store: Arc<dyn HostMetricStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl HostTelemetrySink for HostMetricPipeline {
    async fn ingest(&self, sample: &HostTelemetrySample) -> Result<(), HostTelemetrySinkError> {
        self.store
            .append_host_metrics(&[normalize(sample)])
            .await
            .map(|_report| ())
            .map_err(map_store_error)
    }
}

fn normalize(sample: &HostTelemetrySample) -> HostMetricPoint {
    HostMetricPoint {
        id: HostMetricRecordId {
            cluster_id: sample.cluster_id.clone(),
            node_id: sample.node_id.clone(),
            collected_at: sample.collected_at,
        },
        resources: sample.resources.map(|resources| HostResourceMetricPoint {
            cpu_total_ticks: resources.cpu.total_ticks,
            cpu_idle_ticks: resources.cpu.idle_ticks,
            memory_used_bytes: resources.memory.used_bytes,
            memory_total_bytes: resources.memory.total_bytes,
            network_receive_bytes: resources.network.receive_bytes,
            network_transmit_bytes: resources.network.transmit_bytes,
        }),
        disks: sample.disks.as_ref().map(|disks| {
            disks
                .iter()
                .map(|disk| HostDiskMetricPoint {
                    name: disk.name.clone(),
                    mount_point: disk.mount_point.clone(),
                    total_bytes: disk.total_bytes,
                    available_bytes: disk.available_bytes,
                    file_system: disk.file_system.clone(),
                })
                .collect()
        }),
    }
}

fn map_store_error(error: MetricStoreError) -> HostTelemetrySinkError {
    match error {
        MetricStoreError::Rejected { message } => HostTelemetrySinkError::Rejected { message },
        MetricStoreError::Unavailable { message } => {
            HostTelemetrySinkError::Unavailable { message }
        }
    }
}
