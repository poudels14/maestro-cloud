use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::ClusterId;
use node_agent::{NodeMetricHandler, NodeTraceHandler, StatusClock};
use node_fabric::WorkloadClaims;
use node_fabric::otlp::{metrics, traces};
use prost::Message;
use runtime::WorkloadMetadata;
use tonic::Status;

use crate::{
    OtlpEnvelope, OtlpEnvelopeStore, OtlpEnvelopeStoreError, OtlpEnvelopeValidationError,
    OtlpSignal,
};

/// Authenticated metric and trace receiver backed by a lossless durable spool.
pub struct OtlpSignalHandler {
    cluster_id: ClusterId,
    store: Arc<dyn OtlpEnvelopeStore>,
    clock: Arc<dyn StatusClock>,
}

impl OtlpSignalHandler {
    /// Binds authenticated workload ownership to one node-local OTLP spool.
    pub fn new(
        cluster_id: ClusterId,
        store: Arc<dyn OtlpEnvelopeStore>,
        clock: Arc<dyn StatusClock>,
    ) -> Self {
        Self {
            cluster_id,
            store,
            clock,
        }
    }

    async fn append(
        &self,
        signal: OtlpSignal,
        claims: WorkloadClaims,
        payload: Vec<u8>,
    ) -> Result<(), Status> {
        let metadata = WorkloadMetadata {
            cluster_id: self.cluster_id.clone(),
            node_id: claims.node_id,
            service_id: claims.service_id,
            deployment_id: claims.deployment_id,
            assignment_id: claims.assignment_id,
            workload_id: claims.workload_id,
            labels: claims.labels,
        };
        let envelope = OtlpEnvelope::new(signal, metadata, self.clock.now(), payload)
            .map_err(validation_status)?;
        self.store
            .append_otlp_envelopes(&[envelope])
            .await
            .map_err(store_status)?;
        Ok(())
    }
}

#[async_trait]
impl NodeMetricHandler for OtlpSignalHandler {
    async fn export_metrics(
        &self,
        claims: WorkloadClaims,
        request: metrics::ExportMetricsServiceRequest,
    ) -> Result<metrics::ExportMetricsServiceResponse, Status> {
        if !request.resource_metrics.is_empty() {
            self.append(OtlpSignal::Metrics, claims, request.encode_to_vec())
                .await?;
        }
        Ok(metrics::ExportMetricsServiceResponse {
            partial_success: None,
        })
    }
}

#[async_trait]
impl NodeTraceHandler for OtlpSignalHandler {
    async fn export_traces(
        &self,
        claims: WorkloadClaims,
        request: traces::ExportTraceServiceRequest,
    ) -> Result<traces::ExportTraceServiceResponse, Status> {
        if !request.resource_spans.is_empty() {
            self.append(OtlpSignal::Traces, claims, request.encode_to_vec())
                .await?;
        }
        Ok(traces::ExportTraceServiceResponse {
            partial_success: None,
        })
    }
}

fn validation_status(error: OtlpEnvelopeValidationError) -> Status {
    match error {
        OtlpEnvelopeValidationError::PayloadTooLarge => {
            Status::resource_exhausted(error.to_string())
        }
        OtlpEnvelopeValidationError::Ownership | OtlpEnvelopeValidationError::Digest => {
            Status::invalid_argument(error.to_string())
        }
    }
}

fn store_status(error: OtlpEnvelopeStoreError) -> Status {
    match error {
        OtlpEnvelopeStoreError::Rejected { message } => Status::invalid_argument(message),
        OtlpEnvelopeStoreError::Unavailable { message } => Status::unavailable(message),
    }
}
