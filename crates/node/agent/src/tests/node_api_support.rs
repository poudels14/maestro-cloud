use std::sync::Arc;

use async_trait::async_trait;
use node_fabric::WorkloadClaims;
use node_fabric::otlp::{logs, metrics, traces};
use node_fabric::proto::{MutationResult, ResourceMutation};
use tonic::Status;

use crate::{NodeApiServices, NodeControlHandler, NodeTelemetryHandler};

#[derive(Debug, Default)]
struct NoopHandlers;

#[async_trait]
impl NodeControlHandler for NoopHandlers {
    async fn mutate(
        &self,
        _claims: WorkloadClaims,
        _mutation: ResourceMutation,
    ) -> Result<MutationResult, Status> {
        Ok(MutationResult::default())
    }
}

#[async_trait]
impl NodeTelemetryHandler for NoopHandlers {
    async fn export_logs(
        &self,
        _claims: WorkloadClaims,
        _request: logs::ExportLogsServiceRequest,
    ) -> Result<logs::ExportLogsServiceResponse, Status> {
        Ok(logs::ExportLogsServiceResponse::default())
    }

    async fn export_metrics(
        &self,
        _claims: WorkloadClaims,
        _request: metrics::ExportMetricsServiceRequest,
    ) -> Result<metrics::ExportMetricsServiceResponse, Status> {
        Ok(metrics::ExportMetricsServiceResponse::default())
    }

    async fn export_traces(
        &self,
        _claims: WorkloadClaims,
        _request: traces::ExportTraceServiceRequest,
    ) -> Result<traces::ExportTraceServiceResponse, Status> {
        Ok(traces::ExportTraceServiceResponse::default())
    }
}

pub(super) fn node_api_services() -> NodeApiServices {
    let handlers = Arc::new(NoopHandlers);
    NodeApiServices::new(handlers.clone(), handlers)
}
