use async_trait::async_trait;
use logs::{
    IngressTrafficBreakdown, IngressTrafficQuery, ServiceTrafficQuery, TrafficMetricPoint,
    TrafficQueryError, TrafficQueryStore,
};
use tokio::sync::oneshot;

use crate::duck::{Command, DuckLogStore};

#[async_trait]
impl TrafficQueryStore for DuckLogStore {
    async fn query_ingress_traffic(
        &self,
        query: &IngressTrafficQuery,
    ) -> Result<IngressTrafficBreakdown, TrafficQueryError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::QueryIngressTraffic {
                query: query.clone(),
                response,
            })
            .await
            .map_err(|_| worker_stopped("accepting ingress traffic query"))?;
        result
            .await
            .map_err(|_| worker_stopped("completing ingress traffic query"))?
    }

    async fn query_service_traffic(
        &self,
        query: &ServiceTrafficQuery,
    ) -> Result<Vec<TrafficMetricPoint>, TrafficQueryError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::QueryServiceTraffic {
                query: query.clone(),
                response,
            })
            .await
            .map_err(|_| worker_stopped("accepting service traffic query"))?;
        result
            .await
            .map_err(|_| worker_stopped("completing service traffic query"))?
    }
}

fn worker_stopped(action: &'static str) -> TrafficQueryError {
    TrafficQueryError::Unavailable {
        message: format!("DuckDB worker stopped before {action}"),
    }
}
