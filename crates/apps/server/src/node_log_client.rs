use std::sync::Arc;

use crate::node_http_client::{NodeHttpClient, NodeHttpClientError, NodeHttpRequestError};
use async_trait::async_trait;
use kernel_api::{NodeId, SecretValue};
use logs::{
    LogHistogramBucket, LogHistogramGroupBy, LogHistogramQuery, LogQueryScope, LogQueryStore,
    LogQueryStoreError, LogReadCursor, LogReadOrder, LogReadQuery, NodeLogQueryStore,
    SequencedLogEntry,
};

use crate::TlsIdentity;

/// HTTPS node-local query proxy using cluster trust and one node certificate.
pub struct HttpNodeLogQueryStore {
    local: Arc<dyn LogQueryStore>,
    transport: NodeHttpClient,
}

impl HttpNodeLogQueryStore {
    /// Builds a bounded mutual-TLS client over one validated static topology.
    pub fn new(
        local_node_id: NodeId,
        endpoints: std::collections::BTreeMap<NodeId, std::net::SocketAddr>,
        trust_root_pem: &str,
        identity: &TlsIdentity,
        jwt_secret_key: &SecretValue,
        local: Arc<dyn LogQueryStore>,
    ) -> Result<Self, NodeHttpClientError> {
        Ok(Self {
            local,
            transport: NodeHttpClient::new(
                local_node_id,
                endpoints,
                trust_root_pem,
                identity,
                jwt_secret_key,
            )?,
        })
    }
}

#[async_trait]
impl NodeLogQueryStore for HttpNodeLogQueryStore {
    async fn query_node_logs(
        &self,
        node_id: &NodeId,
        query: &LogReadQuery,
    ) -> Result<Vec<SequencedLogEntry>, LogQueryStoreError> {
        if node_id == self.transport.local_node_id() {
            return self.local.query_logs(query).await;
        }
        self.transport
            .get_json(node_id, "/api/node/logs", &read_parameters(query))
            .await
            .map_err(map_request_error)
    }

    async fn query_node_histogram(
        &self,
        node_id: &NodeId,
        query: &LogHistogramQuery,
    ) -> Result<Vec<LogHistogramBucket>, LogQueryStoreError> {
        if node_id == self.transport.local_node_id() {
            return self.local.query_log_histogram(query).await;
        }
        self.transport
            .get_json(
                node_id,
                "/api/node/logs/histogram",
                &histogram_parameters(query),
            )
            .await
            .map_err(map_request_error)
    }
}

fn read_parameters(query: &LogReadQuery) -> Vec<(String, String)> {
    let mut parameters = scope_parameters(query.scope());
    parameters.push(("tail".to_owned(), query.limit().to_string()));
    parameters.push((
        "order".to_owned(),
        match query.order() {
            LogReadOrder::OldestFirst => "oldest",
            LogReadOrder::NewestFirst => "newest",
        }
        .to_owned(),
    ));
    if let Some(cursor) = query.cursor() {
        let (name, sequence) = match cursor {
            LogReadCursor::After(sequence) => ("after", sequence),
            LogReadCursor::Before(sequence) => ("before", sequence),
        };
        parameters.push((name.to_owned(), sequence.0.to_string()));
    }
    push_common_parameters(&mut parameters, query.search(), query.from(), query.to());
    parameters
}

fn histogram_parameters(query: &LogHistogramQuery) -> Vec<(String, String)> {
    let mut parameters = scope_parameters(query.scope());
    parameters.extend([
        ("from".to_owned(), query.from().0.to_string()),
        ("to".to_owned(), query.to().0.to_string()),
        ("bucketMs".to_owned(), query.bucket_ms().to_string()),
        (
            "groupBy".to_owned(),
            match query.group_by() {
                LogHistogramGroupBy::Level => "level",
                LogHistogramGroupBy::HttpStatusClass => "status",
            }
            .to_owned(),
        ),
    ]);
    if let Some(search) = query.search() {
        parameters.push(("query".to_owned(), search.as_str().to_owned()));
    }
    parameters
}

fn scope_parameters(scope: &LogQueryScope) -> Vec<(String, String)> {
    let (scope, scope_id) = match scope {
        LogQueryScope::All => ("all", None),
        LogQueryScope::Service(service_id) => ("service", Some(service_id.as_str())),
        LogQueryScope::Deployment(deployment_id) => ("deployment", Some(deployment_id.as_str())),
        LogQueryScope::System => ("system", None),
        LogQueryScope::SystemComponent(component) => ("systemComponent", Some(component.as_str())),
        LogQueryScope::Build(build_id) => ("build", Some(build_id.as_str())),
    };
    let mut parameters = vec![("scope".to_owned(), scope.to_owned())];
    if let Some(scope_id) = scope_id {
        parameters.push(("scopeId".to_owned(), scope_id.to_owned()));
    }
    parameters
}

fn push_common_parameters(
    parameters: &mut Vec<(String, String)>,
    search: Option<&logs::LogQuery>,
    from: Option<kernel_api::Timestamp>,
    to: Option<kernel_api::Timestamp>,
) {
    if let Some(search) = search {
        parameters.push(("query".to_owned(), search.as_str().to_owned()));
    }
    if let Some(from) = from {
        parameters.push(("from".to_owned(), from.0.to_string()));
    }
    if let Some(to) = to {
        parameters.push(("to".to_owned(), to.0.to_string()));
    }
}

fn unavailable(message: impl Into<String>) -> LogQueryStoreError {
    LogQueryStoreError::Unavailable {
        message: message.into(),
    }
}

fn map_request_error(error: NodeHttpRequestError) -> LogQueryStoreError {
    match error {
        NodeHttpRequestError::Rejected { message } => LogQueryStoreError::Rejected { message },
        NodeHttpRequestError::Unavailable { message } => unavailable(message),
    }
}
