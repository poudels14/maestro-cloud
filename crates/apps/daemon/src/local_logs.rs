use std::collections::BTreeMap;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use kernel_api::{DeploymentId, NodeId, ServiceId, WorkloadId};
use logs::{
    IngestLogEntry, LogBody, LogOrigin, LogQuery, LogQueryScope, LogReadCursor, LogReadOrder,
    LogReadQuery, LogSequence, NodeLogQueryStore, SequencedLogEntry,
};
use server::{HttpNodeLogClient, TlsIdentity};

use crate::DaemonLaunchConfig;

const FOLLOW_BATCH: usize = 500;
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Bounded selection and streaming behavior for node-local forensic logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalLogOptions {
    /// Legacy source spelling: a system component or service/deployment/workload.
    pub source: Option<String>,
    /// Number of newest matching records to print initially.
    pub tail: usize,
    /// Continue polling after the initial page.
    pub follow: bool,
}

/// Queries the running local node through its authenticated node API.
pub async fn stream_local_logs(
    config: &DaemonLaunchConfig,
    options: &LocalLogOptions,
    output: &mut dyn Write,
) -> Result<(), LocalLogError> {
    config.validate()?;
    let node =
        config
            .cluster
            .nodes
            .get(&config.node_id)
            .ok_or_else(|| LocalLogError::MissingNode {
                node_id: config.node_id.clone(),
            })?;
    let endpoint = SocketAddr::new(
        IpAddr::V4(node.endpoint.host_address),
        node.endpoint.api_port,
    );
    let identity = TlsIdentity::new(
        config.security.identity.certificate_pem.clone(),
        config.security.identity.private_key_pem.clone(),
    );
    let client = HttpNodeLogClient::new(
        config.node_id.clone(),
        BTreeMap::from([(config.node_id.clone(), endpoint)]),
        &config.security.trust_root_pem,
        &identity,
        &config.operator_jwt_secret,
    )?;
    stream_from(&client, &config.node_id, options, output, POLL_INTERVAL).await
}

pub(crate) async fn stream_from(
    queries: &dyn NodeLogQueryStore,
    node_id: &NodeId,
    options: &LocalLogOptions,
    output: &mut dyn Write,
    poll_interval: Duration,
) -> Result<(), LocalLogError> {
    let target = local_log_target(options.source.as_deref())?;
    let mut entries = queries
        .query_node_logs(
            node_id,
            &target.query(LogReadOrder::NewestFirst, options.tail)?,
        )
        .await?;
    let mut cursor = newest_sequence(&entries);
    entries.reverse();
    write_entries(node_id, &entries, output)?;

    if !options.follow {
        return Ok(());
    }
    loop {
        tokio::time::sleep(poll_interval).await;
        let query = target
            .query(LogReadOrder::OldestFirst, FOLLOW_BATCH)?
            .with_cursor(LogReadCursor::After(cursor));
        let entries = queries.query_node_logs(node_id, &query).await?;
        cursor = cursor.max(newest_sequence(&entries));
        write_entries(node_id, &entries, output)?;
        output
            .flush()
            .map_err(|source| LocalLogError::Output { source })?;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LocalLogTarget {
    pub(crate) scope: LogQueryScope,
    pub(crate) search: Option<LogQuery>,
}

impl LocalLogTarget {
    fn query(&self, order: LogReadOrder, limit: usize) -> Result<LogReadQuery, LocalLogError> {
        let mut query = LogReadQuery::new(self.scope.clone(), order, limit)?;
        if let Some(search) = &self.search {
            query = query.with_search(search.clone());
        }
        Ok(query)
    }
}

pub(crate) fn local_log_target(source: Option<&str>) -> Result<LocalLogTarget, LocalLogError> {
    let Some(source) = source else {
        return Ok(LocalLogTarget {
            scope: LogQueryScope::All,
            search: None,
        });
    };
    let parts = source.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        [component] if !component.is_empty() => Ok(LocalLogTarget {
            scope: LogQueryScope::SystemComponent((*component).to_owned()),
            search: None,
        }),
        [service, deployment, workload]
            if !service.is_empty() && !deployment.is_empty() && !workload.is_empty() =>
        {
            let service = ServiceId::new((*service).to_owned())
                .map_err(|error| invalid_source(source, error))?;
            let deployment = DeploymentId::new((*deployment).to_owned())
                .map_err(|error| invalid_source(source, error))?;
            let workload = WorkloadId::new((*workload).to_owned())
                .map_err(|error| invalid_source(source, error))?;
            let search = format!(r#"service:"{service}" AND source:"{workload}""#)
                .parse()
                .map_err(LocalLogError::Search)?;
            Ok(LocalLogTarget {
                scope: LogQueryScope::Deployment(deployment),
                search: Some(search),
            })
        }
        _ => Err(invalid_source(
            source,
            "expected a system component or service/deployment/workload",
        )),
    }
}

fn newest_sequence(entries: &[SequencedLogEntry]) -> LogSequence {
    entries
        .iter()
        .map(|entry| entry.sequence)
        .max()
        .unwrap_or(LogSequence(0))
}

fn write_entries(
    node_id: &NodeId,
    entries: &[SequencedLogEntry],
    output: &mut dyn Write,
) -> Result<(), LocalLogError> {
    for stored in entries {
        writeln!(
            output,
            "{} {:<5} {:<32} {}",
            stored.entry.event_at.0,
            stored.entry.severity,
            format!("{node_id}/{}", entry_source(&stored.entry)),
            body(&stored.entry.body),
        )
        .map_err(|source| LocalLogError::Output { source })?;
    }
    Ok(())
}

fn entry_source(entry: &IngestLogEntry) -> String {
    match &entry.origin {
        LogOrigin::Workload { metadata } => format!(
            "{}/{}/{}",
            metadata.service_id, metadata.deployment_id, metadata.workload_id
        ),
        LogOrigin::System { component, .. } => component.clone(),
        LogOrigin::Build { build_id, .. } => build_id.to_string(),
    }
}

fn body(body: &LogBody) -> String {
    match body {
        LogBody::Text(body) => body.clone(),
        LogBody::Bytes(bytes) => format!("<{} non-UTF-8 bytes>", bytes.len()),
    }
}

fn invalid_source(source: &str, detail: impl std::fmt::Display) -> LocalLogError {
    LocalLogError::InvalidSource {
        value: source.to_owned(),
        detail: detail.to_string(),
    }
}

/// A node-local forensic log query could not be constructed or completed.
#[derive(Debug, thiserror::Error)]
pub enum LocalLogError {
    /// The protected launch document is inconsistent with its topology.
    #[error("local node `{node_id}` is absent from the launch topology")]
    MissingNode { node_id: NodeId },
    /// The compatibility source spelling is malformed.
    #[error("invalid local log source `{value}`: {detail}")]
    InvalidSource { value: String, detail: String },
    /// A bounded query could not be constructed.
    #[error(transparent)]
    Query(#[from] logs::LogQueryError),
    /// The workload source filter is invalid.
    #[error("invalid local log source filter: {0}")]
    Search(logs::LogQueryParseError),
    /// The running node rejected or could not serve the query.
    #[error(transparent)]
    Store(#[from] logs::LogQueryStoreError),
    /// The authenticated node transport could not be constructed.
    #[error(transparent)]
    Client(#[from] server::NodeHttpClientError),
    /// The launch document is invalid.
    #[error(transparent)]
    Launch(#[from] crate::DaemonLaunchError),
    /// A rendered record could not be written.
    #[error("failed to write local log output")]
    Output {
        #[source]
        source: std::io::Error,
    },
}
