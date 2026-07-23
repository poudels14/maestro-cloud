use std::io::Write;
use std::time::Duration;

use clap::{Args, ValueEnum};
use kernel_api::{DeploymentId, ServiceId};
use logs::{ClusterLogEntry, ClusterLogPage, IngestLogEntry, LogBody, LogOrigin};

use crate::CliError;
use crate::api_client::ApiClient;
use crate::contexts::ContextStore;

const DEFAULT_TAIL: usize = 100;
const FOLLOW_BATCH: usize = 500;
const POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Args)]
pub(crate) struct LogCommand {
    /// Service identity to stream; omit for every node-local log.
    #[arg(short = 's', long, conflicts_with = "system")]
    pub(crate) service: Option<String>,
    /// Deployment identity within --service.
    #[arg(long, requires = "service", conflicts_with = "system")]
    pub(crate) deployment: Option<String>,
    /// Only stream one exact Maestro system component.
    #[arg(long, value_name = "COMPONENT", conflicts_with = "service")]
    pub(crate) system: Option<String>,
    /// Number of recent records in the initial page.
    #[arg(long, default_value_t = DEFAULT_TAIL)]
    pub(crate) tail: usize,
    /// Print the recent page and exit instead of polling for new records.
    #[arg(long)]
    pub(crate) no_follow: bool,
    /// Compatibility flag; all-log queries already include system records.
    #[arg(long = "include-system")]
    pub(crate) _include_system: bool,
    /// Server-side LogQL expression.
    #[arg(long)]
    pub(crate) query: Option<String>,
    /// Inclusive event-time lower bound in Unix milliseconds.
    #[arg(long)]
    pub(crate) from: Option<i64>,
    /// Exclusive event-time upper bound in Unix milliseconds.
    #[arg(long)]
    pub(crate) to: Option<i64>,
    /// Render text lines or one complete normalized JSON object per line.
    #[arg(long, value_enum, default_value_t = LogOutput::Text)]
    pub(crate) output: LogOutput,
    /// Compatibility flag; rewritten JSON output is always lossless.
    #[arg(long)]
    pub(crate) full: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub(crate) enum LogOutput {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogRequest {
    pub(crate) path: String,
    pub(crate) parameters: Vec<(String, String)>,
}

pub(crate) async fn run(command: LogCommand, output: &mut dyn Write) -> Result<(), CliError> {
    let mut request = initial_request(&command)?;
    let contexts = ContextStore::from_environment()?;
    let client = ApiClient::new(contexts.active()?)?;
    let mut page: ClusterLogPage = client.get_query(&request.path, &request.parameters).await?;
    page.entries.reverse();
    write_entries(&page.entries, command.output, output)?;
    let mut cursor = page.cursor;
    if command.no_follow {
        return Ok(());
    }

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        request
            .parameters
            .retain(|(name, _)| name != "tail" && name != "cursor");
        request
            .parameters
            .push(("tail".to_owned(), FOLLOW_BATCH.to_string()));
        request
            .parameters
            .push(("cursor".to_owned(), encode_cursor(&cursor)?));
        let page: ClusterLogPage = client.get_query(&request.path, &request.parameters).await?;
        cursor = page.cursor;
        write_entries(&page.entries, command.output, output)?;
        output
            .flush()
            .map_err(|source| CliError::io("failed to flush log output", source))?;
    }
}

pub(crate) fn initial_request(command: &LogCommand) -> Result<LogRequest, CliError> {
    if command.full && command.output != LogOutput::Json {
        return Err(CliError::invalid_input("--full requires --output json"));
    }
    if command.tail == 0 || command.tail > logs::MAXIMUM_LOG_QUERY_LIMIT {
        return Err(CliError::invalid_input(
            "log tail must be between 1 and 10000",
        ));
    }
    if command
        .from
        .zip(command.to)
        .is_some_and(|(from, to)| from >= to)
    {
        return Err(CliError::invalid_input(
            "log start must be earlier than its end",
        ));
    }
    let path = match (&command.service, &command.deployment, &command.system) {
        (Some(service), deployment, None) => {
            let service = ServiceId::new(service)
                .map_err(|error| CliError::invalid_input(error.to_string()))?;
            match deployment {
                Some(deployment) => {
                    let deployment = DeploymentId::new(deployment)
                        .map_err(|error| CliError::invalid_input(error.to_string()))?;
                    format!("/api/services/{service}/deployments/{deployment}/logs")
                }
                None => format!("/api/services/{service}/logs"),
            }
        }
        (None, None, Some(_)) => "/api/system/logs".to_owned(),
        (None, None, None) => "/api/logs".to_owned(),
        _ => {
            return Err(CliError::invalid_input(
                "log target must be all logs, one service/deployment, or one system component",
            ));
        }
    };
    let mut parameters = vec![("tail".to_owned(), command.tail.to_string())];
    if let Some(component) = &command.system {
        if component.is_empty() || component.len() > 128 {
            return Err(CliError::invalid_input(
                "system component must contain 1-128 bytes",
            ));
        }
        parameters.push(("component".to_owned(), component.clone()));
    }
    if let Some(query) = &command.query {
        query
            .parse::<logs::LogQuery>()
            .map_err(|error| CliError::invalid_input(error.to_string()))?;
        parameters.push(("query".to_owned(), query.clone()));
    }
    if let Some(from) = command.from {
        parameters.push(("from".to_owned(), from.to_string()));
    }
    if let Some(to) = command.to {
        parameters.push(("to".to_owned(), to.to_string()));
    }
    Ok(LogRequest { path, parameters })
}

pub(crate) fn write_entries(
    entries: &[ClusterLogEntry],
    format: LogOutput,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    for stored in entries {
        match format {
            LogOutput::Text => writeln!(
                output,
                "{} {:<5} {:<32} {}",
                stored.entry.event_at.0,
                stored.entry.severity,
                format!("{}/{}", stored.node_id, source(&stored.entry)),
                body(&stored.entry.body),
            )
            .map_err(output_error)?,
            LogOutput::Json => {
                serde_json::to_writer(&mut *output, stored)
                    .map_err(|source| CliError::json("failed to encode log output", source))?;
                writeln!(output).map_err(output_error)?;
            }
        }
    }
    Ok(())
}

fn encode_cursor(cursor: &logs::ClusterLogCursor) -> Result<String, CliError> {
    serde_json::to_string(cursor)
        .map_err(|source| CliError::json("failed to encode cluster log cursor", source))
}

fn source(entry: &IngestLogEntry) -> String {
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

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write log output", source)
}
