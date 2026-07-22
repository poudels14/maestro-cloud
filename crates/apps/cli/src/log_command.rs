use std::io::Write;
use std::time::Duration;

use clap::{Args, ValueEnum};
use kernel_api::{DeploymentId, ServiceId};
use logs::{IngestLogEntry, LogBody, LogOrigin, SequencedLogEntry};

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
    service: Option<String>,
    /// Deployment identity within --service.
    #[arg(long, requires = "service", conflicts_with = "system")]
    deployment: Option<String>,
    /// Only stream one exact Maestro system component.
    #[arg(long, value_name = "COMPONENT", conflicts_with = "service")]
    system: Option<String>,
    /// Number of recent records in the initial page.
    #[arg(long, default_value_t = DEFAULT_TAIL)]
    tail: usize,
    /// Print the recent page and exit instead of polling for new records.
    #[arg(long)]
    no_follow: bool,
    /// Server-side LogQL expression.
    #[arg(long)]
    query: Option<String>,
    /// Inclusive event-time lower bound in Unix milliseconds.
    #[arg(long)]
    from: Option<i64>,
    /// Exclusive event-time upper bound in Unix milliseconds.
    #[arg(long)]
    to: Option<i64>,
    /// Render text lines or one complete normalized JSON object per line.
    #[arg(long, value_enum, default_value_t = LogOutput::Text)]
    output: LogOutput,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
enum LogOutput {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogRequest {
    path: String,
    parameters: Vec<(String, String)>,
}

pub(crate) async fn run(command: LogCommand, output: &mut dyn Write) -> Result<(), CliError> {
    let contexts = ContextStore::from_environment()?;
    let client = ApiClient::new(contexts.active()?)?;
    let mut request = initial_request(&command)?;
    let mut entries: Vec<SequencedLogEntry> =
        client.get_query(&request.path, &request.parameters).await?;
    entries.reverse();
    let mut cursor = entries
        .iter()
        .map(|entry| entry.sequence)
        .max()
        .unwrap_or(logs::LogSequence(0));
    write_entries(&entries, command.output, output)?;
    if command.no_follow {
        return Ok(());
    }

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        request
            .parameters
            .retain(|(name, _)| name != "tail" && name != "after");
        request
            .parameters
            .push(("tail".to_owned(), FOLLOW_BATCH.to_string()));
        request
            .parameters
            .push(("after".to_owned(), cursor.0.to_string()));
        let entries: Vec<SequencedLogEntry> =
            client.get_query(&request.path, &request.parameters).await?;
        if let Some(latest) = entries.iter().map(|entry| entry.sequence).max() {
            cursor = cursor.max(latest);
        }
        write_entries(&entries, command.output, output)?;
        output
            .flush()
            .map_err(|source| CliError::io("failed to flush log output", source))?;
    }
}

fn initial_request(command: &LogCommand) -> Result<LogRequest, CliError> {
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

fn write_entries(
    entries: &[SequencedLogEntry],
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
                source(&stored.entry),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use kernel_api::{ClusterId, NodeId, Timestamp};
    use logs::{LogProducer, LogRecordId, LogSequence, LogStream, OriginCursor};

    use super::*;

    #[test]
    fn requests_validate_targets_and_preserve_query_values_for_url_encoding()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = initial_request(&LogCommand {
            service: Some("api".to_owned()),
            deployment: Some("api-v1".to_owned()),
            system: None,
            tail: 25,
            no_follow: true,
            query: Some("message:\"x & y\"".to_owned()),
            from: Some(100),
            to: Some(200),
            output: LogOutput::Json,
        })?;
        assert_eq!(request.path, "/api/services/api/deployments/api-v1/logs");
        assert!(
            request
                .parameters
                .contains(&("query".to_owned(), "message:\"x & y\"".to_owned()))
        );
        assert!(
            request
                .parameters
                .contains(&("tail".to_owned(), "25".to_owned()))
        );
        Ok(())
    }

    #[test]
    fn text_and_json_outputs_keep_chronology_and_structured_details()
    -> Result<(), Box<dyn std::error::Error>> {
        let entry = SequencedLogEntry {
            sequence: LogSequence(7),
            entry: IngestLogEntry {
                id: LogRecordId {
                    node_id: NodeId::new("node-one")?,
                    producer: LogProducer::System("daemon".to_owned()),
                    cursor: OriginCursor::new("7"),
                },
                observed_at: Timestamp(123),
                event_at: Timestamp(123),
                severity: "warn".to_owned(),
                stream: LogStream::System,
                origin: LogOrigin::System {
                    cluster_id: ClusterId::new("cluster-one")?,
                    node_id: Some(NodeId::new("node-one")?),
                    component: "daemon".to_owned(),
                },
                body: LogBody::Text("careful".to_owned()),
                attributes: BTreeMap::from([("attempt".to_owned(), "2".to_owned())]),
            },
        };
        let mut text = Vec::new();
        write_entries(std::slice::from_ref(&entry), LogOutput::Text, &mut text)?;
        assert_eq!(
            String::from_utf8(text)?.trim(),
            "123 warn  daemon                           careful"
        );

        let mut json = Vec::new();
        write_entries(&[entry], LogOutput::Json, &mut json)?;
        let value: serde_json::Value = serde_json::from_slice(&json)?;
        assert_eq!(value.pointer("/sequence"), Some(&serde_json::json!(7)));
        assert_eq!(
            value.pointer("/entry/attributes/attempt"),
            Some(&serde_json::json!("2"))
        );
        Ok(())
    }
}
