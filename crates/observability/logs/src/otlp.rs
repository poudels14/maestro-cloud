use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::{ClusterId, Timestamp};
use node_agent::{NodeLogHandler, StatusClock};
use node_fabric::WorkloadClaims;
use node_fabric::otlp::{common, log_data, logs};
use prost::Message;
use runtime::WorkloadMetadata;
use sha2::{Digest, Sha256};
use tonic::Status;

use crate::{
    IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStore, LogStoreError,
    LogStream, OriginCursor,
};

/// Authenticated OTLP log receiver writing the normalized log-store contract.
pub struct OtlpLogHandler {
    cluster_id: ClusterId,
    store: Arc<dyn LogStore>,
    clock: Arc<dyn StatusClock>,
}

impl OtlpLogHandler {
    /// Binds cluster ownership, durable storage, and an injected wall clock.
    pub fn new(
        cluster_id: ClusterId,
        store: Arc<dyn LogStore>,
        clock: Arc<dyn StatusClock>,
    ) -> Self {
        Self {
            cluster_id,
            store,
            clock,
        }
    }
}

#[async_trait]
impl NodeLogHandler for OtlpLogHandler {
    async fn export_logs(
        &self,
        claims: WorkloadClaims,
        request: logs::ExportLogsServiceRequest,
    ) -> Result<logs::ExportLogsServiceResponse, Status> {
        let request_digest = digest(&request.encode_to_vec());
        let observed_at = self.clock.now();
        let metadata = WorkloadMetadata {
            cluster_id: self.cluster_id.clone(),
            node_id: claims.node_id,
            service_id: claims.service_id,
            deployment_id: claims.deployment_id,
            assignment_id: claims.assignment_id,
            workload_id: claims.workload_id,
            labels: claims.labels,
        };
        let mut entries = Vec::new();
        for resource_logs in request.resource_logs {
            let resource_attributes = resource_logs
                .resource
                .map(|resource| prefixed_attributes("resource", resource.attributes))
                .unwrap_or_default();
            for scope_logs in resource_logs.scope_logs {
                let mut inherited = resource_attributes.clone();
                if let Some(scope) = scope_logs.scope {
                    inherited.extend(prefixed_attributes("scope", scope.attributes));
                    if !scope.name.is_empty() {
                        inherited.insert("scope.name".to_owned(), scope.name);
                    }
                    if !scope.version.is_empty() {
                        inherited.insert("scope.version".to_owned(), scope.version);
                    }
                }
                for record in scope_logs.log_records {
                    let sequence = entries.len();
                    entries.push(normalize_record(
                        &metadata,
                        &request_digest,
                        sequence,
                        observed_at,
                        inherited.clone(),
                        record,
                    ));
                }
            }
        }
        if !entries.is_empty() {
            self.store.append(&entries).await.map_err(store_status)?;
        }
        Ok(logs::ExportLogsServiceResponse {
            partial_success: None,
        })
    }
}

fn normalize_record(
    metadata: &WorkloadMetadata,
    request_digest: &str,
    sequence: usize,
    observed_at: Timestamp,
    mut attributes: BTreeMap<String, String>,
    record: log_data::LogRecord,
) -> IngestLogEntry {
    attributes.extend(key_values(record.attributes));
    if !record.trace_id.is_empty() {
        attributes.insert("trace_id".to_owned(), hex(&record.trace_id));
    }
    if !record.span_id.is_empty() {
        attributes.insert("span_id".to_owned(), hex(&record.span_id));
    }
    let event_at = timestamp_millis(record.time_unix_nano)
        .or_else(|| timestamp_millis(record.observed_time_unix_nano))
        .unwrap_or(observed_at);
    IngestLogEntry {
        id: LogRecordId {
            node_id: metadata.node_id.clone(),
            producer: LogProducer::Workload(metadata.workload_id.clone()),
            cursor: OriginCursor::new(format!("{request_digest}:{sequence}")),
        },
        observed_at,
        event_at,
        severity: severity(record.severity_text, record.severity_number),
        stream: LogStream::Otlp,
        origin: LogOrigin::Workload {
            metadata: metadata.clone(),
        },
        body: record
            .body
            .map_or_else(|| LogBody::Text(String::new()), |body| any_body(body.value)),
        attributes,
    }
}

fn severity(text: String, number: i32) -> String {
    if !text.is_empty() {
        text
    } else {
        log_data::SeverityNumber::try_from(number)
            .ok()
            .filter(|severity| *severity != log_data::SeverityNumber::Unspecified)
            .map_or_else(
                || "info".to_owned(),
                |severity| {
                    severity
                        .as_str_name()
                        .trim_start_matches("SEVERITY_NUMBER_")
                        .to_ascii_lowercase()
                },
            )
    }
}

fn timestamp_millis(nanoseconds: u64) -> Option<Timestamp> {
    if nanoseconds == 0 {
        None
    } else {
        Some(Timestamp(
            i64::try_from(nanoseconds / 1_000_000).unwrap_or(i64::MAX),
        ))
    }
}

fn prefixed_attributes(
    prefix: &str,
    attributes: Vec<common::KeyValue>,
) -> BTreeMap<String, String> {
    key_values(attributes)
        .into_iter()
        .map(|(key, value)| (format!("{prefix}.{key}"), value))
        .collect()
}

fn key_values(attributes: Vec<common::KeyValue>) -> BTreeMap<String, String> {
    attributes
        .into_iter()
        .filter(|attribute| !attribute.key.is_empty())
        .map(|attribute| {
            let value = attribute
                .value
                .and_then(|value| value.value)
                .map_or_else(String::new, any_text);
            (attribute.key, value)
        })
        .collect()
}

fn any_body(value: Option<common::any_value::Value>) -> LogBody {
    match value {
        Some(common::any_value::Value::StringValue(value)) => LogBody::Text(value),
        Some(common::any_value::Value::BytesValue(value)) => LogBody::Bytes(value),
        value => LogBody::Text(value.map_or_else(String::new, any_text)),
    }
}

fn any_text(value: common::any_value::Value) -> String {
    match value {
        common::any_value::Value::StringValue(value) => value,
        common::any_value::Value::BoolValue(value) => value.to_string(),
        common::any_value::Value::IntValue(value) => value.to_string(),
        common::any_value::Value::DoubleValue(value) => value.to_string(),
        common::any_value::Value::BytesValue(value) => hex(&value),
        common::any_value::Value::ArrayValue(value) => serde_json::Value::Array(
            value
                .values
                .into_iter()
                .map(|value| value.value.map_or(serde_json::Value::Null, any_json))
                .collect(),
        )
        .to_string(),
        common::any_value::Value::KvlistValue(value) => serde_json::Value::Object(
            value
                .values
                .into_iter()
                .filter(|entry| !entry.key.is_empty())
                .map(|entry| {
                    let value = entry
                        .value
                        .and_then(|value| value.value)
                        .map_or(serde_json::Value::Null, any_json);
                    (entry.key, value)
                })
                .collect(),
        )
        .to_string(),
        common::any_value::Value::StringValueStrindex(_) => String::new(),
    }
}

fn any_json(value: common::any_value::Value) -> serde_json::Value {
    match value {
        common::any_value::Value::StringValue(value) => serde_json::Value::String(value),
        common::any_value::Value::BoolValue(value) => serde_json::Value::Bool(value),
        common::any_value::Value::IntValue(value) => serde_json::Value::Number(value.into()),
        common::any_value::Value::DoubleValue(value) => serde_json::Number::from_f64(value)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        common::any_value::Value::BytesValue(value) => serde_json::Value::String(hex(&value)),
        common::any_value::Value::ArrayValue(value) => serde_json::Value::Array(
            value
                .values
                .into_iter()
                .map(|value| value.value.map_or(serde_json::Value::Null, any_json))
                .collect(),
        ),
        common::any_value::Value::KvlistValue(value) => serde_json::Value::Object(
            value
                .values
                .into_iter()
                .filter(|entry| !entry.key.is_empty())
                .map(|entry| {
                    let value = entry
                        .value
                        .and_then(|value| value.value)
                        .map_or(serde_json::Value::Null, any_json);
                    (entry.key, value)
                })
                .collect(),
        ),
        common::any_value::Value::StringValueStrindex(_) => serde_json::Value::Null,
    }
}

fn digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex(&digest)
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .concat()
}

fn store_status(error: LogStoreError) -> Status {
    match error {
        LogStoreError::Rejected { message } => Status::invalid_argument(message),
        LogStoreError::Unavailable { message } => Status::unavailable(message),
    }
}
