use std::collections::BTreeMap;
use std::sync::Arc;

use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use node_agent::{NodeLogHandler, StatusClock};
use node_fabric::WorkloadClaims;
use node_fabric::otlp::{common, log_data, logs};

use crate::{InMemoryLogStore, LogBody, LogOrigin, LogStream, OtlpLogHandler};

#[tokio::test]
async fn otlp_handler_uses_authenticated_ownership_and_deduplicates_request_retries()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogStore::new());
    let handler = OtlpLogHandler::new(
        ClusterId::new("cluster-1")?,
        store.clone(),
        Arc::new(FixedClock),
    );
    let request = request();

    handler.export_logs(claims(), request.clone()).await?;
    handler.export_logs(claims(), request).await?;

    let entries = store.entries()?;
    assert_eq!(entries.len(), 2);
    let first = entries.first().ok_or("first OTLP record missing")?;
    assert_eq!(first.stream, LogStream::Otlp);
    assert_eq!(first.event_at, Timestamp(1_750_000_000_123));
    assert_eq!(first.severity, "warn");
    assert_eq!(first.body, LogBody::Text("slow".to_owned()));
    assert_eq!(
        first.attributes.get("resource.service.name"),
        Some(&"untrusted-name".to_owned())
    );
    assert_eq!(
        first.attributes.get("scope.name"),
        Some(&"test-library".to_owned())
    );
    let LogOrigin::Workload { metadata } = &first.origin else {
        return Err("OTLP record did not retain workload ownership".into());
    };
    assert_eq!(metadata.service_id, ServiceId::new("api")?);
    assert_eq!(metadata.deployment_id, DeploymentId::new("deployment-1")?);
    assert_eq!(metadata.labels.get("environment"), Some(&"test".to_owned()));
    Ok(())
}

struct FixedClock;

impl StatusClock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp(1_750_000_000_999)
    }
}

fn claims() -> WorkloadClaims {
    WorkloadClaims {
        workload_id: WorkloadId::new("workload-1").unwrap(),
        assignment_id: AssignmentId::new("assignment-1").unwrap(),
        node_id: NodeId::new("node-1").unwrap(),
        service_id: ServiceId::new("api").unwrap(),
        deployment_id: DeploymentId::new("deployment-1").unwrap(),
        labels: BTreeMap::from([("environment".to_owned(), "test".to_owned())]),
    }
}

fn request() -> logs::ExportLogsServiceRequest {
    logs::ExportLogsServiceRequest {
        resource_logs: vec![log_data::ResourceLogs {
            resource: Some(node_fabric_resource()),
            scope_logs: vec![log_data::ScopeLogs {
                scope: Some(common::InstrumentationScope {
                    name: "test-library".to_owned(),
                    ..Default::default()
                }),
                log_records: vec![
                    log_data::LogRecord {
                        time_unix_nano: 1_750_000_000_123_000_000,
                        severity_number: log_data::SeverityNumber::Warn.into(),
                        body: Some(any_string("slow")),
                        attributes: vec![key_value("attempt", any_int(2))],
                        ..Default::default()
                    },
                    log_data::LogRecord {
                        observed_time_unix_nano: 1_750_000_000_456_000_000,
                        body: Some(common::AnyValue {
                            value: Some(common::any_value::Value::BytesValue(vec![0xff])),
                        }),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn node_fabric_resource() -> node_fabric::otlp::resource::Resource {
    node_fabric::otlp::resource::Resource {
        attributes: vec![key_value("service.name", any_string("untrusted-name"))],
        ..Default::default()
    }
}

fn key_value(key: &str, value: common::AnyValue) -> common::KeyValue {
    common::KeyValue {
        key: key.to_owned(),
        value: Some(value),
        ..Default::default()
    }
}

fn any_string(value: &str) -> common::AnyValue {
    common::AnyValue {
        value: Some(common::any_value::Value::StringValue(value.to_owned())),
    }
}

fn any_int(value: i64) -> common::AnyValue {
    common::AnyValue {
        value: Some(common::any_value::Value::IntValue(value)),
    }
}
