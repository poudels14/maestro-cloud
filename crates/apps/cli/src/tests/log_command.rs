use std::collections::BTreeMap;

use clap::Parser;
use kernel_api::{ClusterId, NodeId, Timestamp};
use logs::{
    ClusterLogEntry, IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogSequence,
    LogStream, OriginCursor,
};

use crate::Cli;
use crate::log_command::{LogCommand, LogOutput, initial_request, write_entries};

#[test]
fn log_command_surface_matches_the_harvested_cli() {
    assert!(Cli::try_parse_from(["maestro-next", "logs", "--no-follow"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro-next",
            "logs",
            "--service",
            "api",
            "--deployment",
            "api-v1",
            "--query",
            "level:error",
            "--tail",
            "250",
            "--output",
            "json",
            "--no-follow",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro-next",
            "logs",
            "--system",
            "daemon",
            "--service",
            "api",
        ])
        .is_err()
    );
    assert!(Cli::try_parse_from(["maestro-next", "logs", "--deployment", "api-v1",]).is_err());
}

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
    let node_id = NodeId::new("node-one")?;
    let entry = ClusterLogEntry {
        node_id: node_id.clone(),
        sequence: LogSequence(7),
        entry: IngestLogEntry {
            id: LogRecordId {
                node_id: node_id.clone(),
                producer: LogProducer::System("daemon".to_owned()),
                cursor: OriginCursor::new("7"),
            },
            observed_at: Timestamp(123),
            event_at: Timestamp(123),
            severity: "warn".to_owned(),
            stream: LogStream::System,
            origin: LogOrigin::System {
                cluster_id: ClusterId::new("cluster-one")?,
                node_id: Some(node_id),
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
        "123 warn  node-one/daemon                  careful"
    );

    let mut json = Vec::new();
    write_entries(&[entry], LogOutput::Json, &mut json)?;
    let value: serde_json::Value = serde_json::from_slice(&json)?;
    assert_eq!(
        value.pointer("/nodeId"),
        Some(&serde_json::json!("node-one"))
    );
    assert_eq!(value.pointer("/sequence"), Some(&serde_json::json!(7)));
    assert_eq!(
        value.pointer("/entry/attributes/attempt"),
        Some(&serde_json::json!("2"))
    );
    Ok(())
}
