use std::collections::BTreeMap;

use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use runtime::{HEALTHCHECK_PATH_LABEL, WorkloadMetadata};

use crate::{
    IngestLogEntry, LogBody, LogFilterChain, LogFilterKind, LogOrigin, LogProducer, LogRecordId,
    LogStream, OriginCursor,
};

#[test]
fn tailscale_filter_drops_every_legacy_noise_prefix_only_for_tailscale_origins() {
    let filters = LogFilterChain::configured([LogFilterKind::TailscaleNoise]);
    let prefixes = [
        "magicsock: disco key changed",
        "derphttp.Client.Recv: connecting",
        "netstack: UDP session ended",
        "netmap: suggested exit node changed",
        "client -> backend close connection",
        "backend -> client close connection",
        "proxy connection closed by peer",
        "[RATELIMIT] format(",
    ];

    for body in prefixes {
        let entry = workload_entry("tailscale", body, BTreeMap::new(), BTreeMap::new());
        assert_eq!(filters.dropped_by(&entry), Some("tailscaleNoise"));
    }
    let useful = workload_entry(
        "tailscale",
        "listening on 100.64.0.1",
        BTreeMap::new(),
        BTreeMap::new(),
    );
    assert_eq!(filters.dropped_by(&useful), None);

    let unrelated = workload_entry(
        "api",
        "magicsock: user application message",
        BTreeMap::new(),
        BTreeMap::new(),
    );
    assert_eq!(filters.dropped_by(&unrelated), None);
}

#[test]
fn successful_healthcheck_filter_matches_access_aliases_and_normalized_paths() {
    let filters = LogFilterChain::configured([LogFilterKind::SuccessfulHealthcheck]);
    let entry = workload_entry(
        "api",
        "request",
        BTreeMap::from([
            ("RequestMethod".to_owned(), "get".to_owned()),
            ("DownstreamStatus".to_owned(), "200".to_owned()),
            (
                "RequestPath".to_owned(),
                "http://api.internal/health/?full=1".to_owned(),
            ),
        ]),
        BTreeMap::from([(HEALTHCHECK_PATH_LABEL.to_owned(), "/health".to_owned())]),
    );

    assert_eq!(filters.dropped_by(&entry), Some("successfulHealthcheck"));

    let root = workload_entry(
        "api",
        "request",
        BTreeMap::from([
            ("http.method".to_owned(), "GET".to_owned()),
            ("http.status_code".to_owned(), "200".to_owned()),
            ("url".to_owned(), "http://api.internal".to_owned()),
        ]),
        BTreeMap::from([(HEALTHCHECK_PATH_LABEL.to_owned(), "/".to_owned())]),
    );
    assert_eq!(filters.dropped_by(&root), Some("successfulHealthcheck"));
}

#[test]
fn successful_healthcheck_filter_reads_nested_json_attributes() {
    let filters = LogFilterChain::configured([LogFilterKind::SuccessfulHealthcheck]);
    let entry = workload_entry(
        "api",
        "request",
        BTreeMap::from([(
            "http".to_owned(),
            r#"{"method":"GET","status_code":"200","url_details":{"path":"/api/_status/db"}}"#
                .to_owned(),
        )]),
        BTreeMap::from([(
            HEALTHCHECK_PATH_LABEL.to_owned(),
            "/api/_status/db".to_owned(),
        )]),
    );

    assert_eq!(filters.dropped_by(&entry), Some("successfulHealthcheck"));
}

#[test]
fn successful_healthcheck_filter_keeps_failures_other_requests_and_unconfigured_workloads() {
    let filters = LogFilterChain::configured([LogFilterKind::SuccessfulHealthcheck]);
    for (method, status, path, labels) in [
        ("GET", "503", "/health", healthcheck_labels()),
        ("POST", "200", "/health", healthcheck_labels()),
        ("GET", "200", "/api/users", healthcheck_labels()),
        ("GET", "200", "/health", BTreeMap::new()),
    ] {
        let entry = workload_entry(
            "api",
            "request",
            BTreeMap::from([
                ("http.method".to_owned(), method.to_owned()),
                ("http.status_code".to_owned(), status.to_owned()),
                ("http.url_details.path".to_owned(), path.to_owned()),
            ]),
            labels,
        );
        assert_eq!(filters.dropped_by(&entry), None);
    }
}

#[test]
fn filter_kinds_deserialize_in_declared_order() -> Result<(), Box<dyn std::error::Error>> {
    let kinds: Vec<LogFilterKind> =
        serde_json::from_str(r#"["successfulHealthcheck","tailscaleNoise"]"#)?;

    assert_eq!(
        kinds,
        vec![
            LogFilterKind::SuccessfulHealthcheck,
            LogFilterKind::TailscaleNoise
        ]
    );
    Ok(())
}

fn healthcheck_labels() -> BTreeMap<String, String> {
    BTreeMap::from([(HEALTHCHECK_PATH_LABEL.to_owned(), "/health".to_owned())])
}

fn workload_entry(
    service_id: &str,
    body: &str,
    attributes: BTreeMap<String, String>,
    labels: BTreeMap<String, String>,
) -> IngestLogEntry {
    let node_id = NodeId::new("node-1").unwrap();
    let workload_id = WorkloadId::new("workload-1").unwrap();
    IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::Workload(workload_id.clone()),
            cursor: OriginCursor::new("cursor-1"),
        },
        observed_at: Timestamp(1),
        event_at: Timestamp(1),
        severity: "info".to_owned(),
        stream: LogStream::Stdout,
        origin: LogOrigin::Workload {
            metadata: WorkloadMetadata {
                cluster_id: ClusterId::new("cluster-1").unwrap(),
                node_id,
                service_id: ServiceId::new(service_id).unwrap(),
                deployment_id: DeploymentId::new("deployment-1").unwrap(),
                assignment_id: AssignmentId::new("assignment-1").unwrap(),
                workload_id,
                labels,
            },
        },
        body: LogBody::Text(body.to_owned()),
        attributes,
    }
}
