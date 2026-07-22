use std::collections::BTreeMap;

use kernel_api::{ClusterId, NodeId, ServiceId, Timestamp};

use crate::{
    InMemoryLogStore, IngestLogEntry, IngressTrafficBreakdown, IngressTrafficQuery,
    IngressTrafficScope, LogBody, LogOrigin, LogProducer, LogRecordId, LogStore, LogStream,
    OriginCursor, ServiceTrafficQuery, TrafficMetricPoint, TrafficQueryStore,
    merge_ingress_traffic, merge_service_traffic,
};

#[tokio::test]
async fn access_log_traffic_is_trusted_scoped_ranked_and_bucketed()
-> Result<(), Box<dyn std::error::Error>> {
    let store = InMemoryLogStore::new();
    let mut entries = vec![
        access_entry(1, 10_001, "svc-router@etcd", "203.0.113.1", "/a", 200),
        access_entry(2, 10_002, "svc-router@etcd", "203.0.113.1", "/a", 500),
        access_entry(
            3,
            10_003,
            "blocked-router@etcd",
            "203.0.113.2",
            "/deny",
            403,
        ),
        access_entry(4, 10_004, "other-router@etcd", "203.0.113.3", "/b", 201),
    ];
    let mut spoof = access_entry(5, 10_005, "svc-router@etcd", "203.0.113.9", "/fake", 200);
    spoof.origin = LogOrigin::Workload {
        metadata: runtime::WorkloadMetadata {
            cluster_id: ClusterId::new("cluster-1")?,
            node_id: NodeId::new("node-1")?,
            service_id: ServiceId::new("spoof")?,
            deployment_id: kernel_api::DeploymentId::new("deployment-spoof")?,
            assignment_id: kernel_api::AssignmentId::new("assignment-spoof")?,
            workload_id: kernel_api::WorkloadId::new("workload-spoof")?,
            labels: BTreeMap::new(),
        },
    };
    entries.push(spoof);
    store.append(&entries).await?;

    let cluster = store
        .query_ingress_traffic(&IngressTrafficQuery::new(
            IngressTrafficScope::Cluster {
                blocked_router_prefix: "blocked-".to_owned(),
            },
            Timestamp(10_000),
            Timestamp(11_000),
            2,
        )?)
        .await?;
    assert_eq!(cluster.by_ip.len(), 3);
    let [first_ip, second_ip, ..] = cluster.by_ip.as_slice() else {
        return Err("cluster traffic rows missing".into());
    };
    assert_eq!(first_ip.value, "203.0.113.1");
    assert_eq!(first_ip.status_code, 200);
    assert_eq!(second_ip.status_code, 500);
    assert!(
        cluster
            .by_ip
            .iter()
            .all(|entry| entry.value != "203.0.113.9")
    );

    let blocked = store
        .query_ingress_traffic(&IngressTrafficQuery::new(
            IngressTrafficScope::Blocked {
                router_prefix: "blocked-".to_owned(),
            },
            Timestamp(10_000),
            Timestamp(11_000),
            8,
        )?)
        .await?;
    assert_eq!(
        blocked.by_path.first().ok_or("blocked path missing")?.value,
        "/deny"
    );

    let service = store
        .query_service_traffic(&ServiceTrafficQuery::new(
            ServiceId::new("api")?,
            "svc-".to_owned(),
            Timestamp(10_000),
            Timestamp(11_000),
            8,
        )?)
        .await?;
    assert_eq!(service.len(), 2);
    assert!(service.iter().all(|point| point.ts == 10_000));
    let [success, failure] = service.as_slice() else {
        return Err("service traffic rows missing".into());
    };
    assert_eq!(success.requests, 1);
    assert_eq!(success.bytes_in, 10);
    assert_eq!(success.bytes_out, 20);
    assert_eq!(success.lat_le_1s, 1);
    assert_eq!(success.lat_total, 1);
    assert_eq!(failure.lat_le_1s, 0);
    assert_eq!(failure.lat_le_5s, 1);
    Ok(())
}

#[test]
fn node_traffic_merges_saturate_and_preserve_value_rank() {
    let first = IngressTrafficBreakdown {
        by_ip: vec![breakdown("b", 200, 2, 10), breakdown("a", 200, 1, 20)],
        by_path: Vec::new(),
    };
    let second = IngressTrafficBreakdown {
        by_ip: vec![breakdown("a", 200, 3, 30)],
        by_path: Vec::new(),
    };
    let merged = merge_ingress_traffic([first, second], 1);
    assert_eq!(merged.by_ip, vec![breakdown("a", 200, 4, 30)]);

    let point = metric_point(i64::MAX);
    let metrics = merge_service_traffic([point.clone(), metric_point(1)], 8);
    assert_eq!(metrics, vec![point]);
}

#[test]
fn traffic_queries_reject_invalid_ranges_prefixes_and_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = || IngressTrafficScope::Service {
        router_prefix: "svc-".to_owned(),
    };
    assert!(IngressTrafficQuery::new(scope(), Timestamp(2), Timestamp(1), 1).is_err());
    assert!(IngressTrafficQuery::new(scope(), Timestamp(1), Timestamp(2), 0).is_err());
    assert!(
        ServiceTrafficQuery::new(
            ServiceId::new("api")?,
            String::new(),
            Timestamp(1),
            Timestamp(2),
            1,
        )
        .is_err()
    );
    Ok(())
}

fn access_entry(
    index: u64,
    at: i64,
    router: &str,
    ip: &str,
    path: &str,
    status: u16,
) -> IngestLogEntry {
    IngestLogEntry {
        id: LogRecordId {
            node_id: NodeId::new("node-1").expect("node id"),
            producer: LogProducer::System("maestro-ingress".to_owned()),
            cursor: OriginCursor::new(format!("cursor-{index}")),
        },
        observed_at: Timestamp(at),
        event_at: Timestamp(at),
        severity: "info".to_owned(),
        stream: LogStream::System,
        origin: LogOrigin::System {
            cluster_id: ClusterId::new("cluster-1").expect("cluster id"),
            node_id: Some(NodeId::new("node-1").expect("node id")),
            component: "maestro-ingress".to_owned(),
        },
        body: LogBody::Text(String::new()),
        attributes: BTreeMap::from([
            ("maestro.log_type".to_owned(), "ingress_access".to_owned()),
            ("RouterName".to_owned(), router.to_owned()),
            ("maestro.client_ip".to_owned(), ip.to_owned()),
            ("RequestPath".to_owned(), path.to_owned()),
            ("RequestMethod".to_owned(), "GET".to_owned()),
            ("DownstreamStatus".to_owned(), status.to_string()),
            ("RequestContentSize".to_owned(), "10".to_owned()),
            ("DownstreamContentSize".to_owned(), "20".to_owned()),
            (
                "Duration".to_owned(),
                if status < 500 {
                    "500000000"
                } else {
                    "2000000000"
                }
                .to_owned(),
            ),
        ]),
    }
}

fn breakdown(
    value: &str,
    status_code: u16,
    requests: u64,
    last_seen_at_ms: i64,
) -> crate::TrafficBreakdownEntry {
    crate::TrafficBreakdownEntry {
        value: value.to_owned(),
        status_code,
        requests,
        last_seen_at_ms,
    }
}

fn metric_point(requests: i64) -> TrafficMetricPoint {
    TrafficMetricPoint {
        ts: 10_000,
        service_id: "api".to_owned(),
        deployment_id: None,
        status_code: 200,
        method: "GET".to_owned(),
        requests,
        bytes_in: requests,
        bytes_out: requests,
        lat_le_1s: requests,
        lat_le_5s: requests,
        lat_le_10s: requests,
        lat_total: requests,
    }
}
