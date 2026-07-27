use std::collections::BTreeMap;

use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use logs::{
    IngestLogEntry, IngressTrafficQuery, IngressTrafficScope, LogBody, LogOrigin, LogProducer,
    LogRecordId, LogStore, LogStream, OriginCursor, ServiceTrafficQuery, TrafficQueryStore,
};
use runtime::WorkloadMetadata;

use crate::{DuckLogStoreRuntime, DuckStoreSettings};

const TEN: i64 = 1_784_628_000_000;
const ELEVEN: i64 = 1_784_631_600_000;

#[tokio::test]
async fn traffic_queries_share_trusted_hot_and_cold_access_logs()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("logs.duckdb"),
        8,
    )?)
    .await?;
    let store = runtime.store();
    store
        .append(&[access_entry(
            1,
            TEN + 1_000,
            "maestro-api-hash-1",
            "203.0.113.1",
            "/cold",
            200,
            "10",
            "20",
            "500000000",
        )?])
        .await?;
    assert_eq!(store.rollover_before(Timestamp(ELEVEN), &[]).await?.rows, 1);

    let mut spoof = access_entry(
        5,
        ELEVEN + 4_000,
        "maestro-api-hash-spoof",
        "192.0.2.99",
        "/spoof",
        599,
        "1000",
        "1000",
        "1000",
    )?;
    let workload_id = WorkloadId::new("spoof-workload")?;
    spoof.id.producer = LogProducer::Workload(workload_id.clone());
    spoof.origin = LogOrigin::Workload {
        metadata: WorkloadMetadata {
            cluster_id: ClusterId::new("cluster-one")?,
            node_id: NodeId::new("node-one")?,
            service_id: ServiceId::new("api")?,
            deployment_id: DeploymentId::new("api-v1")?,
            assignment_id: AssignmentId::new("assignment-spoof")?,
            workload_id,
            labels: BTreeMap::new(),
        },
    };
    store
        .append(&[
            access_entry(
                2,
                ELEVEN + 1_000,
                "maestro-api-hash-2",
                "203.0.113.1",
                "/hot",
                500,
                "30",
                "40",
                "2000000000",
            )?,
            access_entry(
                3,
                ELEVEN + 2_000,
                "maestro.internal-blocked-hash",
                "198.51.100.7",
                "/blocked",
                403,
                "0",
                "5",
                "1000000",
            )?,
            access_entry(
                4,
                ELEVEN + 3_000,
                "maestro-web-hash",
                "198.51.100.8",
                "/web",
                201,
                "1",
                "2",
                "invalid",
            )?,
            spoof,
        ])
        .await?;

    let api_breakdown = store
        .query_ingress_traffic(&IngressTrafficQuery::new(
            IngressTrafficScope::Service {
                router_prefix: "maestro-api-".to_owned(),
            },
            Timestamp(TEN),
            Timestamp(ELEVEN + 4_000),
            10,
        )?)
        .await?;
    assert_eq!(api_breakdown.by_ip.len(), 2);
    assert!(
        api_breakdown
            .by_ip
            .iter()
            .all(|entry| entry.value == "203.0.113.1")
    );
    assert_eq!(api_breakdown.by_path.len(), 2);
    let [hot_path, cold_path] = api_breakdown.by_path.as_slice() else {
        return Err("API paths missing".into());
    };
    assert_eq!(hot_path.value, "/hot");
    assert_eq!(cold_path.value, "/cold");

    let cluster = store
        .query_ingress_traffic(&IngressTrafficQuery::new(
            IngressTrafficScope::Cluster {
                blocked_router_prefix: "maestro.internal-blocked-".to_owned(),
            },
            Timestamp(TEN),
            Timestamp(ELEVEN + 4_000),
            10,
        )?)
        .await?;
    assert_eq!(cluster.by_ip.len(), 3);
    assert!(
        cluster
            .by_ip
            .iter()
            .all(|entry| entry.value != "198.51.100.7" && entry.value != "192.0.2.99")
    );

    let blocked = store
        .query_ingress_traffic(&IngressTrafficQuery::new(
            IngressTrafficScope::Blocked {
                router_prefix: "maestro.internal-blocked-".to_owned(),
            },
            Timestamp(TEN),
            Timestamp(ELEVEN + 4_000),
            10,
        )?)
        .await?;
    assert_eq!(blocked.by_ip.len(), 1);
    assert_eq!(
        blocked.by_ip.first().ok_or("blocked IP missing")?.value,
        "198.51.100.7"
    );

    let points = store
        .query_service_traffic(&ServiceTrafficQuery::new(
            ServiceId::new("api")?,
            "maestro-api-".to_owned(),
            Timestamp(TEN),
            Timestamp(ELEVEN + 4_000),
            10,
        )?)
        .await?;
    assert_eq!(points.len(), 2);
    let [cold, hot] = points.as_slice() else {
        return Err("service traffic rows missing".into());
    };
    assert_eq!(cold.bytes_in, 10);
    assert_eq!(cold.bytes_out, 20);
    assert_eq!(cold.lat_le_1s, 1);
    assert_eq!(cold.lat_total, 1);
    assert_eq!(hot.bytes_in, 30);
    assert_eq!(hot.bytes_out, 40);
    assert_eq!(hot.lat_le_1s, 0);
    assert_eq!(hot.lat_le_5s, 1);
    assert_eq!(hot.lat_total, 1);
    assert!(points.iter().all(|point| point.deployment_id.is_none()));

    runtime.shutdown().await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn access_entry(
    index: u64,
    event_at: i64,
    router: &str,
    ip: &str,
    path: &str,
    status: u16,
    bytes_in: &str,
    bytes_out: &str,
    duration: &str,
) -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    let node_id = NodeId::new("node-one")?;
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::System("maestro-ingress".to_owned()),
            cursor: OriginCursor::new(index.to_string()),
        },
        observed_at: Timestamp(event_at),
        event_at: Timestamp(event_at),
        severity: "info".to_owned(),
        stream: LogStream::System,
        origin: LogOrigin::System {
            cluster_id: ClusterId::new("cluster-one")?,
            node_id: Some(node_id),
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
            ("RequestContentSize".to_owned(), bytes_in.to_owned()),
            ("DownstreamContentSize".to_owned(), bytes_out.to_owned()),
            ("Duration".to_owned(), duration.to_owned()),
        ]),
    })
}
