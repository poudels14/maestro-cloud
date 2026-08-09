use axum::http::StatusCode;
use kernel_api::{
    AssignmentId, ClusterInfo, DeploymentId, NodeId, NodeInstanceId, Object, PlacementHistory,
    PlacementHistorySpec, PlacementHistoryStatus, ServiceId, Timestamp, UnschedulableReplica,
};
use kernel_controller::LeaderIdentity;
use kernel_store::{CasOutcome, ExpectedVersion, Keyspace, PutRequest, Store};

use crate::{ApiServer, ServerSettings};

use super::{decode, metadata, put, request, seeded_store};

#[tokio::test]
async fn cluster_info_summarizes_node_capabilities() -> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let response = request(&server, "/api/cluster", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        decode::<ClusterInfo>(response).await?,
        ClusterInfo {
            cluster_id,
            leader_node_id: None,
            node_count: 1,
            control_plane_node_count: 1,
            workload_node_count: 1,
        }
    );
    Ok(())
}

#[tokio::test]
async fn cluster_info_reports_the_current_elected_master() -> Result<(), Box<dyn std::error::Error>>
{
    let (store, cluster_id) = seeded_store().await?;
    let leader = LeaderIdentity {
        node_id: NodeId::new("node-1")?,
        instance_id: NodeInstanceId::new("instance-1")?,
    };
    let outcome = store
        .put_cas(PutRequest {
            key: Keyspace::new(&cluster_id).leader(),
            value: serde_json::to_vec(&leader)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(outcome, CasOutcome::Applied(_)));
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let info: ClusterInfo = decode(request(&server, "/api/cluster", None).await?).await?;

    assert_eq!(info.leader_node_id, Some(NodeId::new("node-1")?));
    Ok(())
}

#[tokio::test]
async fn placement_history_is_filtered_and_ordered_without_driving_exec()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let older = placement("assignment-old", "api", "api-v1", 0, 1_000)?;
    let newer = placement("assignment-new", "api", "api-v2", 1, 2_000)?;
    let other = placement("assignment-other", "worker", "worker-v1", 0, 3_000)?;
    for value in [&older, &newer, &other] {
        put(
            store.as_ref(),
            &cluster_id,
            "PlacementHistory",
            value.meta.id.as_str(),
            value,
        )
        .await?;
    }
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let response = request(
        &server,
        "/api/cluster/placements?serviceId=api&replicaIndex=1",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let placements: Vec<PlacementHistory> = decode(response).await?;
    assert_eq!(placements.len(), 1);
    assert_eq!(
        placements.first().map(|placement| &placement.meta.id),
        Some(&newer.meta.id)
    );
    assert_eq!(
        placements.first().map(|placement| &placement.spec),
        Some(&newer.spec)
    );

    let response = request(&server, "/api/cluster/placements?serviceId=-bad", None).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn unschedulable_replicas_come_from_the_fenced_scheduler_observation()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let expected = vec![UnschedulableReplica {
        service_id: ServiceId::new("api")?,
        deployment_id: DeploymentId::new("api-v2")?,
        replica_index: 2,
        reason: "no schedulable node".to_string(),
    }];
    let outcome = store
        .put_cas(PutRequest {
            key: Keyspace::new(&cluster_id).scheduler_observation(),
            value: serde_json::to_vec(&expected)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(outcome, CasOutcome::Applied(_)));
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let response = request(&server, "/api/cluster/unschedulable", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        decode::<Vec<UnschedulableReplica>>(response).await?,
        expected
    );
    Ok(())
}

#[tokio::test]
async fn unschedulable_replicas_are_empty_before_the_first_scheduler_pass()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let response = request(&server, "/api/cluster/unschedulable", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        decode::<Vec<UnschedulableReplica>>(response)
            .await?
            .is_empty()
    );
    Ok(())
}

fn placement(
    id: &str,
    service_id: &str,
    deployment_id: &str,
    replica_index: u32,
    started_at: i64,
) -> Result<PlacementHistory, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(AssignmentId::new(id)?),
        spec: PlacementHistorySpec {
            service_id: ServiceId::new(service_id)?,
            deployment_id: DeploymentId::new(deployment_id)?,
            replica_index,
            node_id: NodeId::new("node-1")?,
            cluster_host_address: IpAddr::V4(Ipv4Addr::new(10, 20, 0, 1)),
            cluster_api_port: 3000,
            container_hostname: format!("{service_id}-{replica_index}"),
        },
        status: PlacementHistoryStatus {
            started_at: Timestamp(started_at),
            ended_at: None,
        },
    })
}
use std::net::{IpAddr, Ipv4Addr};
