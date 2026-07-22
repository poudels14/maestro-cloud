use axum::http::StatusCode;
use kernel_api::{ClusterInfo, DeploymentId, ServiceId, UnschedulableReplica};
use kernel_store::{CasOutcome, ExpectedVersion, Keyspace, PutRequest, Store};

use crate::{ApiServer, ServerSettings};

use super::{decode, request, seeded_store};

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
            node_count: 1,
            control_plane_node_count: 1,
            workload_node_count: 1,
        }
    );
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
