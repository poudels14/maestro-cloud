use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};

use axum::http::StatusCode;
use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus, Build, BuildId,
    BuildPhase, BuildSource, BuildSpec, BuildStatus, BuildTemplate, DeploymentId, DeploymentPhase,
    Generation, NodeId, ReplicaState, ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus,
    SecretValue, ServiceId, WorkloadId,
};

use crate::{ApiServer, ServerSettings};

use super::deployments::deployment;
use super::{decode, metadata, put, request, seeded_store};

#[tokio::test]
async fn workload_observations_are_scoped_revisioned_and_secret_safe()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let deployment = deployment("api-deployment", "api")?;
    put(
        &store,
        &cluster_id,
        "Deployment",
        deployment.meta.id.as_str(),
        &deployment,
    )
    .await?;
    let owned_assignment = assignment("api-assignment", "api", "api-deployment")?;
    let unrelated_assignment = assignment("other-assignment", "api", "other-deployment")?;
    put(
        &store,
        &cluster_id,
        "Assignment",
        owned_assignment.meta.id.as_str(),
        &owned_assignment,
    )
    .await?;
    put(
        &store,
        &cluster_id,
        "Assignment",
        unrelated_assignment.meta.id.as_str(),
        &unrelated_assignment,
    )
    .await?;
    let replica = replica("api-replica", "api", "api-deployment", "api-assignment")?;
    put(
        &store,
        &cluster_id,
        "ReplicaState",
        replica.meta.id.as_str(),
        &replica,
    )
    .await?;
    let owned_build = build("api-build", "api", "api-deployment")?;
    let unrelated_build = build("other-build", "other", "api-deployment")?;
    put(
        &store,
        &cluster_id,
        "Build",
        owned_build.meta.id.as_str(),
        &owned_build,
    )
    .await?;
    put(
        &store,
        &cluster_id,
        "Build",
        unrelated_build.meta.id.as_str(),
        &unrelated_build,
    )
    .await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let response = request(
        &server,
        "/api/services/api/deployments/api-deployment/assignments",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let assignments: Vec<Assignment> = decode(response).await?;
    let listed_assignment = assignments.first().ok_or("assignment is missing")?;
    assert_eq!(assignments.len(), 1);
    assert_eq!(listed_assignment.meta.id, owned_assignment.meta.id);
    assert!(listed_assignment.meta.revision.0 > 0);
    let response = request(
        &server,
        "/api/services/api/deployments/api-deployment/assignments/api-assignment",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let fetched_assignment: Assignment = decode(response).await?;
    assert_eq!(fetched_assignment.meta.id, owned_assignment.meta.id);
    assert_eq!(
        request(
            &server,
            "/api/services/api/deployments/api-deployment/assignments/other-assignment",
            None,
        )
        .await?
        .status(),
        StatusCode::NOT_FOUND
    );

    let response = request(
        &server,
        "/api/services/api/deployments/api-deployment/replicas",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let replicas: Vec<ReplicaState> = decode(response).await?;
    assert_eq!(replicas.len(), 1);
    assert_eq!(
        replicas.first().ok_or("replica is missing")?.meta.id,
        replica.meta.id
    );
    let response = request(
        &server,
        "/api/services/api/deployments/api-deployment/replicas/api-replica",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let fetched_replica: ReplicaState = decode(response).await?;
    assert_eq!(fetched_replica.meta.id, replica.meta.id);

    let response = request(&server, "/api/services/api/builds", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let builds: Vec<Build> = decode(response).await?;
    assert_eq!(builds.len(), 1);
    let encoded = serde_json::to_string(&builds)?;
    assert!(!encoded.contains("registry-password"));
    assert!(encoded.contains("••••word"));
    let response = request(&server, "/api/services/api/builds/api-build", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let encoded = String::from_utf8(
        http_body_util::BodyExt::collect(response.into_body())
            .await?
            .to_bytes()
            .to_vec(),
    )?;
    assert!(!encoded.contains("registry-password"));
    assert!(encoded.contains("••••word"));
    assert_eq!(
        request(&server, "/api/services/api/builds/other-build", None)
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}

fn assignment(
    id: &str,
    service_id: &str,
    deployment_id: &str,
) -> Result<Assignment, kernel_api::InvalidIdentifier> {
    Ok(Assignment {
        meta: metadata(AssignmentId::new(id)?),
        spec: AssignmentSpec {
            service_id: ServiceId::new(service_id)?,
            deployment_id: DeploymentId::new(deployment_id)?,
            restart_generation: Generation(1),
            replica_index: 0,
            node_id: NodeId::new("node-1")?,
            placement_epoch: 1,
            workload_address: Some(IpAddr::V4(Ipv4Addr::new(10, 30, 0, 2))),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Running,
            workload_id: Some(WorkloadId::new("workload-1")?),
            workload_address: Some(IpAddr::V4(Ipv4Addr::new(10, 30, 0, 2))),
            conditions: Vec::new(),
        },
    })
}

fn replica(
    id: &str,
    service_id: &str,
    deployment_id: &str,
    assignment_id: &str,
) -> Result<ReplicaState, kernel_api::InvalidIdentifier> {
    Ok(ReplicaState {
        meta: metadata(ReplicaStateId::new(id)?),
        spec: ReplicaStateSpec {
            service_id: ServiceId::new(service_id)?,
            deployment_id: DeploymentId::new(deployment_id)?,
            assignment_id: AssignmentId::new(assignment_id)?,
            replica_index: 0,
        },
        status: ReplicaStateStatus {
            phase: DeploymentPhase::Ready,
            node_id: Some(NodeId::new("node-1")?),
            workload_id: Some(WorkloadId::new("workload-1")?),
            healthcheck_failures: 0,
            restart_attempts: 0,
            restart_pending_attempt: None,
            restart_not_before: None,
            resolved_secrets: Default::default(),
            conditions: Vec::new(),
        },
    })
}

pub(super) fn build(
    id: &str,
    service_id: &str,
    deployment_id: &str,
) -> Result<Build, kernel_api::InvalidIdentifier> {
    Ok(Build {
        meta: metadata(BuildId::new(id)?),
        spec: BuildSpec {
            service_id: ServiceId::new(service_id)?,
            deployment_id: DeploymentId::new(deployment_id)?,
            template: BuildTemplate {
                source: BuildSource::Git {
                    repository: "https://example.test/maestro/api".to_string(),
                    revision: "main".to_string(),
                },
                dockerfile: "Dockerfile".to_string(),
                watch: false,
                registry: None,
                depot: None,
                environment: BTreeMap::new(),
                environment_source: None,
                secrets: BTreeMap::from([(
                    "REGISTRY_PASSWORD".to_string(),
                    SecretValue::new("registry-password"),
                )]),
                secrets_source: None,
            },
        },
        status: BuildStatus {
            phase: BuildPhase::Succeeded,
            image_digest: Some("registry.test/api@sha256:abc".to_string()),
            source_revision: Some("abc123".to_string()),
            source_title: Some("Ship the API".to_string()),
            conditions: Vec::new(),
        },
    })
}
