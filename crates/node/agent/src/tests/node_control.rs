use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use kernel_api::{
    AssignmentId, ClusterId, DeploymentId, ExecPolicy, Generation, NodeApiAccess, NodeId, Object,
    ObjectMeta, PlacementConstraint, RequestId, ResourceKind, ResourceName, ResourceRevision,
    RolloutState, Service, ServiceId, ServiceSpec, ServiceStatus, Timestamp, WorkloadId,
};
use kernel_store::{InMemoryStore, Keyspace, Store, TokioClock};
use node_fabric::WorkloadClaims;
use node_fabric::proto::{MutationDisposition, MutationOperation, ResourceMutation};
use tonic::Code;

use crate::{NodeControlHandler, StatusClock, StoreNodeControlHandler};

#[tokio::test]
async fn privileged_control_applies_replays_updates_and_requests_deletion()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("node-control")?;
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let handler =
        StoreNodeControlHandler::new(store.clone(), &cluster_id, Arc::new(FixedStatusClock));
    let mut resource = service()?;

    let created = handler
        .mutate(claims()?, apply("create-service", &resource)?)
        .await?;
    assert_eq!(
        MutationDisposition::try_from(created.disposition)?,
        MutationDisposition::Committed
    );
    assert!(created.revision > 0);

    let replayed = handler
        .mutate(claims()?, apply("create-service", &resource)?)
        .await?;
    assert_eq!(
        MutationDisposition::try_from(replayed.disposition)?,
        MutationDisposition::Duplicate
    );
    assert_eq!(replayed.revision, created.revision);

    resource.meta.revision = ResourceRevision(created.revision);
    resource.meta.generation = Generation(2);
    resource.spec.version = "v2".to_owned();
    let updated = handler
        .mutate(claims()?, apply("update-service", &resource)?)
        .await?;
    assert_eq!(
        MutationDisposition::try_from(updated.disposition)?,
        MutationDisposition::Committed
    );
    assert!(updated.revision > created.revision);

    let deleted = handler.mutate(claims()?, delete("delete-service")).await?;
    assert_eq!(
        MutationDisposition::try_from(deleted.disposition)?,
        MutationDisposition::Committed
    );
    assert!(deleted.revision > updated.revision);

    let key = Keyspace::new(&cluster_id)
        .resource(&ResourceKind::new("Service")?, &ResourceName::new("api")?);
    let stored = store.get(&key).await?.ok_or("service missing")?;
    let persisted: Service = serde_json::from_slice(&stored.value)?;
    assert_eq!(persisted.spec.version, "v2");
    assert_eq!(persisted.meta.deletion_timestamp, Some(Timestamp(42_000)));
    Ok(())
}

#[tokio::test]
async fn privileged_control_rejects_stale_invalid_and_colliding_mutations()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("node-control-errors")?;
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let handler = StoreNodeControlHandler::new(store, &cluster_id, Arc::new(FixedStatusClock));
    let resource = service()?;
    handler
        .mutate(claims()?, apply("shared-request", &resource)?)
        .await?;

    let mut changed = resource.clone();
    changed.spec.version = "collision".to_owned();
    let collision = handler
        .mutate(claims()?, apply("shared-request", &changed)?)
        .await
        .expect_err("request collision must fail");
    assert_eq!(collision.code(), Code::AlreadyExists);

    let stale = handler
        .mutate(claims()?, apply("stale-update", &changed)?)
        .await
        .expect_err("stale update must fail");
    assert_eq!(stale.code(), Code::Aborted);

    let mut wrong_identity = resource;
    wrong_identity.meta.id = ServiceId::new("other")?;
    let invalid = handler
        .mutate(claims()?, apply("wrong-identity", &wrong_identity)?)
        .await
        .expect_err("envelope mismatch must fail");
    assert_eq!(invalid.code(), Code::InvalidArgument);

    let mut invalid_service = service()?;
    invalid_service.spec.exposed_ports = vec![8080, 8080];
    let invalid = handler
        .mutate(claims()?, apply("invalid-service-spec", &invalid_service)?)
        .await
        .expect_err("semantic service validation must fail");
    assert_eq!(invalid.code(), Code::InvalidArgument);

    let missing = handler
        .mutate(claims()?, delete("missing-service"))
        .await
        .expect_err("missing delete must fail");
    assert_eq!(missing.code(), Code::NotFound);
    Ok(())
}

fn apply(
    request_id: &str,
    resource: &Service,
) -> Result<ResourceMutation, Box<dyn std::error::Error>> {
    Ok(ResourceMutation {
        request_id: RequestId::new(request_id)?.to_string(),
        kind: "Service".to_owned(),
        resource_id: "api".to_owned(),
        operation: MutationOperation::Apply.into(),
        resource_json: serde_json::to_vec(resource)?,
    })
}

fn delete(request_id: &str) -> ResourceMutation {
    ResourceMutation {
        request_id: request_id.to_owned(),
        kind: "Service".to_owned(),
        resource_id: if request_id == "missing-service" {
            "missing".to_owned()
        } else {
            "api".to_owned()
        },
        operation: MutationOperation::Delete.into(),
        resource_json: Vec::new(),
    }
}

fn claims() -> Result<WorkloadClaims, Box<dyn std::error::Error>> {
    Ok(WorkloadClaims {
        workload_id: WorkloadId::new("system-workload")?,
        assignment_id: AssignmentId::new("system-assignment")?,
        node_id: NodeId::new("node-1")?,
        service_id: ServiceId::new("system")?,
        deployment_id: DeploymentId::new("system-deployment")?,
        labels: BTreeMap::new(),
    })
}

fn service() -> Result<Service, Box<dyn std::error::Error>> {
    Ok(Object {
        meta: ObjectMeta {
            id: ServiceId::new("api")?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: ServiceSpec {
            name: "API".to_owned(),
            version: "v1".to_owned(),
            artifact: kernel_api::ArtifactTemplate::Image {
                reference: "registry.test/api:v1".to_owned(),
            },
            preview: None,
            command: None,
            replicas: 1,
            exposed_ports: Vec::new(),
            health_check: None,
            max_restarts: None,
            environment: BTreeMap::new(),
            environment_sources: Vec::new(),
            user: None,
            node_api: NodeApiAccess::Disabled,
            secrets: None,
            volumes: Vec::new(),
            placement: PlacementConstraint::default(),
            exec: ExecPolicy::Denied,
        },
        status: ServiceStatus {
            active_deployment_id: None,
            replica_override: None,
            rollout: RolloutState::Active,
            rollout_bypass_generation: None,
            conditions: Vec::new(),
        },
    })
}

struct FixedStatusClock;

impl StatusClock for FixedStatusClock {
    fn now(&self) -> Timestamp {
        Timestamp(42_000)
    }
}
