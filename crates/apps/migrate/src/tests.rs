use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use kernel_api::{
    ArtifactTemplate, BuiltinResource, ClusterId, ExecPolicy, Generation, NodeApiAccess, Object,
    ObjectMeta, PlacementConstraint, ResourceKind, ResourceName, ResourceRevision, RolloutState,
    Service, ServiceId, ServiceSpec, ServiceStatus,
};
use kernel_store::{
    CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store, TokioClock,
};

use crate::{
    CutoverMigration, LegacyEntry, LegacySnapshot, MigrationError, MigrationOutcome, MigrationPlan,
    PlanError, SnapshotError,
};

type TestResult<Value = ()> = Result<Value, Box<dyn std::error::Error>>;

#[test]
fn snapshot_digest_is_order_independent_and_input_is_bounded() -> TestResult {
    let first = LegacyEntry::new("/maetro/services/api/info", b"api".to_vec());
    let second = LegacyEntry::new("/maetro/services/worker/info", b"worker".to_vec());
    let forward = LegacySnapshot::new(vec![first.clone(), second.clone()])?;
    let reverse = LegacySnapshot::new(vec![second, first])?;

    assert_eq!(forward, reverse);
    let first = forward
        .entries()
        .first()
        .ok_or_else(|| std::io::Error::other("snapshot should contain an entry"))?;
    assert_eq!(first.key(), "/maetro/services/api/info");
    assert_eq!(first.value(), b"api");
    assert!(matches!(
        LegacySnapshot::new(vec![LegacyEntry::new("/maestro/services/api", Vec::new())]),
        Err(SnapshotError::OutsideLegacyNamespace { .. })
    ));
    assert!(matches!(
        LegacySnapshot::new(vec![
            LegacyEntry::new("/maetro/services/api/info", Vec::new()),
            LegacyEntry::new("/maetro/services/api/info", Vec::new()),
        ]),
        Err(SnapshotError::DuplicateKey { .. })
    ));
    Ok(())
}

#[test]
fn plan_orders_resources_and_rejects_destination_aliases() -> TestResult {
    let service = service("api")?;
    let plan = MigrationPlan::new([7; 32], [BuiltinResource::Service(service.clone())])?;

    assert_eq!(plan.writes().len(), 1);
    assert_eq!(
        plan.writes()
            .first()
            .ok_or_else(|| std::io::Error::other("plan should contain a write"))?
            .id()
            .as_str(),
        "api"
    );
    assert!(matches!(
        MigrationPlan::new(
            [7; 32],
            [
                BuiltinResource::Service(service.clone()),
                BuiltinResource::Service(service),
            ],
        ),
        Err(PlanError::DuplicateResource { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn migration_commits_last_and_is_idempotent() -> TestResult {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let keyspace = keyspace()?;
    let migration = migration(store.clone(), keyspace.clone())?;
    let migration_plan = plan("api", [11; 32])?;

    assert_eq!(
        migration.apply(&migration_plan).await?,
        MigrationOutcome::Applied {
            resources: 1,
            written: 1,
            reused: 0,
        }
    );
    assert!(store.get(&resource_key(&keyspace, "api")?).await?.is_some());
    assert!(store.get(&marker_key(&keyspace)?).await?.is_some());
    assert_eq!(
        migration.apply(&migration_plan).await?,
        MigrationOutcome::AlreadyComplete { resources: 1 }
    );

    let changed_plan = plan("api", [12; 32])?;
    assert!(matches!(
        migration.apply(&changed_plan).await,
        Err(MigrationError::MarkerMismatch { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn migration_reuses_exact_partial_writes() -> TestResult {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let keyspace = keyspace()?;
    let migration = migration(store.clone(), keyspace.clone())?;
    let plan = plan("api", [21; 32])?;
    let value = plan
        .writes()
        .first()
        .ok_or_else(|| std::io::Error::other("plan should contain a write"))?
        .value()
        .to_vec();
    put_missing(store.as_ref(), resource_key(&keyspace, "api")?, value).await?;

    assert_eq!(
        migration.apply(&plan).await?,
        MigrationOutcome::Applied {
            resources: 1,
            written: 0,
            reused: 1,
        }
    );
    Ok(())
}

#[tokio::test]
async fn migration_fails_closed_on_destination_collision() -> TestResult {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let keyspace = keyspace()?;
    let migration = migration(store.clone(), keyspace.clone())?;
    let plan = plan("api", [31; 32])?;
    put_missing(
        store.as_ref(),
        resource_key(&keyspace, "api")?,
        b"not-the-migrated-resource".to_vec(),
    )
    .await?;

    assert!(matches!(
        migration.apply(&plan).await,
        Err(MigrationError::DestinationCollision { .. })
    ));
    assert!(store.get(&marker_key(&keyspace)?).await?.is_none());
    Ok(())
}

fn keyspace() -> TestResult<Keyspace> {
    Ok(Keyspace::new(&ClusterId::new("production")?))
}

fn migration(store: Arc<InMemoryStore>, keyspace: Keyspace) -> TestResult<CutoverMigration> {
    Ok(CutoverMigration::new(
        store,
        keyspace,
        ResourceName::new("legacy-v1")?,
    ))
}

fn plan(service_id: &str, digest: [u8; 32]) -> TestResult<MigrationPlan> {
    Ok(MigrationPlan::new(
        digest,
        [BuiltinResource::Service(service(service_id)?)],
    )?)
}

fn service(service_id: &str) -> TestResult<Service> {
    Ok(Object {
        meta: ObjectMeta {
            id: ServiceId::new(service_id)?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision(0),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: ServiceSpec {
            name: service_id.to_string(),
            version: "v1".to_string(),
            artifact: ArtifactTemplate::Image {
                reference: "registry.example/api@sha256:abc".to_string(),
            },
            preview: None,
            command: None,
            replicas: 1,
            exposed_ports: vec![8080],
            health_check: None,
            max_restarts: None,
            environment: BTreeMap::new(),
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
            conditions: Vec::new(),
        },
    })
}

fn resource_key(keyspace: &Keyspace, id: &str) -> TestResult<kernel_store::StoreKey> {
    Ok(keyspace.resource(&ResourceKind::new("Service")?, &ResourceName::new(id)?))
}

fn marker_key(keyspace: &Keyspace) -> TestResult<kernel_store::StoreKey> {
    Ok(keyspace.migration_marker(&ResourceName::new("legacy-v1")?))
}

async fn put_missing(
    store: &InMemoryStore,
    key: kernel_store::StoreKey,
    value: Vec<u8>,
) -> TestResult {
    assert!(matches!(
        store
            .put_cas(PutRequest {
                key,
                value,
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?,
        CasOutcome::Applied(_)
    ));
    Ok(())
}
