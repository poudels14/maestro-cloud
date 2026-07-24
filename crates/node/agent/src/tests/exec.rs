use std::collections::BTreeMap;
use std::sync::Arc;

use kernel_api::{
    Assignment, AssignmentPhase, CommandSpec, Deployment, ExecPolicy, ResourceKind, ResourceName,
    WorkloadId,
};
use kernel_store::{ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store, TokioClock};
use runtime::{
    Capabilities, ExecMode, ExecOutput, ExecRequest, FakeRuntime, RuntimeCapability,
    WorkloadRuntime,
};

use crate::assignment_plan::workload_spec;
use crate::{NodeExecError, NodeExecService, NodeExecSettings};

use super::assignment::{assignment, cluster_id, deployment, node_id};

#[tokio::test]
async fn node_exec_resolves_one_running_owned_workload_and_holds_its_permit()
-> Result<(), Box<dyn std::error::Error>> {
    let world = ExecWorld::new(
        ExecPolicy::Allowed,
        Capabilities::new([RuntimeCapability::Exec, RuntimeCapability::InteractiveExec]),
        1,
        "node-1",
    )
    .await?;
    let mut first = world
        .service
        .open(&world.assignment.meta.id, request(ExecMode::Pipes))
        .await?;
    assert_eq!(
        first.next().await?,
        Some(ExecOutput::Stdout(b"/bin/echo".to_vec()))
    );
    assert!(matches!(
        world
            .service
            .open(&world.assignment.meta.id, request(ExecMode::Pipes))
            .await,
        Err(NodeExecError::SessionLimitReached { maximum: 1 })
    ));
    drop(first);

    let mut reopened = world
        .service
        .open(
            &world.assignment.meta.id,
            request(ExecMode::Terminal {
                columns: 80,
                rows: 24,
            }),
        )
        .await?;
    assert_eq!(
        reopened.next().await?,
        Some(ExecOutput::Stdout(b"/bin/echo".to_vec()))
    );
    Ok(())
}

#[tokio::test]
async fn node_exec_enforces_service_policy_node_ownership_and_runtime_capabilities()
-> Result<(), Box<dyn std::error::Error>> {
    let denied = ExecWorld::new(
        ExecPolicy::Denied,
        Capabilities::new([RuntimeCapability::Exec]),
        8,
        "node-1",
    )
    .await?;
    assert!(matches!(
        denied
            .service
            .open(&denied.assignment.meta.id, request(ExecMode::Pipes))
            .await,
        Err(NodeExecError::PolicyDenied { .. })
    ));

    let wrong_node = ExecWorld::new(
        ExecPolicy::Allowed,
        Capabilities::new([RuntimeCapability::Exec]),
        8,
        "node-2",
    )
    .await?;
    assert!(matches!(
        wrong_node
            .service
            .open(&wrong_node.assignment.meta.id, request(ExecMode::Pipes))
            .await,
        Err(NodeExecError::AssignmentOnAnotherNode { .. })
    ));

    let no_terminal = ExecWorld::new(
        ExecPolicy::Allowed,
        Capabilities::new([RuntimeCapability::Exec]),
        8,
        "node-1",
    )
    .await?;
    assert!(matches!(
        no_terminal
            .service
            .open(
                &no_terminal.assignment.meta.id,
                request(ExecMode::Terminal {
                    columns: 80,
                    rows: 24,
                }),
            )
            .await,
        Err(NodeExecError::CapabilityUnavailable {
            capability: RuntimeCapability::InteractiveExec
        })
    ));
    Ok(())
}

#[tokio::test]
async fn node_exec_rejects_non_running_and_invalid_requests()
-> Result<(), Box<dyn std::error::Error>> {
    let mut world = ExecWorld::new(
        ExecPolicy::Allowed,
        Capabilities::new([RuntimeCapability::Exec]),
        8,
        "node-1",
    )
    .await?;
    assert!(matches!(
        world
            .service
            .open(
                &world.assignment.meta.id,
                request(ExecMode::Terminal {
                    columns: 0,
                    rows: 24,
                }),
            )
            .await,
        Err(NodeExecError::InvalidRequest)
    ));

    world.assignment.status.phase = AssignmentPhase::Pending;
    let key = resource_key("Assignment", world.assignment.meta.id.as_str())?;
    let version = world
        .store
        .get(&key)
        .await?
        .ok_or("assignment missing")?
        .version;
    world
        .store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&world.assignment)?,
            expected: ExpectedVersion::Exact(version),
            session: None,
        })
        .await?;
    assert!(matches!(
        world
            .service
            .open(&world.assignment.meta.id, request(ExecMode::Pipes))
            .await,
        Err(NodeExecError::WorkloadNotRunning { .. })
    ));
    Ok(())
}

fn request(mode: ExecMode) -> ExecRequest {
    ExecRequest {
        command: CommandSpec {
            executable: "/bin/echo".to_owned(),
            arguments: vec!["hello".to_owned()],
        },
        environment: BTreeMap::new(),
        mode,
    }
}

struct ExecWorld {
    store: Arc<InMemoryStore>,
    service: NodeExecService,
    assignment: Assignment,
}

impl ExecWorld {
    async fn new(
        policy: ExecPolicy,
        capabilities: Capabilities,
        maximum_sessions: usize,
        service_node: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
        let runtime = Arc::new(FakeRuntime::with_capabilities(capabilities));
        let mut assignment = assignment();
        assignment.status.phase = AssignmentPhase::Running;
        assignment.status.workload_id = Some(WorkloadId::new("assignment-1")?);
        let mut deployment = deployment();
        deployment.spec.service.exec = policy;
        seed(store.as_ref(), &assignment, &deployment).await?;
        let spec = workload_spec(
            &cluster_id(),
            &assignment,
            &deployment,
            assignment.spec.workload_address,
            Vec::new(),
            Vec::new(),
        )?;
        let handle = runtime.create(&spec).await?;
        runtime.start(&handle).await?;
        let runtime_trait: Arc<dyn WorkloadRuntime> = runtime;
        let service = NodeExecService::new(
            store.clone(),
            runtime_trait,
            NodeExecSettings {
                cluster_id: cluster_id(),
                node_id: node_id(service_node),
                maximum_sessions,
            },
        )?;
        Ok(Self {
            store,
            service,
            assignment,
        })
    }
}

async fn seed(
    store: &dyn Store,
    assignment: &Assignment,
    deployment: &Deployment,
) -> Result<(), Box<dyn std::error::Error>> {
    put_resource(
        store,
        "Assignment",
        assignment.meta.id.as_str(),
        assignment,
        ExpectedVersion::Missing,
    )
    .await?;
    put_resource(
        store,
        "Deployment",
        deployment.meta.id.as_str(),
        deployment,
        ExpectedVersion::Missing,
    )
    .await
}

async fn put_resource<T: serde::Serialize>(
    store: &dyn Store,
    kind: &str,
    id: &str,
    resource: &T,
    expected: ExpectedVersion,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = resource_key(kind, id)?;
    store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(resource)?,
            expected,
            session: None,
        })
        .await?;
    Ok(())
}

fn resource_key(
    kind: &str,
    id: &str,
) -> Result<kernel_store::StoreKey, kernel_api::InvalidIdentifier> {
    Ok(Keyspace::new(&cluster_id()).resource(&ResourceKind::new(kind)?, &ResourceName::new(id)?))
}
