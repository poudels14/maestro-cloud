use maestro_core::{
    Assignment, ByteSize, DeploySpec, Deployment, DeploymentId, DeploymentStatus,
    EffectiveResources, Millicores, NodeCapacity, NodeId, NodeRecord, NodeRole, NodeState,
    ReplicaId, ResolvedRequest, ServiceId, ServiceSpec,
};
use maestro_store::{ChangeKey, Mutation, StateMachine, StoreError};
use tempfile::TempDir;

fn open_store() -> (TempDir, StateMachine) {
    let directory = tempfile::tempdir().unwrap();
    let store = StateMachine::open(directory.path().join("state.redb")).unwrap();
    (directory, store)
}

fn service_spec(name: &str) -> ServiceSpec {
    ServiceSpec {
        name: name.to_string(),
        image: Some("nginx:1.27".to_string()),
        build: None,
        deploy: DeploySpec::default(),
        ingress: None,
    }
}

fn deployment(service: &str, id: &str, created_at_ms: u64, status: DeploymentStatus) -> Deployment {
    Deployment {
        id: DeploymentId::from(id),
        service_id: ServiceId::from(service),
        spec: service_spec(service),
        status,
        image: None,
        failure: None,
        created_at_ms,
        updated_at_ms: created_at_ms,
    }
}

fn put_service(id: &str) -> Mutation {
    Mutation::PutService {
        id: ServiceId::from(id),
        spec: service_spec(id),
    }
}

fn assignment(node: u64, replica: &str) -> Assignment {
    Assignment {
        replica_id: ReplicaId::from(replica),
        service_id: ServiceId::from("api"),
        deployment_id: DeploymentId::from("d1"),
        node_id: NodeId(node),
        index: 0,
        image: "nginx:1.27".to_string(),
        resources: EffectiveResources {
            cpu_limit: Millicores::new(4000),
            memory_limit: ByteSize::new(4 << 30),
            request: ResolvedRequest {
                cpu: Millicores::new(250),
                memory: ByteSize::new(512 << 20),
            },
        },
    }
}

fn node(id: u64) -> NodeRecord {
    NodeRecord {
        id: NodeId(id),
        name: format!("node{id}"),
        role: NodeRole::Voter,
        endpoint: format!("10.0.0.{id}:7400"),
        subnet: Some(format!("10.200.{id}.0/24")),
        runs_workloads: true,
        capacity: NodeCapacity {
            cpu: Millicores::new(16_000),
            memory: ByteSize::new(64 << 30),
        },
        reserved: NodeCapacity {
            cpu: Millicores::new(1000),
            memory: ByteSize::new(4 << 30),
        },
        labels: Default::default(),
        state: NodeState::Active,
    }
}

#[test]
fn applies_batches_atomically_and_bumps_version_once() {
    let (_directory, store) = open_store();
    let version = store
        .apply(&[put_service("api"), put_service("web")])
        .unwrap();
    assert_eq!(version, 1);
    assert_eq!(store.services().unwrap().len(), 2);

    let failed = store.apply(&[
        put_service("worker"),
        Mutation::CreateDeployment {
            deployment: deployment("missing", "d1", 1, DeploymentStatus::Queued),
        },
    ]);
    assert!(matches!(failed, Err(StoreError::ServiceNotFound(_))));
    assert_eq!(store.version().unwrap(), 1);
    assert!(store.service(&ServiceId::from("worker")).unwrap().is_none());
}

#[test]
fn indexes_deployments_by_status() {
    let (_directory, store) = open_store();
    store
        .apply(&[
            put_service("api"),
            put_service("web"),
            Mutation::CreateDeployment {
                deployment: deployment("api", "d1", 1, DeploymentStatus::Queued),
            },
            Mutation::CreateDeployment {
                deployment: deployment("web", "d2", 2, DeploymentStatus::Queued),
            },
        ])
        .unwrap();
    store
        .apply(&[Mutation::SetDeploymentStatus {
            service_id: ServiceId::from("api"),
            deployment_id: DeploymentId::from("d1"),
            status: DeploymentStatus::Ready,
            image: Some("registry/api:d1".to_string()),
            failure: None,
            at_ms: 10,
        }])
        .unwrap();

    let queued = store
        .deployments_with_status(DeploymentStatus::Queued)
        .unwrap();
    assert_eq!(
        queued
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["d2"]
    );

    let ready = store
        .deployments_with_status(DeploymentStatus::Ready)
        .unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].image.as_deref(), Some("registry/api:d1"));
    assert_eq!(ready[0].updated_at_ms, 10);
}

#[test]
fn rejects_duplicate_and_unknown_deployments() {
    let (_directory, store) = open_store();
    let create = Mutation::CreateDeployment {
        deployment: deployment("api", "d1", 1, DeploymentStatus::Queued),
    };
    store.apply(&[put_service("api"), create.clone()]).unwrap();
    assert!(matches!(
        store.apply(&[create]),
        Err(StoreError::DeploymentExists { .. })
    ));
    assert!(matches!(
        store.apply(&[Mutation::SetDeploymentStatus {
            service_id: ServiceId::from("api"),
            deployment_id: DeploymentId::from("nope"),
            status: DeploymentStatus::Ready,
            image: None,
            failure: None,
            at_ms: 1,
        }]),
        Err(StoreError::DeploymentNotFound { .. })
    ));
}

#[test]
fn prunes_only_old_terminal_deployments() {
    let (_directory, store) = open_store();
    store
        .apply(&[
            put_service("api"),
            Mutation::CreateDeployment {
                deployment: deployment("api", "d1", 1, DeploymentStatus::Stopped),
            },
            Mutation::CreateDeployment {
                deployment: deployment("api", "d2", 2, DeploymentStatus::Ready),
            },
            Mutation::CreateDeployment {
                deployment: deployment("api", "d3", 3, DeploymentStatus::Failed),
            },
            Mutation::CreateDeployment {
                deployment: deployment("api", "d4", 4, DeploymentStatus::Ready),
            },
        ])
        .unwrap();
    store
        .apply(&[Mutation::PruneDeployments {
            service_id: ServiceId::from("api"),
            keep: 1,
        }])
        .unwrap();

    let remaining = store.deployments(&ServiceId::from("api")).unwrap();
    assert_eq!(
        remaining
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["d2", "d4"]
    );
    assert!(
        store
            .deployments_with_status(DeploymentStatus::Stopped)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn deleting_service_removes_its_deployments() {
    let (_directory, store) = open_store();
    store
        .apply(&[
            put_service("api"),
            Mutation::CreateDeployment {
                deployment: deployment("api", "d1", 1, DeploymentStatus::Queued),
            },
        ])
        .unwrap();
    store
        .apply(&[Mutation::DeleteService {
            id: ServiceId::from("api"),
        }])
        .unwrap();
    assert!(
        store
            .deployments(&ServiceId::from("api"))
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .deployments_with_status(DeploymentStatus::Queued)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn replaces_assignments_per_node() {
    let (_directory, store) = open_store();
    store
        .apply(&[
            Mutation::PutNode { node: node(1) },
            Mutation::PutNode { node: node(2) },
            Mutation::SetNodeAssignments {
                node_id: NodeId(1),
                assignments: vec![assignment(1, "d1-0"), assignment(1, "d1-1")],
            },
            Mutation::SetNodeAssignments {
                node_id: NodeId(2),
                assignments: vec![assignment(2, "d1-2")],
            },
        ])
        .unwrap();
    store
        .apply(&[Mutation::SetNodeAssignments {
            node_id: NodeId(1),
            assignments: vec![assignment(1, "d1-1")],
        }])
        .unwrap();

    let node_one = store.assignments(NodeId(1)).unwrap();
    assert_eq!(node_one.len(), 1);
    assert_eq!(node_one[0].replica_id.as_str(), "d1-1");
    assert_eq!(store.assignments(NodeId(2)).unwrap().len(), 1);

    store
        .apply(&[Mutation::RemoveNode { id: NodeId(2) }])
        .unwrap();
    assert!(store.assignments(NodeId(2)).unwrap().is_empty());
    assert!(store.node(NodeId(2)).unwrap().is_none());
    assert_eq!(store.nodes().unwrap().len(), 1);
}

#[tokio::test]
async fn publishes_changes_with_versions() {
    let (_directory, store) = open_store();
    let mut changes = store.subscribe();
    store.apply(&[put_service("api")]).unwrap();
    store
        .apply(&[Mutation::SetNodeAssignments {
            node_id: NodeId(3),
            assignments: vec![],
        }])
        .unwrap();

    let first = changes.recv().await.unwrap();
    assert_eq!(first.version, 1);
    assert_eq!(
        first.keys,
        [ChangeKey::Service {
            id: ServiceId::from("api")
        }]
    );
    let second = changes.recv().await.unwrap();
    assert_eq!(second.version, 2);
    assert_eq!(
        second.keys,
        [ChangeKey::NodeAssignments { node_id: NodeId(3) }]
    );
}

#[test]
fn persists_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.redb");
    {
        let store = StateMachine::open(&path).unwrap();
        store.apply(&[put_service("api")]).unwrap();
    }
    let reopened = StateMachine::open(&path).unwrap();
    assert_eq!(reopened.version().unwrap(), 1);
    assert_eq!(
        reopened.service(&ServiceId::from("api")).unwrap(),
        Some(service_spec("api"))
    );
}
