use maestro_core::{
    Assignment, ByteSize, Deployment, DeploymentId, DeploymentStatus, EffectiveResources,
    Millicores, NodeCapacity, NodeId, NodeRecord, NodeRole, NodeState, ReplicaId, ResolvedRequest,
    ServiceId, ServiceSpec,
};
use maestro_store::{DiscoveryKeys, Mutation, SecretKey, StateMachine, replica_plans};

fn spec(service: &str) -> ServiceSpec {
    serde_json::from_value(serde_json::json!({ "name": service, "image": "nginx" })).unwrap()
}

fn assignment(service: &str) -> Assignment {
    let deployment_id = DeploymentId::from(format!("{service}-d1").as_str());
    Assignment {
        replica_id: ReplicaId::for_deployment(&deployment_id, 0),
        service_id: ServiceId::from(service),
        deployment_id,
        node_id: NodeId(1),
        index: 0,
        image: "nginx".to_string(),
        resources: EffectiveResources {
            cpu_limit: Millicores::new(1000),
            memory_limit: ByteSize::new(1 << 30),
            request: ResolvedRequest {
                cpu: Millicores::new(100),
                memory: ByteSize::new(64 << 20),
            },
        },
        retiring_since_ms: None,
    }
}

#[test]
fn gives_every_replica_its_node_discovery_endpoint_and_key() {
    let directory = tempfile::tempdir().unwrap();
    let state = StateMachine::open(directory.path().join("state.redb")).unwrap();
    let key = SecretKey::derive("cluster-secret");
    let discovery_keys = DiscoveryKeys::new("jwt-secret");
    let node = NodeRecord {
        id: NodeId(1),
        name: "node1".to_string(),
        role: NodeRole::Voter,
        endpoint: "10.0.0.1:7400".to_string(),
        subnet: Some("10.50.3.0/24".to_string()),
        runs_workloads: true,
        capacity: NodeCapacity {
            cpu: Millicores::new(4000),
            memory: ByteSize::new(8 << 30),
        },
        reserved: NodeCapacity {
            cpu: Millicores::new(500),
            memory: ByteSize::new(512 << 20),
        },
        labels: Default::default(),
        state: NodeState::Active,
        wireguard: None,
    };
    let mutations = ["web", "api"]
        .into_iter()
        .flat_map(|service| {
            let assignment = assignment(service);
            [
                Mutation::PutService {
                    id: ServiceId::from(service),
                    spec: spec(service),
                },
                Mutation::CreateDeployment {
                    deployment: Deployment {
                        id: assignment.deployment_id.clone(),
                        service_id: ServiceId::from(service),
                        spec: spec(service),
                        status: DeploymentStatus::Ready,
                        image: Some("nginx".to_string()),
                        failure: None,
                        created_at_ms: 1,
                        updated_at_ms: 1,
                        git_commit: None,
                        built_on: None,
                        version: None,
                        restart_of: None,
                    },
                },
            ]
        })
        .chain([
            Mutation::PutNode { node },
            Mutation::SetNodeAssignments {
                node_id: NodeId(1),
                assignments: vec![assignment("web"), assignment("api")],
            },
        ])
        .collect::<Vec<_>>();
    state.apply(&mutations).unwrap();

    let plans = replica_plans(&state, &key, &discovery_keys, NodeId(1)).unwrap();
    let env = |service: &str| {
        plans
            .iter()
            .find(|plan| plan.service_id == ServiceId::from(service))
            .unwrap()
            .env
            .clone()
    };
    assert_eq!(env("web")["MAESTRO_API_HOST"], "http://10.50.3.1:7480");
    assert_eq!(
        env("web")["MAESTRO_API_KEY"],
        discovery_keys.key(&ServiceId::from("web"))
    );
    assert_eq!(
        env("api")["MAESTRO_API_KEY"],
        discovery_keys.key(&ServiceId::from("api"))
    );
}
