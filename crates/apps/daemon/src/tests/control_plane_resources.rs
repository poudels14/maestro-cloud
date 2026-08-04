use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};

use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    ClusterId, Deployment, DeploymentGoal, DeploymentId, DeploymentPhase, DeploymentSpec,
    DeploymentStatus, DnsRecord, DnsRecordId, DnsRecordSpec, DnsRecordStatus, DnsRecordValue,
    ExecPolicy, Generation, HealthCheckSpec, HealthProbe, Node, NodeApiAccess, NodeFirewall,
    NodeFirewallId, NodeFirewallSpec, NodeFirewallStatus, NodeId, Object, ObjectMeta,
    PlacementConstraint, ReplicaState, ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus,
    ResourceKind, ResourceName, ResourceRevision, ServiceId, ServiceSpec, Timestamp,
    WorkloadNetworkMode, WorkloadUserSpec,
};
use kernel_store::{CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store};

use crate::dns_resources::DNS_RESOLVER_SERVICE_ID;

pub(super) async fn seed_agent_resources(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    node_id: &NodeId,
    workload_subnet: cluster::Ipv4Cidr,
    network_mode: WorkloadNetworkMode,
    node_api_user: Option<WorkloadUserSpec>,
) -> Result<(), Box<dyn std::error::Error>> {
    let workload_address = workload_subnet
        .workload_addresses()
        .next()
        .ok_or("workload subnet has no assignable address")?;
    let assignment = workload_assignment(node_id, workload_address, network_mode)?;
    let replica = replica_state(&assignment)?;
    let mut resources = vec![
        (
            "NodeFirewall",
            node_id.as_str(),
            serde_json::to_vec(&firewall(node_id)?)?,
        ),
        (
            "Deployment",
            "deployment-1",
            serde_json::to_vec(&deployment(node_api_user)?)?,
        ),
        (
            "Assignment",
            "assignment-1",
            serde_json::to_vec(&assignment)?,
        ),
        ("ReplicaState", "replica-1", serde_json::to_vec(&replica)?),
    ];
    if network_mode == WorkloadNetworkMode::ClusterRouted {
        resources.push((
            "DnsRecord",
            "api",
            serde_json::to_vec(&dns_record(workload_address)?)?,
        ));
    } else {
        let (resolver_deployment, resolver_assignment, resolver_replica) =
            delegated_dns_resources(node_id, workload_address)?;
        resources.extend([
            (
                "Deployment",
                "maestro-system-dns-deployment",
                serde_json::to_vec(&resolver_deployment)?,
            ),
            (
                "Assignment",
                "maestro-system-dns-assignment",
                serde_json::to_vec(&resolver_assignment)?,
            ),
            (
                "ReplicaState",
                "maestro-system-dns-replica",
                serde_json::to_vec(&resolver_replica)?,
            ),
        ]);
    }
    for (kind, id, value) in resources {
        put(store, cluster_id, kind, id, value).await?;
    }
    Ok(())
}

fn delegated_dns_resources(
    node_id: &NodeId,
    workload_address: Ipv4Addr,
) -> Result<(Deployment, Assignment, ReplicaState), kernel_api::InvalidIdentifier> {
    let service_id = ServiceId::new(DNS_RESOLVER_SERVICE_ID)?;
    let mut deployment = deployment(None)?;
    deployment.meta.id = DeploymentId::new("maestro-system-dns-deployment")?;
    deployment.spec.service_id = service_id.clone();
    deployment.spec.service.name = "Maestro DNS Resolver".to_owned();
    deployment.status.phase = DeploymentPhase::Ready;
    deployment.status.ready_at = Some(Timestamp(1_750_000_000_000));

    let mut assignment = workload_assignment(
        node_id,
        workload_address,
        WorkloadNetworkMode::RuntimeDelegated,
    )?;
    assignment.meta.id = AssignmentId::new("maestro-system-dns-assignment")?;
    assignment.spec.service_id = service_id;
    assignment.spec.deployment_id = deployment.meta.id.clone();

    let mut replica = replica_state(&assignment)?;
    replica.meta.id = ReplicaStateId::new("maestro-system-dns-replica")?;
    Ok((deployment, assignment, replica))
}

pub(super) async fn load_assignment(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
) -> Result<Assignment, Box<dyn std::error::Error>> {
    let key = Keyspace::new(cluster_id).resource(
        &ResourceKind::new("Assignment")?,
        &ResourceName::new("assignment-1")?,
    );
    let stored = store.get(&key).await?.ok_or("assignment was not stored")?;
    Ok(serde_json::from_slice(&stored.value)?)
}

pub(super) async fn load_node(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    node_id: &NodeId,
) -> Result<Node, Box<dyn std::error::Error>> {
    let key = Keyspace::new(cluster_id).resource(
        &ResourceKind::new("Node")?,
        &ResourceName::new(node_id.as_str())?,
    );
    let stored = store.get(&key).await?.ok_or("node was not stored")?;
    Ok(serde_json::from_slice(&stored.value)?)
}

pub(super) async fn load_replica(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
) -> Result<ReplicaState, Box<dyn std::error::Error>> {
    let key = Keyspace::new(cluster_id).resource(
        &ResourceKind::new("ReplicaState")?,
        &ResourceName::new("replica-1")?,
    );
    let stored = store.get(&key).await?.ok_or("replica was not stored")?;
    Ok(serde_json::from_slice(&stored.value)?)
}

async fn put(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    kind: &str,
    id: &str,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = store
        .put_cas(PutRequest {
            key: Keyspace::new(cluster_id)
                .resource(&ResourceKind::new(kind)?, &ResourceName::new(id)?),
            value,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err(format!("{kind} `{id}` create conflicted").into())
    }
}

fn firewall(node_id: &NodeId) -> Result<NodeFirewall, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(NodeFirewallId::new(node_id.as_str())?),
        spec: NodeFirewallSpec {
            node_id: node_id.clone(),
            table_name: "maestro_firewall".to_owned(),
            script: String::new(),
            digest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_owned(),
        },
        status: NodeFirewallStatus {
            applied_generation: Generation::default(),
            applied_digest: None,
            conditions: Vec::new(),
        },
    })
}

fn dns_record(address: Ipv4Addr) -> Result<DnsRecord, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(DnsRecordId::new("api")?),
        spec: DnsRecordSpec {
            name: "api.maestro.internal.".to_owned(),
            values: vec![DnsRecordValue::A(address)],
            ttl_secs: 30,
        },
        status: DnsRecordStatus {
            applied_generation: Generation::default(),
            published_nodes: Vec::new(),
            conditions: Vec::new(),
        },
    })
}

fn workload_assignment(
    node_id: &NodeId,
    workload_address: Ipv4Addr,
    network_mode: WorkloadNetworkMode,
) -> Result<Assignment, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(AssignmentId::new("assignment-1")?),
        spec: AssignmentSpec {
            service_id: ServiceId::new("api")?,
            deployment_id: DeploymentId::new("deployment-1")?,
            restart_generation: Generation(1),
            replica_index: 0,
            node_id: node_id.clone(),
            placement_epoch: 1,
            workload_address: match network_mode {
                WorkloadNetworkMode::ClusterRouted => Some(IpAddr::V4(workload_address)),
                WorkloadNetworkMode::RuntimeDelegated => None,
            },
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Pending,
            workload_id: None,
            workload_address: None,
            conditions: Vec::new(),
        },
    })
}

fn replica_state(assignment: &Assignment) -> Result<ReplicaState, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(ReplicaStateId::new("replica-1")?),
        spec: ReplicaStateSpec {
            service_id: assignment.spec.service_id.clone(),
            deployment_id: assignment.spec.deployment_id.clone(),
            assignment_id: assignment.meta.id.clone(),
            replica_index: assignment.spec.replica_index,
        },
        status: ReplicaStateStatus {
            phase: DeploymentPhase::PendingReady,
            node_id: Some(assignment.spec.node_id.clone()),
            workload_id: None,
            healthcheck_failures: 0,
            restart_attempts: 0,
            restart_pending_attempt: None,
            restart_not_before: None,
            resolved_secrets: Default::default(),
            conditions: Vec::new(),
        },
    })
}

fn deployment(
    node_api_user: Option<WorkloadUserSpec>,
) -> Result<Deployment, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(DeploymentId::new("deployment-1")?),
        spec: DeploymentSpec {
            service_id: ServiceId::new("api")?,
            service_generation: Generation(1),
            restart_generation: Generation(1),
            bypass_rollout_freeze: false,
            goal: DeploymentGoal::Run,
            service: ServiceSpec {
                name: "API".to_owned(),
                version: "1.0.0".to_owned(),
                artifact: ArtifactTemplate::Image {
                    reference: "registry.test/api:latest".to_owned(),
                },
                preview: None,
                command: None,
                replicas: 1,
                exposed_ports: vec![8080],
                health_check: Some(HealthCheckSpec {
                    probe: HealthProbe::Http {
                        port: 8080,
                        path: "/ready".to_owned(),
                    },
                    interval_secs: 30,
                    unhealthy_threshold: 3,
                }),
                max_restarts: Some(3),
                environment: BTreeMap::from([("MODE".to_owned(), "production".to_owned())]),
                environment_sources: Vec::new(),
                user: node_api_user,
                node_api: if node_api_user.is_some() {
                    NodeApiAccess::IdentityAndTelemetry
                } else {
                    NodeApiAccess::Disabled
                },
                secrets: None,
                volumes: Vec::new(),
                placement: PlacementConstraint::default(),
                exec: ExecPolicy::Allowed,
            },
            build_id: None,
        },
        status: DeploymentStatus {
            phase: DeploymentPhase::PendingReady,
            created_at: Timestamp(1_750_000_000_000),
            ready_at: None,
            draining_at: None,
            image_digest: Some("registry.test/api@sha256:abc".to_owned()),
            git_commit: None,
            resolved_secrets: Default::default(),
            conditions: Vec::new(),
        },
    })
}

fn metadata<Id>(id: Id) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision::default(),
        generation: Generation(1),
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}
