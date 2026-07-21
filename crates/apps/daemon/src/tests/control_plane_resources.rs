use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};

use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    ClusterId, Deployment, DeploymentGoal, DeploymentId, DeploymentPhase, DeploymentSpec,
    DeploymentStatus, DnsRecord, DnsRecordId, DnsRecordSpec, DnsRecordStatus, DnsRecordValue,
    ExecPolicy, Generation, NodeApiAccess, NodeFirewall, NodeFirewallId, NodeFirewallSpec,
    NodeFirewallStatus, NodeId, Object, ObjectMeta, PlacementConstraint, ResourceKind,
    ResourceName, ResourceRevision, ServiceId, ServiceSpec, Timestamp,
};
use kernel_store::{CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store};

pub(super) async fn seed_agent_resources(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    node_id: &NodeId,
    workload_subnet: cluster::Ipv4Cidr,
) -> Result<(), Box<dyn std::error::Error>> {
    let workload_address = workload_subnet
        .workload_addresses()
        .next()
        .ok_or("workload subnet has no assignable address")?;
    let assignment = workload_assignment(node_id, workload_address)?;
    let resources = [
        (
            "NodeFirewall",
            node_id.as_str(),
            serde_json::to_vec(&firewall(node_id)?)?,
        ),
        (
            "DnsRecord",
            "api",
            serde_json::to_vec(&dns_record(workload_address)?)?,
        ),
        (
            "Deployment",
            "deployment-1",
            serde_json::to_vec(&deployment()?)?,
        ),
        (
            "Assignment",
            "assignment-1",
            serde_json::to_vec(&assignment)?,
        ),
    ];
    for (kind, id, value) in resources {
        put(store, cluster_id, kind, id, value).await?;
    }
    Ok(())
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
            workload_address: IpAddr::V4(workload_address),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Pending,
            workload_id: None,
            conditions: Vec::new(),
        },
    })
}

fn deployment() -> Result<Deployment, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(DeploymentId::new("deployment-1")?),
        spec: DeploymentSpec {
            service_id: ServiceId::new("api")?,
            service_generation: Generation(1),
            restart_generation: Generation(1),
            goal: DeploymentGoal::Run,
            service: ServiceSpec {
                name: "API".to_owned(),
                version: "1.0.0".to_owned(),
                artifact: ArtifactTemplate::Image {
                    reference: "registry.test/api:latest".to_owned(),
                },
                command: None,
                replicas: 1,
                exposed_ports: vec![8080],
                health_check: None,
                max_restarts: Some(3),
                environment: BTreeMap::from([("MODE".to_owned(), "production".to_owned())]),
                user: None,
                node_api: NodeApiAccess::Disabled,
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
