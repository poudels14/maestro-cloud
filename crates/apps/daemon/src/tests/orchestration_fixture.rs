use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;
use firewall::FirewallSettings;
use kernel_api::{
    ArtifactTemplate, Assignment, Condition, ConditionReason, ConditionState, ConditionType,
    DeploymentPhase, ExecPolicy, Generation, IngressRoute, IngressRouteId, IngressRouteSpec,
    IngressRouteStatus, Node, NodeApiAccess, NodeId, NodeInstanceId, NodeNetwork, NodeNetworkId,
    NodeNetworkSpec, NodeNetworkStatus, NodeRole, NodeSpec, NodeStatus, Object, ObjectMeta,
    PlacementConstraint, ReplicaState, ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus,
    ResourceRevision, RolloutState, Service, ServiceId, ServiceSpec, ServiceStatus, Timestamp,
};
use kernel_controller::{Backoff, RuntimeConfig, TimestampClock};
use kernel_store::{Clock, MonotonicTime};

use crate::OperatorSettings;

pub(super) fn settings() -> Result<OperatorSettings, Box<dyn std::error::Error>> {
    Ok(OperatorSettings {
        runtime: RuntimeConfig::new(
            Duration::from_secs(30),
            Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
        )?,
        scheduler: scheduler::SchedulerSettings {
            replacement_grace: Duration::from_secs(30),
            deployment_drain_grace: Duration::from_secs(30),
        },
        deployment: deployment::LifecycleSettings {
            drain_grace: Duration::from_secs(30),
        },
        ingress: ingress::IngressSettings {
            retirement_grace: Duration::from_secs(30),
        },
        dns: dns::DnsSettings { ttl_secs: 5 },
        firewall: FirewallSettings {
            table_name: "maestro_firewall".to_string(),
            workload_interface: "maestro0".to_string(),
            dns_port: 53,
            protected_host_ports: vec![3000, 3001],
            control_allow_cidrs: vec!["10.0.0.0/8".to_string()],
            system_services: BTreeSet::new(),
        },
    })
}

pub(super) fn node(id: &NodeId, index: u8) -> Result<Node, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(id.clone()),
        spec: NodeSpec {
            hostname: format!("{id}.internal"),
            host_address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, index)),
            role: if index == 1 {
                NodeRole::Master
            } else {
                NodeRole::Worker
            },
            scheduling_labels: BTreeMap::new(),
        },
        status: NodeStatus {
            instance_id: NodeInstanceId::new(format!("instance-{id}"))?,
            last_seen: Timestamp(10_000),
            conditions: Vec::new(),
        },
    })
}

pub(super) fn network(
    id: &NodeId,
    index: u8,
) -> Result<NodeNetwork, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(NodeNetworkId::new(format!("network-{id}"))?),
        spec: NodeNetworkSpec {
            node_id: id.clone(),
            public_key: format!("public-key-{index}"),
            endpoint: SocketAddr::from(([10, 0, 0, index], 51_820)),
            workload_subnet: format!("10.42.{index}.0/24"),
            mtu_bytes: 1_420,
        },
        status: NodeNetworkStatus {
            applied_generation: Generation(1),
            conditions: vec![Condition {
                condition_type: ConditionType("MeshReady".to_string()),
                state: ConditionState::True,
                reason: ConditionReason("Applied".to_string()),
                message: "mesh ready".to_string(),
                observed_generation: Generation(1),
                last_transition_time: Timestamp(10_000),
            }],
        },
    })
}

pub(super) fn service(replicas: u32) -> Result<Service, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(ServiceId::new("api")?),
        spec: ServiceSpec {
            name: "API".to_string(),
            version: "1.0.0".to_string(),
            artifact: ArtifactTemplate::Image {
                reference: "registry.test/api:latest".to_string(),
            },
            command: None,
            replicas,
            exposed_ports: vec![8080],
            health_check: None,
            max_restarts: Some(3),
            environment: BTreeMap::new(),
            user: None,
            node_api: NodeApiAccess::Disabled,
            secrets: None,
            volumes: Vec::new(),
            placement: PlacementConstraint::default(),
            exec: ExecPolicy::Allowed,
        },
        status: ServiceStatus {
            active_deployment_id: None,
            replica_override: None,
            rollout: RolloutState::Active,
            conditions: Vec::new(),
        },
    })
}

pub(super) fn route() -> Result<IngressRoute, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(IngressRouteId::new("api-route")?),
        spec: IngressRouteSpec {
            service_id: ServiceId::new("api")?,
            hosts: vec!["api.example.test".to_string()],
            path_prefix: None,
            target_port: 8080,
            session_affinity: None,
        },
        status: IngressRouteStatus {
            applied_generation: Generation::default(),
            conditions: Vec::new(),
        },
    })
}

pub(super) fn ready_replica(
    assignment: &Assignment,
) -> Result<ReplicaState, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(ReplicaStateId::new(format!(
            "replica-{}",
            assignment.meta.id
        ))?),
        spec: ReplicaStateSpec {
            service_id: assignment.spec.service_id.clone(),
            deployment_id: assignment.spec.deployment_id.clone(),
            assignment_id: assignment.meta.id.clone(),
            replica_index: assignment.spec.replica_index,
        },
        status: ReplicaStateStatus {
            phase: DeploymentPhase::Ready,
            node_id: Some(assignment.spec.node_id.clone()),
            workload_id: None,
            healthcheck_failures: 0,
            restart_attempts: 0,
            restart_pending_attempt: None,
            restart_not_before: None,
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

pub(super) struct FixedTimestampClock;

impl TimestampClock for FixedTimestampClock {
    fn now(&self) -> Timestamp {
        Timestamp(10_000)
    }
}

pub(super) struct NoopClock;

#[async_trait]
impl Clock for NoopClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}
