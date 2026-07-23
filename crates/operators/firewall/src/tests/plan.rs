use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};

use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    DeploymentId, ExecPolicy, FirewallDirection, FirewallPolicy, FirewallPolicyId,
    FirewallPolicySpec, FirewallPolicyStatus, FirewallRule, FirewallSubject, FirewallVerdict,
    Generation, NodeApiAccess, NodeId, NodeNetwork, NodeNetworkId, NodeNetworkSpec,
    NodeNetworkStatus, Object, ObjectMeta, PlacementConstraint, PortRange, ResourceRevision,
    RolloutState, Service, ServiceId, ServiceSpec, ServiceStatus, Timestamp, TransportProtocol,
};

use crate::{FirewallInput, FirewallPlanError, FirewallSettings, plan};

#[test]
fn generated_per_node_rulesets_match_the_reviewed_contract() {
    let output = plan(World::standard().input()).expect("firewall plan");
    let artifact = output
        .rulesets
        .iter()
        .map(|ruleset| {
            format!(
                "node: {}\ndigest: {}\n{}",
                ruleset.node_id, ruleset.digest, ruleset.script
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    insta::assert_snapshot!(artifact);
}

#[test]
fn compilation_is_independent_of_resource_and_set_input_order() {
    let world = World::standard();
    let first = plan(world.input()).unwrap();
    let mut reversed = world.input();
    reversed.policies.reverse();
    reversed.services.reverse();
    reversed.assignments.reverse();
    reversed.node_networks.reverse();
    reversed.settings.protected_host_ports.reverse();
    reversed.settings.control_allow_cidrs.reverse();
    assert_eq!(plan(reversed).unwrap(), first);
}

#[test]
fn service_sources_are_scoped_to_their_local_node_ruleset() {
    let output = plan(World::standard().input()).unwrap();
    let first = output
        .rulesets
        .iter()
        .find(|ruleset| ruleset.node_id.as_str() == "node-1")
        .unwrap();
    let second = output
        .rulesets
        .iter()
        .find(|ruleset| ruleset.node_id.as_str() == "node-2")
        .unwrap();
    assert!(first.script.contains("10.42.1.10"));
    assert!(!first.script.contains("10.42.2.10"));
    assert!(second.script.contains("10.42.2.10"));
    assert!(!second.script.contains("10.42.1.10"));
}

#[test]
fn policy_status_acknowledges_the_exact_complete_bundle_digest() {
    let mut world = World::standard();
    let first = plan(world.input()).unwrap();
    assert_eq!(first.policy_updates.len(), 3);
    for policy in &mut world.policies {
        policy.status.applied_generation = policy.meta.generation;
        policy.status.ruleset_digest = Some(first.bundle_digest.clone());
    }
    assert!(plan(world.input()).unwrap().policy_updates.is_empty());
}

#[test]
fn deleting_policy_is_removed_from_rules_before_finalization() {
    let mut world = World::standard();
    let policy = world
        .policies
        .iter_mut()
        .find(|policy| policy.meta.id.as_str() == "api-egress")
        .unwrap();
    policy.meta.deletion_timestamp = Some(Timestamp(50_000));
    let output = plan(world.input()).unwrap();
    assert!(
        output
            .rulesets
            .iter()
            .all(|ruleset| !ruleset.script.contains("192.0.2.0/24"))
    );
    assert_eq!(output.policy_updates.len(), 2);
}

#[test]
fn system_dns_and_control_guards_precede_user_policy() {
    let output = plan(World::standard().input()).unwrap();
    let script = &output.rulesets[0].script;
    let system = script.find("ip saddr @system_sources_v4 accept").unwrap();
    let global = script.find("iifname \"maestro0\" jump").unwrap();
    assert!(system < global);
    let dns = script
        .find("ip saddr @local_workloads_v4 ip daddr 10.42.1.1 tcp dport 53 accept")
        .unwrap();
    let workload_reject = script.find("ip saddr @all_workloads_v4 reject").unwrap();
    assert!(dns < workload_reject);
    let control_allow = script.find("tcp dport { 3000, 3001 } accept").unwrap();
    let control_reject = script.find("tcp dport { 3000, 3001 } reject").unwrap();
    assert!(control_allow < control_reject);
}

#[test]
fn malformed_cidrs_subjects_scopes_and_ports_fail_closed() {
    let mut noncanonical = World::standard();
    noncanonical.policies[0].spec.rules[0].cidr = "10.0.0.1/8".to_string();
    assert!(matches!(
        plan(noncanonical.input()),
        Err(FirewallPlanError::InvalidCidr { .. })
    ));

    let mut subject = World::standard();
    subject.policies[0].spec.subject = FirewallSubject::Node(NodeId::new("node-1").unwrap());
    assert!(matches!(
        plan(subject.input()),
        Err(FirewallPlanError::InvalidPolicySubject { .. })
    ));

    let mut duplicate = World::standard();
    let mut policy = duplicate.policies[0].clone();
    policy.meta.id = FirewallPolicyId::new("global-egress-other").unwrap();
    duplicate.policies.push(policy);
    assert!(matches!(
        plan(duplicate.input()),
        Err(FirewallPlanError::DuplicatePolicyScope { .. })
    ));

    let mut ports = World::standard();
    ports.policies[0].spec.rules[0].ports = vec![PortRange {
        start: 443,
        end: 80,
    }];
    assert!(matches!(
        plan(ports.input()),
        Err(FirewallPlanError::InvalidPortRange { .. })
    ));
}

pub(super) struct World {
    settings: FirewallSettings,
    policies: Vec<FirewallPolicy>,
    services: Vec<Service>,
    assignments: Vec<Assignment>,
    networks: Vec<NodeNetwork>,
}

impl World {
    pub(super) fn standard() -> Self {
        let api = service("api");
        let system = service("maestro-dns");
        let settings = FirewallSettings {
            table_name: "maestro_firewall".to_string(),
            workload_interface: "maestro0".to_string(),
            dns_port: 53,
            protected_host_ports: vec![3001, 3000],
            control_allow_cidrs: vec!["fd00::/8".to_string(), "10.0.0.0/8".to_string()],
            system_services: BTreeSet::from([system.meta.id.clone()]),
        };
        let policies = vec![
            policy(
                "global-egress",
                FirewallDirection::Egress,
                FirewallSubject::Global,
                vec![
                    rule(
                        "10.0.0.0/8",
                        TransportProtocol::Tcp,
                        vec![PortRange {
                            start: 443,
                            end: 443,
                        }],
                        FirewallVerdict::Allow,
                    ),
                    rule(
                        "2001:db8::/32",
                        TransportProtocol::Any,
                        vec![PortRange {
                            start: 8_000,
                            end: 8_080,
                        }],
                        FirewallVerdict::Allow,
                    ),
                ],
                FirewallVerdict::Deny,
            ),
            policy(
                "api-egress",
                FirewallDirection::Egress,
                FirewallSubject::Service(api.meta.id.clone()),
                vec![rule(
                    "192.0.2.0/24",
                    TransportProtocol::Tcp,
                    vec![PortRange {
                        start: 5432,
                        end: 5432,
                    }],
                    FirewallVerdict::Allow,
                )],
                FirewallVerdict::Deny,
            ),
            policy(
                "global-host",
                FirewallDirection::HostInput,
                FirewallSubject::Global,
                vec![rule(
                    "10.20.0.0/16",
                    TransportProtocol::Tcp,
                    vec![PortRange { start: 22, end: 22 }],
                    FirewallVerdict::Allow,
                )],
                FirewallVerdict::Allow,
            ),
        ];
        let networks = vec![
            network("node-1", "10.42.1.0/24", 51820),
            network("node-2", "10.42.2.0/24", 51821),
        ];
        let assignments = vec![
            assignment("api-node-1", &api, "node-1", "10.42.1.10"),
            assignment("api-node-2", &api, "node-2", "10.42.2.10"),
            assignment("dns-node-1", &system, "node-1", "10.42.1.250"),
        ];
        Self {
            settings,
            policies,
            services: vec![api, system],
            assignments,
            networks,
        }
    }

    pub(super) fn input(&self) -> FirewallInput {
        FirewallInput {
            settings: self.settings.clone(),
            policies: self.policies.clone(),
            services: self.services.clone(),
            assignments: self.assignments.clone(),
            node_networks: self.networks.clone(),
        }
    }
}

fn policy(
    id: &str,
    direction: FirewallDirection,
    subject: FirewallSubject,
    rules: Vec<FirewallRule>,
    default_verdict: FirewallVerdict,
) -> FirewallPolicy {
    Object {
        meta: metadata(FirewallPolicyId::new(id).unwrap(), Generation(3)),
        spec: FirewallPolicySpec {
            direction,
            subject,
            rules,
            default_verdict,
        },
        status: FirewallPolicyStatus {
            applied_generation: Generation(0),
            ruleset_digest: None,
            conditions: Vec::new(),
        },
    }
}

fn rule(
    cidr: &str,
    protocol: TransportProtocol,
    ports: Vec<PortRange>,
    verdict: FirewallVerdict,
) -> FirewallRule {
    FirewallRule {
        cidr: cidr.to_string(),
        protocol,
        ports,
        verdict,
    }
}

fn network(node_id: &str, subnet: &str, port: u16) -> NodeNetwork {
    let node_id = NodeId::new(node_id).unwrap();
    Object {
        meta: metadata(
            NodeNetworkId::new(format!("network-{node_id}")).unwrap(),
            Generation(1),
        ),
        spec: NodeNetworkSpec {
            node_id,
            public_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            endpoint: SocketAddr::from(([10, 0, 0, 1], port)),
            workload_subnet: subnet.to_string(),
            mtu_bytes: 1420,
        },
        status: NodeNetworkStatus {
            applied_generation: Generation(1),
            conditions: Vec::new(),
        },
    }
}

fn assignment(id: &str, service: &Service, node_id: &str, address: &str) -> Assignment {
    Object {
        meta: metadata(AssignmentId::new(id).unwrap(), Generation(1)),
        spec: AssignmentSpec {
            service_id: service.meta.id.clone(),
            deployment_id: DeploymentId::new(format!("deployment-{}", service.meta.id)).unwrap(),
            restart_generation: Generation(1),
            replica_index: 0,
            node_id: NodeId::new(node_id).unwrap(),
            placement_epoch: 1,
            workload_address: address.parse::<IpAddr>().unwrap(),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Running,
            workload_id: None,
            conditions: Vec::new(),
        },
    }
}

fn service(id: &str) -> Service {
    Object {
        meta: metadata(ServiceId::new(id).unwrap(), Generation(1)),
        spec: ServiceSpec {
            name: id.to_string(),
            version: "1.0.0".to_string(),
            artifact: ArtifactTemplate::Image {
                reference: format!("registry.test/{id}:latest"),
            },
            preview: None,
            command: None,
            replicas: 1,
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
            rollout_bypass_generation: None,
            conditions: Vec::new(),
        },
    }
}

fn metadata<Id>(id: Id, generation: Generation) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision(5),
        generation,
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}
