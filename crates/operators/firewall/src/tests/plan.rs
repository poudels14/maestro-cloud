use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    DeploymentId, ExecPolicy, FirewallDirection, FirewallPolicy, FirewallPolicyId,
    FirewallPolicySpec, FirewallPolicyStatus, FirewallRule, FirewallSubject, FirewallVerdict,
    Generation, NodeApiAccess, NodeId, NodeNetwork, NodeNetworkId, NodeNetworkSpec,
    NodeNetworkStatus, Object, ObjectMeta, PlacementConstraint, PortRange, ResourceRevision,
    RolloutState, Service, ServiceId, ServiceSpec, ServiceStatus, Timestamp, TransportProtocol,
};

use crate::{
    FirewallInput, FirewallPlanError, FirewallSettings, HostPortProtocol, HostPortRoute,
    SystemHostAccess, SystemHostEndpoint, plan,
};

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
    reversed.settings.system_host_access.reverse();
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
    let isolation = script
        .find(
            "ip saddr @local_workloads_v4 ip daddr @system_destinations_v4 \
             ct direction original reject",
        )
        .unwrap();
    let established = script.find("ct state established,related accept").unwrap();
    let global = script.find("iifname \"maestro0\" jump").unwrap();
    assert!(system < isolation);
    assert!(isolation < established);
    assert!(established < global);
    let dns = script
        .find("ip saddr @local_workloads_v4 ip daddr 10.42.1.1 tcp dport 53 accept")
        .unwrap();
    let routed_admin = script
        .find("ip saddr @host_access_")
        .expect("system host access");
    let guarded_admin = script
        .find("ip daddr 10.42.1.250 tcp dport 80 reject")
        .expect("Admin endpoint guard");
    let workload_reject = script
        .find("ip saddr @all_workloads_v4 ct direction original reject")
        .unwrap();
    assert!(dns < workload_reject);
    assert!(routed_admin < workload_reject);
    assert!(routed_admin < guarded_admin);
    assert!(guarded_admin < workload_reject);
    let control_allow = script.find("tcp dport { 3000, 3001 } accept").unwrap();
    let control_reject = script.find("tcp dport { 3000, 3001 } reject").unwrap();
    assert!(workload_reject < control_allow);
    assert!(control_allow < control_reject);
}

#[test]
fn system_host_access_trusts_reserved_active_assignments_only() {
    let mut world = World::standard();
    let system = world
        .services
        .iter()
        .find(|service| service.meta.id.as_str() == "maestro-dns")
        .unwrap()
        .clone();

    let mut pending = assignment("dns-pending", &system, "node-2", "10.42.2.30");
    pending.status.phase = AssignmentPhase::Pending;
    pending.status.workload_address = None;
    world.assignments.push(pending);

    let mut draining = assignment("dns-draining", &system, "node-2", "10.42.2.31");
    draining.status.phase = AssignmentPhase::Draining;
    world.assignments.push(draining);

    let mut failed = assignment("dns-failed", &system, "node-2", "10.42.2.32");
    failed.status.phase = AssignmentPhase::Failed;
    world.assignments.push(failed);

    let mut stopped = assignment("dns-stopped", &system, "node-2", "10.42.2.33");
    stopped.status.phase = AssignmentPhase::Stopped;
    world.assignments.push(stopped);

    let mut deleting = assignment("dns-deleting", &system, "node-2", "10.42.2.34");
    deleting.meta.deletion_timestamp = Some(Timestamp(50_000));
    world.assignments.push(deleting);

    let output = plan(world.input()).unwrap();
    let script = &output.rulesets[0].script;
    let start = script.find("set host_access_").unwrap();
    let end = script[start..].find("\n    }\n").unwrap();
    let source_set = &script[start..start + end];
    assert!(source_set.contains("10.42.2.30"));
    assert!(source_set.contains("10.42.2.31"));
    assert!(!source_set.contains("10.42.2.32"));
    assert!(!source_set.contains("10.42.2.33"));
    assert!(!source_set.contains("10.42.2.34"));
}

#[test]
fn user_workloads_cannot_connect_to_local_or_remote_system_assignments() {
    let output = plan(World::standard().input()).unwrap();
    for ruleset in output.rulesets {
        assert!(ruleset.script.contains(
            "set system_destinations_v4 {\n        type ipv4_addr\n        flags interval\n        \
             elements = { 10.42.1.20, 10.42.1.250, 10.42.2.20 }"
        ));
        assert!(ruleset.script.contains(
            "ip saddr @local_workloads_v4 ip daddr @system_destinations_v4 \
             ct direction original reject"
        ));
    }
}

#[test]
fn local_workloads_are_masqueraded_only_when_leaving_cluster_subnets() {
    let output = plan(World::standard().input()).unwrap();
    for ruleset in output.rulesets {
        assert!(
            ruleset
                .script
                .contains("type nat hook postrouting priority srcnat; policy accept;")
        );
        assert!(
            ruleset
                .script
                .contains("ip saddr @local_workloads_v4 ip daddr != @all_workloads_v4 masquerade")
        );
    }
}

#[test]
fn host_ports_route_to_one_running_local_system_assignment_without_capturing_admin() {
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
    assert!(first.script.contains(
        "iifname != \"maestro0\" ip daddr != 10.42.1.250 tcp dport 443 \
                 dnat ip to 10.42.1.20:8443"
    ));
    assert!(!first.script.contains("10.42.2.20:8443"));
    assert!(second.script.contains(
        "iifname != \"maestro0\" ip daddr != 10.42.2.250 tcp dport 443 \
                 dnat ip to 10.42.2.20:8443"
    ));
    assert!(!second.script.contains("10.42.1.20:8443"));
    assert!(first.script.contains("tcp dport 443 reject"));
    assert!(second.script.contains("tcp dport 443 reject"));
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

    let mut addressless = World::standard();
    addressless.assignments[0].spec.workload_address = None;
    assert!(matches!(
        plan(addressless.input()),
        Err(FirewallPlanError::MissingAssignmentAddress { .. })
    ));

    let mut duplicate_route = World::standard();
    duplicate_route
        .settings
        .host_port_routes
        .push(duplicate_route.settings.host_port_routes[0].clone());
    assert!(matches!(
        plan(duplicate_route.input()),
        Err(FirewallPlanError::DuplicateHostPortRoute { .. })
    ));

    let mut protected_route = World::standard();
    protected_route.settings.protected_host_ports.push(443);
    assert!(matches!(
        plan(protected_route.input()),
        Err(FirewallPlanError::HostPortRouteConflictsProtected { port: 443 })
    ));

    let mut ordinary_host_access = World::standard();
    ordinary_host_access.settings.system_host_access[0].service_id = ServiceId::new("api").unwrap();
    assert!(matches!(
        plan(ordinary_host_access.input()),
        Err(FirewallPlanError::SystemHostAccessNotSystem { .. })
    ));

    let mut unprotected_host_access = World::standard();
    unprotected_host_access.settings.system_host_access[0].host_ports = vec![9_999];
    assert!(matches!(
        plan(unprotected_host_access.input()),
        Err(FirewallPlanError::SystemHostAccessPortNotProtected { port: 9_999, .. })
    ));

    let mut noncanonical_system_source = World::standard();
    noncanonical_system_source.settings.system_host_access[0].trusted_source_cidrs =
        vec!["100.64.0.1/10".to_owned()];
    assert!(matches!(
        plan(noncanonical_system_source.input()),
        Err(FirewallPlanError::InvalidCidr { .. })
    ));

    let mut ipv6_system_source = World::standard();
    ipv6_system_source.settings.system_host_access[0].trusted_source_cidrs =
        vec!["fd7a:115c:a1e0::/48".to_owned()];
    assert!(matches!(
        plan(ipv6_system_source.input()),
        Err(FirewallPlanError::InvalidCidr { .. })
    ));
}

#[test]
fn runtime_delegated_assignments_are_absent_from_linux_firewall_input() {
    let mut world = World::standard();
    let mut delegated = world.assignments.remove(0);
    delegated.spec.node_id = NodeId::new("mac-dev").unwrap();
    delegated.spec.workload_address = None;
    delegated.status.workload_address = Some("192.0.2.10".parse().unwrap());
    world.assignments.push(delegated);

    let output = plan(world.input()).expect("delegated assignment is capability-gated");
    assert!(
        output
            .rulesets
            .iter()
            .all(|ruleset| !ruleset.script.contains("192.0.2.10"))
    );
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
        let ingress = service("maestro-system-ingress");
        let settings = FirewallSettings {
            table_name: "maestro_firewall".to_string(),
            workload_interface: "maestro0".to_string(),
            dns_port: 53,
            protected_host_ports: vec![3001, 3000],
            control_allow_cidrs: vec!["fd00::/8".to_string(), "10.0.0.0/8".to_string()],
            system_services: BTreeSet::from([system.meta.id.clone(), ingress.meta.id.clone()]),
            system_host_access: vec![SystemHostAccess {
                service_id: system.meta.id.clone(),
                trusted_source_cidrs: vec!["100.64.0.0/10".to_owned()],
                host_ports: vec![3000],
                endpoints: vec![SystemHostEndpoint {
                    address: Ipv4Addr::new(10, 42, 1, 250),
                    port: 80,
                }],
            }],
            host_port_routes: vec![HostPortRoute {
                service_id: ingress.meta.id.clone(),
                host_port: 443,
                workload_port: 8443,
                protocol: HostPortProtocol::Tcp,
            }],
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
            assignment("ingress-node-1", &ingress, "node-1", "10.42.1.20"),
            assignment("ingress-node-2", &ingress, "node-2", "10.42.2.20"),
        ];
        Self {
            settings,
            policies,
            services: vec![api, system, ingress],
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
            workload_address: Some(address.parse::<IpAddr>().unwrap()),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Running,
            workload_id: None,
            workload_address: address.parse::<IpAddr>().ok(),
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
