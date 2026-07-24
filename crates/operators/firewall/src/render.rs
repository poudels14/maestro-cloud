use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{FirewallVerdict, PortRange, ServiceId, TransportProtocol};
use sha2::{Digest, Sha256};

use crate::FirewallRuleset;
use crate::cidr::AddressFamily;
use crate::validation::{SubjectKey, ValidatedInput, ValidatedPolicy};

const LABEL_DOMAIN: &[u8] = b"maestro-firewall-label-v1\0";

pub(crate) fn render(input: &ValidatedInput) -> Vec<FirewallRuleset> {
    input
        .nodes
        .values()
        .map(|node| {
            let mut renderer = Renderer::new(input.settings.table_name.clone());
            render_node(input, node, &mut renderer);
            let script = renderer.finish();
            FirewallRuleset {
                node_id: node.node_id.clone(),
                table_name: input.settings.table_name.clone(),
                digest: digest(script.as_bytes()),
                script,
            }
        })
        .collect()
}

fn render_node(
    input: &ValidatedInput,
    node: &crate::validation::NodeContext,
    renderer: &mut Renderer,
) {
    let all_workloads = input
        .nodes
        .values()
        .map(|node| node.workload_subnet.to_string())
        .collect::<Vec<_>>();
    renderer.add_set("all_workloads_v4", AddressFamily::V4, all_workloads);
    renderer.add_set(
        "local_workloads_v4",
        AddressFamily::V4,
        vec![node.workload_subnet.to_string()],
    );

    let local_assignments = input
        .assignments
        .iter()
        .filter(|assignment| assignment.spec.node_id == node.node_id)
        .collect::<Vec<_>>();
    let system_sources = local_assignments
        .iter()
        .filter(|assignment| {
            input
                .settings
                .system_services
                .contains(&assignment.spec.service_id)
        })
        .filter_map(|assignment| {
            assignment
                .spec
                .workload_address
                .map(|address| address.to_string())
        })
        .collect::<Vec<_>>();
    renderer.add_set("system_sources_v4", AddressFamily::V4, system_sources);

    let mut forward = vec!["ct state established,related accept".to_string()];
    if renderer.has_set("system_sources_v4") {
        forward.push("ip saddr @system_sources_v4 accept".to_string());
    }
    for (key, policy) in &input.policies {
        let SubjectKey::EgressService(service_id) = key else {
            continue;
        };
        let sources = service_sources(&local_assignments, service_id);
        if sources.is_empty() {
            continue;
        }
        let source_set = format!("src_{}_v4", short_hash(&[service_id.as_str()]));
        renderer.add_set(&source_set, AddressFamily::V4, sources);
        let chain = render_policy(renderer, policy, MatchDirection::Destination);
        forward.push(format!("ip saddr @{source_set} jump {chain}"));
    }
    if let Some(policy) = input.policies.get(&SubjectKey::EgressGlobal) {
        let chain = render_policy(renderer, policy, MatchDirection::Destination);
        forward.push(format!(
            "iifname \"{}\" jump {chain}",
            input.settings.workload_interface
        ));
    }
    renderer.add_hook_chain("forward", "forward", -50, "accept", forward);

    let mut input_rules = vec!["ct state established,related accept".to_string()];
    input_rules.extend([
        format!(
            "ip saddr @local_workloads_v4 ip daddr {} tcp dport {} accept",
            node.bridge_address, input.settings.dns_port
        ),
        format!(
            "ip saddr @local_workloads_v4 ip daddr {} udp dport {} accept",
            node.bridge_address, input.settings.dns_port
        ),
    ]);
    render_control_protection(input, renderer, &mut input_rules);
    input_rules.push("ip saddr @all_workloads_v4 reject".to_string());
    let host_policy = input
        .policies
        .get(&SubjectKey::HostNode(node.node_id.clone()))
        .or_else(|| input.policies.get(&SubjectKey::HostGlobal));
    if let Some(policy) = host_policy {
        let chain = render_policy(renderer, policy, MatchDirection::Source);
        input_rules.push(format!("jump {chain}"));
    }
    renderer.add_hook_chain("input", "input", -50, "accept", input_rules);
}

fn service_sources(assignments: &[&kernel_api::Assignment], service_id: &ServiceId) -> Vec<String> {
    assignments
        .iter()
        .filter(|assignment| &assignment.spec.service_id == service_id)
        .filter_map(|assignment| {
            assignment
                .spec
                .workload_address
                .map(|address| address.to_string())
        })
        .collect()
}

fn render_control_protection(
    input: &ValidatedInput,
    renderer: &mut Renderer,
    rules: &mut Vec<String>,
) {
    if input.settings.protected_host_ports.is_empty() {
        return;
    }
    let ports = render_ports(
        &input
            .settings
            .protected_host_ports
            .iter()
            .map(|port| PortRange {
                start: *port,
                end: *port,
            })
            .collect::<Vec<_>>(),
    );
    for family in [AddressFamily::V4, AddressFamily::V6] {
        let cidrs = input
            .control_cidrs
            .iter()
            .filter(|cidr| cidr.family() == family)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let set = match family {
            AddressFamily::V4 => "control_allow_v4",
            AddressFamily::V6 => "control_allow_v6",
        };
        renderer.add_set(set, family, cidrs);
        if renderer.has_set(set) {
            rules.push(format!(
                "{} saddr @{set} tcp dport {ports} accept",
                family_expression(family)
            ));
        }
    }
    rules.push(format!("tcp dport {ports} reject"));
}

fn render_policy(
    renderer: &mut Renderer,
    policy: &ValidatedPolicy,
    direction: MatchDirection,
) -> String {
    let policy_hash = short_hash(&[policy.resource.meta.id.as_str()]);
    let chain = format!("policy_{policy_hash}");
    if renderer.has_chain(&chain) {
        return chain;
    }
    let mut rules = Vec::new();
    for (index, rule) in policy.rules.iter().enumerate() {
        let family = rule.cidr.family();
        let target_set = format!("p_{policy_hash}_r_{index}");
        renderer.add_set(&target_set, family, vec![rule.cidr.to_string()]);
        let address_match = format!(
            "{} {} @{target_set}",
            family_expression(family),
            direction.field()
        );
        let verdict = verdict(rule.verdict);
        match (rule.protocol, rule.ports.is_empty()) {
            (_, true) => rules.push(format!("{address_match} {verdict}")),
            (TransportProtocol::Tcp, false) => rules.push(format!(
                "{address_match} tcp dport {} {verdict}",
                render_ports(&rule.ports)
            )),
            (TransportProtocol::Udp, false) => rules.push(format!(
                "{address_match} udp dport {} {verdict}",
                render_ports(&rule.ports)
            )),
            (TransportProtocol::Any, false) => {
                let ports = render_ports(&rule.ports);
                rules.push(format!("{address_match} tcp dport {ports} {verdict}"));
                rules.push(format!("{address_match} udp dport {ports} {verdict}"));
            }
        }
    }
    rules.push(verdict(policy.resource.spec.default_verdict).to_string());
    renderer.add_chain(&chain, rules);
    chain
}

fn render_ports(ranges: &[PortRange]) -> String {
    let values = ranges
        .iter()
        .map(|range| {
            if range.start == range.end {
                range.start.to_string()
            } else {
                format!("{}-{}", range.start, range.end)
            }
        })
        .collect::<Vec<_>>();
    if let [value] = values.as_slice() {
        value.clone()
    } else {
        format!("{{ {} }}", values.join(", "))
    }
}

const fn family_expression(family: AddressFamily) -> &'static str {
    match family {
        AddressFamily::V4 => "ip",
        AddressFamily::V6 => "ip6",
    }
}

const fn verdict(verdict: FirewallVerdict) -> &'static str {
    match verdict {
        FirewallVerdict::Allow => "accept",
        FirewallVerdict::Deny => "reject",
    }
}

#[derive(Debug, Clone, Copy)]
enum MatchDirection {
    Source,
    Destination,
}

impl MatchDirection {
    const fn field(self) -> &'static str {
        match self {
            Self::Source => "saddr",
            Self::Destination => "daddr",
        }
    }
}

struct Renderer {
    table_name: String,
    sets: BTreeMap<String, SetDefinition>,
    chains: BTreeMap<String, ChainDefinition>,
}

impl Renderer {
    fn new(table_name: String) -> Self {
        Self {
            table_name,
            sets: BTreeMap::new(),
            chains: BTreeMap::new(),
        }
    }

    fn add_set(&mut self, name: &str, family: AddressFamily, elements: Vec<String>) {
        let elements = elements.into_iter().collect::<BTreeSet<_>>();
        if elements.is_empty() {
            return;
        }
        self.sets
            .insert(name.to_string(), SetDefinition { family, elements });
    }

    fn has_set(&self, name: &str) -> bool {
        self.sets.contains_key(name)
    }

    fn add_chain(&mut self, name: &str, rules: Vec<String>) {
        self.chains
            .insert(name.to_string(), ChainDefinition { hook: None, rules });
    }

    fn has_chain(&self, name: &str) -> bool {
        self.chains.contains_key(name)
    }

    fn add_hook_chain(
        &mut self,
        name: &str,
        hook: &str,
        priority: i32,
        policy: &str,
        rules: Vec<String>,
    ) {
        self.chains.insert(
            name.to_string(),
            ChainDefinition {
                hook: Some(format!(
                    "type filter hook {hook} priority {priority}; policy {policy};"
                )),
                rules,
            },
        );
    }

    fn finish(self) -> String {
        let mut script = format!(
            "destroy table inet {}\ntable inet {} {{\n",
            self.table_name, self.table_name
        );
        for (name, set) in self.sets {
            script.push_str(&format!(
                "    set {name} {{\n        type {}\n        flags interval\n        elements = {{ {} }}\n    }}\n\n",
                match set.family {
                    AddressFamily::V4 => "ipv4_addr",
                    AddressFamily::V6 => "ipv6_addr",
                },
                set.elements.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }
        for (name, chain) in self.chains {
            script.push_str(&format!("    chain {name} {{\n"));
            if let Some(hook) = chain.hook {
                script.push_str(&format!("        {hook}\n"));
            }
            for rule in chain.rules {
                script.push_str(&format!("        {rule}\n"));
            }
            script.push_str("    }\n\n");
        }
        script.push_str("}\n");
        script
    }
}

struct SetDefinition {
    family: AddressFamily,
    elements: BTreeSet<String>,
}

struct ChainDefinition {
    hook: Option<String>,
    rules: Vec<String>,
}

fn short_hash(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    hash.update(LABEL_DOMAIN);
    for part in parts {
        hash.update(part.as_bytes());
        hash.update([0]);
    }
    hash.finalize()
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(bytes);
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn bundle_digest(rulesets: &[FirewallRuleset]) -> String {
    let mut bytes = Vec::new();
    for ruleset in rulesets {
        bytes.extend_from_slice(ruleset.node_id.as_str().as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(ruleset.script.as_bytes());
        bytes.push(0);
    }
    digest(&bytes)
}
