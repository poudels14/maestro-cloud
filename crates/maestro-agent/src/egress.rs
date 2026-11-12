use std::collections::BTreeMap;
use std::net::IpAddr;
use std::time::Duration;

use maestro_core::{DiscoveryTable, EgressRule, ReplicaId, ReplicaPlan, ServiceId};
use tokio::sync::watch;

use crate::error::AgentError;
use crate::status::StatusBoard;
use crate::wireguard::CommandRunner;

const LINK_LOCAL_CIDR: &str = "169.254.0.0/16";
const TABLE: &str = "maestro_egress";
const ISOLATION_TABLE: &str = "maestro_isolation";
const RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EgressPolicy {
    pub deny: Vec<String>,
    pub allow: Vec<String>,
    pub local_subnet: String,
    pub cluster_cidr: String,
    pub workload_interfaces: Vec<String>,
    pub ingress_ports: Vec<u16>,
    pub discovery_port: u16,
}

/// Traffic from the rest of the cluster to `targets` (this node's replicas of one service) is
/// refused unless it comes from `sources`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Isolation {
    pub targets: Vec<IpAddr>,
    pub sources: Vec<IpAddr>,
}

pub struct EgressFirewall<R: CommandRunner> {
    runner: R,
    policy: EgressPolicy,
    board: StatusBoard,
    board_changes: watch::Receiver<u64>,
    plans: watch::Receiver<Vec<ReplicaPlan>>,
    node_addresses: watch::Receiver<Vec<IpAddr>>,
    discovery: watch::Receiver<DiscoveryTable>,
    applied: Option<String>,
    attempted_bridge: Option<String>,
}

impl<R: CommandRunner> EgressFirewall<R> {
    pub fn new(
        runner: R,
        policy: EgressPolicy,
        board: StatusBoard,
        plans: watch::Receiver<Vec<ReplicaPlan>>,
        node_addresses: watch::Receiver<Vec<IpAddr>>,
        discovery: watch::Receiver<DiscoveryTable>,
    ) -> Self {
        Self {
            runner,
            policy,
            board_changes: board.subscribe(),
            board,
            plans,
            node_addresses,
            discovery,
            applied: None,
            attempted_bridge: None,
        }
    }

    pub async fn run(mut self) {
        let mut ticker = tokio::time::interval(RECONCILE_INTERVAL);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = self.plans.changed() => {}
                _ = self.node_addresses.changed() => {}
                _ = self.board_changes.changed() => {}
                _ = self.discovery.changed() => {}
            }
            if let Err(error) = self.reconcile().await {
                tracing::warn!(%error, "egress firewall reconcile failed");
            }
        }
    }

    pub async fn reconcile(&mut self) -> Result<(), AgentError> {
        let rules = self
            .plans
            .borrow()
            .iter()
            .map(|plan| (plan.replica_id.clone(), plan.egress.clone()))
            .collect::<BTreeMap<_, _>>();
        let replica_ips = self
            .board
            .snapshot()
            .into_iter()
            .filter_map(|status| Some((status.replica_id, status.ip?)))
            .collect::<BTreeMap<_, _>>();
        let node_addresses = self.node_addresses.borrow().clone();
        let isolation = isolation(&self.discovery.borrow(), &self.board);
        let script = render(
            &self.policy,
            &rules,
            &replica_ips,
            &node_addresses,
            &isolation,
        );
        if self.applied.as_ref() != Some(&script) {
            let output = self
                .runner
                .run("nft", &["-f".to_string(), "-".to_string()], Some(&script))
                .await?;
            match output.success {
                true => {
                    tracing::info!(denied = self.policy.deny.len(), "applied egress firewall");
                    self.applied = Some(script);
                }
                false => Err(AgentError::Host(format!(
                    "nft rejected the egress ruleset: {}",
                    output.stderr.trim()
                )))?,
            }
        }
        let bridge = render_bridge(&self.policy, &isolation);
        if self.attempted_bridge.as_ref() != Some(&bridge) {
            let output = self
                .runner
                .run("nft", &["-f".to_string(), "-".to_string()], Some(&bridge))
                .await?;
            if !output.success {
                tracing::warn!(
                    error = output.stderr.trim(),
                    "this kernel cannot filter traffic between containers on the same node; only traffic from other nodes is restricted"
                );
            }
            self.attempted_bridge = Some(bridge);
        }
        Ok(())
    }
}

pub fn render(
    policy: &EgressPolicy,
    rules: &BTreeMap<ReplicaId, Vec<EgressRule>>,
    replica_ips: &BTreeMap<ReplicaId, IpAddr>,
    node_addresses: &[IpAddr],
    isolation: &[Isolation],
) -> String {
    let reset = format!("table inet {TABLE} {{}}\ndelete table inet {TABLE}\n");
    let isolation_lines = isolation
        .iter()
        .flat_map(|rule| isolation_lines(rule, &policy.cluster_cidr))
        .collect::<Vec<_>>();
    let subnet = &policy.local_subnet;
    let replica_allows = rules.iter().flat_map(|(replica_id, egress)| {
        replica_ips
            .get(replica_id)
            .into_iter()
            .flat_map(move |ip| egress.iter().flat_map(move |rule| allow_lines(ip, rule)))
    });
    let mut lines = vec!["ct state established,related accept".to_string()];
    lines.extend(isolation_lines.iter().cloned());
    lines.push(format!(
        "ip saddr {subnet} ip daddr {} accept",
        policy.cluster_cidr
    ));
    lines.extend(family_lines(
        subnet,
        &node_addresses
            .iter()
            .map(IpAddr::to_string)
            .collect::<Vec<_>>(),
        "reject",
    ));
    lines.extend(family_lines(subnet, &policy.allow, "accept"));
    lines.push(format!(
        "ip saddr {subnet} ip daddr {LINK_LOCAL_CIDR} reject"
    ));
    lines.extend(replica_allows);
    lines.extend(family_lines(subnet, &policy.deny, "reject"));
    let body = lines
        .iter()
        .map(|line| format!("        {line}\n"))
        .collect::<String>();
    format!(
        "{reset}table inet {TABLE} {{\n    chain forward {{\n        type filter hook forward priority -50; policy accept;\n{body}    }}\n{}}}\n",
        host_input_chain(policy)
    )
}

/// Containers on the same node talk across the bridge without passing the inet forward hook, so
/// the isolation rules also run where packets enter the bridge. Applied on its own, so a kernel
/// without bridge filtering keeps the rest of the firewall.
pub fn render_bridge(policy: &EgressPolicy, isolation: &[Isolation]) -> String {
    let reset =
        format!("table bridge {ISOLATION_TABLE} {{}}\ndelete table bridge {ISOLATION_TABLE}\n");
    let lines = isolation
        .iter()
        .flat_map(|rule| isolation_lines(rule, &policy.cluster_cidr))
        .collect::<Vec<_>>();
    match lines.is_empty() {
        true => reset,
        false => {
            let body = std::iter::once("ct state established,related accept".to_string())
                .chain(lines)
                .map(|line| format!("        {line}\n"))
                .collect::<String>();
            format!(
                "{reset}table bridge {ISOLATION_TABLE} {{\n    chain prerouting {{\n        type filter hook prerouting priority -50; policy accept;\n{body}    }}\n}}\n"
            )
        }
    }
}

/// Isolation rules for the replicas on this node, once the control plane has sent which services
/// may reach which; until then (or from a control plane that predates it) nothing is restricted.
fn isolation(table: &DiscoveryTable, board: &StatusBoard) -> Vec<Isolation> {
    match table.always_allowed.is_empty() {
        true => Vec::new(),
        false => board
            .snapshot()
            .into_iter()
            .filter_map(|status| Some((status.service_id, status.ip.filter(IpAddr::is_ipv4)?)))
            .fold(
                BTreeMap::<ServiceId, Vec<IpAddr>>::new(),
                |mut targets, (service_id, ip)| {
                    targets.entry(service_id).or_default().push(ip);
                    targets
                },
            )
            .into_iter()
            .map(|(service_id, targets)| Isolation {
                targets,
                sources: table
                    .allowed_sources(&service_id)
                    .into_iter()
                    .filter(IpAddr::is_ipv4)
                    .collect(),
            })
            .collect(),
    }
}

fn isolation_lines(rule: &Isolation, cluster_cidr: &str) -> Vec<String> {
    let targets = address_set(&rule.targets);
    let accept = (!rule.sources.is_empty()).then(|| {
        format!(
            "ip daddr {targets} ip saddr {} accept",
            address_set(&rule.sources)
        )
    });
    accept
        .into_iter()
        .chain([format!("ip daddr {targets} ip saddr {cluster_cidr} reject")])
        .collect()
}

fn address_set(addresses: &[IpAddr]) -> String {
    format!(
        "{{ {} }}",
        addresses
            .iter()
            .map(IpAddr::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn host_input_chain(policy: &EgressPolicy) -> String {
    match policy.workload_interfaces.is_empty() {
        true => String::new(),
        false => {
            let interfaces = format!(
                "iifname {{ {} }}",
                policy
                    .workload_interfaces
                    .iter()
                    .map(|interface| format!("\"{interface}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let ingress = match policy.ingress_ports.is_empty() {
                true => Vec::new(),
                false => vec![format!(
                    "{interfaces} tcp dport {{ {} }} accept",
                    policy
                        .ingress_ports
                        .iter()
                        .map(u16::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                )],
            };
            let lines = [
                format!("{interfaces} ct state established,related accept"),
                format!("{interfaces} udp dport 53 accept"),
                format!("{interfaces} tcp dport 53 accept"),
                format!("{interfaces} tcp dport {} accept", policy.discovery_port),
                format!("{interfaces} icmp type echo-request accept"),
            ]
            .into_iter()
            .chain(ingress)
            .chain([format!("{interfaces} reject")])
            .map(|line| format!("        {line}\n"))
            .collect::<String>();
            format!(
                "    chain input {{\n        type filter hook input priority -50; policy accept;\n{lines}    }}\n"
            )
        }
    }
}

fn allow_lines(ip: &IpAddr, rule: &EgressRule) -> Vec<String> {
    let base = format!("ip saddr {ip} ip daddr {}", rule.cidr);
    match rule.ports.is_empty() {
        true => vec![format!("{base} accept")],
        false => {
            let ports = rule
                .ports
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            vec![
                format!("{base} tcp dport {{ {ports} }} accept"),
                format!("{base} udp dport {{ {ports} }} accept"),
            ]
        }
    }
}

fn family_lines(subnet: &str, cidrs: &[String], verdict: &str) -> Vec<String> {
    let (v6, v4): (Vec<&String>, Vec<&String>) = cidrs.iter().partition(|cidr| cidr.contains(':'));
    let join = |cidrs: Vec<&String>| {
        cidrs
            .into_iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let v4_line = (!v4.is_empty())
        .then(|| format!("ip saddr {subnet} ip daddr {{ {} }} {verdict}", join(v4)));
    let v6_line = (!v6.is_empty())
        .then(|| format!("ip saddr {subnet} ip6 daddr {{ {} }} {verdict}", join(v6)));
    v4_line.into_iter().chain(v6_line).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(deny: &[&str]) -> EgressPolicy {
        EgressPolicy {
            deny: deny.iter().map(|cidr| cidr.to_string()).collect(),
            allow: vec!["10.20.0.5/32".to_string()],
            local_subnet: "10.200.1.0/24".to_string(),
            cluster_cidr: "10.200.0.0/16".to_string(),
            workload_interfaces: Vec::new(),
            ingress_ports: Vec::new(),
            discovery_port: 7480,
        }
    }

    #[test]
    fn lets_only_allowed_sources_reach_each_service_across_and_within_nodes() {
        let isolation = [Isolation {
            targets: vec!["10.200.1.5".parse().unwrap(), "10.200.1.6".parse().unwrap()],
            sources: vec!["10.200.1.1".parse().unwrap(), "10.200.2.9".parse().unwrap()],
        }];
        let script = render(
            &policy(&[]),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &[],
            &isolation,
        );
        let accept =
            "ip daddr { 10.200.1.5, 10.200.1.6 } ip saddr { 10.200.1.1, 10.200.2.9 } accept";
        let reject = "ip daddr { 10.200.1.5, 10.200.1.6 } ip saddr 10.200.0.0/16 reject";
        assert!(script.find(accept) < script.find(reject));
        assert!(
            script.find(reject)
                < script.find("ip saddr 10.200.1.0/24 ip daddr 10.200.0.0/16 accept")
        );
        assert!(!script.contains("table bridge"));
        let bridge = render_bridge(&policy(&[]), &isolation);
        assert!(bridge.contains("type filter hook prerouting priority -50"));
        assert!(bridge.find("ct state established,related accept") < bridge.find(accept));
        assert!(bridge.find(accept) < bridge.find(reject));
        assert_eq!(
            render_bridge(&policy(&[]), &[]),
            "table bridge maestro_isolation {}\ndelete table bridge maestro_isolation\n"
        );
    }

    #[test]
    fn keeps_workloads_off_host_ports_except_dns_and_ingress() {
        let mut host = policy(&[]);
        host.workload_interfaces = vec!["maestro0".to_string(), "maestro-wg".to_string()];
        host.ingress_ports = vec![80, 443];
        let script = render(&host, &BTreeMap::new(), &BTreeMap::new(), &[], &[]);
        let input = &script[script.find("chain input").unwrap()..];
        assert!(input.contains("iifname { \"maestro0\", \"maestro-wg\" } udp dport 53 accept"));
        assert!(input.contains("tcp dport { 80, 443 } accept"));
        assert!(input.contains("iifname { \"maestro0\", \"maestro-wg\" } tcp dport 7480 accept"));
        assert!(input.contains("iifname { \"maestro0\", \"maestro-wg\" } reject"));
        assert!(input.find("dport { 80, 443 }") < input.find("} reject"));
        assert!(
            !render(&policy(&[]), &BTreeMap::new(), &BTreeMap::new(), &[], &[])
                .contains("chain input")
        );
    }

    #[test]
    fn keeps_workloads_off_other_nodes_even_when_their_range_is_allowed() {
        let mut open = policy(&[]);
        open.allow = vec!["10.1.0.0/16".to_string()];
        let nodes = ["10.1.0.12".parse().unwrap(), "10.1.0.13".parse().unwrap()];
        let script = render(&open, &BTreeMap::new(), &BTreeMap::new(), &nodes, &[]);
        let lines = script.lines().map(str::trim).collect::<Vec<_>>();
        let deny = lines
            .iter()
            .position(|line| {
                *line == "ip saddr 10.200.1.0/24 ip daddr { 10.1.0.12, 10.1.0.13 } reject"
            })
            .unwrap_or_else(|| panic!("missing node deny:\n{script}"));
        let allow = lines
            .iter()
            .position(|line| *line == "ip saddr 10.200.1.0/24 ip daddr { 10.1.0.0/16 } accept")
            .unwrap();
        assert!(deny < allow);
        assert!(!render(&open, &BTreeMap::new(), &BTreeMap::new(), &[], &[]).contains("10.1.0.12"));
    }

    #[test]
    fn blocks_cloud_metadata_even_without_a_deny_list() {
        let script = render(&policy(&[]), &BTreeMap::new(), &BTreeMap::new(), &[], &[]);
        assert!(script.contains("ip saddr 10.200.1.0/24 ip daddr 169.254.0.0/16 reject"));
    }

    #[test]
    fn orders_operator_allows_metadata_block_service_allows_then_denies() {
        let replica = ReplicaId::from("d1-0");
        let rules = BTreeMap::from([(
            replica.clone(),
            vec![EgressRule {
                cidr: "10.0.5.0/24".to_string(),
                ports: vec![5432],
            }],
        )]);
        let ips = BTreeMap::from([(replica, "10.200.1.7".parse().unwrap())]);
        let script = render(&policy(&["10.0.0.0/8", "fd00::/8"]), &rules, &ips, &[], &[]);
        let lines = script.lines().map(str::trim).collect::<Vec<_>>();
        let position = |needle: &str| {
            lines
                .iter()
                .position(|line| *line == needle)
                .unwrap_or_else(|| panic!("missing {needle}:\n{script}"))
        };
        let cluster = position("ip saddr 10.200.1.0/24 ip daddr 10.200.0.0/16 accept");
        let replica_tcp =
            position("ip saddr 10.200.1.7 ip daddr 10.0.5.0/24 tcp dport { 5432 } accept");
        let allow = position("ip saddr 10.200.1.0/24 ip daddr { 10.20.0.5/32 } accept");
        let deny = position("ip saddr 10.200.1.0/24 ip daddr { 10.0.0.0/8 } reject");
        let deny_v6 = position("ip saddr 10.200.1.0/24 ip6 daddr { fd00::/8 } reject");
        let metadata = position("ip saddr 10.200.1.0/24 ip daddr 169.254.0.0/16 reject");
        assert!(cluster < allow && allow < metadata && metadata < replica_tcp);
        assert!(replica_tcp < deny && deny < deny_v6);
        assert!(script.contains("udp dport { 5432 } accept"));
    }
}
