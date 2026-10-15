use std::collections::BTreeMap;
use std::net::IpAddr;
use std::time::Duration;

use maestro_core::{EgressRule, ReplicaId, ReplicaPlan};
use tokio::sync::watch;

use crate::error::AgentError;
use crate::status::StatusBoard;
use crate::wireguard::CommandRunner;

const LINK_LOCAL_CIDR: &str = "169.254.0.0/16";
const TABLE: &str = "maestro_egress";
const RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EgressPolicy {
    pub deny: Vec<String>,
    pub allow: Vec<String>,
    pub local_subnet: String,
    pub cluster_cidr: String,
}

pub struct EgressFirewall<R: CommandRunner> {
    runner: R,
    policy: EgressPolicy,
    board: StatusBoard,
    plans: watch::Receiver<Vec<ReplicaPlan>>,
    applied: Option<String>,
}

impl<R: CommandRunner> EgressFirewall<R> {
    pub fn new(
        runner: R,
        policy: EgressPolicy,
        board: StatusBoard,
        plans: watch::Receiver<Vec<ReplicaPlan>>,
    ) -> Self {
        Self {
            runner,
            policy,
            board,
            plans,
            applied: None,
        }
    }

    pub async fn run(mut self) {
        let mut ticker = tokio::time::interval(RECONCILE_INTERVAL);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = self.plans.changed() => {}
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
        let script = render(&self.policy, &rules, &replica_ips);
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
        Ok(())
    }
}

pub fn render(
    policy: &EgressPolicy,
    rules: &BTreeMap<ReplicaId, Vec<EgressRule>>,
    replica_ips: &BTreeMap<ReplicaId, IpAddr>,
) -> String {
    let reset = format!("table inet {TABLE} {{}}\ndelete table inet {TABLE}\n");
    let subnet = &policy.local_subnet;
    let replica_allows = rules.iter().flat_map(|(replica_id, egress)| {
        replica_ips
            .get(replica_id)
            .into_iter()
            .flat_map(move |ip| egress.iter().flat_map(move |rule| allow_lines(ip, rule)))
    });
    let mut lines = vec![
        "ct state established,related accept".to_string(),
        format!("ip saddr {subnet} ip daddr {} accept", policy.cluster_cidr),
    ];
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
        "{reset}table inet {TABLE} {{\n    chain forward {{\n        type filter hook forward priority -50; policy accept;\n{body}    }}\n}}\n"
    )
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
        }
    }

    #[test]
    fn blocks_cloud_metadata_even_without_a_deny_list() {
        let script = render(&policy(&[]), &BTreeMap::new(), &BTreeMap::new());
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
        let script = render(&policy(&["10.0.0.0/8", "fd00::/8"]), &rules, &ips);
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
