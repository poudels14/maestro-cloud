use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Result, anyhow};
use tokio::process::Command;
use tokio::sync::{Mutex, broadcast};

use crate::error::Error;
use crate::logs::Logger;
use crate::signal::ShutdownEvent;

pub const DEFAULT_TABLE_NAME: &str = "maestro_egress";
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirewallConfig {
    pub table_name: String,
    pub subnet: Option<String>,
    pub deny: Vec<String>,
    pub allow: Vec<String>,
    pub service_allows: Vec<ServiceEgressAllow>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServiceEgressAllow {
    pub service_id: String,
    pub source: Ipv4Addr,
    pub cidr: String,
    pub ports: Vec<u16>,
}

#[derive(Clone)]
pub struct FirewallManager {
    config: std::sync::Arc<Mutex<FirewallConfig>>,
}

impl FirewallManager {
    pub fn new(config: FirewallConfig) -> Self {
        Self {
            config: std::sync::Arc::new(Mutex::new(config)),
        }
    }

    pub async fn apply(&self) -> Result<()> {
        let config = self.config.lock().await;
        apply(&config).await
    }

    pub async fn replace_service_allows(
        &self,
        mut service_allows: Vec<ServiceEgressAllow>,
    ) -> Result<()> {
        service_allows.sort();
        service_allows.dedup();
        let mut config = self.config.lock().await;
        if config.service_allows == service_allows {
            return Ok(());
        }
        let mut updated = config.clone();
        updated.service_allows = service_allows;
        apply(&updated).await?;
        *config = updated;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AddressFamily {
    V4,
    V6,
}

pub fn normalize_cidrs(values: &[String], kind: &str) -> crate::error::Result<Vec<String>> {
    let mut normalized = Vec::new();
    for value in values {
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        normalized.push(normalize_cidr(value, kind)?);
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

pub fn service_allows_for_source(
    service_id: &str,
    source: Ipv4Addr,
    egress: &crate::deployment::types::ServiceEgressConfig,
) -> Vec<ServiceEgressAllow> {
    egress
        .allow
        .iter()
        .map(|rule| ServiceEgressAllow {
            service_id: service_id.to_string(),
            source,
            cidr: rule.cidr.clone(),
            ports: rule.ports.clone(),
        })
        .collect()
}

pub async fn apply(config: &FirewallConfig) -> Result<()> {
    if config.deny.is_empty() {
        let _ = delete_table(&config.table_name).await;
        return Ok(());
    }

    let subnet = config
        .subnet
        .as_deref()
        .ok_or_else(|| anyhow!("egress deny requires a container subnet"))?;
    let script = render_nft_rules(
        &config.table_name,
        subnet,
        &config.deny,
        &config.allow,
        &config.service_allows,
    )?;
    replace_table(&config.table_name, &script).await
}

pub fn spawn_reconciler(
    manager: FirewallManager,
    logger: Logger,
    mut shutdown_rx: broadcast::Receiver<ShutdownEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(err) = manager.apply().await {
                        logger.emit("warn", &format!("failed to reconcile egress firewall: {err}"));
                    }
                }
                _ = shutdown_rx.recv() => break,
            }
        }
    })
}

fn normalize_cidr(value: &str, kind: &str) -> crate::error::Result<String> {
    let invalid = || Error::invalid_input(format!("invalid egress {kind} CIDR `{value}`"));
    if let Some((addr, prefix)) = value.split_once('/') {
        let addr = addr.parse::<IpAddr>().map_err(|_| invalid())?;
        let prefix = prefix.parse::<u8>().map_err(|_| invalid())?;
        let max_prefix = match addr {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix > max_prefix {
            return Err(invalid());
        }
        Ok(format!("{addr}/{prefix}"))
    } else {
        let addr = value.parse::<IpAddr>().map_err(|_| invalid())?;
        Ok(addr.to_string())
    }
}

fn address_family(value: &str) -> crate::error::Result<AddressFamily> {
    let addr = value.split_once('/').map(|(addr, _)| addr).unwrap_or(value);
    match addr.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => Ok(AddressFamily::V4),
        Ok(IpAddr::V6(_)) => Ok(AddressFamily::V6),
        Err(_) => Err(Error::invalid_input(format!(
            "invalid egress CIDR `{value}`"
        ))),
    }
}

fn render_nft_rules(
    table_name: &str,
    subnet: &str,
    deny: &[String],
    allow: &[String],
    service_allows: &[ServiceEgressAllow],
) -> Result<String> {
    if table_name.is_empty()
        || !table_name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(anyhow!("invalid nftables egress table name"));
    }
    let ipv4_allows = cidrs_for_family(allow, AddressFamily::V4)?;
    let ipv6_allows = cidrs_for_family(allow, AddressFamily::V6)?;
    let ipv4_denies = cidrs_for_family(deny, AddressFamily::V4)?;
    let ipv6_denies = cidrs_for_family(deny, AddressFamily::V6)?;

    let mut script = format!(
        "add table inet {table_name}\n\
         add chain inet {table_name} forward {{ type filter hook forward priority -50; policy accept; }}\n"
    );

    for rule in service_allows {
        let cidr = crate::cluster::network::Ipv4Cidr::parse(&rule.cidr)
            .map_err(|error| anyhow!("invalid service egress allow CIDR: {error}"))?;
        if rule.ports.contains(&0) {
            return Err(anyhow!("service egress allow contains port 0"));
        }
        if rule.ports.is_empty() {
            script.push_str(&format!(
                "add rule inet {table_name} forward ip saddr {} ip daddr {cidr} accept\n",
                rule.source
            ));
        } else {
            let ports = rule
                .ports
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            for protocol in ["tcp", "udp"] {
                script.push_str(&format!(
                    "add rule inet {table_name} forward ip saddr {} ip daddr {cidr} {protocol} dport {{ {ports} }} accept\n",
                    rule.source
                ));
            }
        }
    }

    if !ipv4_allows.is_empty() {
        script.push_str(&format!(
            "add rule inet {table_name} forward ip saddr {subnet} ip daddr {{ {} }} accept\n",
            ipv4_allows.join(", ")
        ));
    }

    if !ipv6_allows.is_empty() {
        script.push_str(&format!(
            "add rule inet {table_name} forward ip6 daddr {{ {} }} accept\n",
            ipv6_allows.join(", ")
        ));
    }

    if !ipv4_denies.is_empty() {
        script.push_str(&format!(
            "add rule inet {table_name} forward ip saddr {subnet} ip daddr {{ {} }} reject\n",
            ipv4_denies.join(", ")
        ));
    }

    if !ipv6_denies.is_empty() {
        script.push_str(&format!(
            "add rule inet {table_name} forward ip6 daddr {{ {} }} reject\n",
            ipv6_denies.join(", ")
        ));
    }

    Ok(script)
}

fn cidrs_for_family(cidrs: &[String], family: AddressFamily) -> crate::error::Result<Vec<String>> {
    cidrs
        .iter()
        .filter_map(|cidr| match address_family(cidr) {
            Ok(found) if found == family => Some(Ok(cidr.clone())),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        })
        .collect()
}

async fn delete_table(table_name: &str) -> Result<()> {
    let status = Command::new("nft")
        .args(["delete", "table", "inet", table_name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map_err(|err| anyhow!("failed to run nft: {err}"))?;
    let _ = status;
    Ok(())
}

async fn replace_table(table_name: &str, script: &str) -> Result<()> {
    let replacement = format!("delete table inet {table_name}\n{script}");
    match apply_script(&replacement).await {
        Ok(()) => Ok(()),
        Err(replace_error) => {
            let exists = Command::new("nft")
                .args(["list", "table", "inet", table_name])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
                .map_err(|error| anyhow!("failed to inspect nftables table: {error}"))?
                .success();
            if exists {
                Err(replace_error)
            } else {
                apply_script(script).await
            }
        }
    }
}

async fn apply_script(script: &str) -> Result<()> {
    let path = temp_script_path();
    tokio::fs::write(&path, script)
        .await
        .map_err(|err| anyhow!("failed to write nft script: {err}"))?;

    let output = Command::new("nft")
        .arg("-f")
        .arg(&path)
        .output()
        .await
        .map_err(|err| anyhow!("failed to run nft: {err}"));
    let _ = tokio::fs::remove_file(&path).await;
    let output = output?;
    if !output.status.success() {
        return Err(anyhow!(
            "nft apply failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

fn temp_script_path() -> PathBuf {
    std::env::temp_dir().join(format!("maestro-egress-{}.nft", std::process::id()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_cidrs_trims_deduplicates_and_validates() {
        let values = vec![
            " 169.254.169.254 ".to_string(),
            "10.0.0.0/8".to_string(),
            "169.254.169.254".to_string(),
            "fd00:ec2::254/128".to_string(),
        ];

        assert_eq!(
            normalize_cidrs(&values, "deny").unwrap(),
            vec!["10.0.0.0/8", "169.254.169.254", "fd00:ec2::254/128"]
        );
        assert!(normalize_cidrs(&["10.0.0.0/99".to_string()], "deny").is_err());
    }

    #[test]
    fn render_nft_rules_splits_ipv4_and_ipv6() {
        let script = render_nft_rules(
            DEFAULT_TABLE_NAME,
            "172.22.0.0/16",
            &[
                "169.254.169.254".to_string(),
                "fd00:ec2::254/128".to_string(),
            ],
            &[],
            &[],
        )
        .unwrap();

        assert!(script.contains("add table inet maestro_egress"));
        assert!(script.contains("ip saddr 172.22.0.0/16"));
        assert!(script.contains("ip daddr { 169.254.169.254 }"));
        assert!(script.contains("ip6 daddr { fd00:ec2::254/128 }"));
    }

    #[test]
    fn render_nft_rules_emits_allow_before_deny() {
        let script = render_nft_rules(
            DEFAULT_TABLE_NAME,
            "172.22.0.0/16",
            &["10.0.0.0/8".to_string()],
            &["10.1.2.3".to_string()],
            &[],
        )
        .unwrap();

        let allow_pos = script.find("ip daddr { 10.1.2.3 } accept").unwrap();
        let deny_pos = script.find("ip daddr { 10.0.0.0/8 } reject").unwrap();
        assert!(allow_pos < deny_pos);
    }

    #[test]
    fn endpoint_nodes_render_independent_tables() {
        let script = render_nft_rules(
            "maestro_egress_3101",
            "172.22.2.0/24",
            &["10.0.0.0/8".to_string()],
            &[],
            &[],
        )
        .unwrap();
        assert!(script.starts_with("add table inet maestro_egress_3101\n"));
        assert!(script.contains("add rule inet maestro_egress_3101 forward"));
    }

    #[test]
    fn service_allow_is_source_and_port_scoped_before_global_deny() {
        let script = render_nft_rules(
            DEFAULT_TABLE_NAME,
            "172.22.1.0/24",
            &["10.0.0.0/8".to_string()],
            &[],
            &[ServiceEgressAllow {
                service_id: "api".to_string(),
                source: "172.22.1.42".parse().unwrap(),
                cidr: "10.0.10.0/24".to_string(),
                ports: vec![5432],
            }],
        )
        .unwrap();

        let tcp = "ip saddr 172.22.1.42 ip daddr 10.0.10.0/24 tcp dport { 5432 } accept";
        let udp = "ip saddr 172.22.1.42 ip daddr 10.0.10.0/24 udp dport { 5432 } accept";
        let deny = "ip saddr 172.22.1.0/24 ip daddr { 10.0.0.0/8 } reject";
        assert!(script.contains(tcp));
        assert!(script.contains(udp));
        assert!(script.find(tcp).unwrap() < script.find(deny).unwrap());
    }

    #[test]
    fn service_allow_without_ports_accepts_every_protocol() {
        let script = render_nft_rules(
            DEFAULT_TABLE_NAME,
            "172.22.1.0/24",
            &["10.0.0.0/8".to_string()],
            &[],
            &[ServiceEgressAllow {
                service_id: "worker".to_string(),
                source: "172.22.1.43".parse().unwrap(),
                cidr: "10.0.20.0/24".to_string(),
                ports: vec![],
            }],
        )
        .unwrap();

        assert!(script.contains("ip saddr 172.22.1.43 ip daddr 10.0.20.0/24 accept"));
    }
}
