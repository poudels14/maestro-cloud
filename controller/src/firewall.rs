use std::net::IpAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Result, anyhow};
use tokio::process::Command;
use tokio::sync::broadcast;

use crate::error::Error;
use crate::logs::Logger;
use crate::signal::ShutdownEvent;

const TABLE_NAME: &str = "maestro_egress";
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirewallConfig {
    pub subnet: Option<String>,
    pub deny: Vec<String>,
    pub allow: Vec<String>,
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

pub async fn apply(config: &FirewallConfig) -> Result<()> {
    if config.deny.is_empty() {
        let _ = delete_table().await;
        return Ok(());
    }

    let subnet = config
        .subnet
        .as_deref()
        .ok_or_else(|| anyhow!("egress deny requires a container subnet"))?;
    let script = render_nft_rules(subnet, &config.deny, &config.allow)?;
    delete_table().await?;
    apply_script(&script).await
}

pub fn spawn_reconciler(
    config: FirewallConfig,
    logger: Logger,
    mut shutdown_rx: broadcast::Receiver<ShutdownEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(err) = apply(&config).await {
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

fn render_nft_rules(subnet: &str, deny: &[String], allow: &[String]) -> Result<String> {
    let ipv4_allows = cidrs_for_family(allow, AddressFamily::V4)?;
    let ipv6_allows = cidrs_for_family(allow, AddressFamily::V6)?;
    let ipv4_denies = cidrs_for_family(deny, AddressFamily::V4)?;
    let ipv6_denies = cidrs_for_family(deny, AddressFamily::V6)?;

    let mut script = format!(
        "add table inet {TABLE_NAME}\n\
         add chain inet {TABLE_NAME} forward {{ type filter hook forward priority -50; policy accept; }}\n"
    );

    if !ipv4_allows.is_empty() {
        script.push_str(&format!(
            "add rule inet {TABLE_NAME} forward ip saddr {subnet} ip daddr {{ {} }} accept\n",
            ipv4_allows.join(", ")
        ));
    }

    if !ipv6_allows.is_empty() {
        script.push_str(&format!(
            "add rule inet {TABLE_NAME} forward ip6 daddr {{ {} }} accept\n",
            ipv6_allows.join(", ")
        ));
    }

    if !ipv4_denies.is_empty() {
        script.push_str(&format!(
            "add rule inet {TABLE_NAME} forward ip saddr {subnet} ip daddr {{ {} }} reject\n",
            ipv4_denies.join(", ")
        ));
    }

    if !ipv6_denies.is_empty() {
        script.push_str(&format!(
            "add rule inet {TABLE_NAME} forward ip6 daddr {{ {} }} reject\n",
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

async fn delete_table() -> Result<()> {
    let status = Command::new("nft")
        .args(["delete", "table", "inet", TABLE_NAME])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map_err(|err| anyhow!("failed to run nft: {err}"))?;
    let _ = status;
    Ok(())
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
            "172.22.0.0/16",
            &[
                "169.254.169.254".to_string(),
                "fd00:ec2::254/128".to_string(),
            ],
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
            "172.22.0.0/16",
            &["10.0.0.0/8".to_string()],
            &["10.1.2.3".to_string()],
        )
        .unwrap();

        let allow_pos = script.find("ip daddr { 10.1.2.3 } accept").unwrap();
        let deny_pos = script.find("ip daddr { 10.0.0.0/8 } reject").unwrap();
        assert!(allow_pos < deny_pos);
    }
}
