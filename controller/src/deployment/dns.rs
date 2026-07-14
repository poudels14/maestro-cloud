use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::Result;

pub struct DnsManager {
    hosts_path: PathBuf,
    inner: Mutex<DnsManagerInner>,
}

struct DnsManagerInner {
    // fqdn → IPs (multiple entries = multiple A records)
    records: HashMap<String, Vec<String>>,
    owned: HashMap<String, HashSet<String>>,
}

impl DnsManager {
    pub fn new(dns_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&dns_dir).expect("failed to create dns directory");
        let hosts_path = dns_dir.join("hosts");
        if !hosts_path.exists() {
            std::fs::write(&hosts_path, "").expect("failed to create initial hosts file");
        }
        Self {
            hosts_path,
            inner: Mutex::new(DnsManagerInner {
                records: HashMap::new(),
                owned: HashMap::new(),
            }),
        }
    }

    pub fn set_record(&self, hostname: &str, domain: &str, ip: &str) {
        self.set_records(hostname, domain, std::slice::from_ref(&ip.to_string()));
    }

    pub fn set_records(&self, hostname: &str, domain: &str, ips: &[String]) {
        let fqdn = format!("{hostname}.{domain}");
        let mut inner = self.inner.lock().expect("dns lock");
        if ips.is_empty() {
            inner.records.remove(&fqdn);
        } else {
            inner.records.insert(fqdn, ips.to_vec());
        }
    }

    pub fn lookup(&self, hostname: &str, domain: &str) -> Vec<String> {
        let fqdn = format!("{hostname}.{domain}");
        let inner = self.inner.lock().expect("dns lock");
        inner.records.get(&fqdn).cloned().unwrap_or_default()
    }

    pub fn remove_records_for_hostname(&self, hostname: &str, domain: &str) {
        let fqdn = format!("{hostname}.{domain}");
        let mut inner = self.inner.lock().expect("dns lock");
        inner.records.remove(&fqdn);
    }

    pub fn replace_owned_records(
        &self,
        owner: &str,
        desired: BTreeMap<String, Vec<String>>,
    ) -> Result<()> {
        for (fqdn, addresses) in &desired {
            if fqdn.trim().is_empty()
                || fqdn.chars().any(|character| {
                    !(character.is_ascii_alphanumeric() || character == '-' || character == '.')
                })
            {
                anyhow::bail!("invalid DNS hostname `{fqdn}`");
            }
            for address in addresses {
                address
                    .parse::<std::net::Ipv4Addr>()
                    .map_err(|_| anyhow::anyhow!("invalid DNS IPv4 address `{address}`"))?;
            }
        }
        let mut inner = self.inner.lock().expect("dns lock");
        for fqdn in inner.owned.remove(owner).unwrap_or_default() {
            inner.records.remove(&fqdn);
        }
        let mut owned = HashSet::new();
        for (fqdn, mut addresses) in desired {
            addresses.sort();
            addresses.dedup();
            if !addresses.is_empty() {
                owned.insert(fqdn.clone());
                inner.records.insert(fqdn, addresses);
            }
        }
        if !owned.is_empty() {
            inner.owned.insert(owner.to_string(), owned);
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let inner = self.inner.lock().expect("dns lock");
        let mut entries: Vec<_> = inner.records.iter().collect();
        entries.sort_by_key(|(fqdn, _)| (*fqdn).clone());
        let content: String = entries
            .iter()
            .flat_map(|(fqdn, ips)| ips.iter().map(move |ip| format!("{ip} {fqdn}")))
            .collect::<Vec<_>>()
            .join("\n");
        let tmp_path = self.hosts_path.with_extension("tmp");
        std::fs::write(&tmp_path, format!("{content}\n"))?;
        std::fs::rename(&tmp_path, &self.hosts_path)?;
        Ok(())
    }

    pub fn write_corefile(dns_dir: &std::path::Path, port: u16) {
        let corefile_path = dns_dir.join("Corefile");
        let corefile_content = format!(
            r#".:{port} {{
    hosts /data/dns/hosts {{
        reload 5s
        no_reverse
        fallthrough
    }}
    forward . 1.1.1.1 8.8.8.8
    cache 5
    errors
}}
"#
        );
        std::fs::write(&corefile_path, corefile_content).expect("failed to write Corefile");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corefile_uses_requested_listener_port() {
        let directory = std::env::temp_dir().join(format!(
            "maestro-dns-test-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        std::fs::create_dir_all(&directory).unwrap();
        DnsManager::write_corefile(&directory, 53);
        let corefile = std::fs::read_to_string(directory.join("Corefile")).unwrap();

        assert!(corefile.starts_with(".:53 {"));
        assert!(corefile.contains("hosts /data/dns/hosts"));
        assert!(!corefile.contains(":5353"));
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn owned_records_replace_without_removing_system_records() {
        let directory = std::env::temp_dir().join(format!(
            "maestro-dns-test-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        let manager = DnsManager::new(directory.clone());
        manager.set_record("maestro-etcd", "cluster.maestro.internal", "172.20.1.2");
        manager
            .replace_owned_records(
                "cluster-service:web",
                BTreeMap::from([(
                    "web.cluster.maestro.internal".to_string(),
                    vec!["172.20.2.10".to_string()],
                )]),
            )
            .unwrap();
        manager
            .replace_owned_records("cluster-service:web", BTreeMap::new())
            .unwrap();
        manager.flush().unwrap();
        let hosts = std::fs::read_to_string(directory.join("hosts")).unwrap();
        assert!(hosts.contains("172.20.1.2 maestro-etcd.cluster.maestro.internal"));
        assert!(!hosts.contains("web.cluster.maestro.internal"));
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn owned_records_reject_non_ip_addresses() {
        let directory = std::env::temp_dir().join(format!(
            "maestro-dns-test-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        let manager = DnsManager::new(directory.clone());
        assert!(
            manager
                .replace_owned_records(
                    "cluster-service:web",
                    BTreeMap::from([(
                        "web.cluster.maestro.internal".to_string(),
                        vec!["remote-hostname".to_string()],
                    )]),
                )
                .is_err()
        );
        let _ = std::fs::remove_dir_all(directory);
    }
}
