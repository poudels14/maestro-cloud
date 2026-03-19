use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::Result;

pub struct DnsManager {
    hosts_path: PathBuf,
    inner: Mutex<DnsManagerInner>,
}

struct DnsManagerInner {
    records: HashMap<String, String>, // fqdn → IP
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
            }),
        }
    }

    pub fn set_record(&self, hostname: &str, domain: &str, ip: &str) {
        let fqdn = format!("{hostname}.{domain}");
        let mut inner = self.inner.lock().expect("dns lock");
        inner.records.insert(fqdn, ip.to_string());
    }

    pub fn remove_records_for_hostname(&self, hostname: &str, domain: &str) {
        let fqdn = format!("{hostname}.{domain}");
        let mut inner = self.inner.lock().expect("dns lock");
        inner.records.remove(&fqdn);
    }

    pub fn flush(&self) -> Result<()> {
        let inner = self.inner.lock().expect("dns lock");
        let mut entries: Vec<_> = inner.records.iter().collect();
        entries.sort_by_key(|(fqdn, _)| (*fqdn).clone());
        let content: String = entries
            .iter()
            .map(|(fqdn, ip)| format!("{ip} {fqdn}"))
            .collect::<Vec<_>>()
            .join("\n");
        let tmp_path = self.hosts_path.with_extension("tmp");
        std::fs::write(&tmp_path, format!("{content}\n"))?;
        std::fs::rename(&tmp_path, &self.hosts_path)?;
        Ok(())
    }

    pub fn write_corefile(dns_dir: &std::path::Path) {
        let corefile_path = dns_dir.join("Corefile");
        let corefile_content = r#".:5353 {
    hosts /data/dns/hosts {
        reload 5s
        no_reverse
        fallthrough
    }
    forward . /etc/resolv.conf
    cache 30
    log
    errors
}
"#;
        std::fs::write(&corefile_path, corefile_content).expect("failed to write Corefile");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_and_read_hosts_file() {
        let dir = std::env::temp_dir().join(format!(
            "maestro-dns-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let dns = DnsManager::new(dir.clone());

        dns.set_record("web", "mycluster.maestro.internal", "172.22.0.3");
        dns.set_record("etcd", "mycluster.maestro.internal", "172.22.0.4");
        dns.flush().unwrap();

        let content = std::fs::read_to_string(dir.join("hosts")).unwrap();
        assert!(content.contains("172.22.0.4 etcd.mycluster.maestro.internal"));
        assert!(content.contains("172.22.0.3 web.mycluster.maestro.internal"));

        dns.remove_records_for_hostname("etcd", "mycluster.maestro.internal");
        dns.flush().unwrap();

        let content = std::fs::read_to_string(dir.join("hosts")).unwrap();
        assert!(!content.contains("etcd"));
        assert!(content.contains("web"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_corefile() {
        let dir = std::env::temp_dir().join(format!(
            "maestro-corefile-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        DnsManager::write_corefile(&dir);
        let content = std::fs::read_to_string(dir.join("Corefile")).unwrap();
        assert!(content.contains("hosts /data/dns/hosts"));
        assert!(content.contains("reload 5s"));
        assert!(content.contains("forward . /etc/resolv.conf"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
