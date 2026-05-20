use std::collections::HashMap;
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

    pub fn write_corefile(dns_dir: &std::path::Path) {
        let corefile_path = dns_dir.join("Corefile");
        let corefile_content = r#".:5353 {
    hosts /data/dns/hosts {
        reload 5s
        no_reverse
        fallthrough
    }
    forward . 1.1.1.1 8.8.8.8
    cache 5
    errors
}
"#;
        std::fs::write(&corefile_path, corefile_content).expect("failed to write Corefile");
    }
}
