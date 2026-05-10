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
    forward . 1.1.1.1 8.8.8.8
    cache 30
    errors
}
"#;
        std::fs::write(&corefile_path, corefile_content).expect("failed to write Corefile");
    }
}
