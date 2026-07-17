use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Result, bail};

const HOST_RESOLVER_PATHS: &[&str] = &[
    "/etc/resolv.conf",
    "/run/systemd/resolve/resolv.conf",
    "/run/NetworkManager/no-stub-resolv.conf",
];

pub fn load_host_resolvers() -> Result<Vec<Ipv4Addr>> {
    let paths = HOST_RESOLVER_PATHS
        .iter()
        .map(Path::new)
        .collect::<Vec<_>>();
    load_host_resolvers_from_paths(&paths)
}

pub fn validate_host_resolvers(
    resolvers: &[Ipv4Addr],
    container_subnet: crate::cluster::network::Ipv4Cidr,
) -> Result<()> {
    if let Some(resolver) = resolvers
        .iter()
        .find(|resolver| container_subnet.contains(**resolver))
    {
        bail!(
            "host DNS resolver `{resolver}` overlaps container subnet `{container_subnet}`; choose a non-overlapping subnet"
        );
    }
    Ok(())
}

fn load_host_resolvers_from_paths(paths: &[&Path]) -> Result<Vec<Ipv4Addr>> {
    let mut read_errors = Vec::new();
    for path in paths {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let resolvers = parse_host_resolvers(&contents);
                if !resolvers.is_empty() {
                    return Ok(resolvers);
                }
            }
            Err(error) => read_errors.push(format!("{}: {error}", path.display())),
        }
    }

    let checked = paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    if read_errors.is_empty() {
        bail!("no usable non-loopback IPv4 nameserver found; checked {checked}");
    }
    bail!(
        "no usable non-loopback IPv4 nameserver found; checked {checked}; read errors: {}",
        read_errors.join("; ")
    );
}

fn parse_host_resolvers(contents: &str) -> Vec<Ipv4Addr> {
    let mut seen = HashSet::new();
    contents
        .lines()
        .filter_map(|line| {
            let line = line.split(['#', ';']).next()?.trim();
            let mut fields = line.split_whitespace();
            if fields.next()? != "nameserver" {
                return None;
            }
            let address = fields.next()?.parse::<Ipv4Addr>().ok()?;
            (!address.is_unspecified() && !address.is_loopback() && !address.is_multicast())
                .then_some(address)
        })
        .filter(|address| seen.insert(*address))
        .collect()
}

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

    pub fn write_corefile(dns_dir: &std::path::Path, port: u16, upstreams: &[Ipv4Addr]) {
        let corefile_path = dns_dir.join("Corefile");
        let forward = if upstreams.is_empty() {
            String::new()
        } else {
            format!(
                "    forward . {}\n",
                upstreams
                    .iter()
                    .map(Ipv4Addr::to_string)
                    .collect::<Vec<_>>()
                    .join(" ")
            )
        };
        let corefile_content = format!(
            r#".:{port} {{
    hosts /data/dns/hosts {{
        reload 5s
        no_reverse
        fallthrough
    }}
{forward}    cache 5
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
        DnsManager::write_corefile(
            &directory,
            53,
            &[
                "10.0.0.2".parse().unwrap(),
                "169.254.169.253".parse().unwrap(),
            ],
        );
        let corefile = std::fs::read_to_string(directory.join("Corefile")).unwrap();

        assert!(corefile.starts_with(".:53 {"));
        assert!(corefile.contains("hosts /data/dns/hosts"));
        assert!(corefile.contains("forward . 10.0.0.2 169.254.169.253"));
        assert!(!corefile.contains("1.1.1.1"));
        assert!(!corefile.contains("8.8.8.8"));
        assert!(!corefile.contains(":5353"));
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn host_resolver_parser_ignores_unusable_addresses_and_deduplicates() {
        let resolvers = parse_host_resolvers(
            r#"
                # Managed by the host
                nameserver 127.0.0.53
                nameserver 10.0.0.2
                nameserver 169.254.169.253 # AWS VPC resolver
                nameserver 10.0.0.2
                nameserver ::1
                search internal
            "#,
        );

        assert_eq!(
            resolvers,
            vec![
                "10.0.0.2".parse::<Ipv4Addr>().unwrap(),
                "169.254.169.253".parse::<Ipv4Addr>().unwrap()
            ]
        );
    }

    #[test]
    fn host_resolver_loader_uses_non_stub_fallback_file() {
        let directory = std::env::temp_dir().join(format!(
            "maestro-resolver-test-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let stub = directory.join("stub-resolv.conf");
        let upstream = directory.join("upstream-resolv.conf");
        std::fs::write(&stub, "nameserver 127.0.0.53\n").unwrap();
        std::fs::write(&upstream, "nameserver 10.0.0.2\n").unwrap();

        assert_eq!(
            load_host_resolvers_from_paths(&[stub.as_path(), upstream.as_path()]).unwrap(),
            vec!["10.0.0.2".parse::<Ipv4Addr>().unwrap()]
        );
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn host_resolver_loader_rejects_stub_only_configuration() {
        let directory = std::env::temp_dir().join(format!(
            "maestro-resolver-test-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let stub = directory.join("resolv.conf");
        std::fs::write(&stub, "nameserver 127.0.0.53\nnameserver ::1\n").unwrap();

        let error = load_host_resolvers_from_paths(&[stub.as_path()])
            .unwrap_err()
            .to_string();
        assert!(error.contains("no usable non-loopback IPv4 nameserver"));
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn host_resolver_validation_rejects_container_subnet_overlap() {
        let subnet = crate::cluster::network::Ipv4Cidr::parse("10.3.0.0/24").unwrap();
        let error = validate_host_resolvers(&["10.3.0.2".parse().unwrap()], subnet)
            .unwrap_err()
            .to_string();

        assert!(error.contains("host DNS resolver `10.3.0.2` overlaps container subnet"));
        assert!(error.contains("choose a non-overlapping subnet"));
        validate_host_resolvers(&["10.0.0.2".parse().unwrap()], subnet).unwrap();
    }

    #[test]
    fn corefile_without_upstreams_has_no_forward_plugin() {
        let directory = std::env::temp_dir().join(format!(
            "maestro-dns-test-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        std::fs::create_dir_all(&directory).unwrap();
        DnsManager::write_corefile(&directory, 53, &[]);
        let corefile = std::fs::read_to_string(directory.join("Corefile")).unwrap();

        assert!(!corefile.contains("forward ."));
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
