use std::collections::BTreeSet;
use std::fmt;
use std::io::Write;
use std::net::{Ipv4Addr, TcpListener};
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use crate::cluster::types::NodeRole;
use crate::config::ClusterConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterPorts {
    version: u8,
    pub gateway: u16,
    pub etcd_client: u16,
    pub etcd_peer: u16,
}

pub fn load_or_allocate_cluster_ports(
    data_dir: &Path,
    host_ip: Ipv4Addr,
    api_port: u16,
) -> Result<ClusterPorts> {
    let system_dir = data_dir.join("system");
    std::fs::create_dir_all(&system_dir)?;
    let path = system_dir.join("cluster-ports.json");
    if path.exists() {
        let ports: ClusterPorts =
            serde_json::from_slice(&std::fs::read(&path)?).with_context(|| {
                format!("failed to parse persisted cluster ports {}", path.display())
            })?;
        validate_cluster_ports(ports, api_port)?;
        return Ok(ports);
    }

    // Keep every listener open until all ports have been selected so the OS
    // cannot hand the same ephemeral port back to a later allocation.
    let mut listeners = Vec::with_capacity(3);
    let mut ports = Vec::with_capacity(3);
    while ports.len() < 3 {
        let listener = TcpListener::bind((host_ip, 0)).with_context(|| {
            format!("failed to allocate an automatic cluster port on {host_ip}")
        })?;
        let port = listener.local_addr()?.port();
        if port == api_port || ports.contains(&port) {
            continue;
        }
        ports.push(port);
        listeners.push(listener);
    }
    let ports = ClusterPorts {
        version: 1,
        gateway: ports[0],
        etcd_client: ports[1],
        etcd_peer: ports[2],
    };
    validate_cluster_ports(ports, api_port)?;

    let temporary = system_dir.join("cluster-ports.json.tmp");
    match std::fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    file.write_all(&serde_json::to_vec_pretty(&ports)?)?;
    file.sync_all()?;
    std::fs::rename(&temporary, &path)?;
    std::fs::File::open(&system_dir)?.sync_all()?;
    drop(listeners);
    Ok(ports)
}

fn validate_cluster_ports(ports: ClusterPorts, api_port: u16) -> Result<()> {
    if ports.version != 1 {
        bail!(
            "unsupported persisted cluster port version {}",
            ports.version
        );
    }
    let values = [api_port, ports.gateway, ports.etcd_client, ports.etcd_peer];
    if values.contains(&0) || values.iter().copied().collect::<BTreeSet<_>>().len() != values.len()
    {
        bail!("persisted API, gateway, and etcd ports must be non-zero and distinct");
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ipv4Cidr {
    network: Ipv4Addr,
    prefix: u8,
}

impl Ipv4Cidr {
    pub const SYSTEM_RESERVED_HOSTS: u32 = 55;

    pub fn parse(value: &str) -> Result<Self> {
        let (ip, prefix) = value
            .split_once('/')
            .ok_or_else(|| anyhow!("invalid IPv4 CIDR `{value}`"))?;
        let ip = ip
            .parse::<Ipv4Addr>()
            .with_context(|| format!("invalid IPv4 address in CIDR `{value}`"))?;
        let prefix = prefix
            .parse::<u8>()
            .with_context(|| format!("invalid prefix in CIDR `{value}`"))?;
        if prefix > 32 {
            bail!("invalid IPv4 prefix in CIDR `{value}`");
        }
        let mask = prefix_mask(prefix);
        let network = Ipv4Addr::from(u32::from(ip) & mask);
        if network != ip {
            bail!("CIDR `{value}` is not a canonical network address");
        }
        Ok(Self { network, prefix })
    }

    pub fn overlaps(self, other: Self) -> bool {
        self.contains(other.network) || other.contains(self.network)
    }

    pub fn contains(self, ip: Ipv4Addr) -> bool {
        let mask = prefix_mask(self.prefix);
        u32::from(ip) & mask == u32::from(self.network)
    }

    pub fn prefix(self) -> u8 {
        self.prefix
    }

    pub fn gateway_address(self) -> Ipv4Addr {
        Ipv4Addr::from(u32::from(self.network) | 254)
    }

    pub fn broadcast_address(self) -> Ipv4Addr {
        Ipv4Addr::from(u32::from(self.network) | !prefix_mask(self.prefix))
    }

    /// Returns an address relative to the broadcast address. An offset of one
    /// is the highest usable address in the subnet.
    pub fn host_address_from_end(self, offset: u32) -> Option<Ipv4Addr> {
        let network = u32::from(self.network);
        let broadcast = u32::from(self.broadcast_address());
        let address = broadcast.checked_sub(offset)?;
        (address > network).then(|| Ipv4Addr::from(address))
    }

    /// Returns a fixed system address from the end of the first `/24` in a
    /// larger subnet. This preserves the original Maestro addresses when a
    /// single-node installation uses a `/16`, while `/24` cluster subnets use
    /// their normal highest addresses.
    pub fn system_address_from_end(self, offset: u32) -> Option<Ipv4Addr> {
        if self.prefix >= 24 {
            return self.host_address_from_end(offset);
        }
        let network = u32::from(self.network);
        let system_block_end = network.checked_add(255)?;
        let address = system_block_end.checked_sub(offset)?;
        (address > network).then(|| Ipv4Addr::from(address))
    }

    /// Addresses available to workload replicas. The first address is kept
    /// for the runtime gateway and the highest addresses are reserved for
    /// Maestro's fixed-address system containers.
    pub fn workload_addresses(self) -> impl Iterator<Item = Ipv4Addr> {
        let first = u32::from(self.network).saturating_add(2);
        let end = u32::from(self.broadcast_address()).saturating_sub(Self::SYSTEM_RESERVED_HOSTS);
        (first..end).map(Ipv4Addr::from)
    }

    pub fn is_workload_address(self, address: Ipv4Addr) -> bool {
        let address = u32::from(address);
        let first = u32::from(self.network).saturating_add(2);
        let end = u32::from(self.broadcast_address()).saturating_sub(Self::SYSTEM_RESERVED_HOSTS);
        address >= first && address < end
    }

    pub fn is_private(self) -> bool {
        self.network.is_private()
            && Ipv4Addr::from(u32::from(self.network) | !prefix_mask(self.prefix)).is_private()
    }
}

impl fmt::Display for Ipv4Cidr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.network, self.prefix)
    }
}

pub fn validate_cluster_config(
    config: &ClusterConfig,
    local_subnet: Option<&str>,
    role: NodeRole,
) -> Result<()> {
    if config.nodes.is_empty() {
        return Ok(());
    }
    config.master_node()?;
    let voter_count = config
        .nodes
        .values()
        .filter(|node| node.role.is_voter())
        .count();
    if voter_count != 1 && voter_count != 3 {
        bail!(
            "field `cluster.nodes` is invalid: must contain exactly 1 or 3 master/hybrid/voter nodes"
        );
    }
    let mut endpoints = BTreeSet::new();
    let mut subnets = Vec::new();
    for (name, node) in &config.nodes {
        if name.is_empty()
            || name.len() > 63
            || !name.chars().all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
            })
            || name.starts_with('-')
            || name.ends_with('-')
        {
            bail!(
                "field `cluster.nodes.{name}` is invalid: node name must be a lowercase DNS label"
            );
        }
        let ip = node.endpoint.host_ip();
        if !ip.is_private() || ip.is_loopback() || ip.is_unspecified() {
            bail!(
                "field `cluster.nodes.{name}.endpoint` is invalid: IP `{ip}` must be private and non-loopback"
            );
        }
        let api_port = node.endpoint.explicit_api_port().unwrap_or(3000);
        if api_port == 0 || !endpoints.insert((ip, api_port)) {
            bail!(
                "field `cluster.nodes.{name}.endpoint` is invalid: `{ip}:{api_port}` has a zero or duplicate port"
            );
        }
        let subnet = Ipv4Cidr::parse(&node.subnet)
            .map_err(|error| anyhow!("field `cluster.nodes.{name}.subnet` is invalid: {error}"))?;
        if subnet.prefix() != 24 || !subnet.is_private() {
            bail!(
                "field `cluster.nodes.{name}.subnet` is invalid: `{}` must be a private IPv4 /24",
                node.subnet
            );
        }
        if subnets
            .iter()
            .any(|(_, existing): &(&str, Ipv4Cidr)| existing.overlaps(subnet))
        {
            bail!(
                "field `cluster.nodes.{name}.subnet` is invalid: `{}` overlaps another node subnet",
                node.subnet
            );
        }
        subnets.push((name.as_str(), subnet));
        for (other_name, other) in &config.nodes {
            if subnet.contains(other.endpoint.host_ip()) {
                bail!(
                    "field `cluster.nodes.{name}.subnet` is invalid: `{}` overlaps `cluster.nodes.{other_name}.endpoint` IP `{}`",
                    node.subnet,
                    other.endpoint.host_ip()
                );
            }
        }
    }
    let (selected_name, selected) = config.selected_node()?;
    let local_subnet = local_subnet
        .ok_or_else(|| anyhow::anyhow!("field `node` is invalid: selected node has no subnet"))?;
    let local = Ipv4Cidr::parse(local_subnet).map_err(|error| {
        anyhow!("field `cluster.nodes.{selected_name}.subnet` is invalid: {error}")
    })?;
    if local.prefix() != 24 {
        bail!(
            "field `cluster.nodes.{selected_name}.subnet` is invalid: `{local_subnet}` must be an IPv4 /24"
        );
    }
    if !local.is_private() {
        bail!(
            "field `cluster.nodes.{selected_name}.subnet` is invalid: `{local_subnet}` must be private IPv4 space"
        );
    }
    if selected.role != role || selected.subnet != local_subnet {
        bail!(
            "field `node` is invalid: selected cluster node does not match the local role and subnet"
        );
    }
    let control_cidrs = config
        .control_allow_cidrs
        .iter()
        .enumerate()
        .map(|(index, cidr)| {
            Ipv4Cidr::parse(cidr).map_err(|error| {
                anyhow!("field `cluster.control-allow-cidrs[{index}]` is invalid: {error}")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    for (index, control) in control_cidrs.iter().enumerate() {
        if !control.is_private() {
            bail!(
                "field `cluster.control-allow-cidrs[{index}]` is invalid: `{control}` must be private IPv4 space"
            );
        }
        for (name, subnet) in &subnets {
            if subnet.overlaps(*control) {
                bail!(
                    "field `cluster.control-allow-cidrs[{index}]` is invalid: `{control}` overlaps `cluster.nodes.{name}.subnet` `{subnet}`"
                );
            }
        }
    }
    if !control_cidrs.is_empty() {
        for (name, node) in &config.nodes {
            if !control_cidrs
                .iter()
                .any(|cidr| cidr.contains(node.endpoint.host_ip()))
            {
                bail!(
                    "field `cluster.nodes.{name}.endpoint` is invalid: IP `{}` is absent from `cluster.control-allow-cidrs`",
                    node.endpoint.host_ip()
                );
            }
        }
    }
    if !config
        .image_registry
        .as_deref()
        .is_some_and(|registry| !registry.trim().is_empty())
    {
        bail!("field `cluster.image-registry` is missing or invalid: required in multi-node mode");
    }
    match config.join_secret.as_deref() {
        Some(secret) if secret.len() >= 32 => {}
        _ => bail!(
            "field `cluster.join-secret` is missing or invalid: must contain at least 32 characters"
        ),
    }
    Ok(())
}

pub fn resolve_cluster_host_ip(
    config: &ClusterConfig,
    data_dir: &Path,
    role: NodeRole,
    local_subnet: Option<&str>,
) -> Result<Option<Ipv4Addr>> {
    if config.nodes.is_empty() {
        return Ok(None);
    }
    let local_addresses = local_ipv4_addresses()?;
    let (name, node) = config.selected_node()?;
    let resolved = node.endpoint.host_ip();
    if !local_addresses.contains(&resolved) {
        bail!(
            "selected cluster node `{name}` endpoint IP `{resolved}` is not assigned to a local non-Tailscale interface"
        );
    }
    if !resolved.is_private() || resolved.is_loopback() || resolved.is_unspecified() {
        bail!("resolved cluster host IP `{resolved}` is not a private host address");
    }
    if !local_addresses.contains(&resolved) {
        bail!(
            "resolved cluster host IP `{resolved}` is not assigned to a local non-Tailscale control interface"
        );
    }
    config.local_endpoint(resolved, role)?;
    if let Some(subnet) = local_subnet
        && Ipv4Cidr::parse(subnet)?.contains(resolved)
    {
        bail!("resolved cluster host IP `{resolved}` overlaps local workload subnet `{subnet}`");
    }
    if !control_ip_allowed(config, resolved)? {
        bail!("resolved cluster host IP `{resolved}` is absent from cluster.control-allow-cidrs");
    }
    persist_control_ip(data_dir, resolved)?;
    persist_control_endpoint(data_dir, resolved, config.api_port)?;
    Ok(Some(resolved))
}

pub fn local_ipv4_addresses() -> Result<Vec<Ipv4Addr>> {
    let output = Command::new("ip")
        .args(["-json", "-4", "address", "show", "up"])
        .output()
        .context("failed to inspect local IPv4 interfaces with `ip`")?;
    if !output.status.success() {
        bail!(
            "`ip -json -4 address show up` exited with {}",
            output.status
        );
    }
    let interfaces: Vec<InterfaceAddress> = serde_json::from_slice(&output.stdout)
        .context("failed to parse local interface information")?;
    Ok(addresses_from_interfaces(&interfaces))
}

pub fn control_ip_allowed(config: &ClusterConfig, ip: Ipv4Addr) -> Result<bool> {
    if config.control_allow_cidrs.is_empty() {
        return Ok(true);
    }
    Ok(config
        .control_allow_cidrs
        .iter()
        .map(|cidr| Ipv4Cidr::parse(cidr))
        .collect::<Result<Vec<_>>>()?
        .iter()
        .any(|cidr| cidr.contains(ip)))
}

fn addresses_from_interfaces(interfaces: &[InterfaceAddress]) -> Vec<Ipv4Addr> {
    let mut addresses = BTreeSet::new();
    for interface in interfaces {
        if is_control_interface(&interface.ifname) {
            for address in &interface.addr_info {
                if address.family == "inet"
                    && let Ok(ip) = address.local.parse::<Ipv4Addr>()
                    && !ip.is_loopback()
                    && !ip.is_unspecified()
                {
                    addresses.insert(ip);
                }
            }
        }
    }
    addresses.into_iter().collect()
}

fn is_control_interface(name: &str) -> bool {
    !name.starts_with("tailscale")
        && !name.starts_with("docker")
        && !name.starts_with("br-")
        && !name.starts_with("veth")
        && !name.starts_with("cni")
        && name != "lo"
}

fn persist_control_ip(data_dir: &Path, resolved: Ipv4Addr) -> Result<()> {
    let system_dir = data_dir.join("system");
    let path = system_dir.join("control-ip");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let existing = existing.trim();
        if existing != resolved.to_string() {
            bail!(
                "resolved cluster host IP changed from `{existing}` to `{resolved}`; restore the selected node endpoint"
            );
        }
    } else {
        std::fs::create_dir_all(&system_dir)?;
        let temporary = system_dir.join("control-ip.tmp");
        std::fs::write(&temporary, resolved.to_string())?;
        std::fs::File::open(&temporary)?.sync_all()?;
        std::fs::rename(&temporary, &path)?;
        std::fs::File::open(&system_dir)?.sync_all()?;
    }
    Ok(())
}

fn persist_control_endpoint(data_dir: &Path, resolved: Ipv4Addr, api_port: u16) -> Result<()> {
    let system_dir = data_dir.join("system");
    let path = system_dir.join("control-endpoint");
    let resolved = format!("{resolved}:{api_port}");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let existing = existing.trim();
        if existing != resolved {
            bail!(
                "resolved cluster endpoint changed from `{existing}` to `{resolved}`; restore the selected node endpoint"
            );
        }
    } else {
        std::fs::create_dir_all(&system_dir)?;
        let temporary = system_dir.join("control-endpoint.tmp");
        std::fs::write(&temporary, &resolved)?;
        std::fs::File::open(&temporary)?.sync_all()?;
        std::fs::rename(&temporary, &path)?;
        std::fs::File::open(&system_dir)?.sync_all()?;
    }
    Ok(())
}

fn prefix_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}

#[derive(Debug, Deserialize)]
struct InterfaceAddress {
    ifname: String,
    #[serde(default)]
    addr_info: Vec<AddressInfo>,
}

#[derive(Debug, Deserialize)]
struct AddressInfo {
    family: String,
    local: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterEndpointConfig, ClusterNodeConfig};

    fn valid_cluster_config() -> ClusterConfig {
        ClusterConfig {
            name: "test".to_string(),
            nodes: [
                (
                    "node1".to_string(),
                    ClusterNodeConfig {
                        endpoint: ClusterEndpointConfig::Address("10.20.0.11".parse().unwrap()),
                        subnet: "172.22.2.0/24".to_string(),
                        role: NodeRole::Master,
                    },
                ),
                (
                    "node2".to_string(),
                    ClusterNodeConfig {
                        endpoint: ClusterEndpointConfig::Address("10.20.0.12".parse().unwrap()),
                        subnet: "172.22.3.0/24".to_string(),
                        role: NodeRole::Voter,
                    },
                ),
                (
                    "node3".to_string(),
                    ClusterNodeConfig {
                        endpoint: ClusterEndpointConfig::Address("10.20.0.13".parse().unwrap()),
                        subnet: "172.22.4.0/24".to_string(),
                        role: NodeRole::Voter,
                    },
                ),
            ]
            .into(),
            control_allow_cidrs: vec!["10.20.0.0/24".to_string()],
            api_port: 3001,
            gateway_port: 3002,
            etcd_client_port: 2379,
            etcd_peer_port: 2380,
            join_secret: Some("x".repeat(32)),
            image_registry: Some("registry.example.com/maestro".to_string()),
            selected_node: Some("node1".to_string()),
            ..ClusterConfig::default()
        }
    }

    #[test]
    fn cidr_requires_canonical_network() {
        assert!(Ipv4Cidr::parse("172.22.1.0/24").is_ok());
        assert!(Ipv4Cidr::parse("172.22.1.1/24").is_err());
    }

    #[test]
    fn cidr_overlap_is_symmetric() {
        let first = Ipv4Cidr::parse("172.22.0.0/16").expect("first");
        let second = Ipv4Cidr::parse("172.22.1.0/24").expect("second");
        assert!(first.overlaps(second));
        assert!(second.overlaps(first));
    }

    #[test]
    fn system_addresses_stay_in_the_first_24_for_larger_subnets() {
        let subnet = Ipv4Cidr::parse("10.100.0.0/16").expect("subnet");
        assert_eq!(
            subnet.system_address_from_end(1),
            Some(Ipv4Addr::new(10, 100, 0, 254))
        );
        assert_eq!(
            subnet.system_address_from_end(6),
            Some(Ipv4Addr::new(10, 100, 0, 249))
        );
    }

    #[test]
    fn cluster_workload_addresses_end_before_the_system_block() {
        let cluster = Ipv4Cidr::parse("172.22.1.0/24").unwrap();
        let workloads = cluster.workload_addresses().collect::<Vec<_>>();
        assert_eq!(workloads.first(), Some(&Ipv4Addr::new(172, 22, 1, 2)));
        assert_eq!(workloads.last(), Some(&Ipv4Addr::new(172, 22, 1, 199)));
        assert_eq!(workloads.len(), 198);
    }

    #[test]
    fn interface_filter_excludes_overlay_and_container_bridges() {
        let interfaces = serde_json::from_str::<Vec<InterfaceAddress>>(
            r#"[
                {"ifname":"eth0","addr_info":[{"family":"inet","local":"10.20.0.11"}]},
                {"ifname":"tailscale0","addr_info":[{"family":"inet","local":"100.64.0.1"}]},
                {"ifname":"docker0","addr_info":[{"family":"inet","local":"172.17.0.1"}]}
            ]"#,
        )
        .expect("interfaces");
        assert_eq!(
            addresses_from_interfaces(&interfaces),
            vec![Ipv4Addr::new(10, 20, 0, 11)]
        );
    }

    #[test]
    fn optional_control_allowlist_is_separate_and_complete() {
        let config = valid_cluster_config();
        validate_cluster_config(&config, Some("172.22.2.0/24"), NodeRole::Master)
            .expect("valid cluster");

        let mut unrestricted = valid_cluster_config();
        unrestricted.control_allow_cidrs.clear();
        validate_cluster_config(&unrestricted, Some("172.22.2.0/24"), NodeRole::Master)
            .expect("control allowlist should be optional");
        assert!(control_ip_allowed(&unrestricted, "10.99.0.1".parse().unwrap()).unwrap());

        let mut overlap = valid_cluster_config();
        overlap.control_allow_cidrs = vec!["172.22.0.0/16".to_string()];
        assert!(
            validate_cluster_config(&overlap, Some("172.22.2.0/24"), NodeRole::Master).is_err()
        );

        let mut incomplete = valid_cluster_config();
        incomplete.control_allow_cidrs = vec!["10.20.0.11/32".to_string()];
        assert!(
            validate_cluster_config(&incomplete, Some("172.22.2.0/24"), NodeRole::Master).is_err()
        );
    }

    #[test]
    fn endpoint_defaults_api_to_3000_and_allows_an_override() {
        let mut config = valid_cluster_config();
        assert_eq!(config.voter_api_endpoints()[0].port(), 3000);
        config.nodes.get_mut("node1").unwrap().endpoint =
            ClusterEndpointConfig::Endpoint("10.20.0.11:3101".parse().unwrap());
        config.api_port = 3101;
        config.set_local_ports(42001, 42002, 42003);

        validate_cluster_config(&config, Some("172.22.2.0/24"), NodeRole::Master)
            .expect("valid endpoint override");
        let node = config
            .local_endpoint("10.20.0.11".parse().unwrap(), NodeRole::Master)
            .unwrap();
        assert_eq!(node.api_port, 3101);
        assert_eq!(node.gateway_port, 42001);
        assert_eq!(node.etcd_client_port, 42002);
        assert_eq!(node.etcd_peer_port, 42003);
    }

    #[test]
    fn duplicate_api_endpoints_are_rejected() {
        let mut config = valid_cluster_config();
        config.nodes.get_mut("node2").unwrap().endpoint =
            ClusterEndpointConfig::Address("10.20.0.11".parse().unwrap());
        assert!(validate_cluster_config(&config, Some("172.22.2.0/24"), NodeRole::Master).is_err());
    }

    #[test]
    fn automatic_cluster_ports_are_distinct_and_persistent() {
        let root = std::env::temp_dir().join(format!(
            "maestro-cluster-ports-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        let first = load_or_allocate_cluster_ports(&root, Ipv4Addr::LOCALHOST, 3000).unwrap();
        let second = load_or_allocate_cluster_ports(&root, Ipv4Addr::LOCALHOST, 3000).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            [3000, first.gateway, first.etcd_client, first.etcd_peer]
                .into_iter()
                .collect::<BTreeSet<_>>()
                .len(),
            4
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
