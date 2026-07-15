use std::collections::BTreeSet;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

use crate::cluster::types::NodeRole;
use crate::config::ClusterConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ipv4Cidr {
    network: Ipv4Addr,
    prefix: u8,
}

impl Ipv4Cidr {
    pub const SYSTEM_RESERVED_HOSTS: u32 = 31;

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
    if config.nodes.len() != 1 && config.nodes.len() != 3 {
        bail!("cluster.nodes must contain 1 or 3 initial voter endpoints");
    }
    let explicit_node_ports = config
        .nodes
        .iter()
        .filter(|node| node.explicit_api_port().is_some())
        .count();
    if explicit_node_ports != 0 && explicit_node_ports != config.nodes.len() {
        bail!("cluster.nodes cannot mix bare IPs and IP:port endpoints");
    }
    let mut voters = BTreeSet::new();
    let resolved_nodes = config.resolved_nodes()?;
    for node in &config.nodes {
        let ip = node.host_ip();
        if !ip.is_private() || ip.is_loopback() || ip.is_unspecified() {
            bail!("cluster voter IP `{ip}` must be a private, non-loopback IPv4 address");
        }
        let identity = (ip, node.explicit_api_port());
        if !voters.insert(identity) {
            bail!("cluster.nodes contains duplicate endpoint `{node}`");
        }
    }
    if config.api_port == 0
        || config.gateway_port == 0
        || config.etcd_client_port == 0
        || config.etcd_peer_port == 0
    {
        bail!("cluster API, gateway, and etcd ports must be non-zero");
    }
    if explicit_node_ports == 0 {
        let ports = [
            config.api_port,
            config.gateway_port,
            config.etcd_client_port,
            config.etcd_peer_port,
        ];
        if ports.iter().copied().collect::<BTreeSet<_>>().len() != ports.len() {
            bail!("cluster API, gateway, etcd client, and etcd peer ports must be distinct");
        }
    } else {
        if config.gateway_port != 3002
            || config.etcd_client_port != 2379
            || config.etcd_peer_port != 2380
        {
            bail!(
                "cluster.gateway-port and etcd port fields are legacy-only when cluster.nodes uses IP:port; remove those overrides"
            );
        }
        let mut host_ports = BTreeSet::new();
        for node in &resolved_nodes {
            for port in [
                node.api_port,
                node.gateway_port,
                node.etcd_client_port,
                node.etcd_peer_port,
            ] {
                if !host_ports.insert((node.host_ip, port)) {
                    bail!(
                        "cluster node port blocks overlap at `{}:{port}`; each IP:port entry reserves four consecutive ports",
                        node.host_ip
                    );
                }
            }
        }
        if role.is_voter()
            && !resolved_nodes
                .iter()
                .any(|node| node.api_port == config.api_port)
        {
            bail!(
                "cluster.api-port must select this voter from cluster.nodes when node ports are used"
            );
        }
    }
    if config.subnets.len() < config.nodes.len() {
        bail!("cluster.subnets must include a Docker /24 for every initial voter");
    }
    let mut parsed_subnets = Vec::new();
    for subnet in &config.subnets {
        let parsed = Ipv4Cidr::parse(subnet)?;
        if parsed.prefix() != 24 {
            bail!("cluster Docker subnet `{subnet}` must be an IPv4 /24");
        }
        if !parsed.is_private() {
            bail!("cluster Docker subnet `{subnet}` must be private IPv4 space");
        }
        if parsed_subnets
            .iter()
            .any(|existing: &Ipv4Cidr| existing.overlaps(parsed))
        {
            bail!("cluster Docker subnet `{subnet}` overlaps another configured subnet");
        }
        parsed_subnets.push(parsed);
    }
    for voter in &resolved_nodes {
        if parsed_subnets
            .iter()
            .any(|subnet| subnet.contains(voter.host_ip))
        {
            bail!(
                "cluster voter IP `{}` overlaps a workload Docker subnet",
                voter.host_ip
            );
        }
    }
    if config.control_allow_cidrs.is_empty() {
        bail!("cluster.control-allow-cidrs must include the private control network");
    }
    let control_cidrs = config
        .control_allow_cidrs
        .iter()
        .map(|cidr| Ipv4Cidr::parse(cidr))
        .collect::<Result<Vec<_>>>()?;
    for control in &control_cidrs {
        if !control.is_private() {
            bail!("cluster control CIDR `{control}` must be private IPv4 space");
        }
        if parsed_subnets
            .iter()
            .any(|subnet| subnet.overlaps(*control))
        {
            bail!("cluster control CIDR `{control}` overlaps a workload Docker subnet");
        }
    }
    for voter in &resolved_nodes {
        if !control_cidrs
            .iter()
            .any(|cidr| cidr.contains(voter.host_ip))
        {
            bail!(
                "cluster voter IP `{}` is absent from cluster.control-allow-cidrs",
                voter.host_ip
            );
        }
    }
    if let Some(local_subnet) = local_subnet {
        let local = Ipv4Cidr::parse(local_subnet)?;
        if !parsed_subnets.contains(&local) {
            bail!("local subnet `{local_subnet}` is absent from cluster.subnets");
        }
    }
    if config.shared_registry.is_none() {
        bail!("cluster.shared-registry is required in multi-node mode");
    }
    match config.join_secret.as_deref() {
        Some(secret) if secret.len() >= 32 => {}
        _ => bail!("cluster.join-secret must contain at least 32 characters"),
    }
    Ok(())
}

pub fn resolve_cluster_host_ip(
    config: &ClusterConfig,
    data_dir: &Path,
    role: NodeRole,
) -> Result<Option<Ipv4Addr>> {
    if config.nodes.is_empty() {
        return Ok(None);
    }
    let local_addresses = local_ipv4_addresses()?;
    let resolved = if let Some(bind_ip) = config.bind_ip {
        if !local_addresses.contains(&bind_ip) {
            bail!("cluster.bind-ip `{bind_ip}` is not assigned to a local non-Tailscale interface");
        }
        bind_ip
    } else {
        let matches = config
            .nodes
            .iter()
            .map(|node| node.host_ip())
            .filter(|ip| local_addresses.contains(ip))
            .collect::<BTreeSet<_>>();
        if role.is_voter() && matches.len() == 1 {
            let host_ip = *matches.first().expect("one matched address");
            config.local_endpoint(host_ip, role)?;
            host_ip
        } else if role.is_voter() {
            bail!(
                "expected exactly one cluster.nodes address on this voter, found {} among {:?}",
                matches.len(),
                local_addresses
            );
        } else {
            let seed = config
                .resolved_nodes()?
                .into_iter()
                .next()
                .expect("cluster nodes is not empty");
            route_source_ip(seed.host_ip, seed.api_port)?
        }
    };
    if !resolved.is_private() || resolved.is_loopback() || resolved.is_unspecified() {
        bail!("resolved cluster host IP `{resolved}` is not a private host address");
    }
    if !local_addresses.contains(&resolved) {
        bail!(
            "resolved cluster host IP `{resolved}` is not assigned to a local non-Tailscale control interface"
        );
    }
    if role.is_voter() {
        config.local_endpoint(resolved, role)?;
    }
    if role == NodeRole::Worker {
        let local = config.local_endpoint(resolved, role)?;
        if config.resolved_nodes()?.contains(&local) {
            bail!(
                "worker endpoint `{}` is listed as an initial voter in cluster.nodes",
                local.api_address()
            );
        }
    }
    for subnet in &config.subnets {
        if Ipv4Cidr::parse(subnet)?.contains(resolved) {
            bail!("resolved cluster host IP `{resolved}` overlaps workload subnet `{subnet}`");
        }
    }
    if !config
        .control_allow_cidrs
        .iter()
        .map(|cidr| Ipv4Cidr::parse(cidr))
        .collect::<Result<Vec<_>>>()?
        .iter()
        .any(|cidr| cidr.contains(resolved))
    {
        bail!("resolved cluster host IP `{resolved}` is absent from cluster.control-allow-cidrs");
    }
    persist_control_ip(data_dir, resolved)?;
    if config.uses_node_ports() {
        persist_control_endpoint(data_dir, resolved, config.api_port)?;
    }
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

fn route_source_ip(destination: Ipv4Addr, port: u16) -> Result<Ipv4Addr> {
    let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0)))
        .context("failed to create route-discovery socket")?;
    socket
        .connect(SocketAddr::from((destination, port)))
        .with_context(|| format!("failed to resolve a route to cluster voter `{destination}`"))?;
    match socket.local_addr()?.ip() {
        IpAddr::V4(ip) => Ok(ip),
        IpAddr::V6(_) => bail!("route to `{destination}` selected IPv6 unexpectedly"),
    }
}

fn persist_control_ip(data_dir: &Path, resolved: Ipv4Addr) -> Result<()> {
    let system_dir = data_dir.join("system");
    let path = system_dir.join("control-ip");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let existing = existing.trim();
        if existing != resolved.to_string() {
            bail!(
                "resolved cluster host IP changed from `{existing}` to `{resolved}`; use cluster.bind-ip or restore the stable host address"
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
                "resolved cluster endpoint changed from `{existing}` to `{resolved}`; restore the stable cluster.api-port"
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

    fn valid_cluster_config() -> ClusterConfig {
        ClusterConfig {
            name: "test".to_string(),
            nodes: vec![
                "10.20.0.11".parse().unwrap(),
                "10.20.0.12".parse().unwrap(),
                "10.20.0.13".parse().unwrap(),
            ],
            subnets: vec![
                "172.22.1.0/24".to_string(),
                "172.22.2.0/24".to_string(),
                "172.22.3.0/24".to_string(),
            ],
            control_allow_cidrs: vec!["10.20.0.0/24".to_string()],
            api_port: 3001,
            gateway_port: 3002,
            etcd_client_port: 2379,
            etcd_peer_port: 2380,
            join_secret: Some("x".repeat(32)),
            shared_registry: Some("registry.example.com/maestro".to_string()),
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
    fn clustered_networks_are_separate_and_complete() {
        let config = valid_cluster_config();
        validate_cluster_config(&config, Some("172.22.2.0/24"), NodeRole::Hybrid)
            .expect("valid cluster");

        let mut overlap = valid_cluster_config();
        overlap.control_allow_cidrs = vec!["172.22.0.0/16".to_string()];
        assert!(
            validate_cluster_config(&overlap, Some("172.22.2.0/24"), NodeRole::Hybrid).is_err()
        );

        let mut incomplete = valid_cluster_config();
        incomplete.control_allow_cidrs = vec!["10.20.0.11/32".to_string()];
        assert!(
            validate_cluster_config(&incomplete, Some("172.22.2.0/24"), NodeRole::Hybrid).is_err()
        );
    }

    #[test]
    fn endpoint_nodes_allow_three_voters_on_one_host() {
        let mut config = valid_cluster_config();
        config.nodes = vec![
            "10.20.0.11:3001".parse().unwrap(),
            "10.20.0.11:3101".parse().unwrap(),
            "10.20.0.11:3201".parse().unwrap(),
        ];
        config.api_port = 3101;

        validate_cluster_config(&config, Some("172.22.2.0/24"), NodeRole::Hybrid)
            .expect("same-host cluster");
        let nodes = config.resolved_nodes().unwrap();
        assert_eq!(nodes[1].api_port, 3101);
        assert_eq!(nodes[1].gateway_port, 3102);
        assert_eq!(nodes[1].etcd_client_port, 3103);
        assert_eq!(nodes[1].etcd_peer_port, 3104);
        assert_eq!(
            config
                .local_endpoint("10.20.0.11".parse().unwrap(), NodeRole::Hybrid)
                .unwrap(),
            nodes[1]
        );
    }

    #[test]
    fn endpoint_node_port_blocks_must_not_overlap() {
        let mut config = valid_cluster_config();
        config.nodes = vec![
            "10.20.0.11:3001".parse().unwrap(),
            "10.20.0.11:3004".parse().unwrap(),
            "10.20.0.11:3201".parse().unwrap(),
        ];
        assert!(validate_cluster_config(&config, Some("172.22.1.0/24"), NodeRole::Hybrid).is_err());

        config.nodes[1] = "10.20.0.11".parse().unwrap();
        assert!(validate_cluster_config(&config, Some("172.22.1.0/24"), NodeRole::Hybrid).is_err());

        let mut legacy_port_override = valid_cluster_config();
        legacy_port_override.nodes = vec![
            "10.20.0.11:3001".parse().unwrap(),
            "10.20.0.11:3101".parse().unwrap(),
            "10.20.0.11:3201".parse().unwrap(),
        ];
        legacy_port_override.etcd_client_port = 5000;
        assert!(
            validate_cluster_config(
                &legacy_port_override,
                Some("172.22.1.0/24"),
                NodeRole::Hybrid,
            )
            .is_err()
        );
    }

    #[test]
    fn bare_ip_nodes_keep_legacy_ports_and_identity() {
        let config = valid_cluster_config();
        let node = config.resolved_nodes().unwrap()[0];
        assert_eq!(node.api_port, 3001);
        assert_eq!(node.gateway_port, 3002);
        assert_eq!(node.etcd_client_port, 2379);
        assert_eq!(node.etcd_peer_port, 2380);
        assert_eq!(node.identity_api_port, None);
        assert_eq!(node.member_name(), "maestro-0a14000b");
    }
}
