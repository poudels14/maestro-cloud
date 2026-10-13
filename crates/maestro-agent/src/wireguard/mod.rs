mod command;
mod key;

use std::collections::{BTreeMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use maestro_core::MeshPeer;

use crate::error::AgentError;

pub use command::{CommandOutput, CommandRunner, HostCommandRunner};
pub use key::{NodeKey, load_or_generate_key, public_key};

pub const DEFAULT_INTERFACE: &str = "maestro-wg";
pub const DEFAULT_LISTEN_PORT: u16 = 51820;
const PERSISTENT_KEEPALIVE: u16 = 25;
const NAT_TABLE: &str = "maestro";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshConfig {
    pub interface: String,
    pub listen_port: u16,
    pub private_key_path: PathBuf,
    pub local_subnet: String,
    pub cluster_cidr: String,
    pub bridge: String,
    pub peers: Vec<MeshPeer>,
}

pub struct WireGuardMesh<R: CommandRunner = HostCommandRunner> {
    runner: R,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct DeviceState {
    public_key: Option<String>,
    listen_port: u16,
    peers: BTreeMap<String, PeerState>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct PeerState {
    endpoint: Option<SocketAddr>,
    allowed_ips: Vec<String>,
    persistent_keepalive: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
struct LinkEntry {
    #[serde(default)]
    flags: Vec<String>,
    #[serde(default)]
    addr_info: Vec<AddressEntry>,
}

#[derive(Debug, Deserialize)]
struct AddressEntry {
    family: String,
    local: String,
    prefixlen: u8,
}

#[derive(Debug, Deserialize)]
struct RouteEntry {
    dst: String,
}

impl WireGuardMesh<HostCommandRunner> {
    pub fn host() -> Self {
        Self::new(HostCommandRunner)
    }
}

impl<R: CommandRunner> WireGuardMesh<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }

    pub async fn apply(&self, config: &MeshConfig) -> Result<(), AgentError> {
        let link = self.ensure_link(config).await?;
        self.ensure_address(config, &link).await?;
        self.ensure_device(config).await?;
        if !link.flags.iter().any(|flag| flag == "UP") {
            self.mutate("ip", &["link", "set", &config.interface, "up"], None)
                .await?;
        }
        self.ensure_routes(config).await?;
        self.ensure_nat(config).await
    }

    async fn ensure_link(&self, config: &MeshConfig) -> Result<LinkEntry, AgentError> {
        let current = self.query_link(&config.interface).await?;
        match current {
            Some(link) => Ok(link),
            None => {
                self.mutate(
                    "ip",
                    &["link", "add", &config.interface, "type", "wireguard"],
                    None,
                )
                .await?;
                Ok(LinkEntry::default())
            }
        }
    }

    async fn ensure_address(
        &self,
        config: &MeshConfig,
        link: &LinkEntry,
    ) -> Result<(), AgentError> {
        let desired = format!("{}/32", gateway_address(&config.local_subnet)?);
        let current = link
            .addr_info
            .iter()
            .filter(|address| address.family == "inet")
            .map(|address| format!("{}/{}", address.local, address.prefixlen))
            .collect::<Vec<_>>();
        for stale in current.iter().filter(|address| **address != desired) {
            self.mutate(
                "ip",
                &["addr", "del", stale, "dev", &config.interface],
                None,
            )
            .await?;
        }
        if !current.contains(&desired) {
            self.mutate(
                "ip",
                &["addr", "add", &desired, "dev", &config.interface],
                None,
            )
            .await?;
        }
        Ok(())
    }

    async fn ensure_device(&self, config: &MeshConfig) -> Result<(), AgentError> {
        let private_key_path = config.private_key_path.to_string_lossy().into_owned();
        let private_key = tokio::fs::read_to_string(&config.private_key_path)
            .await
            .map_err(|error| {
                AgentError::Host(format!("failed to read {private_key_path}: {error}"))
            })?;
        let dump = self
            .checked("wg", &["show", &config.interface, "dump"], None)
            .await?;
        let state = parse_dump(&dump);
        if state.public_key != Some(public_key(&private_key)?)
            || state.listen_port != config.listen_port
        {
            self.mutate(
                "wg",
                &[
                    "set",
                    &config.interface,
                    "listen-port",
                    &config.listen_port.to_string(),
                    "private-key",
                    &private_key_path,
                ],
                None,
            )
            .await?;
        }
        let desired_keys = config
            .peers
            .iter()
            .map(|peer| peer.public_key.as_str())
            .collect::<HashSet<_>>();
        for stale in state
            .peers
            .keys()
            .filter(|public_key| !desired_keys.contains(public_key.as_str()))
        {
            self.mutate(
                "wg",
                &["set", &config.interface, "peer", stale, "remove"],
                None,
            )
            .await?;
        }
        for peer in config
            .peers
            .iter()
            .filter(|peer| state.peers.get(&peer.public_key) != Some(&desired_peer_state(peer)))
        {
            self.mutate(
                "wg",
                &[
                    "set",
                    &config.interface,
                    "peer",
                    &peer.public_key,
                    "endpoint",
                    &peer.endpoint.to_string(),
                    "allowed-ips",
                    &peer.subnet,
                    "persistent-keepalive",
                    &PERSISTENT_KEEPALIVE.to_string(),
                ],
                None,
            )
            .await?;
        }
        Ok(())
    }

    async fn ensure_routes(&self, config: &MeshConfig) -> Result<(), AgentError> {
        let output = self
            .checked(
                "ip",
                &["-j", "route", "show", "dev", &config.interface],
                None,
            )
            .await?;
        let current = parse_json::<RouteEntry>(&output)?
            .into_iter()
            .map(|route| normalize_prefix(&route.dst))
            .collect::<Vec<_>>();
        let desired = config
            .peers
            .iter()
            .map(|peer| normalize_prefix(&peer.subnet))
            .collect::<Vec<_>>();
        for stale in current.iter().filter(|route| !desired.contains(route)) {
            self.mutate(
                "ip",
                &["route", "del", stale, "dev", &config.interface],
                None,
            )
            .await?;
        }
        for missing in desired.iter().filter(|route| !current.contains(route)) {
            self.mutate(
                "ip",
                &["route", "replace", missing, "dev", &config.interface],
                None,
            )
            .await?;
        }
        Ok(())
    }

    async fn ensure_nat(&self, config: &MeshConfig) -> Result<(), AgentError> {
        let marker = nat_marker(config);
        let current = self
            .runner
            .run("nft", &to_args(&["list", "table", "ip", NAT_TABLE]), None)
            .await?;
        if !(current.success && current.stdout.contains(&format!("comment \"{marker}\""))) {
            self.mutate("nft", &["-f", "-"], Some(&nat_ruleset(config)))
                .await?;
        }
        Ok(())
    }

    async fn query_link(&self, interface: &str) -> Result<Option<LinkEntry>, AgentError> {
        let output = self
            .runner
            .run(
                "ip",
                &to_args(&["-j", "addr", "show", "dev", interface]),
                None,
            )
            .await?;
        match output.success {
            true => Ok(parse_json::<LinkEntry>(&output.stdout)?.into_iter().next()),
            false => Ok(None),
        }
    }

    async fn mutate(
        &self,
        program: &str,
        args: &[&str],
        stdin: Option<&str>,
    ) -> Result<(), AgentError> {
        tracing::info!(program, args = args.join(" "), "converging WireGuard mesh");
        self.checked(program, args, stdin).await.map(|_| ())
    }

    async fn checked(
        &self,
        program: &str,
        args: &[&str],
        stdin: Option<&str>,
    ) -> Result<String, AgentError> {
        let output = self.runner.run(program, &to_args(args), stdin).await?;
        match output.success {
            true => Ok(output.stdout),
            false => Err(AgentError::Host(format!(
                "{program} {} failed: {}",
                args.join(" "),
                output.stderr.trim()
            ))),
        }
    }
}

fn nat_ruleset(config: &MeshConfig) -> String {
    format!(
        "table ip {NAT_TABLE} {{\n}}\n\
         delete table ip {NAT_TABLE}\n\
         table ip {NAT_TABLE} {{\n\
         \tchain postrouting {{\n\
         \t\ttype nat hook postrouting priority srcnat; policy accept;\n\
         \t\tip saddr {} ip daddr != {} oifname != \"{}\" masquerade comment \"{}\"\n\
         \t}}\n\
         }}\n",
        config.local_subnet,
        config.cluster_cidr,
        config.bridge,
        nat_marker(config)
    )
}

fn nat_marker(config: &MeshConfig) -> String {
    format!(
        "maestro-mesh {} {} {}",
        config.local_subnet, config.cluster_cidr, config.bridge
    )
}

fn desired_peer_state(peer: &MeshPeer) -> PeerState {
    PeerState {
        endpoint: Some(peer.endpoint),
        allowed_ips: vec![normalize_prefix(&peer.subnet)],
        persistent_keepalive: Some(PERSISTENT_KEEPALIVE),
    }
}

fn parse_dump(dump: &str) -> DeviceState {
    let mut lines = dump.lines().filter(|line| !line.trim().is_empty());
    let interface_fields = lines
        .next()
        .map(|line| line.split('\t').collect::<Vec<_>>())
        .unwrap_or_default();
    DeviceState {
        public_key: interface_fields
            .get(1)
            .and_then(|field| present(field))
            .map(str::to_string),
        listen_port: interface_fields
            .get(2)
            .and_then(|field| field.parse().ok())
            .unwrap_or_default(),
        peers: lines.filter_map(parse_peer_line).collect(),
    }
}

fn parse_peer_line(line: &str) -> Option<(String, PeerState)> {
    let fields = line.split('\t').collect::<Vec<_>>();
    let public_key = fields.first()?.to_string();
    let state = PeerState {
        endpoint: fields
            .get(2)
            .and_then(|field| present(field))
            .and_then(|field| field.parse().ok()),
        allowed_ips: fields
            .get(3)
            .and_then(|field| present(field))
            .map(|field| field.split(',').map(normalize_prefix).collect())
            .unwrap_or_default(),
        persistent_keepalive: fields.get(7).and_then(|field| field.parse().ok()),
    };
    Some((public_key, state))
}

fn present(field: &str) -> Option<&str> {
    match field {
        "" | "(none)" => None,
        value => Some(value),
    }
}

fn normalize_prefix(prefix: &str) -> String {
    match prefix.trim().contains('/') {
        true => prefix.trim().to_string(),
        false => format!("{}/32", prefix.trim()),
    }
}

fn gateway_address(subnet: &str) -> Result<Ipv4Addr, AgentError> {
    subnet
        .split_once('/')
        .and_then(|(network, _)| network.parse::<Ipv4Addr>().ok())
        .map(|network| Ipv4Addr::from(u32::from(network) + 1))
        .ok_or_else(|| AgentError::Host(format!("invalid subnet {subnet}")))
}

fn parse_json<T: DeserializeOwned>(output: &str) -> Result<Vec<T>, AgentError> {
    match output.trim().is_empty() {
        true => Ok(Vec::new()),
        false => serde_json::from_str(output)
            .map_err(|error| AgentError::Host(format!("failed to parse ip output: {error}"))),
    }
}

fn to_args(args: &[&str]) -> Vec<String> {
    args.iter().map(|argument| argument.to_string()).collect()
}

#[cfg(test)]
mod tests;
