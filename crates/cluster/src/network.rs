use std::fmt::{Display, Formatter};
use std::net::Ipv4Addr;
use std::str::FromStr;

use ipnet::Ipv4Net;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A canonical IPv4 network and prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ipv4Cidr {
    network: Ipv4Net,
}

impl Ipv4Cidr {
    /// Number of highest host addresses reserved for Maestro system services.
    pub const SYSTEM_RESERVED_HOSTS: u32 = 55;
    /// Stable offset from the first `/24` broadcast address used by Admin.
    pub const ADMIN_ADDRESS_OFFSET: u32 = 5;

    /// Creates a CIDR when `network` is the canonical address for `prefix`.
    pub fn new(network: Ipv4Addr, prefix: u8) -> Result<Self, CidrError> {
        let network =
            Ipv4Net::new(network, prefix).map_err(|_| CidrError::InvalidPrefix { prefix })?;
        let supplied = network.addr();
        let canonical = network.network();
        if canonical != supplied {
            return Err(CidrError::NonCanonical {
                supplied,
                canonical,
                prefix,
            });
        }

        Ok(Self { network })
    }

    /// Returns the canonical network address.
    pub fn network_address(self) -> Ipv4Addr {
        self.network.network()
    }

    /// Returns the prefix length.
    pub fn prefix(self) -> u8 {
        self.network.prefix_len()
    }

    /// Returns whether this network contains `address`.
    pub fn contains(self, address: Ipv4Addr) -> bool {
        self.network.contains(&address)
    }

    /// Returns whether every address in `network` belongs to this network.
    pub fn contains_network(self, network: Self) -> bool {
        self.contains(network.network_address()) && self.contains(network.broadcast_address())
    }

    /// Returns the number of addresses represented by this network.
    pub fn address_count(self) -> u64 {
        1_u64 << (32 - self.prefix())
    }

    /// Returns whether either network contains any address from the other.
    pub fn overlaps(self, other: Self) -> bool {
        self.contains(other.network_address()) || other.contains(self.network_address())
    }

    /// Returns the broadcast address.
    pub fn broadcast_address(self) -> Ipv4Addr {
        self.network.broadcast()
    }

    /// Returns the runtime gateway address reserved at the start of a subnet.
    pub fn gateway_address(self) -> Option<Ipv4Addr> {
        let network = u32::from(self.network_address());
        let broadcast = u32::from(self.broadcast_address());
        network
            .checked_add(1)
            .filter(|address| *address < broadcast)
            .map(Ipv4Addr::from)
    }

    /// Returns a usable address relative to the broadcast address.
    pub fn host_address_from_end(self, offset: u32) -> Option<Ipv4Addr> {
        let network = u32::from(self.network_address());
        let broadcast = u32::from(self.broadcast_address());
        broadcast
            .checked_sub(offset)
            .filter(|address| *address > network && *address < broadcast)
            .map(Ipv4Addr::from)
    }

    /// Returns a fixed system address from the end of the first `/24`.
    ///
    /// Keeping system addresses in the first `/24` preserves the historical
    /// Maestro address layout for larger standalone networks.
    pub fn system_address_from_end(self, offset: u32) -> Option<Ipv4Addr> {
        if self.prefix() >= 24 {
            return self.host_address_from_end(offset);
        }

        let network = u32::from(self.network_address());
        network
            .checked_add(255)
            .and_then(|end| end.checked_sub(offset))
            .filter(|address| *address > network)
            .map(Ipv4Addr::from)
    }

    /// Returns the predictable Admin endpoint reserved at `.250` in a `/24`.
    pub fn admin_address(self) -> Option<Ipv4Addr> {
        self.system_address_from_end(Self::ADMIN_ADDRESS_OFFSET)
    }

    /// Iterates addresses available for workload replicas.
    ///
    /// The network and gateway addresses are excluded, as are the broadcast
    /// address and Maestro's fixed system allocation in the first `/24`.
    pub fn workload_addresses(self) -> impl Iterator<Item = Ipv4Addr> {
        self.network
            .hosts()
            .filter(move |address| self.is_workload_address(*address))
    }

    /// Returns whether `address` belongs to the workload allocation range.
    pub fn is_workload_address(self, address: Ipv4Addr) -> bool {
        let address = u32::from(address);
        let first = u32::from(self.network_address()).saturating_add(2);
        let broadcast = u32::from(self.broadcast_address());
        let system_end = if self.prefix() < 24 {
            u32::from(self.network_address()).saturating_add(255)
        } else {
            broadcast
        };
        let system_start = system_end.saturating_sub(Self::SYSTEM_RESERVED_HOSTS);
        address >= first
            && address < broadcast
            && !(address >= system_start && address < system_end)
    }

    /// Returns whether the whole network is RFC 1918 private space.
    pub fn is_private(self) -> bool {
        self.network.network().is_private() && self.network.broadcast().is_private()
    }
}

impl FromStr for Ipv4Cidr {
    type Err = CidrError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (address, prefix) = value
            .split_once('/')
            .ok_or_else(|| CidrError::InvalidFormat(value.to_owned()))?;
        let address = address
            .parse::<Ipv4Addr>()
            .map_err(|_| CidrError::InvalidAddress(address.to_owned()))?;
        let prefix = prefix
            .parse::<u8>()
            .map_err(|_| CidrError::InvalidPrefixText(prefix.to_owned()))?;
        Self::new(address, prefix)
    }
}

impl Display for Ipv4Cidr {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.network, formatter)
    }
}

impl Serialize for Ipv4Cidr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Ipv4Cidr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

/// Why an IPv4 CIDR was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CidrError {
    /// The separating slash was missing.
    #[error("invalid IPv4 CIDR `{0}`")]
    InvalidFormat(String),
    /// The address portion was not IPv4.
    #[error("invalid IPv4 address `{0}`")]
    InvalidAddress(String),
    /// The prefix was not a decimal integer.
    #[error("invalid IPv4 prefix `{0}`")]
    InvalidPrefixText(String),
    /// The prefix exceeded the IPv4 width.
    #[error("IPv4 prefix {prefix} exceeds 32")]
    InvalidPrefix { prefix: u8 },
    /// Host bits were set in the supplied network address.
    #[error("CIDR `{supplied}/{prefix}` is not canonical; use `{canonical}/{prefix}`")]
    NonCanonical {
        /// Address supplied by the caller.
        supplied: Ipv4Addr,
        /// Canonical network address.
        canonical: Ipv4Addr,
        /// Requested prefix.
        prefix: u8,
    },
}
