use std::fmt::{Display, Formatter};
use std::net::Ipv4Addr;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A canonical IPv4 network and prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ipv4Cidr {
    network: Ipv4Addr,
    prefix: u8,
}

impl Ipv4Cidr {
    /// Number of highest host addresses reserved for Maestro system services.
    pub const SYSTEM_RESERVED_HOSTS: u32 = 55;
    /// Stable offset from the first `/24` broadcast address used by Admin.
    pub const ADMIN_ADDRESS_OFFSET: u32 = 5;

    /// Creates a CIDR when `network` is the canonical address for `prefix`.
    pub fn new(network: Ipv4Addr, prefix: u8) -> Result<Self, CidrError> {
        if prefix > 32 {
            return Err(CidrError::InvalidPrefix { prefix });
        }

        let canonical = Ipv4Addr::from(u32::from(network) & prefix_mask(prefix));
        if canonical != network {
            return Err(CidrError::NonCanonical {
                supplied: network,
                canonical,
                prefix,
            });
        }

        Ok(Self { network, prefix })
    }

    /// Returns the canonical network address.
    pub fn network_address(self) -> Ipv4Addr {
        self.network
    }

    /// Returns the prefix length.
    pub fn prefix(self) -> u8 {
        self.prefix
    }

    /// Returns whether this network contains `address`.
    pub fn contains(self, address: Ipv4Addr) -> bool {
        u32::from(address) & prefix_mask(self.prefix) == u32::from(self.network)
    }

    /// Returns whether every address in `network` belongs to this network.
    pub fn contains_network(self, network: Self) -> bool {
        self.contains(network.network_address()) && self.contains(network.broadcast_address())
    }

    /// Returns the number of addresses represented by this network.
    pub fn address_count(self) -> u64 {
        1_u64 << (32 - self.prefix)
    }

    /// Returns whether either network contains any address from the other.
    pub fn overlaps(self, other: Self) -> bool {
        self.contains(other.network) || other.contains(self.network)
    }

    /// Returns the broadcast address.
    pub fn broadcast_address(self) -> Ipv4Addr {
        Ipv4Addr::from(u32::from(self.network) | !prefix_mask(self.prefix))
    }

    /// Returns the runtime gateway address reserved at the start of a subnet.
    pub fn gateway_address(self) -> Option<Ipv4Addr> {
        let network = u32::from(self.network);
        let broadcast = u32::from(self.broadcast_address());
        network
            .checked_add(1)
            .filter(|address| *address < broadcast)
            .map(Ipv4Addr::from)
    }

    /// Returns a usable address relative to the broadcast address.
    pub fn host_address_from_end(self, offset: u32) -> Option<Ipv4Addr> {
        let network = u32::from(self.network);
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
        if self.prefix >= 24 {
            return self.host_address_from_end(offset);
        }

        let network = u32::from(self.network);
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
    /// address and Maestro's fixed-address system allocation at the top.
    pub fn workload_addresses(self) -> impl Iterator<Item = Ipv4Addr> {
        let first = u32::from(self.network).saturating_add(2);
        let end = u32::from(self.broadcast_address()).saturating_sub(Self::SYSTEM_RESERVED_HOSTS);
        (first..end).map(Ipv4Addr::from)
    }

    /// Returns whether `address` belongs to the workload allocation range.
    pub fn is_workload_address(self, address: Ipv4Addr) -> bool {
        let address = u32::from(address);
        let first = u32::from(self.network).saturating_add(2);
        let end = u32::from(self.broadcast_address()).saturating_sub(Self::SYSTEM_RESERVED_HOSTS);
        address >= first && address < end
    }

    /// Returns whether the whole network is RFC 1918 private space.
    pub fn is_private(self) -> bool {
        self.network.is_private() && self.broadcast_address().is_private()
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
        write!(formatter, "{}/{}", self.network, self.prefix)
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

fn prefix_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}
