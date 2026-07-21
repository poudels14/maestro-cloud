use std::net::{IpAddr, Ipv4Addr};

use ipnet::IpNet;

use crate::FirewallPlanError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum AddressFamily {
    V4,
    V6,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CanonicalCidr(IpNet);

impl CanonicalCidr {
    pub(crate) fn parse(value: &str, field: &str) -> Result<Self, FirewallPlanError> {
        let network = value
            .parse::<IpNet>()
            .map_err(|error| FirewallPlanError::InvalidCidr {
                field: field.to_string(),
                value: value.to_string(),
                message: error.to_string(),
            })?;
        if network.addr() != network.network() {
            return Err(FirewallPlanError::InvalidCidr {
                field: field.to_string(),
                value: value.to_string(),
                message: format!("CIDR is not canonical; use {}", network.trunc()),
            });
        }
        Ok(Self(network))
    }

    pub(crate) const fn family(&self) -> AddressFamily {
        match self.0 {
            IpNet::V4(_) => AddressFamily::V4,
            IpNet::V6(_) => AddressFamily::V6,
        }
    }

    pub(crate) fn contains(&self, address: &IpAddr) -> bool {
        self.0.contains(address)
    }

    pub(crate) fn bridge_address(&self) -> Option<Ipv4Addr> {
        let IpNet::V4(network) = self.0 else {
            return None;
        };
        let address = u32::from(network.network()).checked_add(1)?;
        let address = Ipv4Addr::from(address);
        network.contains(&address).then_some(address)
    }
}

impl std::fmt::Display for CanonicalCidr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}
