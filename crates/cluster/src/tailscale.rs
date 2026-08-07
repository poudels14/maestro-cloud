use std::collections::BTreeSet;
use std::net::Ipv4Addr;

use kernel_api::{ClusterId, SecretValue};
use serde::{Deserialize, Serialize};

use crate::Ipv4Cidr;

/// Durable replacement for the launch-document key used by managed gateways.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TailscaleAuthKeyRecord {
    /// Credential used only when a gateway replica has no persisted identity.
    pub auth_key: SecretValue,
}

impl TailscaleAuthKeyRecord {
    /// Validates and wraps one key before it crosses the cluster-store boundary.
    pub fn new(auth_key: SecretValue) -> Result<Self, TailscaleConfigError> {
        let auth_key = SecretValue::new(auth_key.expose().trim());
        validate_auth_key(&auth_key)?;
        Ok(Self { auth_key })
    }

    /// Rejects a malformed record loaded from durable state.
    pub fn validate(&self) -> Result<(), TailscaleConfigError> {
        validate_auth_key(&self.auth_key)
    }
}

/// Optional cluster-wide Tailscale subnet-router configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TailscaleGatewayConfig {
    /// Credential used only when a gateway replica has no persisted identity.
    pub auth_key: SecretValue,
    /// Private workload routes advertised to the tailnet, or every node subnet by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertise_routes: Option<Vec<Ipv4Cidr>>,
    /// Tailnet policy tags assigned during initial authentication.
    #[serde(default = "default_tags")]
    pub tags: Vec<String>,
    /// Explicit remote cluster suffixes reachable only through managed Tailscale gateways.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cross_cluster_dns: Vec<CrossClusterDnsRoute>,
}

/// One remote Maestro DNS suffix and its bridge resolver addresses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CrossClusterDnsRoute {
    /// Remote cluster identity embedded in its authoritative DNS names.
    pub cluster_id: ClusterId,
    /// Remote bridge resolver addresses advertised into the shared tailnet.
    pub nameservers: Vec<Ipv4Addr>,
}

impl TailscaleGatewayConfig {
    pub(crate) fn validate(
        &self,
        local_cluster_id: &ClusterId,
        workload_subnets: &[Ipv4Cidr],
    ) -> Result<(), TailscaleConfigError> {
        validate_auth_key(&self.auth_key)?;
        if self.advertise_routes.as_ref().is_some_and(Vec::is_empty) {
            return Err(TailscaleConfigError::EmptyAdvertiseRoutes);
        }
        let mut routes = BTreeSet::new();
        for (index, route) in self
            .advertised_routes(workload_subnets)
            .into_iter()
            .enumerate()
        {
            if !workload_subnets
                .iter()
                .any(|subnet| subnet.contains_network(route))
            {
                return Err(TailscaleConfigError::RouteOutsideWorkloadSubnets { index, route });
            }
            if !routes.insert(route) {
                return Err(TailscaleConfigError::DuplicateRoute { index, route });
            }
        }
        let has_reachable_resolver = workload_subnets.iter().any(|subnet| {
            subnet
                .gateway_address()
                .is_some_and(|address| routes.iter().any(|route| route.contains(address)))
        });
        if !has_reachable_resolver {
            return Err(TailscaleConfigError::NoReachableDnsResolver);
        }

        let mut tags = BTreeSet::new();
        for (index, tag) in self.tags.iter().enumerate() {
            if !valid_tag(tag) {
                return Err(TailscaleConfigError::InvalidTag {
                    index,
                    tag: tag.clone(),
                });
            }
            if !tags.insert(tag) {
                return Err(TailscaleConfigError::DuplicateTag {
                    index,
                    tag: tag.clone(),
                });
            }
        }
        self.validate_dns_routes(local_cluster_id, workload_subnets)?;
        Ok(())
    }

    /// Returns the configured routes or every explicit workload subnet.
    pub fn advertised_routes(&self, workload_subnets: &[Ipv4Cidr]) -> Vec<Ipv4Cidr> {
        self.advertise_routes
            .clone()
            .unwrap_or_else(|| workload_subnets.to_vec())
    }

    fn validate_dns_routes(
        &self,
        local_cluster_id: &ClusterId,
        workload_subnets: &[Ipv4Cidr],
    ) -> Result<(), TailscaleConfigError> {
        let mut cluster_ids = BTreeSet::new();
        for (route_index, route) in self.cross_cluster_dns.iter().enumerate() {
            if &route.cluster_id == local_cluster_id {
                return Err(TailscaleConfigError::LocalDnsRoute { route_index });
            }
            if !cluster_ids.insert(route.cluster_id.clone()) {
                return Err(TailscaleConfigError::DuplicateDnsRoute {
                    route_index,
                    cluster_id: route.cluster_id.clone(),
                });
            }
            if route.nameservers.is_empty() {
                return Err(TailscaleConfigError::EmptyDnsNameservers { route_index });
            }
            let mut nameservers = BTreeSet::new();
            for (nameserver_index, nameserver) in route.nameservers.iter().enumerate() {
                if !nameserver.is_private()
                    || nameserver.is_loopback()
                    || nameserver.is_multicast()
                    || nameserver == &Ipv4Addr::BROADCAST
                    || workload_subnets
                        .iter()
                        .any(|subnet| subnet.contains(*nameserver))
                {
                    return Err(TailscaleConfigError::UnsafeDnsNameserver {
                        route_index,
                        nameserver_index,
                        nameserver: *nameserver,
                    });
                }
                if !nameservers.insert(nameserver) {
                    return Err(TailscaleConfigError::DuplicateDnsNameserver {
                        route_index,
                        nameserver_index,
                        nameserver: *nameserver,
                    });
                }
            }
        }
        Ok(())
    }
}

/// Why optional Tailscale gateway settings cannot be admitted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TailscaleConfigError {
    #[error("Tailscale auth key must contain at least 16 characters")]
    WeakAuthKey,
    #[error("Tailscale auth key must contain no more than 512 characters")]
    AuthKeyTooLong,
    #[error("Tailscale advertise routes must not be an empty list")]
    EmptyAdvertiseRoutes,
    #[error("Tailscale advertise route {index} `{route}` is outside every workload subnet")]
    RouteOutsideWorkloadSubnets { index: usize, route: Ipv4Cidr },
    #[error("Tailscale advertise route {index} duplicates `{route}`")]
    DuplicateRoute { index: usize, route: Ipv4Cidr },
    #[error("Tailscale advertise routes must include at least one workload bridge DNS resolver")]
    NoReachableDnsResolver,
    #[error("Tailscale tag {index} `{tag}` must use the form `tag:<lowercase-dns-label>`")]
    InvalidTag { index: usize, tag: String },
    #[error("Tailscale tag {index} duplicates `{tag}`")]
    DuplicateTag { index: usize, tag: String },
    /// A remote route attempted to delegate the local cluster suffix.
    #[error("Tailscale cross-cluster DNS route {route_index} targets the local cluster")]
    LocalDnsRoute {
        /// Position of the rejected route.
        route_index: usize,
    },
    /// More than one route attempted to own the same remote cluster suffix.
    #[error(
        "Tailscale cross-cluster DNS route {route_index} duplicates remote cluster `{cluster_id}`"
    )]
    DuplicateDnsRoute {
        /// Position of the duplicate route.
        route_index: usize,
        /// Remote identity already owned by an earlier route.
        cluster_id: ClusterId,
    },
    /// A remote suffix was declared without any authoritative bridge resolver.
    #[error("Tailscale cross-cluster DNS route {route_index} has no nameservers")]
    EmptyDnsNameservers {
        /// Position of the empty route.
        route_index: usize,
    },
    /// A remote resolver address was public, unsafe, or part of a local workload subnet.
    #[error(
        "Tailscale cross-cluster DNS route {route_index} nameserver {nameserver_index} `{nameserver}` is not a remote private bridge address"
    )]
    UnsafeDnsNameserver {
        /// Position of the owning route.
        route_index: usize,
        /// Position of the rejected nameserver.
        nameserver_index: usize,
        /// Unsafe remote resolver address.
        nameserver: Ipv4Addr,
    },
    /// One remote route listed the same resolver address more than once.
    #[error(
        "Tailscale cross-cluster DNS route {route_index} nameserver {nameserver_index} duplicates `{nameserver}`"
    )]
    DuplicateDnsNameserver {
        /// Position of the owning route.
        route_index: usize,
        /// Position of the duplicate nameserver.
        nameserver_index: usize,
        /// Resolver address already present in the route.
        nameserver: Ipv4Addr,
    },
}

fn default_tags() -> Vec<String> {
    Vec::new()
}

fn validate_auth_key(auth_key: &SecretValue) -> Result<(), TailscaleConfigError> {
    let length = auth_key.expose().trim().chars().count();
    if length < 16 {
        Err(TailscaleConfigError::WeakAuthKey)
    } else if length > 512 {
        Err(TailscaleConfigError::AuthKeyTooLong)
    } else {
        Ok(())
    }
}

fn valid_tag(tag: &str) -> bool {
    let Some(label) = tag.strip_prefix("tag:") else {
        return false;
    };
    !label.is_empty()
        && label.len() <= 63
        && label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && label
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && label
            .bytes()
            .next_back()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}
