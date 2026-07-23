use std::collections::BTreeSet;

use kernel_api::SecretValue;
use serde::{Deserialize, Serialize};

use crate::Ipv4Cidr;

/// Optional cluster-wide Tailscale subnet-router configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TailscaleGatewayConfig {
    /// Credential used only when a gateway replica has no persisted identity.
    pub auth_key: SecretValue,
    /// Private cluster routes advertised to the tailnet, or the cluster CIDR by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertise_routes: Option<Vec<Ipv4Cidr>>,
    /// Desired high-availability gateway replicas.
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    /// Tailnet policy tags assigned during initial authentication.
    #[serde(default = "default_tags")]
    pub tags: Vec<String>,
}

impl TailscaleGatewayConfig {
    pub(crate) fn validate(
        &self,
        cluster_cidr: Ipv4Cidr,
        workload_subnets: &[Ipv4Cidr],
    ) -> Result<(), TailscaleConfigError> {
        if self.auth_key.expose().trim().chars().count() < 16 {
            return Err(TailscaleConfigError::WeakAuthKey);
        }
        if self.replicas == 0 {
            return Err(TailscaleConfigError::ZeroReplicas);
        }
        if self.replicas as usize > workload_subnets.len() {
            return Err(TailscaleConfigError::InsufficientWorkloadNodes {
                replicas: self.replicas,
                workload_nodes: workload_subnets.len(),
            });
        }

        if self.advertise_routes.as_ref().is_some_and(Vec::is_empty) {
            return Err(TailscaleConfigError::EmptyAdvertiseRoutes);
        }
        let mut routes = BTreeSet::new();
        for (index, route) in self.advertised_routes(cluster_cidr).into_iter().enumerate() {
            if !cluster_cidr.contains_network(route) {
                return Err(TailscaleConfigError::RouteOutsideCluster {
                    index,
                    route,
                    cluster_cidr,
                });
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

        if self.tags.is_empty() {
            return Err(TailscaleConfigError::NoTags);
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
        Ok(())
    }

    /// Returns the configured routes or the complete cluster address pool.
    pub fn advertised_routes(&self, cluster_cidr: Ipv4Cidr) -> Vec<Ipv4Cidr> {
        self.advertise_routes
            .clone()
            .unwrap_or_else(|| vec![cluster_cidr])
    }
}

/// Why optional Tailscale gateway settings cannot be admitted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TailscaleConfigError {
    #[error("Tailscale auth key must contain at least 16 characters")]
    WeakAuthKey,
    #[error("Tailscale replica count must be greater than zero")]
    ZeroReplicas,
    #[error("Tailscale requests {replicas} replicas but only {workload_nodes} nodes run workloads")]
    InsufficientWorkloadNodes {
        replicas: u32,
        workload_nodes: usize,
    },
    #[error("Tailscale advertise routes must not be an empty list")]
    EmptyAdvertiseRoutes,
    #[error("Tailscale advertise route {index} `{route}` is outside cluster CIDR `{cluster_cidr}`")]
    RouteOutsideCluster {
        index: usize,
        route: Ipv4Cidr,
        cluster_cidr: Ipv4Cidr,
    },
    #[error("Tailscale advertise route {index} duplicates `{route}`")]
    DuplicateRoute { index: usize, route: Ipv4Cidr },
    #[error("Tailscale advertise routes must include at least one workload bridge DNS resolver")]
    NoReachableDnsResolver,
    #[error("Tailscale must configure at least one device tag")]
    NoTags,
    #[error("Tailscale tag {index} `{tag}` must use the form `tag:<lowercase-dns-label>`")]
    InvalidTag { index: usize, tag: String },
    #[error("Tailscale tag {index} duplicates `{tag}`")]
    DuplicateTag { index: usize, tag: String },
}

const fn default_replicas() -> u32 {
    2
}

fn default_tags() -> Vec<String> {
    vec!["tag:maestro-gateway".to_owned()]
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
