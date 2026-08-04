use std::collections::BTreeSet;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, TAILSCALE_GATEWAY_SERVICE_ID};

use crate::dns_socks::{DnsForwardClient, Socks5DnsForwardClient};
use crate::{DnsLookup, DnsQueryType, DnsZoneReader, MAESTRO_DNS_ZONE};

const TAILSCALE_SOCKS_PORT: u16 = 1_055;
const REMOTE_DNS_PORT: u16 = 53;

/// Optional lookup boundary invoked only after the local authoritative zone misses a name.
#[async_trait]
pub trait DnsResolverPlugin: Send + Sync {
    /// Resolves a configured name or returns `None` when the plugin does not own its suffix.
    async fn lookup(
        &self,
        name: &str,
        query_type: DnsQueryType,
    ) -> Result<Option<DnsLookup>, DnsResolverPluginError>;
}

/// One remote cluster suffix and its bridge-scoped authoritative nameservers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TailscaleDnsRoute {
    cluster_id: ClusterId,
    nameservers: Vec<Ipv4Addr>,
}

impl TailscaleDnsRoute {
    /// Defines one remote cluster route after cluster-level validation.
    pub fn new(cluster_id: ClusterId, nameservers: Vec<Ipv4Addr>) -> Self {
        Self {
            cluster_id,
            nameservers,
        }
    }

    fn domain(&self) -> String {
        format!("{}.{}", self.cluster_id, MAESTRO_DNS_ZONE)
    }
}

/// Explicit cross-cluster forwarding through managed Tailscale SOCKS gateways.
pub(crate) struct TailscaleDnsResolverPlugin {
    zone: DnsZoneReader,
    gateway_name: String,
    routes: Vec<TailscaleDnsRoute>,
    timeout: Duration,
    client: Arc<dyn DnsForwardClient>,
}

/// Validated node-local view of optional cross-cluster DNS configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TailscaleDnsPluginSettings {
    local_cluster_id: ClusterId,
    routes: Vec<TailscaleDnsRoute>,
    timeout: Duration,
}

impl TailscaleDnsPluginSettings {
    /// Validates suffix ownership, nameserver safety, and a bounded lookup timeout.
    pub fn new(
        local_cluster_id: ClusterId,
        routes: Vec<TailscaleDnsRoute>,
        timeout: Duration,
    ) -> Result<Self, DnsResolverPluginError> {
        if routes.is_empty() {
            return Err(DnsResolverPluginError::new(
                "at least one cross-cluster DNS route is required",
            ));
        }
        if timeout.is_zero() {
            return Err(DnsResolverPluginError::new(
                "cross-cluster DNS timeout must be nonzero",
            ));
        }
        validate_routes(&local_cluster_id, &routes)?;
        Ok(Self {
            local_cluster_id,
            routes,
            timeout,
        })
    }

    /// Attaches the configured plugin to one local authoritative resolver.
    pub fn attach(
        &self,
        resolver: crate::AuthoritativeDnsResolver,
    ) -> crate::AuthoritativeDnsResolver {
        let plugin = TailscaleDnsResolverPlugin::with_client(
            resolver.zone_reader(),
            &self.local_cluster_id,
            self.routes.clone(),
            self.timeout,
            Arc::new(Socks5DnsForwardClient::default()),
        );
        resolver.with_plugin(Arc::new(plugin))
    }
}

impl TailscaleDnsResolverPlugin {
    pub(crate) fn with_client(
        zone: DnsZoneReader,
        local_cluster_id: &ClusterId,
        routes: Vec<TailscaleDnsRoute>,
        timeout: Duration,
        client: Arc<dyn DnsForwardClient>,
    ) -> Self {
        Self {
            zone,
            gateway_name: gateway_name(local_cluster_id),
            routes,
            timeout,
            client,
        }
    }

    async fn proxy_addresses(&self) -> Result<Vec<SocketAddr>, DnsResolverPluginError> {
        let lookup = self
            .zone
            .lookup(&self.gateway_name, DnsQueryType::A)
            .await?;
        let proxies = lookup
            .answers
            .into_iter()
            .filter_map(|answer| match answer.value {
                kernel_api::DnsRecordValue::A(address) => {
                    Some(SocketAddr::from((address, TAILSCALE_SOCKS_PORT)))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if proxies.is_empty() {
            Err(DnsResolverPluginError::new(
                "no ready managed Tailscale gateway has a DNS record",
            ))
        } else {
            Ok(proxies)
        }
    }

    fn route(&self, name: &str) -> Option<&TailscaleDnsRoute> {
        self.routes.iter().find(|route| {
            let domain = route.domain();
            name == domain || name.ends_with(&format!(".{domain}"))
        })
    }
}

fn gateway_name(local_cluster_id: &ClusterId) -> String {
    format!("{TAILSCALE_GATEWAY_SERVICE_ID}.{local_cluster_id}.{MAESTRO_DNS_ZONE}")
}

fn validate_routes(
    local_cluster_id: &ClusterId,
    routes: &[TailscaleDnsRoute],
) -> Result<(), DnsResolverPluginError> {
    let mut cluster_ids = BTreeSet::new();
    for route in routes {
        if &route.cluster_id == local_cluster_id {
            return Err(DnsResolverPluginError::new(format!(
                "cross-cluster DNS route `{}` targets the local cluster",
                route.cluster_id
            )));
        }
        if !cluster_ids.insert(route.cluster_id.clone()) {
            return Err(DnsResolverPluginError::new(format!(
                "cross-cluster DNS route `{}` is duplicated",
                route.cluster_id
            )));
        }
        if route.nameservers.is_empty() {
            return Err(DnsResolverPluginError::new(format!(
                "cross-cluster DNS route `{}` requires a nameserver",
                route.cluster_id
            )));
        }
        let mut nameservers = BTreeSet::new();
        for nameserver in &route.nameservers {
            if nameserver.is_unspecified()
                || nameserver.is_loopback()
                || nameserver.is_multicast()
                || nameserver == &Ipv4Addr::BROADCAST
            {
                return Err(DnsResolverPluginError::new(format!(
                    "cross-cluster DNS route `{}` has unsafe nameserver `{nameserver}`",
                    route.cluster_id
                )));
            }
            if !nameservers.insert(nameserver) {
                return Err(DnsResolverPluginError::new(format!(
                    "cross-cluster DNS route `{}` duplicates nameserver `{nameserver}`",
                    route.cluster_id
                )));
            }
        }
    }
    Ok(())
}

#[async_trait]
impl DnsResolverPlugin for TailscaleDnsResolverPlugin {
    async fn lookup(
        &self,
        name: &str,
        query_type: DnsQueryType,
    ) -> Result<Option<DnsLookup>, DnsResolverPluginError> {
        let Some(route) = self.route(name) else {
            return Ok(None);
        };
        let proxies = self.proxy_addresses().await?;
        let mut failures = Vec::new();
        for proxy in proxies {
            for nameserver in &route.nameservers {
                match self
                    .client
                    .lookup(
                        proxy,
                        SocketAddr::from((*nameserver, REMOTE_DNS_PORT)),
                        name,
                        query_type,
                        self.timeout,
                    )
                    .await
                {
                    Ok(lookup) => return Ok(Some(lookup)),
                    Err(error) => failures.push(error.to_string()),
                }
            }
        }
        Err(DnsResolverPluginError::new(format!(
            "every Tailscale DNS path failed: {}",
            failures.join("; ")
        )))
    }
}

/// A bounded failure from optional DNS routing or its Tailscale transport.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{detail}")]
pub struct DnsResolverPluginError {
    detail: String,
}

impl DnsResolverPluginError {
    /// Wraps one bounded plugin failure without exposing transport-specific errors.
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}
