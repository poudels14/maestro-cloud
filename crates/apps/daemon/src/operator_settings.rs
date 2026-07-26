use std::collections::BTreeSet;
use std::time::Duration;

use build::BuildWatchSettings;
use cluster::ClusterConfig;
use deployment::LifecycleSettings;
use dns::DnsSettings;
use firewall::FirewallSettings;
use ingress::IngressSettings;
use kernel_controller::{Backoff, RuntimeConfig};
use node_agent::{AUTHORITATIVE_DNS_PORT, WORKLOAD_BRIDGE_NAME};
use preview::{PreviewSettings, PreviewSourceSettings};
use scheduler::SchedulerSettings;
use upgrade::UpgradeSettings;
use webhook::WebhookSettings;

use crate::OperatorSuiteError;

/// Pure settings used to construct every leader-owned operator runtime.
#[derive(Debug, Clone)]
pub struct OperatorSettings {
    /// Shared watch resync and retry policy.
    pub runtime: RuntimeConfig,
    /// Placement replacement and drain grace periods.
    pub scheduler: SchedulerSettings,
    /// Deployment lifecycle drain grace period.
    pub deployment: LifecycleSettings,
    /// Retired ingress generation grace period.
    pub ingress: IngressSettings,
    /// HTTPS node API endpoints serving the public ingress-denied response.
    pub ingress_denied_backends: Vec<std::net::SocketAddr>,
    /// Authoritative service-record TTL.
    pub dns: DnsSettings,
    /// Static host, DNS, and egress firewall settings.
    pub firewall: FirewallSettings,
    /// Git revision polling cadence for watched build-backed services.
    pub build_watch: BuildWatchSettings,
    /// Pull-request preview discovery and derivation, when configured.
    pub preview: Option<PreviewOperatorSettings>,
    /// Coordinated node upgrades, when a host-maintenance backend is configured.
    pub upgrade: Option<UpgradeSettings>,
    /// Outbound transition delivery retry policy.
    pub webhook: WebhookSettings,
}

/// Leader-owned settings for both halves of pull-request preview reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewOperatorSettings {
    /// GitHub polling, retry, and global quota policy.
    pub source: PreviewSourceSettings,
    /// Stable preview hostname derivation policy.
    pub derivation: PreviewSettings,
}

impl OperatorSettings {
    /// Derives bounded operator views from the validated cluster configuration.
    pub fn production(cluster: &ClusterConfig) -> Result<Self, OperatorSuiteError> {
        let mut protected_host_ports = vec![
            cluster.ports.gateway,
            cluster.ports.store_client,
            cluster.ports.store_peer,
        ];
        protected_host_ports.extend(cluster.nodes.values().map(|node| node.endpoint.api_port));
        protected_host_ports.sort_unstable();
        protected_host_ports.dedup();
        let mut control_allow_cidrs = cluster
            .control_allow_cidrs
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        control_allow_cidrs.extend(
            cluster
                .nodes
                .values()
                .filter(|node| {
                    cluster
                        .control_allow_cidrs
                        .iter()
                        .all(|network| !network.contains(node.endpoint.host_address))
                })
                .map(|node| format!("{}/32", node.endpoint.host_address)),
        );
        Ok(Self {
            runtime: RuntimeConfig::new(
                Duration::from_secs(30),
                Backoff::new(Duration::from_millis(100), Duration::from_secs(5))?,
            )?,
            scheduler: SchedulerSettings {
                replacement_grace: Duration::from_secs(30),
                deployment_drain_grace: Duration::from_secs(30),
            },
            deployment: LifecycleSettings {
                drain_grace: Duration::from_secs(30),
            },
            ingress: IngressSettings {
                retirement_grace: Duration::from_secs(30),
            },
            ingress_denied_backends: cluster
                .nodes
                .values()
                .map(|node| {
                    std::net::SocketAddr::new(
                        std::net::IpAddr::V4(node.endpoint.host_address),
                        node.endpoint.api_port,
                    )
                })
                .collect(),
            dns: DnsSettings { ttl_secs: 5 },
            firewall: FirewallSettings {
                table_name: "maestro_firewall".to_string(),
                workload_interface: WORKLOAD_BRIDGE_NAME.to_string(),
                dns_port: AUTHORITATIVE_DNS_PORT,
                protected_host_ports,
                control_allow_cidrs: control_allow_cidrs.into_iter().collect(),
                system_services: BTreeSet::new(),
                system_host_access: Vec::new(),
                host_port_routes: Vec::new(),
            },
            build_watch: BuildWatchSettings {
                poll_interval: Duration::from_secs(60),
            },
            preview: None,
            upgrade: None,
            webhook: WebhookSettings {
                retry_delay: Duration::from_secs(30),
            },
        })
    }
}
