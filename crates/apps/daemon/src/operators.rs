use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use build::{
    BuildReconciler, BuildRevisionResolver, BuildSourceProvider, BuildWatchReconciler,
    BuildWatchSettings,
};
use cluster::ClusterConfig;
use deployment::{DeploymentReconciler, LifecycleSettings};
use dns::{DnsReconciler, DnsSettings};
use firewall::{
    FirewallBaselineReconciler, FirewallController, FirewallPolicyReconciler, FirewallSettings,
};
use ingress::{
    IngressBackend, IngressReconciler, IngressSettings, StoreTraefikProvider, TraefikBackend,
};
use kernel_api::ClusterId;
use kernel_controller::{
    Backoff, ControllerError, ControllerRuntime, FencedStore, RuntimeConfig, TimestampClock,
};
use kernel_store::Clock;
use node_agent::{AUTHORITATIVE_DNS_PORT, WORKLOAD_BRIDGE_NAME};
use runtime::ArtifactStore;
use scheduler::{SchedulerReconciler, SchedulerSettings};
use tokio::sync::watch;

use crate::{LeaderWorkload, RoleError};

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
    /// Authoritative service-record TTL.
    pub dns: DnsSettings,
    /// Static host, DNS, and egress firewall settings.
    pub firewall: FirewallSettings,
    /// Git revision polling cadence for watched build-backed services.
    pub build_watch: BuildWatchSettings,
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
            dns: DnsSettings { ttl_secs: 5 },
            firewall: FirewallSettings {
                table_name: "maestro_firewall".to_string(),
                workload_interface: WORKLOAD_BRIDGE_NAME.to_string(),
                dns_port: AUTHORITATIVE_DNS_PORT,
                protected_host_ports,
                control_allow_cidrs: control_allow_cidrs.into_iter().collect(),
                system_services: BTreeSet::new(),
            },
            build_watch: BuildWatchSettings {
                poll_interval: Duration::from_secs(60),
            },
        })
    }
}

/// Side-effect integrations shared by leader-owned operators.
#[derive(Clone)]
pub struct OperatorBackends {
    /// Publishes staged and active ingress configuration.
    pub ingress: Arc<dyn IngressBackend>,
    /// Materializes Git and uploaded-archive build sources.
    pub build_source: Arc<dyn BuildSourceProvider>,
    /// Resolves immutable revisions for watched Git sources.
    pub build_revisions: Arc<dyn BuildRevisionResolver>,
    /// Builds and stores immutable runtime artifacts.
    pub artifacts: Arc<dyn ArtifactStore>,
}

/// Fence-independent build integrations retained across leadership terms.
#[derive(Clone)]
pub struct BuildOperatorBackends {
    /// Materializes Git and uploaded-archive build sources.
    pub source: Arc<dyn BuildSourceProvider>,
    /// Resolves immutable revisions for watched Git sources.
    pub revisions: Arc<dyn BuildRevisionResolver>,
    /// Builds and stores immutable runtime artifacts.
    pub artifacts: Arc<dyn ArtifactStore>,
}

/// Rebuilds and runs the complete operator suite for each leadership fence.
pub struct OperatorLeaderWorkload {
    cluster_id: ClusterId,
    monotonic_clock: Arc<dyn Clock>,
    timestamp_clock: Arc<dyn TimestampClock>,
    settings: OperatorSettings,
    builds: BuildOperatorBackends,
}

impl OperatorLeaderWorkload {
    /// Captures fence-independent dependencies shared by successive election terms.
    pub fn new(
        cluster_id: ClusterId,
        monotonic_clock: Arc<dyn Clock>,
        timestamp_clock: Arc<dyn TimestampClock>,
        settings: OperatorSettings,
        builds: BuildOperatorBackends,
    ) -> Self {
        Self {
            cluster_id,
            monotonic_clock,
            timestamp_clock,
            settings,
            builds,
        }
    }
}

#[async_trait::async_trait]
impl LeaderWorkload for OperatorLeaderWorkload {
    async fn run(
        &self,
        store: Arc<FencedStore>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), RoleError> {
        let provider = Arc::new(StoreTraefikProvider::new(
            self.cluster_id.clone(),
            store.clone(),
        ));
        let suite = OperatorSuite::new(
            self.cluster_id.clone(),
            store,
            self.monotonic_clock.clone(),
            self.timestamp_clock.clone(),
            self.settings.clone(),
            OperatorBackends {
                ingress: Arc::new(TraefikBackend::new(self.cluster_id.clone(), provider)),
                build_source: self.builds.source.clone(),
                build_revisions: self.builds.revisions.clone(),
                artifacts: self.builds.artifacts.clone(),
            },
        )
        .map_err(|error| RoleError::new(format!("failed to construct operator suite: {error}")))?;
        suite
            .run(shutdown)
            .await
            .map_err(|error| RoleError::new(format!("operator suite failed: {error}")))
    }
}

/// Invocation counts from one deterministic bounded suite pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OperatorInvocationReport {
    /// Build resources passed to artifact reconciliation.
    pub builds: usize,
    /// Services passed to Git revision polling.
    pub build_watch: usize,
    /// Service resources passed to deployment reconciliation.
    pub deployment: usize,
    /// Service resources passed to scheduling reconciliation.
    pub scheduler: usize,
    /// Service resources passed to ingress reconciliation.
    pub ingress: usize,
    /// Service resources passed to DNS reconciliation.
    pub dns: usize,
    /// FirewallPolicy resources passed to policy reconciliation.
    pub firewall_policies: usize,
    /// NodeNetwork resources passed to baseline firewall reconciliation.
    pub firewall_baselines: usize,
}

/// All leader-owned reconciliation loops sharing one fencing token.
pub struct OperatorSuite {
    builds: ControllerRuntime<BuildReconciler>,
    build_watch: ControllerRuntime<BuildWatchReconciler>,
    deployment: ControllerRuntime<DeploymentReconciler>,
    scheduler: ControllerRuntime<SchedulerReconciler>,
    ingress: ControllerRuntime<IngressReconciler>,
    dns: ControllerRuntime<DnsReconciler>,
    firewall_policies: ControllerRuntime<FirewallPolicyReconciler>,
    firewall_baselines: ControllerRuntime<FirewallBaselineReconciler>,
}

impl OperatorSuite {
    /// Constructs every operator against the same store fence and injected seams.
    pub fn new(
        cluster_id: ClusterId,
        store: Arc<FencedStore>,
        monotonic_clock: Arc<dyn Clock>,
        timestamp_clock: Arc<dyn TimestampClock>,
        settings: OperatorSettings,
        backends: OperatorBackends,
    ) -> Result<Self, OperatorSuiteError> {
        let builds = Arc::new(BuildReconciler::new(
            cluster_id.clone(),
            backends.build_source,
            backends.artifacts,
            timestamp_clock.clone(),
        )?)
        .runtime(
            store.clone(),
            monotonic_clock.clone(),
            settings.runtime.clone(),
        );
        let build_watch = Arc::new(BuildWatchReconciler::new(
            cluster_id.clone(),
            backends.build_revisions,
            settings.build_watch,
        )?)
        .runtime(
            store.clone(),
            monotonic_clock.clone(),
            settings.runtime.clone(),
        );
        let deployment = Arc::new(DeploymentReconciler::new(
            cluster_id.clone(),
            settings.deployment,
            timestamp_clock.clone(),
        )?)
        .runtime(
            store.clone(),
            monotonic_clock.clone(),
            settings.runtime.clone(),
        );
        let scheduler = Arc::new(SchedulerReconciler::new(
            cluster_id.clone(),
            settings.scheduler,
            timestamp_clock.clone(),
        )?)
        .runtime(
            store.clone(),
            monotonic_clock.clone(),
            settings.runtime.clone(),
        );
        let ingress = Arc::new(IngressReconciler::new(
            cluster_id.clone(),
            settings.ingress,
            backends.ingress,
            timestamp_clock,
        )?)
        .runtime(
            store.clone(),
            monotonic_clock.clone(),
            settings.runtime.clone(),
        );
        let dns = Arc::new(DnsReconciler::new(cluster_id.clone(), settings.dns)?).runtime(
            store.clone(),
            monotonic_clock.clone(),
            settings.runtime.clone(),
        );
        let firewall = Arc::new(FirewallController::new(cluster_id, settings.firewall));
        let firewall_policies = Arc::new(FirewallPolicyReconciler::new(firewall.clone())?).runtime(
            store.clone(),
            monotonic_clock.clone(),
            settings.runtime.clone(),
        );
        let firewall_baselines = Arc::new(FirewallBaselineReconciler::new(firewall)?).runtime(
            store,
            monotonic_clock,
            settings.runtime,
        );
        Ok(Self {
            builds,
            build_watch,
            deployment,
            scheduler,
            ingress,
            dns,
            firewall_policies,
            firewall_baselines,
        })
    }

    /// Runs one bounded pass in dependency order for startup and deterministic tests.
    pub async fn reconcile_snapshot(&self) -> Result<OperatorInvocationReport, ControllerError> {
        Ok(OperatorInvocationReport {
            deployment: self.deployment.reconcile_snapshot().await?,
            builds: self.builds.reconcile_snapshot().await?,
            scheduler: self.scheduler.reconcile_snapshot().await?,
            ingress: self.ingress.reconcile_snapshot().await?,
            dns: self.dns.reconcile_snapshot().await?,
            firewall_policies: self.firewall_policies.reconcile_snapshot().await?,
            firewall_baselines: self.firewall_baselines.reconcile_snapshot().await?,
            build_watch: self.build_watch.reconcile_snapshot().await?,
        })
    }

    /// Runs every event-driven operator until shutdown or loss of its shared fence.
    pub async fn run(&self, shutdown: watch::Receiver<bool>) -> Result<(), ControllerError> {
        let deployment = self.deployment.run(shutdown.clone());
        let builds = self.builds.run(shutdown.clone());
        let build_watch = self.build_watch.run(shutdown.clone());
        let scheduler = self.scheduler.run(shutdown.clone());
        let ingress = self.ingress.run(shutdown.clone());
        let dns = self.dns.run(shutdown.clone());
        let policies = self.firewall_policies.run(shutdown.clone());
        let baselines = self.firewall_baselines.run(shutdown);
        tokio::try_join!(
            deployment,
            builds,
            build_watch,
            scheduler,
            ingress,
            dns,
            policies,
            baselines
        )?;
        Ok(())
    }
}

/// Construction failure from one concrete operator contract.
#[derive(Debug, thiserror::Error)]
pub enum OperatorSuiteError {
    /// The shared retry policy was invalid.
    #[error(transparent)]
    Backoff(#[from] kernel_controller::BackoffError),
    /// The shared watch resync policy was invalid.
    #[error(transparent)]
    RuntimeConfig(#[from] kernel_controller::RuntimeConfigError),
    /// Deployment lifecycle settings or identifiers were invalid.
    #[error(transparent)]
    Deployment(#[from] deployment::DeploymentError),
    /// Scheduler settings or identifiers were invalid.
    #[error(transparent)]
    Scheduler(#[from] scheduler::SchedulerError),
    /// Ingress settings or identifiers were invalid.
    #[error(transparent)]
    Ingress(#[from] ingress::IngressError),
    /// DNS settings or identifiers were invalid.
    #[error(transparent)]
    Dns(#[from] dns::DnsError),
    /// Firewall settings or identifiers were invalid.
    #[error(transparent)]
    Firewall(#[from] firewall::FirewallError),
    /// Build resource settings or identifiers were invalid.
    #[error(transparent)]
    Build(#[from] kernel_api::InvalidIdentifier),
    /// Git build-watch settings or identifiers were invalid.
    #[error(transparent)]
    BuildWatch(#[from] build::BuildWatchError),
}
