use std::sync::Arc;

use deployment::{DeploymentReconciler, LifecycleSettings};
use dns::{DnsReconciler, DnsSettings};
use firewall::{
    FirewallBaselineReconciler, FirewallController, FirewallPolicyReconciler, FirewallSettings,
};
use ingress::{IngressBackend, IngressReconciler, IngressSettings};
use kernel_api::ClusterId;
use kernel_controller::{
    ControllerError, ControllerRuntime, FencedStore, RuntimeConfig, TimestampClock,
};
use kernel_store::Clock;
use scheduler::{SchedulerReconciler, SchedulerSettings};
use tokio::sync::watch;

use crate::{LeaderWorkload, RoleError};

/// Pure settings used to construct every M4 operator runtime.
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
}

/// Side-effect integrations shared by leader-owned operators.
#[derive(Clone)]
pub struct OperatorBackends {
    /// Publishes staged and active ingress configuration.
    pub ingress: Arc<dyn IngressBackend>,
}

/// Rebuilds and runs the complete operator suite for each leadership fence.
pub struct OperatorLeaderWorkload {
    cluster_id: ClusterId,
    monotonic_clock: Arc<dyn Clock>,
    timestamp_clock: Arc<dyn TimestampClock>,
    settings: OperatorSettings,
    backends: OperatorBackends,
}

impl OperatorLeaderWorkload {
    /// Captures fence-independent dependencies shared by successive election terms.
    pub fn new(
        cluster_id: ClusterId,
        monotonic_clock: Arc<dyn Clock>,
        timestamp_clock: Arc<dyn TimestampClock>,
        settings: OperatorSettings,
        backends: OperatorBackends,
    ) -> Self {
        Self {
            cluster_id,
            monotonic_clock,
            timestamp_clock,
            settings,
            backends,
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
        let suite = OperatorSuite::new(
            self.cluster_id.clone(),
            store,
            self.monotonic_clock.clone(),
            self.timestamp_clock.clone(),
            self.settings.clone(),
            self.backends.clone(),
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

/// All leader-owned M4 reconciliation loops sharing one fencing token.
pub struct OperatorSuite {
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
            scheduler: self.scheduler.reconcile_snapshot().await?,
            ingress: self.ingress.reconcile_snapshot().await?,
            dns: self.dns.reconcile_snapshot().await?,
            firewall_policies: self.firewall_policies.reconcile_snapshot().await?,
            firewall_baselines: self.firewall_baselines.reconcile_snapshot().await?,
        })
    }

    /// Runs every event-driven operator until shutdown or loss of its shared fence.
    pub async fn run(&self, shutdown: watch::Receiver<bool>) -> Result<(), ControllerError> {
        let deployment = self.deployment.run(shutdown.clone());
        let scheduler = self.scheduler.run(shutdown.clone());
        let ingress = self.ingress.run(shutdown.clone());
        let dns = self.dns.run(shutdown.clone());
        let policies = self.firewall_policies.run(shutdown.clone());
        let baselines = self.firewall_baselines.run(shutdown);
        tokio::try_join!(deployment, scheduler, ingress, dns, policies, baselines)?;
        Ok(())
    }
}

/// Construction failure from one concrete operator contract.
#[derive(Debug, thiserror::Error)]
pub enum OperatorSuiteError {
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
}
