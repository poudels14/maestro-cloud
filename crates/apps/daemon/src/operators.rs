use std::sync::Arc;

use build::{BuildReconciler, BuildWatchReconciler};
use deployment::DeploymentReconciler;
use dns::DnsReconciler;
use firewall::{FirewallBaselineReconciler, FirewallController, FirewallPolicyReconciler};
use ingress::{IngressBlocklistReconciler, IngressReconciler};
use kernel_api::ClusterId;
use kernel_controller::{ControllerError, ControllerRuntime, FencedStore, TimestampClock};
use kernel_store::Clock;
use preview::{PreviewReconciler, PreviewSourceReconciler};
use scheduler::SchedulerReconciler;
use tokio::sync::watch;
use upgrade::UpgradeReconciler;
use webhook::WebhookReconciler;

use crate::operator_leader::OperatorBackends;
use crate::{OperatorSettings, OperatorSuiteError};

/// Invocation counts from one deterministic bounded suite pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OperatorInvocationReport {
    /// Build resources passed to artifact reconciliation.
    pub builds: usize,
    /// Services passed to Git revision polling.
    pub build_watch: usize,
    /// Nodes considered for cluster-wide pull-request discovery.
    pub preview_sources: usize,
    /// Preview resources passed to derived-resource reconciliation.
    pub previews: usize,
    /// UpgradeRun resources passed to coordinated node maintenance.
    pub upgrades: usize,
    /// Service resources passed to deployment reconciliation.
    pub deployment: usize,
    /// Service resources passed to scheduling reconciliation.
    pub scheduler: usize,
    /// Service resources passed to ingress reconciliation.
    pub ingress: usize,
    /// Singleton blocklist resources passed to ingress reconciliation.
    pub ingress_blocklists: usize,
    /// Service resources passed to DNS reconciliation.
    pub dns: usize,
    /// FirewallPolicy resources passed to policy reconciliation.
    pub firewall_policies: usize,
    /// NodeNetwork resources passed to baseline firewall reconciliation.
    pub firewall_baselines: usize,
    /// Webhook resources passed to transition delivery.
    pub webhooks: usize,
}

/// All leader-owned reconciliation loops sharing one fencing token.
pub struct OperatorSuite {
    builds: ControllerRuntime<BuildReconciler>,
    build_watch: ControllerRuntime<BuildWatchReconciler>,
    preview_sources: Option<ControllerRuntime<PreviewSourceReconciler>>,
    previews: Option<ControllerRuntime<PreviewReconciler>>,
    upgrades: Option<ControllerRuntime<UpgradeReconciler>>,
    deployment: ControllerRuntime<DeploymentReconciler>,
    scheduler: ControllerRuntime<SchedulerReconciler>,
    ingress: ControllerRuntime<IngressReconciler>,
    ingress_blocklists: ControllerRuntime<IngressBlocklistReconciler>,
    dns: ControllerRuntime<DnsReconciler>,
    firewall_policies: ControllerRuntime<FirewallPolicyReconciler>,
    firewall_baselines: ControllerRuntime<FirewallBaselineReconciler>,
    webhooks: ControllerRuntime<WebhookReconciler>,
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
        let (preview_sources, previews) =
            match (settings.preview.as_ref(), backends.pull_requests.as_ref()) {
                (Some(preview), Some(pull_requests)) => (
                    Some(
                        Arc::new(PreviewSourceReconciler::new(
                            cluster_id.clone(),
                            pull_requests.clone(),
                            preview.source,
                            timestamp_clock.clone(),
                            monotonic_clock.clone(),
                        )?)
                        .runtime(store.clone(), settings.runtime.clone()),
                    ),
                    Some(
                        Arc::new(PreviewReconciler::new(
                            cluster_id.clone(),
                            timestamp_clock.clone(),
                            monotonic_clock.clone(),
                            preview.derivation.clone(),
                        )?)
                        .runtime(store.clone(), settings.runtime.clone()),
                    ),
                ),
                (None, None) => (None, None),
                (Some(_), None) => return Err(OperatorSuiteError::PreviewBackendMissing),
                (None, Some(_)) => return Err(OperatorSuiteError::PreviewBackendUnexpected),
            };
        let upgrades = match (settings.upgrade, backends.upgrades.as_ref()) {
            (Some(upgrade_settings), Some(backend)) => Some(
                Arc::new(UpgradeReconciler::new(
                    cluster_id.clone(),
                    monotonic_clock.clone(),
                    timestamp_clock.clone(),
                    upgrade_settings,
                    backend.clone(),
                )?)
                .runtime(store.clone(), settings.runtime.clone()),
            ),
            (None, None) => None,
            (Some(_), None) => return Err(OperatorSuiteError::UpgradeBackendMissing),
            (None, Some(_)) => return Err(OperatorSuiteError::UpgradeBackendUnexpected),
        };
        let webhooks = Arc::new(WebhookReconciler::new(
            cluster_id.clone(),
            backends.webhooks,
            timestamp_clock.clone(),
            settings.webhook,
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
        let ingress_blocklists = Arc::new(IngressBlocklistReconciler::new(
            cluster_id.clone(),
            settings.ingress,
            backends.ingress.clone(),
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
            preview_sources,
            previews,
            upgrades,
            deployment,
            scheduler,
            ingress,
            ingress_blocklists,
            dns,
            firewall_policies,
            firewall_baselines,
            webhooks,
        })
    }

    /// Runs one bounded pass in dependency order for startup and deterministic tests.
    pub async fn reconcile_snapshot(&self) -> Result<OperatorInvocationReport, ControllerError> {
        Ok(OperatorInvocationReport {
            upgrades: match &self.upgrades {
                Some(runtime) => runtime.reconcile_snapshot().await?,
                None => 0,
            },
            deployment: self.deployment.reconcile_snapshot().await?,
            builds: self.builds.reconcile_snapshot().await?,
            scheduler: self.scheduler.reconcile_snapshot().await?,
            ingress: self.ingress.reconcile_snapshot().await?,
            ingress_blocklists: self.ingress_blocklists.reconcile_snapshot().await?,
            dns: self.dns.reconcile_snapshot().await?,
            firewall_policies: self.firewall_policies.reconcile_snapshot().await?,
            firewall_baselines: self.firewall_baselines.reconcile_snapshot().await?,
            webhooks: self.webhooks.reconcile_snapshot().await?,
            build_watch: self.build_watch.reconcile_snapshot().await?,
            preview_sources: match &self.preview_sources {
                Some(runtime) => runtime.reconcile_snapshot().await?,
                None => 0,
            },
            previews: match &self.previews {
                Some(runtime) => runtime.reconcile_snapshot().await?,
                None => 0,
            },
        })
    }

    /// Runs every event-driven operator until shutdown or loss of its shared fence.
    pub async fn run(&self, shutdown: watch::Receiver<bool>) -> Result<(), ControllerError> {
        let deployment = self.deployment.run(shutdown.clone());
        let builds = self.builds.run(shutdown.clone());
        let build_watch = self.build_watch.run(shutdown.clone());
        let preview_sources = run_optional(self.preview_sources.as_ref(), shutdown.clone());
        let previews = run_optional(self.previews.as_ref(), shutdown.clone());
        let upgrades = run_optional(self.upgrades.as_ref(), shutdown.clone());
        let scheduler = self.scheduler.run(shutdown.clone());
        let ingress = self.ingress.run(shutdown.clone());
        let ingress_blocklists = self.ingress_blocklists.run(shutdown.clone());
        let dns = self.dns.run(shutdown.clone());
        let policies = self.firewall_policies.run(shutdown.clone());
        let webhooks = self.webhooks.run(shutdown.clone());
        let baselines = self.firewall_baselines.run(shutdown);
        tokio::try_join!(
            deployment,
            builds,
            build_watch,
            preview_sources,
            previews,
            upgrades,
            scheduler,
            ingress,
            ingress_blocklists,
            dns,
            policies,
            baselines,
            webhooks
        )?;
        Ok(())
    }
}

async fn run_optional<Reconciler>(
    runtime: Option<&ControllerRuntime<Reconciler>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ControllerError>
where
    Reconciler: kernel_controller::Reconciler + 'static,
    Reconciler::Id: serde::de::DeserializeOwned + serde::Serialize + std::fmt::Display,
    Reconciler::Spec: serde::de::DeserializeOwned + serde::Serialize,
    Reconciler::Status: serde::de::DeserializeOwned + serde::Serialize,
{
    match runtime {
        Some(runtime) => runtime.run(shutdown).await,
        None => {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
            Ok(())
        }
    }
}
