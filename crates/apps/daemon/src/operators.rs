use std::sync::Arc;

use build::{BuildReconciler, BuildRevisionResolver, BuildSourceProvider, BuildWatchReconciler};
use deployment::DeploymentReconciler;
use dns::DnsReconciler;
use firewall::{FirewallBaselineReconciler, FirewallController, FirewallPolicyReconciler};
use ingress::{
    IngressBackend, IngressBlocklistReconciler, IngressReconciler, StoreTraefikProvider,
    TraefikBackend,
};
use kernel_api::ClusterId;
use kernel_controller::{ControllerError, ControllerRuntime, FencedStore, TimestampClock};
use kernel_store::Clock;
use preview::{PreviewReconciler, PreviewSourceReconciler, PullRequestApi};
use runtime::ArtifactStore;
use scheduler::SchedulerReconciler;
use tokio::sync::watch;
use upgrade::{
    NodeUpgradeBackend, StoreNodeUpgradeBackend, StoreNodeUpgradeBackendSettings, UpgradeReconciler,
};
use webhook::{WebhookDeliveryBackend, WebhookReconciler};

use crate::tailscale_reconciler::TailscaleResourceReconciler;
use crate::tailscale_resources::TailscaleSystemResources;
use crate::{LeaderWorkload, OperatorSettings, OperatorSuiteError, RoleError};

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
    /// Lists pull requests and upserts preview feedback when previews are configured.
    pub pull_requests: Option<Arc<dyn PullRequestApi>>,
    /// Applies idempotent rolling or all-node host upgrade batches.
    pub upgrades: Option<Arc<dyn NodeUpgradeBackend>>,
    /// Delivers signed transition payloads to configured endpoints.
    pub webhooks: Arc<dyn WebhookDeliveryBackend>,
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
    /// Fence-independent pull-request API retained across leadership terms.
    pub pull_requests: Option<Arc<dyn PullRequestApi>>,
    /// Fence-independent node-upgrade backend retained across leadership terms.
    pub upgrades: Option<Arc<dyn NodeUpgradeBackend>>,
    /// Production store-command policy rebuilt against each exact leadership fence.
    pub store_upgrades: Option<StoreNodeUpgradeBackendSettings>,
    /// Fence-independent outbound webhook transport.
    pub webhooks: Arc<dyn WebhookDeliveryBackend>,
}

/// Rebuilds and runs the complete operator suite for each leadership fence.
pub struct OperatorLeaderWorkload {
    cluster_id: ClusterId,
    monotonic_clock: Arc<dyn Clock>,
    timestamp_clock: Arc<dyn TimestampClock>,
    settings: OperatorSettings,
    builds: BuildOperatorBackends,
    tailscale: Option<TailscaleSystemResources>,
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
            tailscale: None,
        }
    }

    pub(crate) fn with_tailscale_resources(
        mut self,
        tailscale: Option<TailscaleSystemResources>,
    ) -> Self {
        self.tailscale = tailscale;
        self
    }
}

#[async_trait::async_trait]
impl LeaderWorkload for OperatorLeaderWorkload {
    async fn run(
        &self,
        store: Arc<FencedStore>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), RoleError> {
        TailscaleResourceReconciler::new(&self.cluster_id, self.tailscale.clone())
            .map_err(|error| {
                RoleError::new(format!(
                    "failed to construct Tailscale resource reconciler: {error}"
                ))
            })?
            .reconcile(store.as_ref(), self.timestamp_clock.now())
            .await
            .map_err(|error| {
                RoleError::new(format!("failed to reconcile Tailscale resources: {error}"))
            })?;
        let upgrades: Option<Arc<dyn NodeUpgradeBackend>> =
            match (&self.builds.upgrades, self.builds.store_upgrades) {
                (Some(_), Some(_)) => {
                    return Err(RoleError::new(
                        "fixed and store-backed node upgrade backends are both configured",
                    ));
                }
                (Some(backend), None) => Some(backend.clone()),
                (None, Some(settings)) => Some(Arc::new(StoreNodeUpgradeBackend::new(
                    &self.cluster_id,
                    store.clone(),
                    self.monotonic_clock.clone(),
                    settings,
                ))),
                (None, None) => None,
            };
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
                ingress: Arc::new(
                    TraefikBackend::new(self.cluster_id.clone(), provider)
                        .with_ingress_denied_backends(
                            self.settings.ingress_denied_backends.clone(),
                        ),
                ),
                build_source: self.builds.source.clone(),
                build_revisions: self.builds.revisions.clone(),
                artifacts: self.builds.artifacts.clone(),
                pull_requests: self.builds.pull_requests.clone(),
                upgrades,
                webhooks: self.builds.webhooks.clone(),
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
