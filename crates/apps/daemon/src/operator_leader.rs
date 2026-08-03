use std::sync::Arc;

use build::{BuildRevisionResolver, BuildSourceProvider, DepotBuildBackend};
use ingress::{IngressBackend, StoreTraefikProvider, TraefikBackend};
use kernel_api::ClusterId;
use kernel_controller::{FencedStore, TimestampClock};
use kernel_store::Clock;
use logs::LogStore;
use preview::PullRequestApi;
use runtime::{ArtifactStore, ValueSourceResolver};
use tokio::sync::watch;
use upgrade::{NodeUpgradeBackend, StoreNodeUpgradeBackend, StoreNodeUpgradeBackendSettings};
use webhook::WebhookDeliveryBackend;

use crate::cloudflare_resources::{
    CLOUDFLARE_MANAGED_OWNER, CLOUDFLARE_SERVICE_ID, CloudflareSystemResources,
};
use crate::dns_reconciler::DnsResolverResourceReconciler;
use crate::dns_resources::DnsResolverSystemResources;
use crate::operators::OperatorSuite;
use crate::system_service_reconciler::SystemServiceReconciler;
use crate::tailscale_reconciler::TailscaleResourceReconciler;
use crate::tailscale_resources::TailscaleSystemResources;
use crate::traefik_resources::{
    TRAEFIK_MANAGED_OWNER, TRAEFIK_SERVICE_ID, TraefikSystemResources, preserve_client_identity,
};
use crate::{LeaderWorkload, OperatorSettings, RoleError};

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
    /// Stores normalized build output for the build logs API.
    pub build_logs: Arc<dyn LogStore>,
    /// Runs service builds that select a Depot project.
    pub depot: Option<Arc<dyn DepotBuildBackend>>,
    /// Resolves external build environment and secret references at build execution time.
    pub value_sources: Option<Arc<dyn ValueSourceResolver>>,
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
    /// Stores normalized build output for the build logs API.
    pub logs: Arc<dyn LogStore>,
    /// Fence-independent Depot process adapter retained across leadership terms.
    pub depot: Option<Arc<dyn DepotBuildBackend>>,
    /// Resolves external build environment and secret references at build execution time.
    pub value_sources: Option<Arc<dyn ValueSourceResolver>>,
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
    cloudflare: Option<CloudflareSystemResources>,
    dns_resolver: Option<DnsResolverSystemResources>,
    tailscale: Option<TailscaleSystemResources>,
    traefik: Option<TraefikSystemResources>,
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
            cloudflare: None,
            dns_resolver: None,
            tailscale: None,
            traefik: None,
        }
    }

    pub(crate) fn with_dns_resolver_resources(
        mut self,
        dns_resolver: Option<DnsResolverSystemResources>,
    ) -> Self {
        self.dns_resolver = dns_resolver;
        self
    }

    pub(crate) fn with_cloudflare_resources(
        mut self,
        cloudflare: Option<CloudflareSystemResources>,
    ) -> Self {
        self.cloudflare = cloudflare;
        self
    }

    pub(crate) fn with_tailscale_resources(
        mut self,
        tailscale: Option<TailscaleSystemResources>,
    ) -> Self {
        self.tailscale = tailscale;
        self
    }

    pub(crate) fn with_traefik_resources(
        mut self,
        traefik: Option<TraefikSystemResources>,
    ) -> Self {
        self.traefik = traefik;
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
        DnsResolverResourceReconciler::new(&self.cluster_id, self.dns_resolver.clone())
            .map_err(|error| {
                RoleError::new(format!(
                    "failed to construct DNS resolver resource reconciler: {error}"
                ))
            })?
            .reconcile(store.as_ref(), self.timestamp_clock.now())
            .await
            .map_err(|error| {
                RoleError::new(format!(
                    "failed to reconcile DNS resolver resources: {error}"
                ))
            })?;
        let traefik_provider = Arc::new(StoreTraefikProvider::new(
            self.cluster_id.clone(),
            store.clone(),
        ));
        if self.traefik.is_some() {
            traefik_provider
                .ensure_watchable_root()
                .await
                .map_err(|error| {
                    RoleError::new(format!(
                        "failed to initialize Traefik provider root: {error}"
                    ))
                })?;
        }
        SystemServiceReconciler::new(
            &self.cluster_id,
            "Traefik",
            TRAEFIK_SERVICE_ID,
            TRAEFIK_MANAGED_OWNER,
            self.traefik
                .as_ref()
                .map(|resources| resources.service.clone()),
        )
        .map_err(|error| {
            RoleError::new(format!(
                "failed to construct Traefik resource reconciler: {error}"
            ))
        })?
        .with_desired_adapter(preserve_client_identity)
        .reconcile(store.as_ref(), self.timestamp_clock.now())
        .await
        .map_err(|error| {
            RoleError::new(format!("failed to reconcile Traefik resources: {error}"))
        })?;
        SystemServiceReconciler::new(
            &self.cluster_id,
            "Cloudflare Tunnel",
            CLOUDFLARE_SERVICE_ID,
            CLOUDFLARE_MANAGED_OWNER,
            self.cloudflare
                .as_ref()
                .map(|resources| resources.service.clone()),
        )
        .map_err(|error| {
            RoleError::new(format!(
                "failed to construct Cloudflare Tunnel resource reconciler: {error}"
            ))
        })?
        .reconcile(store.as_ref(), self.timestamp_clock.now())
        .await
        .map_err(|error| {
            RoleError::new(format!(
                "failed to reconcile Cloudflare Tunnel resources: {error}"
            ))
        })?;
        let tailscale = TailscaleResourceReconciler::new(&self.cluster_id, self.tailscale.clone())
            .map_err(|error| {
                RoleError::new(format!(
                    "failed to construct Tailscale resource reconciler: {error}"
                ))
            })?;
        tailscale
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
        let suite = OperatorSuite::new(
            self.cluster_id.clone(),
            store.clone(),
            self.monotonic_clock.clone(),
            self.timestamp_clock.clone(),
            self.settings.clone(),
            OperatorBackends {
                ingress: Arc::new(
                    TraefikBackend::new(self.cluster_id.clone(), traefik_provider)
                        .with_ingress_denied_backends(
                            self.settings.ingress_denied_backends.clone(),
                        ),
                ),
                build_source: self.builds.source.clone(),
                build_revisions: self.builds.revisions.clone(),
                artifacts: self.builds.artifacts.clone(),
                build_logs: self.builds.logs.clone(),
                depot: self.builds.depot.clone(),
                value_sources: self.builds.value_sources.clone(),
                pull_requests: self.builds.pull_requests.clone(),
                upgrades,
                webhooks: self.builds.webhooks.clone(),
            },
        )
        .map_err(|error| RoleError::new(format!("failed to construct operator suite: {error}")))?;
        let tailscale_shutdown = shutdown.clone();
        let timestamp_clock = self.timestamp_clock.clone();
        let tailscale_run = async move {
            tailscale
                .run(store, timestamp_clock, tailscale_shutdown)
                .await
                .map_err(|error| {
                    RoleError::new(format!("Tailscale resource reconciler failed: {error}"))
                })
        };
        let operator_run = async move {
            suite
                .run(shutdown)
                .await
                .map_err(|error| RoleError::new(format!("operator suite failed: {error}")))
        };
        tokio::try_join!(tailscale_run, operator_run)?;
        Ok(())
    }
}
