use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(test)]
use std::collections::BTreeSet;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, TrafficGenerationId, TrafficRoute, TrafficTarget};
use sha2::{Digest, Sha256};

use crate::{
    BackendChange, IngressBackend, IngressBackendError, IngressBlocklistChange, PublishedTraffic,
};

const LABEL_DOMAIN: &[u8] = b"maestro-traefik-label-v1\0";
const AFFINITY_DOMAIN: &[u8] = b"maestro-node-affinity-v1\0";

/// Stable namespace reserved for routers that reject blocklisted client addresses.
pub const TRAEFIK_BLOCKED_ROUTER_PREFIX: &str = "maestro.internal-blocked-";
const TRAEFIK_BLOCKED_SERVICE_LABEL: &str = "maestro.internal-blocked";
const TRAEFIK_BLOCKED_SERVICE_PREFIX: &str = "http/services/maestro.internal-blocked";
const TRAEFIK_BLOCKED_MIDDLEWARE_PREFIX: &str = "http/middlewares/maestro.internal-blocked";
const TRAEFIK_BLOCKED_TRANSPORT_PREFIX: &str = "http/serversTransports/maestro.internal-blocked";
const TRAEFIK_BLOCKED_ROUTER_KEY_PREFIX: &str = "http/routers/maestro.internal-blocked-";
const BLOCKLIST_ADDRESSES_PER_ROUTER: usize = 256;

/// Returns the stable access-log router prefix owned by one service.
pub fn traefik_service_router_prefix(service_id: &kernel_api::ServiceId) -> String {
    router_label_prefix(&service_label(service_id))
}

/// Generation-specific entries written before any stable router references them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraefikStage {
    /// Immutable generation being staged.
    pub generation_id: TrafficGenerationId,
    /// Dynamic-provider key/value entries below the provider root.
    pub entries: BTreeMap<String, String>,
}

/// Atomic stable-router replacement and obsolete-generation cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraefikCutover {
    /// Prefix containing every stable router owned by this Service.
    pub router_prefix: String,
    /// Complete desired router entries under `router_prefix`.
    pub routers: BTreeMap<String, String>,
    /// Generation-specific service prefixes safe to delete in the same cutover.
    pub remove_prefixes: Vec<String>,
}

/// Complete desired blocklist entries and the reserved prefixes they replace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraefikBlocklistConfig {
    /// Dynamic-provider entries for the desired address set.
    pub entries: BTreeMap<String, String>,
    /// Reserved prefixes atomically replaced by `entries`.
    pub owned_prefixes: Vec<String>,
}

/// Thin dynamic-provider persistence boundary used by the Traefik backend.
#[async_trait]
pub trait TraefikProvider: Send + Sync {
    /// Idempotently writes every generation-specific entry.
    async fn stage(&self, stage: &TraefikStage) -> Result<(), IngressBackendError>;

    /// Atomically replaces stable routers and removes explicitly retired prefixes.
    async fn cutover(&self, cutover: &TraefikCutover) -> Result<(), IngressBackendError>;

    /// Atomically replaces every reserved blocklist router, service, and middleware entry.
    async fn replace_blocklist(
        &self,
        config: &TraefikBlocklistConfig,
    ) -> Result<(), IngressBackendError>;
}

/// Ingress backend which renders and publishes Traefik dynamic configuration.
pub struct TraefikBackend {
    cluster_id: ClusterId,
    provider: Arc<dyn TraefikProvider>,
    ingress_denied_backends: Vec<std::net::SocketAddr>,
}

impl TraefikBackend {
    /// Binds a cluster-scoped renderer to a dynamic-provider persistence client.
    pub fn new(cluster_id: ClusterId, provider: Arc<dyn TraefikProvider>) -> Self {
        Self {
            cluster_id,
            provider,
            ingress_denied_backends: Vec::new(),
        }
    }

    /// Configures host API endpoints serving the ingress-denied response.
    pub fn with_ingress_denied_backends(mut self, mut backends: Vec<std::net::SocketAddr>) -> Self {
        backends.sort();
        backends.dedup();
        self.ingress_denied_backends = backends;
        self
    }
}

#[async_trait]
impl IngressBackend for TraefikBackend {
    async fn apply(&self, change: &BackendChange) -> Result<(), IngressBackendError> {
        let rendered = change
            .active
            .as_ref()
            .map(|active| render_active(&self.cluster_id, active));
        if let Some(rendered) = rendered.as_ref() {
            self.provider.stage(&rendered.stage).await?;
        }
        let service_label = service_label(&change.service_id);
        let remove_prefixes = change
            .remove
            .iter()
            .filter(|generation_id| {
                change
                    .active
                    .as_ref()
                    .is_none_or(|active| &active.generation_id != *generation_id)
            })
            .map(|generation_id| {
                format!(
                    "http/services/{}",
                    generation_prefix(&service_label, generation_id)
                )
            })
            .collect();
        self.provider
            .cutover(&TraefikCutover {
                router_prefix: format!("http/routers/{}", router_label_prefix(&service_label)),
                routers: rendered
                    .map(|rendered| rendered.routers)
                    .unwrap_or_default(),
                remove_prefixes,
            })
            .await
    }

    async fn apply_blocklist(
        &self,
        change: &IngressBlocklistChange,
    ) -> Result<(), IngressBackendError> {
        if !change.addresses.is_empty() && self.ingress_denied_backends.is_empty() {
            return Err(IngressBackendError::new(
                "ingress blocklist has no ingress-denied API backends",
            ));
        }
        self.provider
            .replace_blocklist(&render_blocklist(change, &self.ingress_denied_backends))
            .await
    }
}

fn render_blocklist(
    change: &IngressBlocklistChange,
    backends: &[std::net::SocketAddr],
) -> TraefikBlocklistConfig {
    let owned_prefixes = vec![
        TRAEFIK_BLOCKED_ROUTER_KEY_PREFIX.to_owned(),
        TRAEFIK_BLOCKED_SERVICE_PREFIX.to_owned(),
        TRAEFIK_BLOCKED_MIDDLEWARE_PREFIX.to_owned(),
        TRAEFIK_BLOCKED_TRANSPORT_PREFIX.to_owned(),
    ];
    if change.addresses.is_empty() {
        return TraefikBlocklistConfig {
            entries: BTreeMap::new(),
            owned_prefixes,
        };
    }
    let mut entries = BTreeMap::new();
    for (index, backend) in backends.iter().enumerate() {
        entries.insert(
            format!("{TRAEFIK_BLOCKED_SERVICE_PREFIX}/loadBalancer/servers/{index}/url"),
            format!("https://{backend}"),
        );
    }
    entries.insert(
        format!("{TRAEFIK_BLOCKED_SERVICE_PREFIX}/loadBalancer/passHostHeader"),
        "true".to_owned(),
    );
    entries.insert(
        format!("{TRAEFIK_BLOCKED_SERVICE_PREFIX}/loadBalancer/serversTransport"),
        TRAEFIK_BLOCKED_SERVICE_LABEL.to_owned(),
    );
    entries.insert(
        format!("{TRAEFIK_BLOCKED_TRANSPORT_PREFIX}/insecureSkipVerify"),
        "true".to_owned(),
    );
    entries.insert(
        format!("{TRAEFIK_BLOCKED_MIDDLEWARE_PREFIX}/replacePath/path"),
        "/_maestro/ingress-denied".to_owned(),
    );
    let fingerprint = change
        .configuration_digest
        .chars()
        .take(16)
        .collect::<String>();
    for (index, addresses) in change
        .addresses
        .chunks(BLOCKLIST_ADDRESSES_PER_ROUTER)
        .enumerate()
    {
        let label = format!("{TRAEFIK_BLOCKED_ROUTER_PREFIX}{fingerprint}-{index}");
        let prefix = format!("http/routers/{label}");
        entries.extend([
            (format!("{prefix}/rule"), blocked_ip_matcher(addresses)),
            (
                format!("{prefix}/service"),
                TRAEFIK_BLOCKED_SERVICE_LABEL.to_owned(),
            ),
            (format!("{prefix}/entryPoints/0"), "web".to_owned()),
            (format!("{prefix}/priority"), "10000".to_owned()),
            (
                format!("{prefix}/middlewares/0"),
                TRAEFIK_BLOCKED_SERVICE_LABEL.to_owned(),
            ),
        ]);
    }
    TraefikBlocklistConfig {
        entries,
        owned_prefixes,
    }
}

fn blocked_ip_matcher(addresses: &[std::net::IpAddr]) -> String {
    let addresses = addresses
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let direct = addresses
        .iter()
        .map(|address| format!("ClientIP(`{address}`)"))
        .collect::<Vec<_>>()
        .join(" || ");
    let alternatives = addresses
        .iter()
        .map(|address| address.replace('.', r"\."))
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "{direct} || HeaderRegexp(`CF-Connecting-IP`, `^({alternatives})$`) || \
         HeaderRegexp(`X-Real-IP`, `^({alternatives})$`) || \
         HeaderRegexp(`X-Forwarded-For`, \
         `(^[[:space:]]*|,[[:space:]]*)({alternatives})([[:space:]]*,|$)`)"
    )
}

struct RenderedActive {
    stage: TraefikStage,
    routers: BTreeMap<String, String>,
}

fn render_active(cluster_id: &ClusterId, active: &PublishedTraffic) -> RenderedActive {
    let service_label = service_label(&active.spec.service_id);
    let generation_prefix = generation_prefix(&service_label, &active.generation_id);
    let router_prefix = router_label_prefix(&service_label);
    let mut stage = BTreeMap::new();
    let mut routers = BTreeMap::new();
    for route in &active.spec.routes {
        render_route(
            cluster_id,
            route,
            &active.spec.targets,
            &generation_prefix,
            &router_prefix,
            &mut stage,
            &mut routers,
        );
    }
    RenderedActive {
        stage: TraefikStage {
            generation_id: active.generation_id.clone(),
            entries: stage,
        },
        routers,
    }
}

fn render_route(
    cluster_id: &ClusterId,
    route: &TrafficRoute,
    targets: &[TrafficTarget],
    generation_prefix: &str,
    router_prefix: &str,
    stage: &mut BTreeMap<String, String>,
    routers: &mut BTreeMap<String, String>,
) {
    let route_label = short_hash(&[route.route_id.as_str()]);
    let service = format!("{generation_prefix}r-{route_label}");
    let route_targets = targets
        .iter()
        .filter(|target| target.endpoint.port() == route.target_port)
        .collect::<Vec<_>>();
    insert_servers(stage, &service, &route_targets);
    let rule = route_rule(route);
    let router = format!("{router_prefix}r-{route_label}");
    insert_router(routers, &router, &rule, &service, 10);

    let Some(affinity) = route.session_affinity.as_ref() else {
        return;
    };
    let mut by_node = BTreeMap::<&NodeId, Vec<&TrafficTarget>>::new();
    for target in route_targets {
        by_node.entry(&target.node_id).or_default().push(target);
    }
    for (node_id, node_targets) in by_node {
        let node_label = short_hash(&[node_id.as_str()]);
        let affinity_service = format!("{service}-a-{node_label}");
        insert_servers(stage, &affinity_service, &node_targets);
        let affinity_router = format!("{router}-a-{node_label}");
        let token = affinity_token(cluster_id, node_id);
        insert_router(
            routers,
            &affinity_router,
            &format!("({rule}) && Header(`{}`, `{token}`)", affinity.header),
            &affinity_service,
            100,
        );
    }
}

fn insert_servers(entries: &mut BTreeMap<String, String>, label: &str, targets: &[&TrafficTarget]) {
    let prefix = format!("http/services/{label}/loadBalancer");
    for (index, target) in targets.iter().enumerate() {
        entries.insert(
            format!("{prefix}/servers/{index}/url"),
            format!("http://{}", target.endpoint),
        );
    }
    entries.insert(format!("{prefix}/passHostHeader"), "true".to_string());
}

fn insert_router(
    entries: &mut BTreeMap<String, String>,
    label: &str,
    rule: &str,
    service: &str,
    priority: u32,
) {
    entries.extend([
        (format!("http/routers/{label}/rule"), rule.to_string()),
        (format!("http/routers/{label}/service"), service.to_string()),
        (
            format!("http/routers/{label}/entryPoints/0"),
            "web".to_string(),
        ),
        (
            format!("http/routers/{label}/priority"),
            priority.to_string(),
        ),
    ]);
}

pub(crate) fn route_rule(route: &TrafficRoute) -> String {
    let hosts = route
        .hosts
        .iter()
        .map(|host| {
            if let Some(suffix) = host.strip_prefix("*.") {
                format!("HostRegexp(`^[^.]+\\.{}$`)", regex_domain(suffix))
            } else {
                format!("Host(`{host}`)")
            }
        })
        .collect::<Vec<_>>()
        .join(" || ");
    if let Some(path) = route.path_prefix.as_ref() {
        format!("({hosts}) && PathPrefix(`{path}`)")
    } else {
        hosts
    }
}

fn regex_domain(domain: &str) -> String {
    domain.replace('.', "\\.")
}

fn service_label(service_id: &kernel_api::ServiceId) -> String {
    format!("maestro-s-{}-", short_hash(&[service_id.as_str()]))
}

fn router_label_prefix(service_label: &str) -> String {
    format!("{service_label}router-")
}

fn generation_prefix(service_label: &str, generation_id: &TrafficGenerationId) -> String {
    format!(
        "{service_label}g-{}-",
        short_hash(&[generation_id.as_str()])
    )
}

fn short_hash(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    hash.update(LABEL_DOMAIN);
    for part in parts {
        hash.update(part.as_bytes());
        hash.update([0]);
    }
    hash.finalize()
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn affinity_token(cluster_id: &ClusterId, node_id: &NodeId) -> String {
    let mut hash = Sha256::new();
    hash.update(AFFINITY_DOMAIN);
    hash.update(cluster_id.as_str().as_bytes());
    hash.update([0]);
    hash.update(node_id.as_str().as_bytes());
    hash.finalize()
        .iter()
        .take(16)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
pub(crate) fn owned_prefixes(change: &BackendChange) -> BTreeSet<String> {
    let label = service_label(&change.service_id);
    change
        .remove
        .iter()
        .map(|generation| format!("http/services/{}", generation_prefix(&label, generation)))
        .collect()
}
