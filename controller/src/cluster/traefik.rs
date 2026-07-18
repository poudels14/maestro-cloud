use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Result, bail};
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, DeleteOptions, EventType, GetOptions, TlsOptions,
    Txn, TxnOp,
};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, broadcast, watch};

use crate::{
    cluster::types::{
        Assignment, DnsRecordSet, LeadershipToken, ReplicaEndpoint, TraefikServiceIdentity,
        TrafficGeneration,
    },
    deployment::ingress_blocklist,
    deployment::store::AtomicDeploymentUpdates,
    deployment::{dns::DnsManager, types::IngressConfig},
    logs::Logger,
    signal::ShutdownEvent,
};

const TRAFFIC_PREFIX: &str = "/maetro/cluster/traffic/";
const DNS_PREFIX: &str = "/maetro/cluster/dns/";
const SERVICE_MAP_PREFIX: &str = "/maetro/cluster/traefik-service-map/";
const GATEWAY_PREFIX: &str = "maestro-gateway/";
const GATEWAY_TRANSPORT: &str = "cluster-gateway@file";
pub const GATEWAY_HEALTH_PATH: &str = "/_maestro/gateway-ready";
const AFFINITY_TOKEN_DOMAIN: &[u8] = b"maestro-node-affinity-v1\0";
const MAX_ATOMIC_CUTOVER_OPS: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingTarget {
    pub assignment: Assignment,
    pub endpoint: Option<ReplicaEndpoint>,
}

pub struct EtcdTrafficManager {
    client: Arc<Mutex<Client>>,
}

impl EtcdTrafficManager {
    pub async fn connect(endpoints: &[String], tls: Option<TlsOptions>) -> Result<Self> {
        let options = tls.map(|tls| ConnectOptions::new().with_tls(tls));
        let client = Client::connect(endpoints, options).await?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    pub async fn read_traffic(&self, service_id: &str) -> Result<Option<TrafficGeneration>> {
        let response = self
            .client
            .lock()
            .await
            .get(traffic_key(service_id), None)
            .await?;
        response
            .kvs()
            .first()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .transpose()
    }

    pub async fn list_traffic(&self) -> Result<Vec<TrafficGeneration>> {
        let response = self
            .client
            .lock()
            .await
            .get(TRAFFIC_PREFIX, Some(GetOptions::new().with_prefix()))
            .await?;
        response
            .kvs()
            .iter()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .collect()
    }

    async fn read_dns(&self, service_id: &str) -> Result<Option<DnsRecordSet>> {
        let response = self
            .client
            .lock()
            .await
            .get(dns_key(service_id), None)
            .await?;
        response
            .kvs()
            .first()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .transpose()
    }

    pub async fn cutover(
        &self,
        token: &LeadershipToken,
        cluster_name: &str,
        service_id: &str,
        deployment_id: &str,
        ingress: Option<&IngressConfig>,
        mut targets: Vec<RoutingTarget>,
        switched_at_ms: i64,
        status_updates: AtomicDeploymentUpdates,
    ) -> Result<TrafficGeneration> {
        targets.sort_by(|left, right| {
            left.assignment
                .assignment_id
                .cmp(&right.assignment.assignment_id)
        });
        targets.dedup_by(|left, right| {
            left.assignment.assignment_id == right.assignment.assignment_id
        });
        let active_assignment_ids = targets
            .iter()
            .map(|target| target.assignment.assignment_id.clone())
            .collect::<Vec<_>>();
        let active_node_ids = targets
            .iter()
            .map(|target| target.assignment.node_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let current = self.read_traffic(service_id).await?;
        let dns = dns_record_set(cluster_name, service_id, ingress.is_some(), &targets);
        self.reconcile_ingress_blocklist(token).await?;
        let routing_fingerprint = routing_fingerprint(ingress)?;
        if let Some(current) = &current
            && current.deployment_id == deployment_id
            && current.active_assignment_ids == active_assignment_ids
            && current.routing_fingerprint == routing_fingerprint
            && status_updates.operations.is_empty()
        {
            let staged = ingress
                .map(|ingress| {
                    stage_generation(
                        cluster_name,
                        service_id,
                        deployment_id,
                        &current.generation,
                        ingress,
                        &targets,
                    )
                })
                .transpose()?
                .unwrap_or_default();
            if self.staged_matches(&staged).await?
                && self.read_dns(service_id).await?.as_ref() == Some(&dns)
            {
                return Ok(current.clone());
            }
        }
        let traffic_epoch = current
            .as_ref()
            .map(|generation| generation.traffic_epoch.saturating_add(1))
            .unwrap_or(1);
        let generation_id = generation_id(
            service_id,
            deployment_id,
            traffic_epoch,
            &active_assignment_ids,
        );
        let generation = TrafficGeneration {
            service_id: service_id.to_string(),
            deployment_id: deployment_id.to_string(),
            traffic_epoch,
            active_assignment_ids,
            active_node_ids: active_node_ids.clone(),
            generation: generation_id.clone(),
            routing_fingerprint,
            switched_at_ms,
            drain_old_after_ms: switched_at_ms.saturating_add(30_000),
        };
        let staged = ingress
            .map(|ingress| {
                stage_generation(
                    cluster_name,
                    service_id,
                    deployment_id,
                    &generation_id,
                    ingress,
                    &targets,
                )
            })
            .transpose()?
            .unwrap_or_default();
        if staged.len().saturating_add(1) > MAX_ATOMIC_CUTOVER_OPS {
            bail!(
                "Traefik generation for `{service_id}` requires {} operations; maximum is {MAX_ATOMIC_CUTOVER_OPS}",
                staged.len()
            );
        }
        if !staged.is_empty() {
            let transaction = Txn::new().when([leadership_compare(token)]).and_then(
                staged
                    .iter()
                    .map(|(key, value)| TxnOp::put(key.clone(), value.clone(), None))
                    .collect::<Vec<_>>(),
            );
            if !self.client.lock().await.txn(transaction).await?.succeeded() {
                bail!("leadership fence rejected Traefik generation staging");
            }
            self.verify_staged(&staged).await?;
        }

        let mut desired_router_entries = BTreeMap::<String, Vec<u8>>::new();
        let mut router_prefixes = BTreeSet::new();
        let mut metadata_ops = Vec::new();
        let router_prefix = format!("traefik/http/routers/{service_id}");
        router_prefixes.insert(format!("{router_prefix}/"));
        router_prefixes.insert(format!("traefik/http/routers/{service_id}-aff-"));
        let mut gateway_nodes = active_node_ids.iter().cloned().collect::<BTreeSet<_>>();
        if let Some(current) = &current {
            gateway_nodes.extend(current.active_node_ids.iter().cloned());
        }
        for node_id in &gateway_nodes {
            router_prefixes.insert(format!("{}/{service_id}/", gateway_router_prefix(node_id)));
        }
        if let Some(ingress) = ingress
            && !targets.is_empty()
        {
            let rule = ingress_rule(cluster_name, service_id, ingress)?;
            let service_label = format!("{service_id}-g-{generation_id}");
            desired_router_entries.extend([
                (format!("{router_prefix}/rule"), rule.clone().into_bytes()),
                (
                    format!("{router_prefix}/service"),
                    service_label.clone().into_bytes(),
                ),
                (format!("{router_prefix}/entryPoints/0"), b"web".to_vec()),
                (
                    format!("{router_prefix}/entryPoints/1"),
                    b"internal".to_vec(),
                ),
                (format!("{router_prefix}/priority"), b"10".to_vec()),
            ]);
            metadata_ops.push(put_json(
                format!("{SERVICE_MAP_PREFIX}{service_label}"),
                &TraefikServiceIdentity {
                    service_id: service_id.to_string(),
                    deployment_id: deployment_id.to_string(),
                    node_id: None,
                },
            )?);
            let mut by_node = BTreeMap::<&str, Vec<&RoutingTarget>>::new();
            for target in &targets {
                by_node
                    .entry(target.assignment.node_id.as_str())
                    .or_default()
                    .push(target);
            }
            for (node_id, _) in by_node {
                let affinity_label = format!("{service_label}-aff-{node_id}");
                let affinity_router = format!("{router_prefix}-aff-{node_id}");
                let gateway_router = format!("{}/{service_id}", gateway_router_prefix(node_id));
                desired_router_entries.extend([
                    (
                        format!("{affinity_router}/rule"),
                        affinity_rule(cluster_name, service_id, ingress, node_id)?.into_bytes(),
                    ),
                    (
                        format!("{affinity_router}/service"),
                        affinity_label.clone().into_bytes(),
                    ),
                    (format!("{affinity_router}/entryPoints/0"), b"web".to_vec()),
                    (
                        format!("{affinity_router}/entryPoints/1"),
                        b"internal".to_vec(),
                    ),
                    (format!("{affinity_router}/priority"), b"100".to_vec()),
                    (format!("{gateway_router}/rule"), rule.clone().into_bytes()),
                    (
                        format!("{gateway_router}/service"),
                        service_label.clone().into_bytes(),
                    ),
                    (
                        format!("{gateway_router}/entryPoints/0"),
                        b"gateway".to_vec(),
                    ),
                    (format!("{gateway_router}/tls"), b"true".to_vec()),
                    (
                        format!("{gateway_router}/tls/options"),
                        GATEWAY_TRANSPORT.as_bytes().to_vec(),
                    ),
                    (format!("{gateway_router}/priority"), b"10".to_vec()),
                    (
                        format!("{gateway_router}/middlewares/0"),
                        service_label.clone().into_bytes(),
                    ),
                ]);
                metadata_ops.push(put_json(
                    format!("{SERVICE_MAP_PREFIX}{affinity_label}"),
                    &TraefikServiceIdentity {
                        service_id: service_id.to_string(),
                        deployment_id: deployment_id.to_string(),
                        node_id: Some(node_id.to_string()),
                    },
                )?);
            }
        }
        let existing_router_keys = self.router_keys(&router_prefixes).await?;
        let router_plan = router_cutover_plan(existing_router_keys, desired_router_entries);
        let mut cutover_ops = router_plan
            .stale_keys
            .into_iter()
            .map(|key| TxnOp::delete(key, None))
            .collect::<Vec<_>>();
        cutover_ops.extend(
            router_plan
                .desired_entries
                .into_iter()
                .map(|(key, value)| TxnOp::put(key, value, None)),
        );
        cutover_ops.extend(metadata_ops);
        cutover_ops.extend([
            put_json(traffic_key(service_id), &generation)?,
            put_json(dns_key(service_id), &dns)?,
        ]);
        cutover_ops.extend(status_updates.operations);
        let transaction_size = cutover_ops
            .len()
            .saturating_add(status_updates.comparisons.len())
            .saturating_add(2);
        if transaction_size > MAX_ATOMIC_CUTOVER_OPS {
            bail!(
                "Traefik cutover for `{service_id}` requires {transaction_size} operations; maximum is {MAX_ATOMIC_CUTOVER_OPS}",
            );
        }
        let traffic_compare = current.as_ref().map_or_else(
            || Compare::version(traffic_key(service_id), CompareOp::Equal, 0),
            |current| {
                Compare::value(
                    traffic_key(service_id),
                    CompareOp::Equal,
                    serde_json::to_vec(current).expect("traffic generation serializes"),
                )
            },
        );
        let mut comparisons = vec![leadership_compare(token), traffic_compare];
        comparisons.extend(status_updates.comparisons);
        let transaction = Txn::new().when(comparisons).and_then(cutover_ops);
        if !self.client.lock().await.txn(transaction).await?.succeeded() {
            bail!("leadership or traffic-generation fence rejected atomic cutover");
        }
        Ok(generation)
    }

    async fn router_keys(&self, prefixes: &BTreeSet<String>) -> Result<BTreeSet<String>> {
        let mut keys = BTreeSet::new();
        let mut client = self.client.lock().await;
        for prefix in prefixes {
            let response = client
                .get(
                    prefix.as_str(),
                    Some(GetOptions::new().with_prefix().with_keys_only()),
                )
                .await?;
            for entry in response.kvs() {
                keys.insert(std::str::from_utf8(entry.key())?.to_string());
            }
        }
        Ok(keys)
    }

    pub async fn reconcile_ingress_blocklist(&self, token: &LeadershipToken) -> Result<()> {
        let blocked_ips = ingress_blocklist::read(&self.client).await?;
        ingress_blocklist::reconcile_traefik(&self.client, &blocked_ips, Some(token)).await
    }

    pub async fn remove_service(&self, token: &LeadershipToken, service_id: &str) -> Result<()> {
        let mut operations = vec![
            TxnOp::delete(
                format!("traefik/http/routers/{service_id}/"),
                Some(DeleteOptions::new().with_prefix()),
            ),
            TxnOp::delete(
                format!("traefik/http/routers/{service_id}-aff-"),
                Some(DeleteOptions::new().with_prefix()),
            ),
            TxnOp::delete(
                format!("traefik/http/services/{service_id}-g-"),
                Some(DeleteOptions::new().with_prefix()),
            ),
            TxnOp::delete(
                format!("traefik/http/middlewares/{service_id}-g-"),
                Some(DeleteOptions::new().with_prefix()),
            ),
            TxnOp::delete(traffic_key(service_id), None),
            TxnOp::delete(dns_key(service_id), None),
            TxnOp::delete(
                format!("{SERVICE_MAP_PREFIX}{service_id}-g-"),
                Some(DeleteOptions::new().with_prefix()),
            ),
        ];
        for prefix in self.gateway_cleanup_prefixes(service_id, None).await? {
            operations.push(TxnOp::delete(
                prefix,
                Some(DeleteOptions::new().with_prefix()),
            ));
        }
        if operations.len().saturating_add(1) > MAX_ATOMIC_CUTOVER_OPS {
            bail!("routing cleanup for `{service_id}` exceeds the atomic operation limit");
        }
        let transaction = Txn::new()
            .when([leadership_compare(token)])
            .and_then(operations);
        if !self.client.lock().await.txn(transaction).await?.succeeded() {
            bail!("leadership fence rejected routing cleanup");
        }
        Ok(())
    }

    pub async fn gc_old_generations(
        &self,
        token: &LeadershipToken,
        service_id: &str,
        now_ms: i64,
    ) -> Result<()> {
        let Some(active) = self.read_traffic(service_id).await? else {
            return Ok(());
        };
        if now_ms < active.drain_old_after_ms {
            return Ok(());
        }
        let active_prefix = format!("{service_id}-g-{}", active.generation);
        let services_prefix = format!("traefik/http/services/{service_id}-g-");
        let response = self
            .client
            .lock()
            .await
            .get(
                services_prefix.as_str(),
                Some(GetOptions::new().with_prefix().with_keys_only()),
            )
            .await?;
        let mut labels = BTreeSet::new();
        for entry in response.kvs() {
            let key = std::str::from_utf8(entry.key())?;
            if let Some(label) = key
                .strip_prefix("traefik/http/services/")
                .and_then(|suffix| suffix.split('/').next())
                && !label.starts_with(&active_prefix)
            {
                labels.insert(label.to_string());
            }
        }
        for label in labels {
            let gateway_prefixes = self
                .gateway_cleanup_prefixes(service_id, Some(&label))
                .await?;
            let mut operations = vec![
                TxnOp::delete(
                    format!("traefik/http/services/{label}/"),
                    Some(DeleteOptions::new().with_prefix()),
                ),
                TxnOp::delete(
                    format!("traefik/http/middlewares/{label}/"),
                    Some(DeleteOptions::new().with_prefix()),
                ),
                TxnOp::delete(format!("{SERVICE_MAP_PREFIX}{label}"), None),
            ];
            operations.extend(
                gateway_prefixes
                    .into_iter()
                    .map(|prefix| TxnOp::delete(prefix, Some(DeleteOptions::new().with_prefix()))),
            );
            let transaction = Txn::new()
                .when([leadership_compare(token)])
                .and_then(operations);
            if !self.client.lock().await.txn(transaction).await?.succeeded() {
                bail!("leadership fence rejected Traefik generation garbage collection");
            }
        }
        Ok(())
    }

    async fn gateway_cleanup_prefixes(
        &self,
        service_id: &str,
        exact_service_label: Option<&str>,
    ) -> Result<BTreeSet<String>> {
        let response = self
            .client
            .lock()
            .await
            .get(
                GATEWAY_PREFIX,
                Some(GetOptions::new().with_prefix().with_keys_only()),
            )
            .await?;
        let generation_prefix = format!("{service_id}-g-");
        let mut prefixes = BTreeSet::new();
        for entry in response.kvs() {
            let key = std::str::from_utf8(entry.key())?;
            let parts = key.split('/').collect::<Vec<_>>();
            if parts.len() < 5 || parts[0] != "maestro-gateway" || parts[2] != "http" {
                continue;
            }
            let matches = match parts[3] {
                "routers" => exact_service_label.is_none() && parts[4] == service_id,
                "services" | "middlewares" => exact_service_label.map_or_else(
                    || parts[4].starts_with(&generation_prefix),
                    |label| parts[4] == label,
                ),
                _ => false,
            };
            if matches {
                prefixes.insert(parts[..5].join("/") + "/");
            }
        }
        Ok(prefixes)
    }

    pub async fn list_dns(&self) -> Result<Vec<DnsRecordSet>> {
        let response = self
            .client
            .lock()
            .await
            .get(DNS_PREFIX, Some(GetOptions::new().with_prefix()))
            .await?;
        response
            .kvs()
            .iter()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .collect()
    }

    pub async fn watch_dns(&self) -> Result<watch::Receiver<u64>> {
        let (sender, receiver) = watch::channel(0_u64);
        let client = self.client.clone();
        tokio::spawn(async move {
            let mut generation = 0_u64;
            loop {
                let result = client
                    .lock()
                    .await
                    .watch(
                        DNS_PREFIX,
                        Some(etcd_client::WatchOptions::new().with_prefix()),
                    )
                    .await;
                let Ok(mut stream) = result else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };
                while let Ok(Some(response)) = stream.message().await {
                    if response.events().iter().any(|event| {
                        matches!(event.event_type(), EventType::Put | EventType::Delete)
                    }) {
                        generation = generation.saturating_add(1);
                        if sender.send(generation).is_err() {
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        Ok(receiver)
    }

    async fn verify_staged(&self, expected: &BTreeMap<String, Vec<u8>>) -> Result<()> {
        let mut client = self.client.lock().await;
        for (key, value) in expected {
            let response = client.get(key.as_str(), None).await?;
            if response
                .kvs()
                .first()
                .is_none_or(|entry| entry.value() != value)
            {
                bail!("Traefik staged key `{key}` failed verification");
            }
        }
        Ok(())
    }

    async fn staged_matches(&self, expected: &BTreeMap<String, Vec<u8>>) -> Result<bool> {
        let mut client = self.client.lock().await;
        for (key, value) in expected {
            let response = client.get(key.as_str(), None).await?;
            if response
                .kvs()
                .first()
                .is_none_or(|entry| entry.value() != value)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

pub async fn run_dns_sync(
    store: Arc<EtcdTrafficManager>,
    dns: Arc<DnsManager>,
    local_ingress_ip: String,
    mut shutdown: broadcast::Receiver<ShutdownEvent>,
    logger: Logger,
) {
    let mut watcher = match store.watch_dns().await {
        Ok(watcher) => watcher,
        Err(error) => {
            logger.emit("error", &format!("failed to watch cluster DNS: {error}"));
            return;
        }
    };
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut previous = BTreeSet::new();
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = interval.tick() => {}
            changed = watcher.changed() => {
                if changed.is_err() { break; }
            }
        }
        let sets = match store.list_dns().await {
            Ok(sets) => sets,
            Err(error) => {
                logger.emit("warn", &format!("cluster DNS resync failed: {error}"));
                continue;
            }
        };
        let mut current = BTreeSet::new();
        let mut failed = false;
        for set in sets {
            current.insert(set.service_id.clone());
            let mut records = BTreeMap::<String, Vec<String>>::new();
            records.insert(
                set.stable_fqdn,
                if set.via_ingress {
                    vec![local_ingress_ip.clone()]
                } else {
                    set.addresses
                },
            );
            for (fqdn, address) in set.replica_records {
                records.entry(fqdn).or_default().push(address);
            }
            if let Err(error) = dns.replace_owned_records(&owner(&set.service_id), records) {
                logger.emit("error", &format!("invalid cluster DNS record set: {error}"));
                failed = true;
            }
        }
        for removed in previous.difference(&current) {
            let _ = dns.replace_owned_records(&owner(removed), BTreeMap::new());
        }
        if !failed {
            if let Err(error) = dns.flush() {
                logger.emit("error", &format!("failed to flush cluster DNS: {error}"));
            } else {
                previous = current;
            }
        }
    }
}

fn stage_generation(
    cluster_name: &str,
    service_id: &str,
    deployment_id: &str,
    generation: &str,
    ingress: &IngressConfig,
    targets: &[RoutingTarget],
) -> Result<BTreeMap<String, Vec<u8>>> {
    let _ = ingress_rule(cluster_name, service_id, ingress)?;
    let label = format!("{service_id}-g-{generation}");
    let prefix = format!("traefik/http/services/{label}/loadBalancer");
    let mut values = BTreeMap::from([
        (
            format!("{prefix}/sticky/cookie/name"),
            b"maestro-node-affinity".to_vec(),
        ),
        (format!("{prefix}/sticky/cookie/httpOnly"), b"true".to_vec()),
        (
            format!("{prefix}/serversTransport"),
            GATEWAY_TRANSPORT.as_bytes().to_vec(),
        ),
        (
            format!("{prefix}/healthCheck/path"),
            GATEWAY_HEALTH_PATH.as_bytes().to_vec(),
        ),
        (format!("{prefix}/healthCheck/interval"), b"5s".to_vec()),
        (format!("{prefix}/healthCheck/timeout"), b"2s".to_vec()),
    ]);
    let mut by_node = BTreeMap::<&str, Vec<&RoutingTarget>>::new();
    for target in targets {
        let Some(endpoint) = target.endpoint.as_ref() else {
            bail!(
                "assignment `{}` has no routable ingress endpoint",
                target.assignment.assignment_id
            );
        };
        if endpoint.gateway.port == 0 {
            bail!(
                "assignment `{}` has an invalid node gateway port",
                target.assignment.assignment_id
            );
        }
        by_node
            .entry(target.assignment.node_id.as_str())
            .or_default()
            .push(target);
    }
    for (node_index, (node_id, node_targets)) in by_node.into_iter().enumerate() {
        let gateway = &node_targets
            .first()
            .and_then(|target| target.endpoint.as_ref())
            .expect("routing endpoints validated above")
            .gateway;
        if node_targets.iter().any(|target| {
            target
                .endpoint
                .as_ref()
                .is_none_or(|endpoint| &endpoint.gateway != gateway)
        }) {
            bail!("node `{node_id}` advertised inconsistent gateway endpoints");
        }
        values.insert(
            format!("{prefix}/servers/{node_index}/url"),
            gateway_url(gateway).into_bytes(),
        );
        values.insert(
            format!("{prefix}/servers/{node_index}/weight"),
            node_targets.len().to_string().into_bytes(),
        );

        let affinity_prefix = format!("traefik/http/services/{label}-aff-{node_id}/loadBalancer");
        for (key, value) in [
            ("servers/0/url", gateway_url(gateway)),
            ("serversTransport", GATEWAY_TRANSPORT.to_string()),
            ("healthCheck/path", GATEWAY_HEALTH_PATH.to_string()),
            ("healthCheck/interval", "5s".to_string()),
            ("healthCheck/timeout", "2s".to_string()),
        ] {
            values.insert(format!("{affinity_prefix}/{key}"), value.into_bytes());
        }

        let local_prefix = format!("{GATEWAY_PREFIX}{node_id}/http/services/{label}/loadBalancer");
        values.insert(
            format!("{local_prefix}/sticky/cookie/name"),
            b"maestro-affinity".to_vec(),
        );
        values.insert(
            format!("{local_prefix}/sticky/cookie/httpOnly"),
            b"true".to_vec(),
        );
        for (index, target) in node_targets.into_iter().enumerate() {
            let endpoint = target
                .endpoint
                .as_ref()
                .expect("routing endpoints validated above");
            values.insert(
                format!("{local_prefix}/servers/{index}/url"),
                container_url(endpoint).into_bytes(),
            );
        }

        let affinity_token = affinity_token(cluster_name, node_id);
        let middleware_prefix = format!("{GATEWAY_PREFIX}{node_id}/http/middlewares/{label}");
        for direction in ["customRequestHeaders", "customResponseHeaders"] {
            values.insert(
                format!(
                    "{middleware_prefix}/headers/{direction}/{}",
                    ingress.session_affinity_header()
                ),
                affinity_token.as_bytes().to_vec(),
            );
        }
    }
    let identity = TraefikServiceIdentity {
        service_id: service_id.to_string(),
        deployment_id: deployment_id.to_string(),
        node_id: None,
    };
    values.insert(
        format!("{SERVICE_MAP_PREFIX}{label}"),
        serde_json::to_vec(&identity)?,
    );
    Ok(values)
}

fn dns_record_set(
    cluster_name: &str,
    service_id: &str,
    via_ingress: bool,
    targets: &[RoutingTarget],
) -> DnsRecordSet {
    let domain = format!("{cluster_name}.maestro.internal");
    let mut addresses = targets
        .iter()
        .filter_map(|target| {
            target
                .endpoint
                .as_ref()
                .map(|endpoint| endpoint.container_ip.clone())
        })
        .collect::<Vec<_>>();
    addresses.sort();
    addresses.dedup();
    let mut replica_records = targets
        .iter()
        .filter_map(|target| {
            let endpoint = target.endpoint.as_ref()?;
            Some((
                format!("{}.{}", endpoint.container_hostname, domain),
                endpoint.container_ip.clone(),
            ))
        })
        .collect::<Vec<_>>();
    replica_records.sort();
    replica_records.dedup();
    DnsRecordSet {
        service_id: service_id.to_string(),
        stable_fqdn: format!("{service_id}.{domain}"),
        via_ingress,
        addresses,
        replica_records,
    }
}

fn ingress_rule(cluster_name: &str, service_id: &str, ingress: &IngressConfig) -> Result<String> {
    let mut hosts = ingress
        .hosts()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    hosts.push(format!("{service_id}.{cluster_name}.maestro.internal"));
    hosts.sort();
    hosts.dedup();
    let rule = hosts
        .iter()
        .map(|host| {
            if host.chars().any(|character| {
                !character.is_ascii_alphanumeric() && character != '.' && character != '-'
            }) {
                format!("HostRegexp(`{host}`)")
            } else {
                format!("Host(`{host}`)")
            }
        })
        .collect::<Vec<_>>()
        .join(" || ");
    if rule.is_empty() {
        bail!("ingress requires at least one host");
    }
    Ok(rule)
}

fn affinity_rule(
    cluster_name: &str,
    service_id: &str,
    ingress: &IngressConfig,
    node_id: &str,
) -> Result<String> {
    let token = affinity_token(cluster_name, node_id);
    Ok(format!(
        "({}) && Header(`{}`, `{token}`)",
        ingress_rule(cluster_name, service_id, ingress)?,
        ingress.session_affinity_header()
    ))
}

pub(super) fn affinity_token(cluster_name: &str, node_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(AFFINITY_TOKEN_DOMAIN);
    hasher.update(cluster_name.as_bytes());
    hasher.update([0]);
    hasher.update(node_id.as_bytes());
    format!("{:x}", hasher.finalize())[..32].to_string()
}

fn routing_fingerprint(ingress: Option<&IngressConfig>) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"node-gateway-v1");
    hasher.update(serde_json::to_vec(&ingress)?);
    Ok(format!("{:x}", hasher.finalize()))
}

fn generation_id(
    service_id: &str,
    deployment_id: &str,
    traffic_epoch: u64,
    assignments: &[String],
) -> String {
    let mut hasher = Sha256::new();
    for value in [service_id, deployment_id, &traffic_epoch.to_string()] {
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    let mut assignments = assignments.to_vec();
    assignments.sort();
    assignments.dedup();
    for assignment in assignments {
        hasher.update(assignment.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())[..16].to_string()
}

fn container_url(endpoint: &ReplicaEndpoint) -> String {
    format!(
        "http://{}:{}",
        endpoint.container_ip, endpoint.ingress_container_port
    )
}

fn gateway_url(gateway: &crate::cluster::NodeGatewayEndpoint) -> String {
    format!("https://{}:{}", gateway.host_ip, gateway.port)
}

fn gateway_router_prefix(node_id: &str) -> String {
    format!("{GATEWAY_PREFIX}{node_id}/http/routers")
}

struct RouterCutoverPlan {
    stale_keys: BTreeSet<String>,
    desired_entries: BTreeMap<String, Vec<u8>>,
}

fn router_cutover_plan(
    existing: BTreeSet<String>,
    desired_entries: BTreeMap<String, Vec<u8>>,
) -> RouterCutoverPlan {
    let stale_keys = existing
        .into_iter()
        .filter(|key| !desired_entries.contains_key(key))
        .collect();
    RouterCutoverPlan {
        stale_keys,
        desired_entries,
    }
}

fn leadership_compare(token: &LeadershipToken) -> Compare {
    Compare::create_revision(
        token.election_key.clone(),
        CompareOp::Equal,
        token.create_revision,
    )
}

fn put_json(key: impl Into<Vec<u8>>, value: &impl serde::Serialize) -> Result<TxnOp> {
    Ok(TxnOp::put(key, serde_json::to_vec(value)?, None))
}

fn traffic_key(service_id: &str) -> String {
    format!("{TRAFFIC_PREFIX}{service_id}")
}

fn dns_key(service_id: &str) -> String {
    format!("{DNS_PREFIX}{service_id}")
}

fn owner(service_id: &str) -> String {
    format!("cluster-service:{service_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(id: &str, node: &str, ip: &str) -> RoutingTarget {
        RoutingTarget {
            assignment: Assignment {
                assignment_id: id.to_string(),
                placement_epoch: 1,
                service_id: "web".to_string(),
                deployment_id: "dep".to_string(),
                replica_index: 0,
                node_id: node.to_string(),
                container_ip: ip.parse().ok(),
                replaces_assignment_id: None,
                created_at_ms: 1,
            },
            endpoint: Some(ReplicaEndpoint {
                container_ip: ip.to_string(),
                container_hostname: format!("web-{id}"),
                ingress_container_port: 8080,
                gateway: crate::cluster::NodeGatewayEndpoint {
                    host_ip: if node == "node-a" {
                        "10.20.0.11".parse().unwrap()
                    } else {
                        "10.20.0.12".parse().unwrap()
                    },
                    port: 3002,
                },
            }),
        }
    }

    #[test]
    fn stages_node_gateways_and_node_local_replica_services() {
        let ingress = IngressConfig {
            host: Some("web.example.com".to_string()),
            hosts: Vec::new(),
            port: Some(8080),
            session_affinity: None,
        };
        let staged = stage_generation(
            "cluster-abcd",
            "web",
            "dep",
            "generation",
            &ingress,
            &[
                target("a", "node-a", "172.20.1.4"),
                target("b", "node-a", "172.20.1.5"),
                target("c", "node-b", "172.20.2.4"),
            ],
        )
        .unwrap();
        assert!(
            staged
                .values()
                .any(|value| value == b"https://10.20.0.11:3002")
        );
        assert_eq!(
            staged.get("traefik/http/services/web-g-generation/loadBalancer/servers/0/weight"),
            Some(&b"2".to_vec())
        );
        assert_eq!(
            staged.get("traefik/http/services/web-g-generation/loadBalancer/servers/1/weight"),
            Some(&b"1".to_vec())
        );
        assert_eq!(
            staged.get(
                "maestro-gateway/node-a/http/services/web-g-generation/loadBalancer/servers/0/url"
            ),
            Some(&b"http://172.20.1.4:8080".to_vec())
        );
        assert!(!staged.iter().any(|(key, value)| {
            key.starts_with("traefik/") && value.starts_with(b"http://172.20.")
        }));
        assert!(staged.keys().any(|key| key.contains("aff-node-a")));
        assert!(
            staged
                .values()
                .any(|value| value == b"maestro-node-affinity")
        );
        assert!(staged.values().any(|value| value == b"maestro-affinity"));
        let node_a_token = affinity_token("cluster-abcd", "node-a").into_bytes();
        for direction in ["customRequestHeaders", "customResponseHeaders"] {
            assert_eq!(
                staged.get(&format!(
                    "maestro-gateway/node-a/http/middlewares/web-g-generation/headers/{direction}/X-Session-Affinity"
                )),
                Some(&node_a_token)
            );
        }
    }

    #[test]
    fn affinity_rule_uses_an_opaque_stable_token_and_configured_header() {
        let mut ingress = IngressConfig {
            host: Some("web.example.com".to_string()),
            hosts: Vec::new(),
            port: Some(8080),
            session_affinity: None,
        };
        assert_eq!(
            affinity_rule("cluster-abcd", "web", &ingress, "node00000001").unwrap(),
            "(Host(`web.cluster-abcd.maestro.internal`) || Host(`web.example.com`)) && Header(`X-Session-Affinity`, `35db6715ce01a73669cb3e4293d52ca1`)"
        );

        ingress.session_affinity = Some(crate::deployment::types::SessionAffinityConfig {
            header: "X-Session-Node".to_string(),
        });
        assert_eq!(
            affinity_rule("cluster-abcd", "web", &ingress, "node00000001").unwrap(),
            "(Host(`web.cluster-abcd.maestro.internal`) || Host(`web.example.com`)) && Header(`X-Session-Node`, `35db6715ce01a73669cb3e4293d52ca1`)"
        );
    }

    #[test]
    fn affinity_tokens_are_cluster_scoped_and_do_not_expose_node_ids() {
        let token = affinity_token("cluster-abcd", "node00000001");
        assert_eq!(token, "35db6715ce01a73669cb3e4293d52ca1");
        assert_eq!(token.len(), 32);
        assert!(token.chars().all(|character| character.is_ascii_hexdigit()));
        assert!(!token.contains("node00000001"));
        assert_ne!(token, affinity_token("cluster-abcd", "node00000002"));
        assert_ne!(token, affinity_token("other-cluster", "node00000001"));
    }

    #[test]
    fn routing_fingerprint_changes_with_ingress_config() {
        let mut ingress = IngressConfig {
            host: Some("web.example.com".to_string()),
            hosts: Vec::new(),
            port: Some(8080),
            session_affinity: None,
        };
        let original = routing_fingerprint(Some(&ingress)).unwrap();
        ingress.session_affinity = Some(crate::deployment::types::SessionAffinityConfig {
            header: "X-Session-Node".to_string(),
        });
        assert_ne!(original, routing_fingerprint(Some(&ingress)).unwrap());
    }

    #[test]
    fn dns_marks_ingress_records_for_local_gateway_resolution() {
        let records = dns_record_set(
            "cluster-abcd",
            "web",
            false,
            &[
                target("a", "node-a", "172.20.1.4"),
                target("b", "node-b", "172.20.2.4"),
            ],
        );
        assert_eq!(records.stable_fqdn, "web.cluster-abcd.maestro.internal");
        assert!(!records.via_ingress);
        assert_eq!(records.addresses, ["172.20.1.4", "172.20.2.4"]);

        let ingress_records = dns_record_set(
            "cluster-abcd",
            "web",
            true,
            &[target("a", "node-a", "172.20.1.4")],
        );
        assert!(ingress_records.via_ingress);
    }

    #[test]
    fn generation_is_independent_of_target_input_order() {
        let one = generation_id("web", "dep", 1, &["a".to_string(), "b".to_string()]);
        let two = generation_id("web", "dep", 1, &["b".to_string(), "a".to_string()]);
        assert_eq!(one, two);
    }

    #[test]
    fn router_cutover_never_deletes_a_key_it_writes() {
        let existing = BTreeSet::from([
            "traefik/http/routers/web/rule".to_string(),
            "traefik/http/routers/web/obsolete".to_string(),
        ]);
        let desired = BTreeMap::from([(
            "traefik/http/routers/web/rule".to_string(),
            b"Host(`web.local`)".to_vec(),
        )]);

        let plan = router_cutover_plan(existing, desired);
        assert_eq!(
            plan.stale_keys,
            BTreeSet::from(["traefik/http/routers/web/obsolete".to_string()])
        );
        assert!(
            plan.stale_keys
                .iter()
                .all(|key| !plan.desired_entries.contains_key(key))
        );
        assert_eq!(
            plan.desired_entries["traefik/http/routers/web/rule"],
            b"Host(`web.local`)"
        );
    }
}
