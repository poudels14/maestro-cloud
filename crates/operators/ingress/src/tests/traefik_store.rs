use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId, NodeInstanceId, TrafficGenerationId};
use kernel_controller::{
    FencedStore, LeaderElector, LeaderIdentity, LeadershipLease, StoreLeaderElector,
};
use kernel_store::{InMemoryStore, Keyspace, Store, TokioClock};

use crate::{
    StoreTraefikProvider, TraefikBlocklistConfig, TraefikCutover, TraefikProvider, TraefikStage,
};

#[tokio::test]
async fn store_provider_stages_then_atomically_replaces_owned_traefik_state()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("store-traefik")?;
    let clock = Arc::new(TokioClock::new());
    let store = Arc::new(InMemoryStore::new(clock));
    let keys = Keyspace::new(&cluster_id);
    let (fenced, lease) = campaign(store.clone(), &keys, "leader-1").await?;
    let provider = StoreTraefikProvider::new(cluster_id, fenced);

    provider.ensure_watchable_root().await?;
    provider.ensure_watchable_root().await?;
    let provider_root = store.list(&keys.traefik()).await?;
    assert_eq!(provider_root.values.len(), 1);
    assert!(provider_root.values[0].key.as_str().ends_with(
        "/http/middlewares/maestro.internal-provider-ready/headers/\
                        customRequestHeaders/X-Maestro-Provider-Ready"
    ));
    assert_eq!(provider_root.values[0].value, b"true");

    provider
        .stage(&TraefikStage {
            generation_id: TrafficGenerationId::new("old-generation")?,
            entries: BTreeMap::from([(
                "http/services/old-main/loadBalancer/servers/0/url".to_string(),
                "http://172.22.0.2:8080".to_string(),
            )]),
        })
        .await?;
    provider
        .cutover(&TraefikCutover {
            router_prefix: "http/routers/maestro-api-".to_string(),
            routers: BTreeMap::from([
                (
                    "http/routers/maestro-api-main/rule".to_string(),
                    "Host(`old.example.test`)".to_string(),
                ),
                (
                    "http/routers/maestro-api-obsolete/rule".to_string(),
                    "Host(`obsolete.example.test`)".to_string(),
                ),
            ]),
            remove_prefixes: Vec::new(),
        })
        .await?;
    provider
        .cutover(&TraefikCutover {
            router_prefix: "http/routers/maestro-api-".to_string(),
            routers: BTreeMap::from([(
                "http/routers/maestro-api-main/rule".to_string(),
                "Host(`new.example.test`)".to_string(),
            )]),
            remove_prefixes: vec!["http/services/old-".to_string()],
        })
        .await?;

    let routers = store.list(&keys.traefik_prefix("http/routers")?).await?;
    assert_eq!(routers.values.len(), 1);
    assert_eq!(routers.values[0].value, b"Host(`new.example.test`)");
    assert!(
        store
            .list(&keys.traefik_prefix("http/services")?)
            .await?
            .values
            .is_empty()
    );

    lease.resign().await?;
    let (_successor, successor_lease) = campaign(store.clone(), &keys, "leader-2").await?;
    let stale = provider
        .stage(&TraefikStage {
            generation_id: TrafficGenerationId::new("stale-generation")?,
            entries: BTreeMap::from([(
                "http/services/stale/loadBalancer/passHostHeader".to_string(),
                "true".to_string(),
            )]),
        })
        .await;
    assert!(stale.is_err());
    assert!(
        store
            .list(&keys.traefik_prefix("http/services/stale")?)
            .await?
            .values
            .is_empty()
    );
    successor_lease.resign().await?;
    Ok(())
}

#[tokio::test]
async fn blocklist_replacement_removes_only_obsolete_reserved_entries()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("blocklist-traefik")?;
    let clock = Arc::new(TokioClock::new());
    let store = Arc::new(InMemoryStore::new(clock));
    let keys = Keyspace::new(&cluster_id);
    let (fenced, lease) = campaign(store.clone(), &keys, "leader-1").await?;
    let provider = StoreTraefikProvider::new(cluster_id, fenced);
    let owned_prefixes = vec![
        "http/routers/maestro.internal-blocked-".to_string(),
        "http/services/maestro.internal-blocked".to_string(),
        "http/middlewares/maestro.internal-blocked".to_string(),
        "http/serversTransports/maestro.internal-blocked".to_string(),
    ];
    provider
        .replace_blocklist(&TraefikBlocklistConfig {
            entries: BTreeMap::from([
                (
                    "http/routers/maestro.internal-blocked-old/rule".to_string(),
                    "ClientIP(`203.0.113.1`)".to_string(),
                ),
                (
                    "http/routers/maestro.internal-blocked-stale/rule".to_string(),
                    "ClientIP(`203.0.113.2`)".to_string(),
                ),
                (
                    "http/services/maestro.internal-blocked/loadBalancer/passHostHeader"
                        .to_string(),
                    "true".to_string(),
                ),
                (
                    "http/middlewares/maestro.internal-blocked/replacePath/path".to_string(),
                    "/_maestro/ingress-denied".to_string(),
                ),
                (
                    "http/serversTransports/maestro.internal-blocked/insecureSkipVerify"
                        .to_string(),
                    "true".to_string(),
                ),
            ]),
            owned_prefixes: owned_prefixes.clone(),
        })
        .await?;
    provider
        .replace_blocklist(&TraefikBlocklistConfig {
            entries: BTreeMap::from([(
                "http/routers/maestro.internal-blocked-new/rule".to_string(),
                "ClientIP(`203.0.113.3`)".to_string(),
            )]),
            owned_prefixes: owned_prefixes.clone(),
        })
        .await?;

    let blocked = store
        .list(&keys.traefik_prefix("http/routers")?)
        .await?
        .values
        .into_iter()
        .filter(|value| value.key.as_str().contains("/maestro.internal-blocked-"))
        .collect::<Vec<_>>();
    assert_eq!(blocked.len(), 1);
    assert_eq!(blocked[0].value, b"ClientIP(`203.0.113.3`)");
    assert!(
        store
            .list(&keys.traefik_prefix("http/services/maestro.internal-blocked")?)
            .await?
            .values
            .is_empty()
    );
    assert!(
        store
            .list(&keys.traefik_prefix("http/middlewares/maestro.internal-blocked")?)
            .await?
            .values
            .is_empty()
    );
    assert!(
        store
            .list(&keys.traefik_prefix("http/serversTransports/maestro.internal-blocked")?)
            .await?
            .values
            .is_empty()
    );

    provider
        .replace_blocklist(&TraefikBlocklistConfig {
            entries: BTreeMap::new(),
            owned_prefixes,
        })
        .await?;
    assert!(
        store
            .list(&keys.traefik_prefix("http/routers")?)
            .await?
            .values
            .into_iter()
            .filter(|value| value.key.as_str().contains("/maestro.internal-blocked-"))
            .collect::<Vec<_>>()
            .is_empty()
    );
    lease.resign().await?;
    Ok(())
}

async fn campaign(
    store: Arc<InMemoryStore>,
    keys: &Keyspace,
    instance: &str,
) -> Result<(Arc<FencedStore>, Box<dyn LeadershipLease>), Box<dyn std::error::Error>> {
    let elector = StoreLeaderElector::new(store.clone(), keys.leader());
    let lease = elector
        .campaign(
            LeaderIdentity {
                node_id: NodeId::new("master")?,
                instance_id: NodeInstanceId::new(instance)?,
            },
            Duration::from_secs(30),
        )
        .await?
        .ok_or("leadership campaign lost")?;
    let fenced = Arc::new(FencedStore::new(
        store,
        keys.leader(),
        lease.token().clone(),
    ));
    Ok((fenced, lease))
}
