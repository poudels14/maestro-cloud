use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId, NodeInstanceId, TrafficGenerationId};
use kernel_controller::{
    FencedStore, LeaderElector, LeaderIdentity, LeadershipLease, StoreLeaderElector,
};
use kernel_store::{InMemoryStore, Keyspace, Store, TokioClock};

use crate::{StoreTraefikProvider, TraefikCutover, TraefikProvider, TraefikStage};

#[tokio::test]
async fn store_provider_stages_then_atomically_replaces_owned_traefik_state()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("store-traefik")?;
    let clock = Arc::new(TokioClock::new());
    let store = Arc::new(InMemoryStore::new(clock));
    let keys = Keyspace::new(&cluster_id);
    let (fenced, lease) = campaign(store.clone(), &keys, "leader-1").await?;
    let provider = StoreTraefikProvider::new(cluster_id, fenced);

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
