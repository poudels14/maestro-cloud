use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, DeleteOptions, GetOptions, Txn, TxnOp};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{
    cluster::types::LeadershipToken,
    deployment::{
        keys::{INGRESS_BLOCKLIST_PREFIX, ingress_blocklist_ip_key},
        types::{blocked_ip_matcher, validate_ingress_blocklist},
    },
};

const APPLIED_KEY: &str = "/maetro/cluster/ingress-blocklist-applied";
pub const ROUTER_LABEL_PREFIX: &str = "maestro.internal-blocked-";
pub const SERVICE_LABEL: &str = "maestro.internal-blocked";
const ROUTER_KEY_PREFIX: &str = "traefik/http/routers/maestro.internal-blocked-";
const SERVICE_PREFIX: &str = "traefik/http/services/maestro.internal-blocked";
const MIDDLEWARE_PREFIX: &str = "traefik/http/middlewares/maestro.internal-blocked";
const ADDRESSES_PER_ROUTER: usize = 256;

pub fn is_internal_router_label(label: &str) -> bool {
    label.starts_with(ROUTER_LABEL_PREFIX)
}

pub fn is_internal_service_label(label: &str) -> bool {
    label == SERVICE_LABEL
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PolicyOperation {
    Put { key: String, value: String },
    DeletePrefix(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LeadershipFence {
    election_key: Vec<u8>,
    create_revision: i64,
}

#[async_trait]
trait PolicyStore: Sync {
    async fn get_value(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>>;
    async fn apply(
        &self,
        operations: Vec<PolicyOperation>,
        fence: Option<&LeadershipFence>,
    ) -> Result<bool>;
}

struct EtcdPolicyStore<'a> {
    client: &'a Arc<Mutex<Client>>,
}

#[async_trait]
impl PolicyStore for EtcdPolicyStore<'_> {
    async fn get_value(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .client
            .lock()
            .await
            .get(key, None)
            .await?
            .kvs()
            .first()
            .map(|entry| entry.value().to_vec()))
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        self.client
            .lock()
            .await
            .get(
                prefix,
                Some(GetOptions::new().with_prefix().with_keys_only()),
            )
            .await?
            .kvs()
            .iter()
            .map(|entry| {
                std::str::from_utf8(entry.key())
                    .map(str::to_string)
                    .map_err(Into::into)
            })
            .collect()
    }

    async fn apply(
        &self,
        operations: Vec<PolicyOperation>,
        fence: Option<&LeadershipFence>,
    ) -> Result<bool> {
        let operations = operations
            .into_iter()
            .map(|operation| match operation {
                PolicyOperation::Put { key, value } => TxnOp::put(key, value, None),
                PolicyOperation::DeletePrefix(prefix) => {
                    TxnOp::delete(prefix, Some(DeleteOptions::new().with_prefix()))
                }
            })
            .collect::<Vec<_>>();
        let transaction = if let Some(fence) = fence {
            Txn::new()
                .when([Compare::create_revision(
                    fence.election_key.clone(),
                    CompareOp::Equal,
                    fence.create_revision,
                )])
                .and_then(operations)
        } else {
            Txn::new().and_then(operations)
        };
        Ok(self.client.lock().await.txn(transaction).await?.succeeded())
    }
}

pub async fn read(client: &Arc<Mutex<Client>>) -> Result<Vec<String>> {
    let response = client
        .lock()
        .await
        .get(
            INGRESS_BLOCKLIST_PREFIX,
            Some(GetOptions::new().with_prefix()),
        )
        .await?;
    let mut addresses = response
        .kvs()
        .iter()
        .map(|entry| {
            std::str::from_utf8(entry.value())
                .map(str::to_string)
                .map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    addresses.sort();
    validate_ingress_blocklist(&addresses).map_err(anyhow::Error::msg)?;
    Ok(addresses)
}

pub async fn set(
    client: &Arc<Mutex<Client>>,
    address: &str,
    blocked: bool,
    token: Option<&LeadershipToken>,
) -> Result<Vec<String>> {
    let canonical = address
        .parse::<std::net::IpAddr>()
        .map_err(|_| anyhow!("invalid IP address `{address}`"))?
        .to_string();
    if canonical != address {
        bail!("IP address `{address}` must use canonical form `{canonical}`");
    }
    let key = ingress_blocklist_ip_key(address);
    let mut comparisons = Vec::new();
    if let Some(token) = token {
        comparisons.push(Compare::create_revision(
            token.election_key.clone(),
            CompareOp::Equal,
            token.create_revision,
        ));
    }
    let operation = if blocked {
        TxnOp::put(key, address, None)
    } else {
        TxnOp::delete(key, None)
    };
    let mut connection = client.lock().await;
    if !connection
        .txn(Txn::new().when(comparisons).and_then([operation]))
        .await?
        .succeeded()
    {
        bail!("leadership changed while updating the ingress blocklist");
    }
    drop(connection);
    read(client).await
}

pub async fn reconcile_traefik(
    client: &Arc<Mutex<Client>>,
    addresses: &[String],
    token: Option<&LeadershipToken>,
) -> Result<()> {
    let store = EtcdPolicyStore { client };
    let fence = token.map(|token| LeadershipFence {
        election_key: token.election_key.clone(),
        create_revision: token.create_revision,
    });
    reconcile_policy(&store, addresses, fence.as_ref()).await
}

async fn reconcile_policy(
    store: &impl PolicyStore,
    addresses: &[String],
    fence: Option<&LeadershipFence>,
) -> Result<()> {
    validate_ingress_blocklist(addresses).map_err(anyhow::Error::msg)?;
    let fingerprint = fingerprint(addresses);
    if store
        .get_value(APPLIED_KEY)
        .await?
        .is_some_and(|applied| applied == fingerprint.as_bytes())
    {
        return Ok(());
    }

    if addresses.is_empty() {
        apply_policy(
            store,
            vec![
                delete_prefix(ROUTER_KEY_PREFIX),
                delete_prefix(format!("{SERVICE_PREFIX}/")),
                delete_prefix(format!("{MIDDLEWARE_PREFIX}/")),
                put(APPLIED_KEY, fingerprint),
            ],
            fence,
        )
        .await?;
        return Ok(());
    }

    apply_policy(
        store,
        vec![
            put(
                format!("{SERVICE_PREFIX}/loadBalancer/servers/0/url"),
                "http://maestro-probe:3001",
            ),
            put(
                format!("{MIDDLEWARE_PREFIX}/replacePath/path"),
                "/_maestro/ingress-denied",
            ),
        ],
        fence,
    )
    .await?;

    let mut active_labels = BTreeSet::new();
    for (index, chunk) in addresses.chunks(ADDRESSES_PER_ROUTER).enumerate() {
        let label = format!("{ROUTER_LABEL_PREFIX}{}-{index}", &fingerprint[..16]);
        let prefix = format!("traefik/http/routers/{label}");
        let rule = blocked_ip_matcher(chunk).expect("non-empty blocklist chunk");
        active_labels.insert(label);
        apply_policy(
            store,
            vec![
                put(format!("{prefix}/rule"), rule),
                put(format!("{prefix}/service"), SERVICE_LABEL),
                put(format!("{prefix}/entryPoints/0"), "web"),
                put(format!("{prefix}/priority"), "10000"),
                put(format!("{prefix}/middlewares/0"), SERVICE_LABEL),
            ],
            fence,
        )
        .await?;
    }

    let old_labels = store
        .list_keys(ROUTER_KEY_PREFIX)
        .await?
        .iter()
        .filter_map(|key| {
            key.strip_prefix("traefik/http/routers/")
                .and_then(|suffix| suffix.split('/').next())
        })
        .filter(|label| !active_labels.contains(*label))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    for label in old_labels {
        apply_policy(
            store,
            vec![delete_prefix(format!("traefik/http/routers/{label}/"))],
            fence,
        )
        .await?;
    }
    apply_policy(store, vec![put(APPLIED_KEY, fingerprint)], fence).await
}

async fn apply_policy(
    store: &impl PolicyStore,
    operations: Vec<PolicyOperation>,
    fence: Option<&LeadershipFence>,
) -> Result<()> {
    if !store.apply(operations, fence).await? {
        bail!("leadership fence rejected global ingress blocklist reconciliation");
    }
    Ok(())
}

fn put(key: impl Into<String>, value: impl Into<String>) -> PolicyOperation {
    PolicyOperation::Put {
        key: key.into(),
        value: value.into(),
    }
}

fn delete_prefix(prefix: impl Into<String>) -> PolicyOperation {
    PolicyOperation::DeletePrefix(prefix.into())
}

fn fingerprint(addresses: &[String]) -> String {
    let mut hasher = Sha256::new();
    for address in addresses {
        hasher.update(address.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeMap,
        sync::{
            Mutex as StdMutex,
            atomic::{AtomicBool, Ordering},
        },
    };

    #[derive(Default)]
    struct FakePolicyStore {
        values: StdMutex<BTreeMap<String, Vec<u8>>>,
        phases: StdMutex<Vec<(Vec<PolicyOperation>, Option<LeadershipFence>)>>,
        reject_fenced_writes: AtomicBool,
    }

    impl FakePolicyStore {
        fn seed(&self, key: &str, value: &str) {
            self.values
                .lock()
                .unwrap()
                .insert(key.to_string(), value.as_bytes().to_vec());
        }
    }

    #[async_trait]
    impl PolicyStore for FakePolicyStore {
        async fn get_value(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }

        async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }

        async fn apply(
            &self,
            operations: Vec<PolicyOperation>,
            fence: Option<&LeadershipFence>,
        ) -> Result<bool> {
            self.phases
                .lock()
                .unwrap()
                .push((operations.clone(), fence.cloned()));
            if fence.is_some() && self.reject_fenced_writes.load(Ordering::SeqCst) {
                return Ok(false);
            }
            let mut values = self.values.lock().unwrap();
            for operation in operations {
                match operation {
                    PolicyOperation::Put { key, value } => {
                        values.insert(key, value.into_bytes());
                    }
                    PolicyOperation::DeletePrefix(prefix) => {
                        values.retain(|key, _| !key.starts_with(&prefix));
                    }
                }
            }
            Ok(true)
        }
    }

    #[test]
    fn blocklist_is_chunked_without_a_global_entry_limit() {
        let addresses = (1..=600)
            .map(|index| format!("2001:db8::{index:x}"))
            .collect::<Vec<_>>();
        validate_ingress_blocklist(&addresses).unwrap();
        assert_eq!(addresses.chunks(ADDRESSES_PER_ROUTER).count(), 3);
        assert_ne!(fingerprint(&addresses), fingerprint(&addresses[..599]));
    }

    #[tokio::test]
    async fn replacement_stages_every_new_chunk_before_removing_the_old_generation() {
        let store = FakePolicyStore::default();
        let old_router = format!("{ROUTER_KEY_PREFIX}old-0/rule");
        store.seed(&old_router, "ClientIP(`192.0.2.1`)");
        store.seed(APPLIED_KEY, "old");
        let addresses = (1..=600)
            .map(|index| format!("2001:db8::{index:x}"))
            .collect::<Vec<_>>();
        let fence = LeadershipFence {
            election_key: b"/maetro/cluster/leader/abc".to_vec(),
            create_revision: 42,
        };

        reconcile_policy(&store, &addresses, Some(&fence))
            .await
            .unwrap();

        let phases = store.phases.lock().unwrap().clone();
        assert!(
            phases
                .iter()
                .all(|(_, actual)| actual.as_ref() == Some(&fence))
        );
        let router_phase_indexes = phases
            .iter()
            .enumerate()
            .filter(|(_, (operations, _))| {
                operations.iter().any(|operation| {
                    matches!(operation, PolicyOperation::Put { key, .. } if key.starts_with(ROUTER_KEY_PREFIX))
                })
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        assert_eq!(router_phase_indexes.len(), 3);
        let delete_index = phases
            .iter()
            .position(|(operations, _)| {
                operations.contains(&PolicyOperation::DeletePrefix(format!(
                    "traefik/http/routers/{ROUTER_LABEL_PREFIX}old-0/"
                )))
            })
            .unwrap();
        assert!(
            router_phase_indexes
                .iter()
                .all(|index| *index < delete_index)
        );
        assert_eq!(
            phases.last().unwrap().0,
            vec![put(APPLIED_KEY, fingerprint(&addresses))]
        );
        assert!(!store.values.lock().unwrap().contains_key(&old_router));
    }

    #[tokio::test]
    async fn empty_policy_removes_only_internal_traefik_state() {
        let store = FakePolicyStore::default();
        store.seed(&format!("{ROUTER_KEY_PREFIX}old-0/rule"), "old");
        store.seed(
            &format!("{SERVICE_PREFIX}/loadBalancer/servers/0/url"),
            "old",
        );
        store.seed(&format!("{MIDDLEWARE_PREFIX}/replacePath/path"), "old");
        store.seed("traefik/http/routers/app/rule", "Host(`app.example.com`)");
        store.seed(APPLIED_KEY, "old");

        reconcile_policy(&store, &[], None).await.unwrap();

        let values = store.values.lock().unwrap();
        assert!(values.keys().all(|key| !key.starts_with(ROUTER_KEY_PREFIX)));
        assert!(values.keys().all(|key| !key.starts_with(SERVICE_PREFIX)));
        assert!(values.keys().all(|key| !key.starts_with(MIDDLEWARE_PREFIX)));
        assert!(values.contains_key("traefik/http/routers/app/rule"));
        assert_eq!(
            values.get(APPLIED_KEY),
            Some(&fingerprint(&[]).into_bytes())
        );
    }

    #[tokio::test]
    async fn stale_leader_cannot_mutate_the_policy() {
        let store = FakePolicyStore::default();
        store.reject_fenced_writes.store(true, Ordering::SeqCst);
        let fence = LeadershipFence {
            election_key: b"/maetro/cluster/leader/stale".to_vec(),
            create_revision: 7,
        };
        let error = reconcile_policy(&store, &["203.0.113.9".to_string()], Some(&fence))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("leadership fence rejected"));
        assert!(
            !store
                .values
                .lock()
                .unwrap()
                .keys()
                .any(|key| key.starts_with(SERVICE_PREFIX))
        );
    }

    #[tokio::test]
    async fn applied_fingerprint_makes_reconciliation_a_noop() {
        let store = FakePolicyStore::default();
        let addresses = vec!["203.0.113.9".to_string()];
        store.seed(APPLIED_KEY, &fingerprint(&addresses));

        reconcile_policy(&store, &addresses, None).await.unwrap();

        assert!(store.phases.lock().unwrap().is_empty());
    }

    #[test]
    fn global_rule_matches_direct_and_forwarded_client_addresses() {
        let rule =
            blocked_ip_matcher(&["203.0.113.9".to_string(), "2001:db8::9".to_string()]).unwrap();
        assert!(rule.contains("ClientIP(`203.0.113.9`)"));
        assert!(rule.contains("ClientIP(`2001:db8::9`)"));
        assert!(rule.contains("HeaderRegexp(`CF-Connecting-IP`"));
        assert!(rule.contains("HeaderRegexp(`X-Forwarded-For`"));
        assert!(!rule.contains("Host("));
    }

    #[test]
    fn internal_labels_cannot_collide_with_service_ids() {
        assert!(ROUTER_LABEL_PREFIX.contains('.'));
        assert!(SERVICE_LABEL.contains('.'));
        assert!(is_internal_router_label(
            "maestro.internal-blocked-deadbeef-0"
        ));
        assert!(!is_internal_router_label("app"));
        assert!(is_internal_service_label("maestro.internal-blocked"));
        assert!(!is_internal_service_label("app"));
    }
}
