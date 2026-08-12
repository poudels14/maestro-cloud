use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::ClusterId;
use kernel_controller::FencedStore;
use kernel_store::{
    Keyspace, Mutation, StoreKey, StorePrefix, TRANSACTION_OPERATION_LIMIT, Transaction,
    TransactionOutcome,
};

use crate::{
    IngressBackendError, TraefikBlocklistConfig, TraefikCutover, TraefikProvider, TraefikStage,
};

const PROVIDER_READY_ENTRY: &str = "http/middlewares/maestro.internal-provider-ready/headers/customRequestHeaders/\
     X-Maestro-Provider-Ready";
const FENCED_STORE_COMPARE_COUNT: usize = 1;
const MAXIMUM_MUTATIONS_PER_TRANSACTION: usize =
    TRANSACTION_OPERATION_LIMIT - FENCED_STORE_COMPARE_COUNT;

#[derive(Debug, Clone, Copy)]
enum Namespace {
    Canonical,
    Provider,
}

impl Namespace {
    const fn label(self) -> &'static str {
        match self {
            Self::Canonical => "canonical mirror",
            Self::Provider => "live provider",
        }
    }
}

/// Fenced persistence adapter for Traefik's cluster-scoped dynamic provider.
pub struct StoreTraefikProvider {
    store: Arc<FencedStore>,
    keyspace: Keyspace,
}

impl StoreTraefikProvider {
    /// Binds provider writes to one exact controller leadership term.
    pub fn new(cluster_id: ClusterId, store: Arc<FencedStore>) -> Self {
        Self {
            store,
            keyspace: Keyspace::new(&cluster_id),
        }
    }

    /// Creates valid inert provider state so Traefik can watch an otherwise empty root.
    pub async fn ensure_watchable_root(&self) -> Result<(), IngressBackendError> {
        self.replace_exact(
            "provider root initialization",
            &[PROVIDER_READY_ENTRY.to_string()],
            &BTreeMap::from([(PROVIDER_READY_ENTRY.to_string(), "true".to_string())]),
        )
        .await
    }

    async fn commit(
        &self,
        action: &str,
        namespace: Namespace,
        mutations: Vec<Mutation>,
    ) -> Result<(), IngressBackendError> {
        if mutations.is_empty() {
            return Ok(());
        }
        let outcome = self
            .store
            .txn(Transaction {
                compares: Vec::new(),
                mutations,
            })
            .await
            .map_err(|error| {
                backend_error(&format!("{action} in the {}", namespace.label()), error)
            })?;
        if outcome == TransactionOutcome::Conflict {
            Err(IngressBackendError::new(format!(
                "Traefik {action} in the {} conflicted while the leadership fence was active",
                namespace.label()
            )))
        } else {
            Ok(())
        }
    }

    async fn list_owned(
        &self,
        namespace: Namespace,
        relative: &str,
    ) -> Result<BTreeMap<StoreKey, Vec<u8>>, IngressBackendError> {
        let (parent, _) = relative.rsplit_once('/').ok_or_else(|| {
            IngressBackendError::new("Traefik owned prefix has no parent directory")
        })?;
        let owned = self.entry(namespace, relative)?;
        let parent = self.prefix(namespace, parent)?;
        Ok(self
            .store
            .list(&parent)
            .await
            .map_err(|error| backend_error("list owned Traefik entries", error))?
            .values
            .into_iter()
            .filter(|stored| stored.key.as_str().starts_with(owned.as_str()))
            .map(|stored| (stored.key, stored.value))
            .collect())
    }

    fn entry(&self, namespace: Namespace, relative: &str) -> Result<StoreKey, IngressBackendError> {
        match namespace {
            Namespace::Canonical => self.keyspace.traefik_entry(relative),
            Namespace::Provider => self.keyspace.traefik_provider_entry(relative),
        }
        .map_err(|error| backend_error("provider path validation", error))
    }

    fn prefix(
        &self,
        namespace: Namespace,
        relative: &str,
    ) -> Result<StorePrefix, IngressBackendError> {
        match namespace {
            Namespace::Canonical => self.keyspace.traefik_prefix(relative),
            Namespace::Provider => self.keyspace.traefik_provider_prefix(relative),
        }
        .map_err(|error| backend_error("provider prefix validation", error))
    }

    async fn plan_exact_replacement(
        &self,
        namespace: Namespace,
        owned_prefixes: &[String],
        desired: &BTreeMap<String, String>,
    ) -> Result<Vec<Mutation>, IngressBackendError> {
        let mut current = BTreeMap::new();
        for prefix in owned_prefixes {
            current.extend(self.list_owned(namespace, prefix).await?);
        }
        let desired = desired
            .iter()
            .map(|(relative, value)| {
                self.entry(namespace, relative)
                    .map(|key| (key, value.as_bytes().to_vec()))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let mut mutations = current
            .keys()
            .filter(|key| !desired.contains_key(*key))
            .cloned()
            .map(|key| Mutation::Delete { key })
            .collect::<Vec<_>>();
        mutations.extend(
            desired
                .into_iter()
                .filter(|(key, value)| current.get(key) != Some(value))
                .map(|(key, value)| Mutation::Put {
                    key,
                    value,
                    session: None,
                }),
        );
        Ok(mutations)
    }

    fn validate_transaction_size(
        &self,
        action: &str,
        namespace: Namespace,
        mutations: &[Mutation],
    ) -> Result<(), IngressBackendError> {
        if mutations.len() <= MAXIMUM_MUTATIONS_PER_TRANSACTION {
            Ok(())
        } else {
            Err(IngressBackendError::terminal(
                "TraefikConfigurationTooLarge",
                format!(
                    "Traefik {action} requires {} mutations in the {} transaction; maximum is {}",
                    mutations.len(),
                    namespace.label(),
                    MAXIMUM_MUTATIONS_PER_TRANSACTION
                ),
            ))
        }
    }

    async fn replace_exact(
        &self,
        action: &str,
        owned_prefixes: &[String],
        desired: &BTreeMap<String, String>,
    ) -> Result<(), IngressBackendError> {
        if desired
            .keys()
            .any(|key| owned_prefixes.iter().all(|prefix| !key.starts_with(prefix)))
        {
            return Err(IngressBackendError::new(format!(
                "Traefik {action} contains an entry outside its owned prefixes"
            )));
        }
        let provider = self
            .plan_exact_replacement(Namespace::Provider, owned_prefixes, desired)
            .await?;
        let canonical = self
            .plan_exact_replacement(Namespace::Canonical, owned_prefixes, desired)
            .await?;
        self.validate_transaction_size(action, Namespace::Provider, &provider)?;
        self.validate_transaction_size(action, Namespace::Canonical, &canonical)?;

        // Traefik reads the slashless provider namespace. Keep its replacement
        // atomic, then converge the canonical shadow in a separate transaction
        // so mirroring does not double the live transaction's operation count.
        self.commit(action, Namespace::Provider, provider).await?;
        self.commit(action, Namespace::Canonical, canonical).await
    }
}

#[async_trait]
impl TraefikProvider for StoreTraefikProvider {
    async fn stage(&self, stage: &TraefikStage) -> Result<(), IngressBackendError> {
        self.replace_exact(
            "generation staging",
            std::slice::from_ref(&stage.service_prefix),
            &stage.entries,
        )
        .await
    }

    async fn cutover(&self, cutover: &TraefikCutover) -> Result<(), IngressBackendError> {
        let mut owned = vec![cutover.router_prefix.clone()];
        owned.extend(cutover.remove_prefixes.iter().cloned());
        self.replace_exact("router cutover", &owned, &cutover.routers)
            .await
    }

    async fn replace_blocklist(
        &self,
        config: &TraefikBlocklistConfig,
    ) -> Result<(), IngressBackendError> {
        self.replace_exact(
            "blocklist replacement",
            &config.owned_prefixes,
            &config.entries,
        )
        .await
    }
}

fn backend_error(action: &str, error: impl std::fmt::Display) -> IngressBackendError {
    IngressBackendError::new(format!("failed to {action}: {error}"))
}
