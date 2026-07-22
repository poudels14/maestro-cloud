use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::ClusterId;
use kernel_controller::FencedStore;
use kernel_store::{Keyspace, Mutation, StoreKey, Transaction, TransactionOutcome};

use crate::{
    IngressBackendError, TraefikBlocklistConfig, TraefikCutover, TraefikProvider, TraefikStage,
};

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

    async fn commit(
        &self,
        action: &str,
        mutations: Vec<Mutation>,
    ) -> Result<(), IngressBackendError> {
        let outcome = self
            .store
            .txn(Transaction {
                compares: Vec::new(),
                mutations,
            })
            .await
            .map_err(|error| backend_error(action, error))?;
        if outcome == TransactionOutcome::Conflict {
            Err(IngressBackendError::new(format!(
                "Traefik {action} conflicted while the leadership fence was active"
            )))
        } else {
            Ok(())
        }
    }

    async fn list_owned(&self, relative: &str) -> Result<Vec<StoreKey>, IngressBackendError> {
        let (parent, _) = relative.rsplit_once('/').ok_or_else(|| {
            IngressBackendError::new("Traefik owned prefix has no parent directory")
        })?;
        let owned = self
            .keyspace
            .traefik_entry(relative)
            .map_err(|error| backend_error("owned prefix validation", error))?;
        let parent = self
            .keyspace
            .traefik_prefix(parent)
            .map_err(|error| backend_error("owned parent validation", error))?;
        Ok(self
            .store
            .list(&parent)
            .await
            .map_err(|error| backend_error("list owned Traefik entries", error))?
            .values
            .into_iter()
            .map(|stored| stored.key)
            .filter(|key| key.as_str().starts_with(owned.as_str()))
            .collect())
    }
}

#[async_trait]
impl TraefikProvider for StoreTraefikProvider {
    async fn stage(&self, stage: &TraefikStage) -> Result<(), IngressBackendError> {
        let mutations = stage
            .entries
            .iter()
            .map(|(relative, value)| {
                self.keyspace
                    .traefik_entry(relative)
                    .map(|key| Mutation::Put {
                        key,
                        value: value.as_bytes().to_vec(),
                        session: None,
                    })
                    .map_err(|error| backend_error("stage path validation", error))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.commit("generation staging", mutations).await
    }

    async fn cutover(&self, cutover: &TraefikCutover) -> Result<(), IngressBackendError> {
        let router_prefix = self
            .keyspace
            .traefik_entry(&cutover.router_prefix)
            .map_err(|error| backend_error("router prefix validation", error))?;
        let desired = cutover
            .routers
            .iter()
            .map(|(relative, value)| {
                self.keyspace
                    .traefik_entry(relative)
                    .map(|key| (key, value.as_bytes().to_vec()))
                    .map_err(|error| backend_error("router path validation", error))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if desired
            .keys()
            .any(|key| !key.as_str().starts_with(router_prefix.as_str()))
        {
            return Err(IngressBackendError::new(
                "Traefik cutover contains a router outside its owned prefix",
            ));
        }

        let mut deletes = self
            .list_owned(&cutover.router_prefix)
            .await?
            .into_iter()
            .filter(|key| !desired.contains_key(key))
            .collect::<BTreeSet<StoreKey>>();
        for relative in &cutover.remove_prefixes {
            deletes.extend(self.list_owned(relative).await?);
        }

        let mut mutations = deletes
            .into_iter()
            .map(|key| Mutation::Delete { key })
            .collect::<Vec<_>>();
        mutations.extend(desired.into_iter().map(|(key, value)| Mutation::Put {
            key,
            value,
            session: None,
        }));
        self.commit("router cutover", mutations).await
    }

    async fn replace_blocklist(
        &self,
        config: &TraefikBlocklistConfig,
    ) -> Result<(), IngressBackendError> {
        let owned_prefixes = config
            .owned_prefixes
            .iter()
            .map(|relative| {
                self.keyspace
                    .traefik_entry(relative)
                    .map_err(|error| backend_error("blocklist prefix validation", error))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let desired = config
            .entries
            .iter()
            .map(|(relative, value)| {
                self.keyspace
                    .traefik_entry(relative)
                    .map(|key| (key, value.as_bytes().to_vec()))
                    .map_err(|error| backend_error("blocklist path validation", error))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if desired.keys().any(|key| {
            owned_prefixes
                .iter()
                .all(|prefix| !key.as_str().starts_with(prefix.as_str()))
        }) {
            return Err(IngressBackendError::new(
                "Traefik blocklist contains an entry outside its owned prefixes",
            ));
        }
        let mut deletes = BTreeSet::new();
        for prefix in &config.owned_prefixes {
            deletes.extend(
                self.list_owned(prefix)
                    .await?
                    .into_iter()
                    .filter(|key| !desired.contains_key(key)),
            );
        }
        let mut mutations = deletes
            .into_iter()
            .map(|key| Mutation::Delete { key })
            .collect::<Vec<_>>();
        mutations.extend(desired.into_iter().map(|(key, value)| Mutation::Put {
            key,
            value,
            session: None,
        }));
        self.commit("blocklist replacement", mutations).await
    }
}

fn backend_error(action: &str, error: impl std::fmt::Display) -> IngressBackendError {
    IngressBackendError::new(format!("failed to {action}: {error}"))
}
