use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, Generation, NodeId, NodeInstanceId, Object, ObjectMeta, ResourceKind, ResourceName,
    ResourceRevision,
};
use kernel_store::{
    CasOutcome, Compare, DeleteRequest, EtcdStore, ExpectedVersion, Keyspace, Mutation, PutRequest,
    Store, TokioClock, Transaction, TransactionOutcome,
};
use serde::{Deserialize, Serialize};

use crate::{
    Action, Backoff, ControllerError, ControllerRuntime, FencedStore, LeaderElector,
    LeaderIdentity, LeadershipLease, ReconcileContext, ReconcileError, Reconciler, RuntimeConfig,
    StoreLeaderElector,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToySpec {
    desired: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToyStatus {
    reconciliations: u32,
    last_leader: Option<String>,
}

struct ToyReconciler {
    leader: String,
    key: kernel_store::StoreKey,
}

#[async_trait]
impl Reconciler for ToyReconciler {
    type Id = ResourceName;
    type Spec = ToySpec;
    type Status = ToyStatus;

    const KIND: &'static str = "Toy";

    async fn reconcile(
        &self,
        mut resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        resource.status.reconciliations = resource.status.reconciliations.saturating_add(1);
        resource.status.last_leader = Some(self.leader.clone());
        let key = self.key.clone();
        let outcome = context
            .store()
            .txn(Transaction {
                compares: vec![Compare {
                    key: key.clone(),
                    expected: ExpectedVersion::Exact(context.observed_version()),
                }],
                mutations: vec![Mutation::Put {
                    key,
                    value: serde_json::to_vec(&resource).map_err(|error| {
                        ControllerError::Contract {
                            message: format!("toy resource could not be encoded: {error}"),
                        }
                    })?,
                    session: None,
                }],
            })
            .await?;
        match outcome {
            TransactionOutcome::Applied { .. } => Ok(Action::Done),
            TransactionOutcome::Conflict => Err(ReconcileError::Retryable {
                message: "toy resource changed during reconcile".to_string(),
            }),
        }
    }
}

#[tokio::test]
#[ignore = "requires three distinct MAESTRO_ETCD_ENDPOINTS members"]
async fn toy_operator_reconciles_across_leader_failover_on_three_member_etcd()
-> Result<(), Box<dyn std::error::Error>> {
    let endpoints = std::env::var("MAESTRO_ETCD_ENDPOINTS")
        .map_err(|_| "MAESTRO_ETCD_ENDPOINTS must contain three comma-separated endpoints")?
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    if endpoints.len() != 3 {
        return Err("failover scenario requires exactly three distinct etcd endpoints".into());
    }
    let store: Arc<dyn Store> = Arc::new(EtcdStore::connect(endpoints).await?);
    cleanup(store.as_ref()).await?;
    let keys = Keyspace::new(&cluster_id()?);
    let leader_key = keys.leader();
    let kind = ResourceKind::new("Toy")?;
    let resource_key = toy_key()?;
    let created = store
        .put_cas(PutRequest {
            key: resource_key.clone(),
            value: serde_json::to_vec(&toy_resource()?)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(created, CasOutcome::Applied(_)));

    let elector = StoreLeaderElector::new(store.clone(), leader_key.clone());
    let first = campaign(&elector, "node-1", "instance-1").await?;
    let stale = Arc::new(FencedStore::new(
        store.clone(),
        leader_key.clone(),
        first.token().clone(),
    ));
    reconcile_once(stale.clone(), &keys, &kind, "node-1/instance-1").await?;
    assert_status(store.as_ref(), 1, "node-1/instance-1").await?;

    first.resign().await?;
    let second = campaign(&elector, "node-2", "instance-2").await?;
    let stale_marker = keys.resource(&kind, &ResourceName::new("stale-write")?);
    assert_eq!(
        stale
            .txn(Transaction {
                compares: Vec::new(),
                mutations: vec![Mutation::Put {
                    key: stale_marker.clone(),
                    value: b"must-not-commit".to_vec(),
                    session: None,
                }],
            })
            .await,
        Err(ControllerError::LeadershipLost)
    );
    assert!(store.get(&stale_marker).await?.is_none());

    let successor = Arc::new(FencedStore::new(
        store.clone(),
        leader_key,
        second.token().clone(),
    ));
    reconcile_once(successor, &keys, &kind, "node-2/instance-2").await?;
    assert_status(store.as_ref(), 2, "node-2/instance-2").await?;
    second.resign().await?;
    cleanup(store.as_ref()).await?;
    Ok(())
}

async fn campaign(
    elector: &StoreLeaderElector,
    node_id: &str,
    instance_id: &str,
) -> Result<Box<dyn LeadershipLease>, Box<dyn std::error::Error>> {
    elector
        .campaign(
            LeaderIdentity {
                node_id: NodeId::new(node_id)?,
                instance_id: NodeInstanceId::new(instance_id)?,
            },
            Duration::from_secs(15),
        )
        .await?
        .ok_or_else(|| "candidate did not acquire vacant leadership".into())
}

async fn reconcile_once(
    fenced: Arc<FencedStore>,
    keys: &Keyspace,
    kind: &ResourceKind,
    leader: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(TokioClock::new());
    let runtime = ControllerRuntime::new(
        Arc::new(ToyReconciler {
            leader: leader.to_string(),
            key: toy_key()?,
        }),
        keys.resource_kind(kind),
        fenced,
        clock,
        RuntimeConfig::new(
            Duration::from_secs(30),
            Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
        )?,
    );
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    Ok(())
}

async fn assert_status(
    store: &dyn Store,
    reconciliations: u32,
    leader: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let stored = store
        .get(&toy_key()?)
        .await?
        .ok_or("toy resource disappeared")?;
    let resource: Object<ResourceName, ToySpec, ToyStatus> = serde_json::from_slice(&stored.value)?;
    assert_eq!(resource.status.reconciliations, reconciliations);
    assert_eq!(resource.status.last_leader.as_deref(), Some(leader));
    Ok(())
}

async fn cleanup(store: &dyn Store) -> Result<(), Box<dyn std::error::Error>> {
    let keys = Keyspace::new(&cluster_id()?);
    for key in [toy_key()?, keys.leader()] {
        if let Some(value) = store.get(&key).await? {
            let _ = store
                .delete_cas(DeleteRequest {
                    key,
                    expected: value.version,
                })
                .await?;
        }
    }
    Ok(())
}

fn cluster_id() -> Result<ClusterId, kernel_api::InvalidIdentifier> {
    ClusterId::new("toy-failover")
}

fn toy_key() -> Result<kernel_store::StoreKey, kernel_api::InvalidIdentifier> {
    let keys = Keyspace::new(&cluster_id()?);
    Ok(keys.resource(&ResourceKind::new("Toy")?, &ResourceName::new("sample")?))
}

fn toy_resource() -> Result<Object<ResourceName, ToySpec, ToyStatus>, kernel_api::InvalidIdentifier>
{
    Ok(Object {
        meta: ObjectMeta {
            id: ResourceName::new("sample")?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation::default(),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: ToySpec {
            desired: "ready".to_string(),
        },
        status: ToyStatus {
            reconciliations: 0,
            last_leader: None,
        },
    })
}
