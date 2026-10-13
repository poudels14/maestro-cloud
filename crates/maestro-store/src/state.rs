use std::path::Path;

use maestro_core::{
    Assignment, Deployment, DeploymentId, DeploymentStatus, NodeId, NodeRecord, ServiceId,
    ServiceSpec,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::error::StoreError;
use crate::mutation::{Change, ChangeKey, Mutation};

const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
const NODES: TableDefinition<u64, &[u8]> = TableDefinition::new("nodes");
const SERVICES: TableDefinition<&str, &[u8]> = TableDefinition::new("services");
const DEPLOYMENTS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("deployments");
const DEPLOYMENTS_BY_STATUS: TableDefinition<(u8, &str, &str), ()> =
    TableDefinition::new("deployments_by_status");
const ASSIGNMENTS: TableDefinition<(u64, &str), &[u8]> = TableDefinition::new("assignments");
const RAFT_META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const VERSION_KEY: &str = "version";
const KEY_MAX: &str = "\u{10FFFF}";
const CHANGE_BUFFER: usize = 1024;

type Result<T> = std::result::Result<T, StoreError>;

pub struct StateMachine {
    database: Database,
    changes: broadcast::Sender<Change>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct StateExport {
    version: u64,
    services: Vec<(ServiceId, ServiceSpec)>,
    deployments: Vec<Deployment>,
    nodes: Vec<NodeRecord>,
    assignments: Vec<Assignment>,
}

impl StateMachine {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let database = Database::create(path)?;
        let transaction = database.begin_write()?;
        create_tables(&transaction)?;
        transaction.commit()?;
        let (changes, _) = broadcast::channel(CHANGE_BUFFER);
        Ok(Self { database, changes })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Change> {
        self.changes.subscribe()
    }

    pub fn apply(&self, mutations: &[Mutation]) -> Result<u64> {
        self.apply_recorded(mutations, |_| Ok(()))
    }

    pub(crate) fn apply_recorded(
        &self,
        mutations: &[Mutation],
        record: impl FnOnce(&WriteTransaction) -> Result<()>,
    ) -> Result<u64> {
        let transaction = self.database.begin_write()?;
        let keys = mutations
            .iter()
            .map(|mutation| apply_mutation(&transaction, mutation))
            .collect::<Result<Vec<_>>>()?
            .concat();
        let version = {
            let mut meta = transaction.open_table(META)?;
            let next = meta.get(VERSION_KEY)?.map_or(0, |guard| guard.value()) + 1;
            meta.insert(VERSION_KEY, next)?;
            next
        };
        record(&transaction)?;
        transaction.commit()?;
        let _ = self.changes.send(Change { version, keys });
        Ok(version)
    }

    pub(crate) fn record(
        &self,
        record: impl FnOnce(&WriteTransaction) -> Result<()>,
    ) -> Result<()> {
        let transaction = self.database.begin_write()?;
        record(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn raft_meta<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(RAFT_META)?;
        table
            .get(key)?
            .map(|guard| decode(guard.value()))
            .transpose()
    }

    pub(crate) fn export(&self) -> Result<StateExport> {
        let deployments = self
            .services()?
            .iter()
            .map(|(service_id, _)| self.deployments(service_id))
            .collect::<Result<Vec<_>>>()?
            .concat();
        let assignments = self
            .nodes()?
            .iter()
            .map(|node| self.assignments(node.id))
            .collect::<Result<Vec<_>>>()?
            .concat();
        Ok(StateExport {
            version: self.version()?,
            services: self.services()?,
            deployments,
            nodes: self.nodes()?,
            assignments,
        })
    }

    pub(crate) fn import(
        &self,
        export: &StateExport,
        record: impl FnOnce(&WriteTransaction) -> Result<()>,
    ) -> Result<()> {
        let transaction = self.database.begin_write()?;
        delete_tables(&transaction)?;
        create_tables(&transaction)?;
        transaction
            .open_table(META)?
            .insert(VERSION_KEY, export.version)?;
        export.services.iter().try_for_each(|(id, spec)| {
            apply_mutation(
                &transaction,
                &Mutation::PutService {
                    id: id.clone(),
                    spec: spec.clone(),
                },
            )
            .map(|_| ())
        })?;
        export
            .deployments
            .iter()
            .try_for_each(|deployment| write_deployment(&transaction, None, deployment))?;
        export.nodes.iter().try_for_each(|node| {
            apply_mutation(&transaction, &Mutation::PutNode { node: node.clone() }).map(|_| ())
        })?;
        export.assignments.iter().try_for_each(|assignment| {
            let encoded = encode(assignment)?;
            transaction.open_table(ASSIGNMENTS)?.insert(
                (assignment.node_id.0, assignment.replica_id.as_str()),
                encoded.as_slice(),
            )?;
            Ok::<_, StoreError>(())
        })?;
        record(&transaction)?;
        transaction.commit()?;
        let _ = self.changes.send(Change {
            version: export.version,
            keys: vec![ChangeKey::Everything],
        });
        Ok(())
    }

    pub fn version(&self) -> Result<u64> {
        let transaction = self.database.begin_read()?;
        let meta = transaction.open_table(META)?;
        Ok(meta.get(VERSION_KEY)?.map_or(0, |guard| guard.value()))
    }

    pub fn service(&self, id: &ServiceId) -> Result<Option<ServiceSpec>> {
        let transaction = self.database.begin_read()?;
        let services = transaction.open_table(SERVICES)?;
        services
            .get(id.as_str())?
            .map(|guard| decode(guard.value()))
            .transpose()
    }

    pub fn services(&self) -> Result<Vec<(ServiceId, ServiceSpec)>> {
        let transaction = self.database.begin_read()?;
        let services = transaction.open_table(SERVICES)?;
        services
            .iter()?
            .map(|entry| {
                let (key, value) = entry?;
                Ok((ServiceId::new(key.value()), decode(value.value())?))
            })
            .collect()
    }

    pub fn deployment(
        &self,
        service_id: &ServiceId,
        deployment_id: &DeploymentId,
    ) -> Result<Option<Deployment>> {
        let transaction = self.database.begin_read()?;
        let deployments = transaction.open_table(DEPLOYMENTS)?;
        deployments
            .get((service_id.as_str(), deployment_id.as_str()))?
            .map(|guard| decode(guard.value()))
            .transpose()
    }

    pub fn deployments(&self, service_id: &ServiceId) -> Result<Vec<Deployment>> {
        let transaction = self.database.begin_read()?;
        let deployments = transaction.open_table(DEPLOYMENTS)?;
        deployments
            .range((service_id.as_str(), "")..=(service_id.as_str(), KEY_MAX))?
            .map(|entry| decode(entry?.1.value()))
            .collect()
    }

    pub fn deployments_with_status(&self, status: DeploymentStatus) -> Result<Vec<Deployment>> {
        let transaction = self.database.begin_read()?;
        let index = transaction.open_table(DEPLOYMENTS_BY_STATUS)?;
        let deployments = transaction.open_table(DEPLOYMENTS)?;
        let code = status_code(status);
        index
            .range((code, "", "")..=(code, KEY_MAX, KEY_MAX))?
            .map(|entry| {
                let (key, _) = entry?;
                let (_, service_id, deployment_id) = key.value();
                let stored = deployments.get((service_id, deployment_id))?;
                stored.map(|guard| decode(guard.value())).transpose()
            })
            .filter_map(Result::transpose)
            .collect()
    }

    pub fn node(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        let transaction = self.database.begin_read()?;
        let nodes = transaction.open_table(NODES)?;
        nodes
            .get(id.0)?
            .map(|guard| decode(guard.value()))
            .transpose()
    }

    pub fn nodes(&self) -> Result<Vec<NodeRecord>> {
        let transaction = self.database.begin_read()?;
        let nodes = transaction.open_table(NODES)?;
        nodes
            .iter()?
            .map(|entry| decode(entry?.1.value()))
            .collect()
    }

    pub fn assignments(&self, node_id: NodeId) -> Result<Vec<Assignment>> {
        let transaction = self.database.begin_read()?;
        let assignments = transaction.open_table(ASSIGNMENTS)?;
        assignments
            .range((node_id.0, "")..=(node_id.0, KEY_MAX))?
            .map(|entry| decode(entry?.1.value()))
            .collect()
    }
}

fn create_tables(transaction: &WriteTransaction) -> Result<()> {
    transaction.open_table(META)?;
    transaction.open_table(NODES)?;
    transaction.open_table(SERVICES)?;
    transaction.open_table(DEPLOYMENTS)?;
    transaction.open_table(DEPLOYMENTS_BY_STATUS)?;
    transaction.open_table(ASSIGNMENTS)?;
    transaction.open_table(RAFT_META)?;
    Ok(())
}

fn delete_tables(transaction: &WriteTransaction) -> Result<()> {
    transaction.delete_table(META)?;
    transaction.delete_table(NODES)?;
    transaction.delete_table(SERVICES)?;
    transaction.delete_table(DEPLOYMENTS)?;
    transaction.delete_table(DEPLOYMENTS_BY_STATUS)?;
    transaction.delete_table(ASSIGNMENTS)?;
    transaction.delete_table(RAFT_META)?;
    Ok(())
}

pub(crate) fn write_raft_meta<T: Serialize>(
    transaction: &WriteTransaction,
    key: &str,
    value: &T,
) -> Result<()> {
    let encoded = encode(value)?;
    transaction
        .open_table(RAFT_META)?
        .insert(key, encoded.as_slice())?;
    Ok(())
}

fn apply_mutation(transaction: &WriteTransaction, mutation: &Mutation) -> Result<Vec<ChangeKey>> {
    match mutation {
        Mutation::PutService { id, spec } => {
            transaction
                .open_table(SERVICES)?
                .insert(id.as_str(), encode(spec)?.as_slice())?;
            Ok(vec![ChangeKey::Service { id: id.clone() }])
        }
        Mutation::DeleteService { id } => delete_service(transaction, id),
        Mutation::CreateDeployment { deployment } => create_deployment(transaction, deployment),
        Mutation::SetDeploymentStatus {
            service_id,
            deployment_id,
            status,
            image,
            failure,
            at_ms,
        } => {
            let existing = read_deployment(transaction, service_id, deployment_id)?;
            let updated = Deployment {
                status: *status,
                image: image.clone().or(existing.image.clone()),
                failure: failure.clone(),
                updated_at_ms: *at_ms,
                ..existing.clone()
            };
            write_deployment(transaction, Some(&existing), &updated)?;
            Ok(vec![deployment_key(&updated)])
        }
        Mutation::PruneDeployments { service_id, keep } => {
            prune_deployments(transaction, service_id, *keep)
        }
        Mutation::PutNode { node } => {
            transaction
                .open_table(NODES)?
                .insert(node.id.0, encode(node)?.as_slice())?;
            Ok(vec![ChangeKey::Node { id: node.id }])
        }
        Mutation::RemoveNode { id } => {
            transaction.open_table(NODES)?.remove(id.0)?;
            replace_assignments(transaction, *id, &[])?;
            Ok(vec![
                ChangeKey::Node { id: *id },
                ChangeKey::NodeAssignments { node_id: *id },
            ])
        }
        Mutation::SetNodeAssignments {
            node_id,
            assignments,
        } => {
            replace_assignments(transaction, *node_id, assignments)?;
            Ok(vec![ChangeKey::NodeAssignments { node_id: *node_id }])
        }
    }
}

fn delete_service(transaction: &WriteTransaction, id: &ServiceId) -> Result<Vec<ChangeKey>> {
    transaction.open_table(SERVICES)?.remove(id.as_str())?;
    let deployment_keys = service_deployments(transaction, id)?
        .iter()
        .map(|deployment| {
            remove_deployment(transaction, deployment)?;
            Ok(deployment_key(deployment))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok([vec![ChangeKey::Service { id: id.clone() }], deployment_keys].concat())
}

fn create_deployment(
    transaction: &WriteTransaction,
    deployment: &Deployment,
) -> Result<Vec<ChangeKey>> {
    let service_exists = transaction
        .open_table(SERVICES)?
        .get(deployment.service_id.as_str())?
        .is_some();
    let deployment_exists = transaction
        .open_table(DEPLOYMENTS)?
        .get((deployment.service_id.as_str(), deployment.id.as_str()))?
        .is_some();
    match (service_exists, deployment_exists) {
        (false, _) => Err(StoreError::ServiceNotFound(deployment.service_id.clone())),
        (true, true) => Err(StoreError::DeploymentExists {
            service_id: deployment.service_id.clone(),
            deployment_id: deployment.id.clone(),
        }),
        (true, false) => {
            write_deployment(transaction, None, deployment)?;
            Ok(vec![deployment_key(deployment)])
        }
    }
}

fn prune_deployments(
    transaction: &WriteTransaction,
    service_id: &ServiceId,
    keep: usize,
) -> Result<Vec<ChangeKey>> {
    let mut deployments = service_deployments(transaction, service_id)?;
    deployments.sort_by_key(|deployment| std::cmp::Reverse(deployment.created_at_ms));
    deployments
        .iter()
        .skip(keep)
        .filter(|deployment| deployment.status.is_terminal())
        .map(|deployment| {
            remove_deployment(transaction, deployment)?;
            Ok(deployment_key(deployment))
        })
        .collect()
}

fn read_deployment(
    transaction: &WriteTransaction,
    service_id: &ServiceId,
    deployment_id: &DeploymentId,
) -> Result<Deployment> {
    let deployments = transaction.open_table(DEPLOYMENTS)?;
    let stored = deployments.get((service_id.as_str(), deployment_id.as_str()))?;
    match stored {
        Some(guard) => decode(guard.value()),
        None => Err(StoreError::DeploymentNotFound {
            service_id: service_id.clone(),
            deployment_id: deployment_id.clone(),
        }),
    }
}

fn service_deployments(
    transaction: &WriteTransaction,
    service_id: &ServiceId,
) -> Result<Vec<Deployment>> {
    let deployments = transaction.open_table(DEPLOYMENTS)?;
    deployments
        .range((service_id.as_str(), "")..=(service_id.as_str(), KEY_MAX))?
        .map(|entry| decode(entry?.1.value()))
        .collect()
}

fn write_deployment(
    transaction: &WriteTransaction,
    previous: Option<&Deployment>,
    deployment: &Deployment,
) -> Result<()> {
    let mut index = transaction.open_table(DEPLOYMENTS_BY_STATUS)?;
    if let Some(previous) = previous {
        index.remove(status_index_key(previous))?;
    }
    index.insert(status_index_key(deployment), ())?;
    transaction.open_table(DEPLOYMENTS)?.insert(
        (deployment.service_id.as_str(), deployment.id.as_str()),
        encode(deployment)?.as_slice(),
    )?;
    Ok(())
}

fn remove_deployment(transaction: &WriteTransaction, deployment: &Deployment) -> Result<()> {
    transaction
        .open_table(DEPLOYMENTS_BY_STATUS)?
        .remove(status_index_key(deployment))?;
    transaction
        .open_table(DEPLOYMENTS)?
        .remove((deployment.service_id.as_str(), deployment.id.as_str()))?;
    Ok(())
}

fn replace_assignments(
    transaction: &WriteTransaction,
    node_id: NodeId,
    assignments: &[Assignment],
) -> Result<()> {
    let mut table = transaction.open_table(ASSIGNMENTS)?;
    let existing_keys = table
        .range((node_id.0, "")..=(node_id.0, KEY_MAX))?
        .map(|entry| Ok(entry?.0.value().1.to_string()))
        .collect::<Result<Vec<_>>>()?;
    existing_keys
        .iter()
        .try_for_each(|replica_id| table.remove((node_id.0, replica_id.as_str())).map(|_| ()))?;
    assignments.iter().try_for_each(|assignment| {
        let encoded = encode(assignment)?;
        table.insert(
            (node_id.0, assignment.replica_id.as_str()),
            encoded.as_slice(),
        )?;
        Ok(())
    })
}

fn status_index_key(deployment: &Deployment) -> (u8, &str, &str) {
    (
        status_code(deployment.status),
        deployment.service_id.as_str(),
        deployment.id.as_str(),
    )
}

fn deployment_key(deployment: &Deployment) -> ChangeKey {
    ChangeKey::Deployment {
        service_id: deployment.service_id.clone(),
        deployment_id: deployment.id.clone(),
    }
}

fn status_code(status: DeploymentStatus) -> u8 {
    match status {
        DeploymentStatus::Queued => 0,
        DeploymentStatus::Building => 1,
        DeploymentStatus::Deploying => 2,
        DeploymentStatus::Ready => 3,
        DeploymentStatus::Draining => 4,
        DeploymentStatus::Stopped => 5,
        DeploymentStatus::Failed => 6,
        DeploymentStatus::Cancelled => 7,
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_slice(bytes)?)
}
