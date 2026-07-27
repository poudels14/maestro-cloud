use kernel_api::{AssignmentId, DeploymentId, NodeId, ServiceId, WorkloadId};
use node_fabric::proto::identity_client::IdentityClient;
use node_fabric::{WORKLOAD_NODE_DIRECTORY, WORKLOAD_TOKEN_HEADER, WorkloadClaims};
use runtime::{MountAccess, MountSource};
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;

use crate::node_api_files::{
    PreparedNodeApiFiles, cleanup_node_api_directory, list_workload_directories,
    prepare_node_api_files, remove_stale_socket,
};
use crate::node_api_mount::{NodeApiFileSystem, NodeApiMountManager};
use crate::{NodeApiMountError, NodeApiSocketOwner, WorkloadControlAccess};

use super::node_api_support::node_api_services;

#[tokio::test]
async fn credentials_survive_agent_restart_and_are_zeroized_on_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("node-api");
    let owner = current_owner(temporary.path())?;
    let workload_id = workload_id("workload-1");
    let manager = NodeApiMountManager::new(root.clone(), Some(node_api_services()))?;
    let mount = manager
        .ensure(
            &workload_id,
            owner,
            claims("workload-1"),
            WorkloadControlAccess::Allowed,
        )
        .await?;
    assert_eq!(mount.target.to_string_lossy(), WORKLOAD_NODE_DIRECTORY);
    assert_eq!(mount.access, MountAccess::ReadOnly);
    assert_eq!(mount.source, MountSource::HostPath(root.join("workload-1")));

    let directory = root.join("workload-1");
    let token_path = directory.join("node.token");
    let socket_path = directory.join("node.sock");
    let token = std::fs::read_to_string(&token_path)?;
    let token_metadata = MetadataValue::try_from(token.clone())?;
    let token_metadata_for_restart = token_metadata.clone();
    assert_eq!(
        std::fs::metadata(&token_path)?.permissions().mode() & 0o777,
        0o400
    );
    let socket_metadata = std::fs::metadata(&socket_path)?;
    assert_eq!(socket_metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(socket_metadata.uid(), owner.user_id);
    assert_eq!(socket_metadata.gid(), owner.group_id);

    let channel = Endpoint::from_shared(format!("unix:{}", socket_path.display()))?
        .connect()
        .await?;
    let mut request = Request::new(node_fabric::proto::GetIdentityRequest {});
    request
        .metadata_mut()
        .insert(WORKLOAD_TOKEN_HEADER, token_metadata);
    let identity = IdentityClient::new(channel.clone())
        .get_identity(request)
        .await?
        .into_inner();
    assert_eq!(identity.workload_id, "workload-1");
    drop(channel);
    manager.shutdown_all().await?;
    assert!(!socket_path.exists());
    assert_eq!(std::fs::read_to_string(&token_path)?, token);

    let restarted = NodeApiMountManager::new(root.clone(), Some(node_api_services()))?;
    restarted
        .ensure(
            &workload_id,
            owner,
            claims("workload-1"),
            WorkloadControlAccess::Allowed,
        )
        .await?;
    assert_eq!(std::fs::read_to_string(&token_path)?, token);
    let channel = Endpoint::from_shared(format!("unix:{}", socket_path.display()))?
        .connect()
        .await?;
    let mut request = Request::new(node_fabric::proto::GetIdentityRequest {});
    request
        .metadata_mut()
        .insert(WORKLOAD_TOKEN_HEADER, token_metadata_for_restart);
    IdentityClient::new(channel.clone())
        .get_identity(request)
        .await?;
    drop(channel);

    let zeroized_alias = temporary.path().join("token-alias");
    std::fs::hard_link(&token_path, &zeroized_alias)?;
    restarted.cleanup(&workload_id).await?;
    assert!(!directory.exists());
    assert!(std::fs::read(zeroized_alias)?.iter().all(|byte| *byte == 0));
    Ok(())
}

#[tokio::test]
async fn binding_mutation_and_unsafe_stale_socket_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("node-api");
    let owner = current_owner(temporary.path())?;
    let workload_id = workload_id("workload-1");
    let manager = NodeApiMountManager::new(root.clone(), Some(node_api_services()))?;
    manager
        .ensure(
            &workload_id,
            owner,
            claims("workload-1"),
            WorkloadControlAccess::Denied,
        )
        .await?;
    let mut changed_claims = claims("workload-1");
    changed_claims
        .labels
        .insert("changed".into(), "true".into());
    assert!(matches!(
        manager
            .ensure(
                &workload_id,
                owner,
                changed_claims,
                WorkloadControlAccess::Denied,
            )
            .await,
        Err(NodeApiMountError::BindingConflict { .. })
    ));
    manager.shutdown_all().await?;

    let socket_path = root.join("workload-1/node.sock");
    std::fs::write(&socket_path, b"not a socket")?;
    assert!(matches!(
        manager
            .ensure(
                &workload_id,
                owner,
                claims("workload-1"),
                WorkloadControlAccess::Denied,
            )
            .await,
        Err(NodeApiMountError::UnsafePath { .. })
    ));
    manager.cleanup(&workload_id).await?;
    Ok(())
}

#[tokio::test]
async fn stale_cleanup_stops_only_inactive_workload_servers()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("node-api");
    let owner = current_owner(temporary.path())?;
    let manager = NodeApiMountManager::new(root.clone(), Some(node_api_services()))?;
    for id in ["workload-1", "workload-2"] {
        manager
            .ensure(
                &workload_id(id),
                owner,
                claims(id),
                WorkloadControlAccess::Denied,
            )
            .await?;
    }

    assert_eq!(
        manager
            .cleanup_stale(&BTreeSet::from(["workload-1".to_owned()]))
            .await?,
        1
    );
    assert!(root.join("workload-1/node.sock").exists());
    assert!(!root.join("workload-2").exists());
    manager.cleanup(&workload_id("workload-1")).await?;
    Ok(())
}

#[tokio::test]
async fn dropping_manager_stops_owned_workload_servers() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("node-api");
    let owner = current_owner(temporary.path())?;
    let workload_id = workload_id("workload-1");
    let socket_path = root.join("workload-1/node.sock");
    let manager = NodeApiMountManager::new(root, Some(node_api_services()))?;
    manager
        .ensure(
            &workload_id,
            owner,
            claims("workload-1"),
            WorkloadControlAccess::Denied,
        )
        .await?;
    tokio::net::UnixStream::connect(&socket_path).await?;

    drop(manager);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if tokio::net::UnixStream::connect(&socket_path).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "node API listener remained active after its manager was dropped",
        )
    })?;
    Ok(())
}

#[tokio::test]
async fn slow_file_preparation_does_not_block_an_unrelated_workload()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("node-api");
    let owner = current_owner(temporary.path())?;
    let blocked_workload = workload_id("workload-1");
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let files = Arc::new(BlockingNodeApiFileSystem {
        blocked_workload: blocked_workload.clone(),
        started_sender,
        release_receiver: Mutex::new(Some(release_receiver)),
    });
    let manager = Arc::new(NodeApiMountManager::with_file_system(
        root,
        Some(node_api_services()),
        files,
    )?);

    let blocked_manager = manager.clone();
    let blocked = tokio::spawn(async move {
        blocked_manager
            .ensure(
                &blocked_workload,
                owner,
                claims("workload-1"),
                WorkloadControlAccess::Denied,
            )
            .await
    });
    tokio::task::spawn_blocking(move || started_receiver.recv()).await??;

    let unrelated = tokio::time::timeout(
        Duration::from_secs(2),
        manager.ensure(
            &workload_id("workload-2"),
            owner,
            claims("workload-2"),
            WorkloadControlAccess::Denied,
        ),
    )
    .await;
    release_sender.send(())?;
    blocked.await??;
    unrelated.map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "unrelated node API preparation waited for another workload",
        )
    })??;
    manager.shutdown_all().await?;
    Ok(())
}

struct BlockingNodeApiFileSystem {
    blocked_workload: WorkloadId,
    started_sender: mpsc::SyncSender<()>,
    release_receiver: Mutex<Option<mpsc::Receiver<()>>>,
}

impl NodeApiFileSystem for BlockingNodeApiFileSystem {
    fn prepare(
        &self,
        root: &std::path::Path,
        workload_id: &WorkloadId,
        owner: NodeApiSocketOwner,
    ) -> Result<PreparedNodeApiFiles, NodeApiMountError> {
        if workload_id == &self.blocked_workload {
            self.started_sender
                .send(())
                .map_err(|error| task_failure(error.to_string()))?;
            self.release_receiver
                .lock()
                .map_err(|error| task_failure(error.to_string()))?
                .take()
                .ok_or_else(|| task_failure("release receiver was already consumed"))?
                .recv()
                .map_err(|error| task_failure(error.to_string()))?;
        }
        prepare_node_api_files(root, workload_id, owner)
    }

    fn remove_stale_socket(&self, socket_path: &std::path::Path) -> Result<(), NodeApiMountError> {
        remove_stale_socket(socket_path)
    }

    fn cleanup(&self, directory: &std::path::Path) -> Result<(), NodeApiMountError> {
        cleanup_node_api_directory(directory)
    }

    fn list_workloads(&self, root: &std::path::Path) -> Result<Vec<WorkloadId>, NodeApiMountError> {
        list_workload_directories(root)
    }
}

fn task_failure(message: impl Into<String>) -> NodeApiMountError {
    NodeApiMountError::Task {
        message: message.into(),
    }
}

fn current_owner(path: &std::path::Path) -> std::io::Result<NodeApiSocketOwner> {
    let metadata = std::fs::metadata(path)?;
    Ok(NodeApiSocketOwner {
        user_id: metadata.uid(),
        group_id: metadata.gid(),
    })
}

fn workload_id(value: &str) -> WorkloadId {
    WorkloadId::new(value).expect("workload id")
}

fn claims(workload: &str) -> WorkloadClaims {
    WorkloadClaims {
        workload_id: workload_id(workload),
        assignment_id: AssignmentId::new(workload).expect("assignment id"),
        node_id: NodeId::new("node-1").expect("node id"),
        service_id: ServiceId::new("api").expect("service id"),
        deployment_id: DeploymentId::new("deployment-1").expect("deployment id"),
        labels: BTreeMap::new(),
    }
}
