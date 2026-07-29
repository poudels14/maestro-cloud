use std::io::Write;
use std::path::{Path, PathBuf};

use cluster::PreviewLaunchConfig;
use kernel_api::{ClusterId, NodeId};

use crate::{DaemonLaunchConfig, load_launch_document};

/// Atomic node-local editor for the protected launch document used on restart.
pub(crate) struct FileLaunchConfigAdmin {
    path: PathBuf,
    cluster_id: ClusterId,
    node_id: NodeId,
    update: tokio::sync::Mutex<()>,
}

impl FileLaunchConfigAdmin {
    pub(crate) fn new(path: PathBuf, config: &DaemonLaunchConfig) -> Self {
        Self {
            path,
            cluster_id: config.cluster.cluster_id.clone(),
            node_id: config.node_id.clone(),
            update: tokio::sync::Mutex::new(()),
        }
    }
}

#[async_trait::async_trait]
impl server::LaunchConfigAdmin for FileLaunchConfigAdmin {
    fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    async fn replace_preview(
        &self,
        preview: PreviewLaunchConfig,
    ) -> Result<bool, server::LaunchConfigAdminError> {
        let _guard = self.update.lock().await;
        let path = self.path.clone();
        let cluster_id = self.cluster_id.clone();
        let node_id = self.node_id.clone();
        tokio::task::spawn_blocking(move || replace_preview(&path, &cluster_id, &node_id, preview))
            .await
            .map_err(|error| {
                server::LaunchConfigAdminError::new(format!(
                    "launch-config update task failed: {error}"
                ))
            })?
            .map_err(|error| server::LaunchConfigAdminError::new(error.to_string()))
    }
}

pub(crate) fn replace_preview(
    path: &Path,
    cluster_id: &ClusterId,
    node_id: &NodeId,
    preview: PreviewLaunchConfig,
) -> Result<bool, LaunchConfigUpdateError> {
    let mut launch = load_launch_document(path)?;
    if &launch.cluster.cluster_id != cluster_id || &launch.node_id != node_id {
        return Err(LaunchConfigUpdateError::TargetMismatch);
    }
    if launch.preview.as_ref() == Some(&preview) {
        return Ok(false);
    }
    launch.preview = Some(preview);
    launch.validate()?;
    replace_document(path, &launch)?;
    Ok(true)
}

fn replace_document(
    path: &Path,
    launch: &DaemonLaunchConfig,
) -> Result<(), LaunchConfigUpdateError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut encoded = serde_json::to_vec_pretty(launch)?;
    encoded.push(b'\n');
    let mut temporary =
        tempfile::NamedTempFile::new_in(parent).map_err(|source| LaunchConfigUpdateError::Io {
            action: "create replacement",
            path: path.to_path_buf(),
            source,
        })?;
    temporary
        .write_all(&encoded)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|source| LaunchConfigUpdateError::Io {
            action: "persist replacement",
            path: path.to_path_buf(),
            source,
        })?;
    temporary
        .persist(path)
        .map_err(|error| LaunchConfigUpdateError::Io {
            action: "install replacement",
            path: path.to_path_buf(),
            source: error.error,
        })?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| LaunchConfigUpdateError::Io {
            action: "sync parent directory for",
            path: path.to_path_buf(),
            source,
        })
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LaunchConfigUpdateError {
    #[error(transparent)]
    InvalidLaunch(#[from] crate::DaemonLaunchError),
    #[error("launch document does not match the requested cluster and node")]
    TargetMismatch,
    #[error("failed to encode launch document: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("failed to {action} launch document `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
