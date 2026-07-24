use std::path::Path;

use kernel_api::{ClusterId, NodeId};

use crate::StoreProviderError;
use crate::embedded_etcd::{
    LOCAL_STATE_FORMAT_VERSION, LocalInitialization, LocalProviderState, write_new_state,
};

/// Binds an etcdutl-restored member directory to the embedded provider.
///
/// The caller owns native snapshot validation and must invoke this only inside
/// a private staging directory before atomically installing the provider tree.
pub fn initialize_restored_etcd_member(
    provider_directory: &Path,
    cluster_id: ClusterId,
    node_id: NodeId,
    source_sha256: &str,
) -> Result<(), StoreProviderError> {
    if !provider_directory.is_absolute() {
        return Err(StoreProviderError::InvalidConfiguration {
            reason: "restored store provider directory must be absolute".to_owned(),
        });
    }
    if source_sha256.len() != 64
        || !source_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StoreProviderError::InvalidConfiguration {
            reason: "restored store source digest must be lowercase SHA-256".to_owned(),
        });
    }
    if !provider_directory.join("data/member").is_dir() {
        return Err(StoreProviderError::UnsafeRecovery {
            reason: "restored member data is absent".to_owned(),
        });
    }
    write_new_state(
        &provider_directory.join("provider-state.json"),
        &LocalProviderState {
            format_version: LOCAL_STATE_FORMAT_VERSION,
            cluster_id,
            node_id,
            initialization: LocalInitialization::Restored(source_sha256.to_owned()),
        },
    )
}
