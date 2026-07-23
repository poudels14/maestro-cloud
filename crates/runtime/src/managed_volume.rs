use kernel_api::ClusterId;
use sha2::{Digest, Sha256};

use crate::RuntimeError;

pub(crate) fn managed_volume_key(
    cluster_id: &ClusterId,
    name: &str,
) -> Result<String, RuntimeError> {
    validate_name(name)?;
    let mut digest = Sha256::new();
    digest.update(cluster_id.as_str().as_bytes());
    digest.update([0]);
    digest.update(name.as_bytes());
    Ok(format!("sha256-{}", hex::encode(digest.finalize())))
}

fn validate_name(name: &str) -> Result<(), RuntimeError> {
    if name.trim().is_empty() || name.chars().any(char::is_control) {
        Err(RuntimeError::InvalidSpec {
            message: "managed-volume name must be nonempty and contain no control characters"
                .to_owned(),
        })
    } else {
        Ok(())
    }
}
