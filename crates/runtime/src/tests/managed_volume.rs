use kernel_api::ClusterId;

use crate::RuntimeError;
use crate::managed_volume::managed_volume_key;

#[test]
fn managed_volume_keys_are_stable_cluster_scoped_and_name_sensitive()
-> Result<(), Box<dyn std::error::Error>> {
    let first_cluster = ClusterId::new("cluster-1")?;
    let second_cluster = ClusterId::new("cluster-2")?;
    let first = managed_volume_key(&first_cluster, "data")?;

    assert_eq!(first, managed_volume_key(&first_cluster, "data")?);
    assert_ne!(first, managed_volume_key(&second_cluster, "data")?);
    assert_ne!(first, managed_volume_key(&first_cluster, "cache")?);
    assert_eq!(first.len(), "sha256-".len() + 64);
    assert!(first.starts_with("sha256-"));
    Ok(())
}

#[test]
fn managed_volume_keys_reject_blank_and_control_bearing_names()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("cluster-1")?;
    for name in ["", "  ", "data\nbackup", "data\0backup"] {
        assert!(matches!(
            managed_volume_key(&cluster_id, name),
            Err(RuntimeError::InvalidSpec { .. })
        ));
    }
    Ok(())
}
