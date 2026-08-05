use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

use crate::containerd_volume::{managed_volume_path, prepare_managed_volumes};
use crate::{MountSource, RuntimeError, WorkloadSpec};

use super::containerd_fixture::container_spec;

#[tokio::test]
async fn managed_volume_directories_are_cluster_scoped_private_and_persistent()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut spec = managed_spec("shared-data");
    let owner = std::fs::metadata(root.path())?;
    let WorkloadSpec::Container(workload) = &mut spec else {
        unreachable!();
    };
    workload.configuration.user = Some(crate::WorkloadUser {
        user_id: owner.uid(),
        group_id: owner.gid(),
    });
    let configuration = spec.configuration();
    prepare_managed_volumes(root.path(), configuration).await?;

    let path = managed_volume_path(
        root.path(),
        &configuration.metadata.cluster_id,
        "shared-data",
    )?;
    assert!(path.is_dir());
    assert_eq!(
        std::fs::metadata(root.path().join("volumes"))?
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&path)?.permissions().mode() & 0o777,
        0o700
    );
    let metadata = std::fs::metadata(&path)?;
    assert_eq!(metadata.uid(), owner.uid());
    assert_eq!(metadata.gid(), owner.gid());
    std::fs::write(path.join("marker"), "persisted")?;

    prepare_managed_volumes(root.path(), configuration).await?;
    assert_eq!(std::fs::read_to_string(path.join("marker"))?, "persisted");
    let other_cluster = kernel_api::ClusterId::new("cluster-2")?;
    assert_ne!(
        path,
        managed_volume_path(root.path(), &other_cluster, "shared-data")?
    );
    Ok(())
}

#[tokio::test]
async fn managed_volume_preparation_rejects_invalid_names_and_symlinked_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let invalid = managed_spec("\n");
    assert!(matches!(
        prepare_managed_volumes(root.path(), invalid.configuration()).await,
        Err(RuntimeError::InvalidSpec { .. })
    ));

    let linked_root = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    symlink(outside.path(), linked_root.path().join("volumes"))?;
    assert!(matches!(
        prepare_managed_volumes(linked_root.path(), managed_spec("data").configuration()).await,
        Err(RuntimeError::Unavailable { .. })
    ));
    assert!(std::fs::read_dir(outside.path())?.next().is_none());
    Ok(())
}

fn managed_spec(name: &str) -> WorkloadSpec {
    let mut spec = container_spec();
    let WorkloadSpec::Container(workload) = &mut spec else {
        unreachable!();
    };
    workload.configuration.mounts = vec![crate::WorkloadMount {
        source: MountSource::ManagedVolume(name.to_owned()),
        target: "/data".into(),
        access: crate::MountAccess::ReadWrite,
    }];
    spec
}
