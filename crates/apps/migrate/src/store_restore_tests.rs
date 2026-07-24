use std::fs;
use std::path::Path;

use kernel_api::NodeId;

use crate::legacy_fixtures::cluster_state_for;
use crate::legacy_node_tests::node_entries;
use crate::{
    LegacySnapshot, LegacyStoreRestoreError, LegacyStoreRestoreOutcome, plan_legacy_store_restore,
    restore_legacy_store, verify_legacy_store_restore,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn store_plan_rekeys_validated_legacy_members_to_node_ids() -> TestResult {
    let plan = three_member_plan()?;

    assert_eq!(
        plan.cluster_id().as_str(),
        "0123456789abcdef0123456789abcdef"
    );
    assert_eq!(plan.members().len(), 3);
    let node_b = plan
        .members()
        .get(&NodeId::new("node-b")?)
        .ok_or("node-b is absent")?;
    assert_eq!(node_b.member_name(), "maestro-node-b");
    assert_eq!(node_b.peer_url(), "https://10.0.0.11:2380");
    Ok(())
}

#[cfg(unix)]
#[test]
fn store_restore_is_atomic_verifiable_and_idempotent() -> TestResult {
    let directory = tempfile::tempdir()?;
    let native_snapshot = directory.path().join("post-migration.db");
    private_file(&native_snapshot, b"native snapshot")?;
    let etcdutl = directory.path().join("etcdutl");
    executable(
        &etcdutl,
        b"#!/bin/sh\n\
          data=''\n\
          while [ \"$#\" -gt 0 ]; do\n\
            if [ \"$1\" = '--data-dir' ]; then shift; data=\"$1\"; fi\n\
            shift\n\
          done\n\
          mkdir -p \"$data/member\"\n\
          printf restored >\"$data/member/snapshot\"\n",
    )?;
    let data_directory = directory.path().join("maestro");
    let plan = one_member_plan()?;
    let node_id = NodeId::new("node-a")?;

    let restored =
        restore_legacy_store(&plan, &node_id, &native_snapshot, &data_directory, &etcdutl)?;
    assert_eq!(restored.outcome(), LegacyStoreRestoreOutcome::Restored);
    assert!(restored.store_directory().join("data/member").is_dir());
    assert!(
        restored
            .store_directory()
            .join("provider-state.json")
            .is_file()
    );
    assert!(
        restored
            .store_directory()
            .join("cutover-restore.json")
            .is_file()
    );

    let verified = verify_legacy_store_restore(&plan, &node_id, &native_snapshot, &data_directory)?;
    assert_eq!(
        verified.outcome(),
        LegacyStoreRestoreOutcome::AlreadyComplete
    );
    let repeated =
        restore_legacy_store(&plan, &node_id, &native_snapshot, &data_directory, &etcdutl)?;
    assert_eq!(
        repeated.outcome(),
        LegacyStoreRestoreOutcome::AlreadyComplete
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn store_restore_retries_only_its_matching_partial_directory() -> TestResult {
    let directory = tempfile::tempdir()?;
    let native_snapshot = directory.path().join("post-migration.db");
    private_file(&native_snapshot, b"native snapshot")?;
    let etcdutl = directory.path().join("etcdutl");
    executable(&etcdutl, b"#!/bin/sh\nprintf rejected >&2\nexit 42\n")?;
    let data_directory = directory.path().join("maestro");
    let plan = one_member_plan()?;
    let node_id = NodeId::new("node-a")?;

    assert!(matches!(
        restore_legacy_store(&plan, &node_id, &native_snapshot, &data_directory, &etcdutl,),
        Err(LegacyStoreRestoreError::EtcdutlFailed {
            status: Some(42),
            ..
        })
    ));
    assert!(data_directory.join(".store-cutover.partial").is_dir());

    executable(
        &etcdutl,
        b"#!/bin/sh\n\
          while [ \"$#\" -gt 0 ]; do\n\
            if [ \"$1\" = '--data-dir' ]; then shift; data=\"$1\"; fi\n\
            shift\n\
          done\n\
          mkdir -p \"$data/member\"\n",
    )?;
    let report =
        restore_legacy_store(&plan, &node_id, &native_snapshot, &data_directory, &etcdutl)?;
    assert_eq!(report.outcome(), LegacyStoreRestoreOutcome::Restored);
    assert!(!data_directory.join(".store-cutover.partial").exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn store_verify_rejects_another_native_snapshot() -> TestResult {
    let directory = tempfile::tempdir()?;
    let native_snapshot = directory.path().join("post-migration.db");
    private_file(&native_snapshot, b"native snapshot")?;
    let etcdutl = directory.path().join("etcdutl");
    executable(
        &etcdutl,
        b"#!/bin/sh\n\
          while [ \"$#\" -gt 0 ]; do\n\
            if [ \"$1\" = '--data-dir' ]; then shift; data=\"$1\"; fi\n\
            shift\n\
          done\n\
          mkdir -p \"$data/member\"\n",
    )?;
    let data_directory = directory.path().join("maestro");
    let plan = one_member_plan()?;
    let node_id = NodeId::new("node-a")?;
    restore_legacy_store(&plan, &node_id, &native_snapshot, &data_directory, &etcdutl)?;

    private_file(&native_snapshot, b"different snapshot")?;
    assert!(matches!(
        verify_legacy_store_restore(&plan, &node_id, &native_snapshot, &data_directory),
        Err(LegacyStoreRestoreError::BindingMismatch { .. })
    ));
    Ok(())
}

fn one_member_plan() -> Result<crate::LegacyStoreRestorePlan, Box<dyn std::error::Error>> {
    let mut entries = node_entries("node-a", "master", 10, 1);
    entries.extend(cluster_state_for(&[10]));
    Ok(plan_legacy_store_restore(&LegacySnapshot::new(entries)?)?)
}

fn three_member_plan() -> Result<crate::LegacyStoreRestorePlan, Box<dyn std::error::Error>> {
    let mut entries = node_entries("node-a", "master", 10, 1);
    entries.extend(node_entries("node-b", "voter", 11, 2));
    entries.extend(node_entries("node-c", "voter", 12, 3));
    entries.extend(cluster_state_for(&[10, 11, 12]));
    Ok(plan_legacy_store_restore(&LegacySnapshot::new(entries)?)?)
}

#[cfg(unix)]
fn private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::write(path, bytes)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(unix)]
fn executable(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::write(path, bytes)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}
