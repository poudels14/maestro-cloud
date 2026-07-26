use std::fs;
use std::path::Path;

use kernel_api::NodeId;
use serde_json::{Value, json};

use crate::legacy_fixtures::cluster_state_for;
use crate::legacy_node_tests::{json_entry, node_entries};
use crate::{
    LegacySnapshot, LegacyStoreRestoreError, LegacyStoreRestoreOutcome, plan_legacy_store_restore,
    restore_legacy_store, verify_legacy_store_restore,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn store_plan_rekeys_validated_legacy_members_to_node_ids() -> TestResult {
    let snapshot = three_member_snapshot()?;
    let plan = plan_legacy_store_restore(&snapshot, 4_480)?;

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
    assert_eq!(node_b.peer_url(), "https://10.0.0.11:4480");
    Ok(())
}

#[test]
fn store_plan_normalizes_different_legacy_member_ports() -> TestResult {
    let legacy_ports = [
        (10_u8, 30_002_u16, 32_379_u16, 32_380_u16),
        (11, 31_002, 33_379, 33_380),
        (12, 32_002, 34_379, 34_380),
    ];
    let mut entries = node_entries("node-a", "master", 10, 1);
    entries.extend(node_entries("node-b", "voter", 11, 2));
    entries.extend(node_entries("node-c", "voter", 12, 3));
    for (host_octet, gateway, client, peer) in legacy_ports {
        rewrite_node_ports(&mut entries, host_octet, gateway, client, peer)?;
    }
    let mut cluster_state = cluster_state_for(&[10, 11, 12]);
    rewrite_cluster_state_ports(&mut cluster_state, &legacy_ports)?;
    entries.extend(cluster_state);

    let plan = plan_legacy_store_restore(&LegacySnapshot::new(entries)?, 4_480)?;
    assert_eq!(plan.members().len(), 3);
    assert!(
        plan.members()
            .values()
            .all(|member| member.peer_url().ends_with(":4480"))
    );
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
    Ok(plan_legacy_store_restore(
        &LegacySnapshot::new(entries)?,
        2_380,
    )?)
}

fn three_member_snapshot() -> Result<LegacySnapshot, Box<dyn std::error::Error>> {
    let mut entries = node_entries("node-a", "master", 10, 1);
    entries.extend(node_entries("node-b", "voter", 11, 2));
    entries.extend(node_entries("node-c", "voter", 12, 3));
    entries.extend(cluster_state_for(&[10, 11, 12]));
    Ok(LegacySnapshot::new(entries)?)
}

fn rewrite_node_ports(
    entries: &mut [crate::LegacyEntry],
    host_octet: u8,
    gateway: u16,
    client: u16,
    peer: u16,
) -> TestResult {
    let host_ip = format!("10.0.0.{host_octet}");
    for entry in entries {
        let key = entry.key().to_owned();
        let mut value: Value = serde_json::from_slice(entry.value())?;
        if key.contains("/node-records/")
            && value.pointer("/lastInfo/clusterHostIp") == Some(&json!(host_ip))
        {
            *value
                .pointer_mut("/lastInfo/clusterGatewayPort")
                .ok_or("node gateway port is absent")? = json!(gateway);
        } else if key.contains("/control-addresses/")
            && value.get("hostIp") == Some(&json!(host_ip))
        {
            *value
                .get_mut("gatewayPort")
                .ok_or("control gateway port is absent")? = json!(gateway);
            *value
                .get_mut("etcdClientPort")
                .ok_or("control client port is absent")? = json!(client);
            *value
                .get_mut("etcdPeerPort")
                .ok_or("control peer port is absent")? = json!(peer);
        } else {
            continue;
        }
        *entry = json_entry(&key, value);
    }
    Ok(())
}

fn rewrite_cluster_state_ports(
    entries: &mut [crate::LegacyEntry],
    ports: &[(u8, u16, u16, u16)],
) -> TestResult {
    for entry in entries {
        let key = entry.key().to_owned();
        let mut value: Value = serde_json::from_slice(entry.value())?;
        if key == "/maetro/system/cluster-meta" {
            let endpoints = value
                .get_mut("initialVoterEndpoints")
                .and_then(Value::as_array_mut)
                .ok_or("cluster endpoints are absent")?;
            for endpoint in endpoints {
                let host = endpoint
                    .get("hostIp")
                    .and_then(Value::as_str)
                    .ok_or("cluster endpoint host is absent")?;
                let (_, gateway, client, peer) = ports
                    .iter()
                    .find(|(octet, _, _, _)| host == format!("10.0.0.{octet}"))
                    .ok_or("cluster endpoint has no port fixture")?;
                *endpoint
                    .get_mut("gatewayPort")
                    .ok_or("cluster endpoint gateway port is absent")? = json!(gateway);
                *endpoint
                    .get_mut("etcdClientPort")
                    .ok_or("cluster endpoint client port is absent")? = json!(client);
                *endpoint
                    .get_mut("etcdPeerPort")
                    .ok_or("cluster endpoint peer port is absent")? = json!(peer);
            }
        } else if key.contains("/cluster/voters/") {
            let member = value
                .get("memberId")
                .and_then(Value::as_u64)
                .ok_or("voter member ID is absent")?;
            let (host_octet, _, client, peer) = ports
                .iter()
                .find(|(octet, _, _, _)| member == u64::from(*octet))
                .ok_or("voter has no port fixture")?;
            *value
                .get_mut("peerUrls")
                .ok_or("voter peer URLs are absent")? =
                json!([format!("https://10.0.0.{host_octet}:{peer}")]);
            *value
                .get_mut("clientUrls")
                .ok_or("voter client URLs are absent")? =
                json!([format!("https://10.0.0.{host_octet}:{client}")]);
        } else {
            continue;
        }
        *entry = json_entry(&key, value);
    }
    Ok(())
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
