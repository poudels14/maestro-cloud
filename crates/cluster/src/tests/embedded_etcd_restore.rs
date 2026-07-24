use std::fs;

use kernel_api::{ClusterId, NodeId};
use serde_json::Value;

use crate::{StoreProviderError, initialize_restored_etcd_member};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn restored_member_state_is_bound_to_source_and_identity() -> TestResult {
    let directory = tempfile::tempdir()?;
    let provider = directory.path().join("store");
    fs::create_dir_all(provider.join("data/member"))?;

    initialize_restored_etcd_member(
        &provider,
        ClusterId::new("cluster-a")?,
        NodeId::new("node-a")?,
        &"ab".repeat(32),
    )?;

    let state: Value = serde_json::from_slice(&fs::read(provider.join("provider-state.json"))?)?;
    assert_eq!(state.pointer("/formatVersion"), Some(&1.into()));
    assert_eq!(state.pointer("/clusterId"), Some(&"cluster-a".into()));
    assert_eq!(state.pointer("/nodeId"), Some(&"node-a".into()));
    assert_eq!(
        state.pointer("/initialization/kind"),
        Some(&"restored".into())
    );
    assert_eq!(
        state.pointer("/initialization/digest"),
        Some(&"ab".repeat(32).into())
    );
    Ok(())
}

#[test]
fn restored_member_state_rejects_missing_data_and_overwrite() -> TestResult {
    let directory = tempfile::tempdir()?;
    let provider = directory.path().join("store");
    fs::create_dir_all(&provider)?;
    let cluster_id = ClusterId::new("cluster-a")?;
    let node_id = NodeId::new("node-a")?;

    assert!(matches!(
        initialize_restored_etcd_member(
            &provider,
            cluster_id.clone(),
            node_id.clone(),
            &"ab".repeat(32),
        ),
        Err(StoreProviderError::UnsafeRecovery { .. })
    ));
    fs::create_dir_all(provider.join("data/member"))?;
    initialize_restored_etcd_member(
        &provider,
        cluster_id.clone(),
        node_id.clone(),
        &"ab".repeat(32),
    )?;
    assert!(
        initialize_restored_etcd_member(&provider, cluster_id, node_id, &"ab".repeat(32),).is_err()
    );
    Ok(())
}
