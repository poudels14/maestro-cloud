use std::collections::BTreeSet;

use kernel_api::{ClusterId, NodeId, NodeInstanceId, UpgradeOperation, UpgradeRunId};

use crate::{
    FileStoreRecoveryMarker, NodeUpgradeCommand, NodeUpgradeCommandState, PlannedStoreRecovery,
};

#[test]
fn recovery_marker_activates_only_after_a_reboot_and_is_run_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let boot_id = directory.path().join("boot-id");
    std::fs::write(&boot_id, "boot-one\n")?;
    let marker = FileStoreRecoveryMarker::with_boot_id_path(directory.path(), boot_id.clone());
    let cluster_id = ClusterId::new("cluster-1")?;
    let command = command()?;

    marker.prepare(&cluster_id, &command)?;
    marker.release(&cluster_id, &command)?;
    assert!(marker.activated(&cluster_id, &command.node_id)?.is_none());

    std::fs::write(&boot_id, "boot-two\n")?;
    let activated = marker
        .activated(&cluster_id, &command.node_id)?
        .ok_or("recovery marker did not activate after reboot")?;
    assert_eq!(activated.run_id, command.run_id);
    assert_eq!(activated.plan, command.store_recovery.clone().unwrap());

    marker.clear(&command.run_id)?;
    assert!(marker.activated(&cluster_id, &command.node_id)?.is_none());
    Ok(())
}

fn command() -> Result<NodeUpgradeCommand, kernel_api::InvalidIdentifier> {
    let node_id = NodeId::new("node-1")?;
    Ok(NodeUpgradeCommand {
        run_id: UpgradeRunId::new("upgrade-1")?,
        node_id: node_id.clone(),
        operation: UpgradeOperation::Upgrade,
        target_version: "2.0.0".to_owned(),
        previous_instance_id: NodeInstanceId::new("instance-1")?,
        store_recovery: Some(PlannedStoreRecovery {
            canonical_node_id: node_id.clone(),
            expected_members: [node_id, NodeId::new("node-2")?, NodeId::new("node-3")?]
                .into_iter()
                .collect::<BTreeSet<_>>(),
        }),
        state: NodeUpgradeCommandState::Requested,
        failure: None,
    })
}
