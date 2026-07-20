use crate::NodeRole;

#[test]
fn node_roles_expose_capabilities_without_store_vocabulary() {
    assert!(NodeRole::Master.is_control_plane());
    assert!(NodeRole::Master.runs_workloads());
    assert!(NodeRole::ControlPlane.is_control_plane());
    assert!(!NodeRole::ControlPlane.runs_workloads());
    assert!(!NodeRole::Worker.is_control_plane());
    assert!(NodeRole::Worker.runs_workloads());
}
