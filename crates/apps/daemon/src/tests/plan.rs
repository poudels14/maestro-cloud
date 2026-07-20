use std::path::PathBuf;

use kernel_api::{NodeId, NodeRole};

use crate::{DaemonPlan, DaemonRole};

use super::cluster_with_nodes;

#[test]
fn role_plan_wires_capabilities_without_single_node_special_cases()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster = cluster_with_nodes(&[
        ("master", NodeRole::Master),
        ("control", NodeRole::ControlPlane),
        ("hybrid", NodeRole::Hybrid),
        ("worker", NodeRole::Worker),
    ])?;

    let master = DaemonPlan::new(
        cluster.clone(),
        NodeId::new("master")?,
        PathBuf::from("/var/lib/maestro/master"),
    )?;
    assert_eq!(
        master
            .roles()
            .iter()
            .map(|role| (role.role, role.workload_enabled))
            .collect::<Vec<_>>(),
        vec![(DaemonRole::Agent, true), (DaemonRole::Controller, false)]
    );

    let control = DaemonPlan::new(
        cluster.clone(),
        NodeId::new("control")?,
        PathBuf::from("/var/lib/maestro/control"),
    )?;
    assert_eq!(
        control
            .roles()
            .iter()
            .map(|role| (role.role, role.workload_enabled))
            .collect::<Vec<_>>(),
        vec![(DaemonRole::Agent, false), (DaemonRole::Controller, false)]
    );

    let worker = DaemonPlan::new(
        cluster,
        NodeId::new("worker")?,
        PathBuf::from("/var/lib/maestro/worker"),
    )?;
    assert_eq!(
        worker
            .roles()
            .iter()
            .map(|role| (role.role, role.workload_enabled))
            .collect::<Vec<_>>(),
        vec![(DaemonRole::Agent, true)]
    );
    Ok(())
}
