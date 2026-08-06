use super::*;
use crate::tests::node_api_support::node_api_services;

#[tokio::test]
async fn assignment_reconcile_mounts_and_cleans_private_node_api_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let owner = std::fs::metadata(world.node_api.path())?;
    let mut deployment = deployment();
    deployment.spec.service.user = Some(WorkloadUserSpec {
        user_id: owner.uid(),
        group_id: owner.gid(),
    });
    deployment.spec.service.node_api = NodeApiAccess::IdentityAndTelemetry;
    world.seed(&deployment, &assignment()).await?;
    let agent = world.agent_with_node_api(Some(node_api_services()));
    let report = agent.reconcile_once().await?;
    let credential_directory = world.node_api.path().join("mounts/assignment-1");
    assert!(
        credential_directory.join("node.sock").exists(),
        "report={report:?}; assignment={:?}",
        world.load_assignment().await?
    );
    assert!(credential_directory.join("node.token").exists());

    let key = world.assignment_key();
    let stored = world.store.get(&key).await?.ok_or("assignment missing")?;
    world
        .store
        .delete_cas(DeleteRequest {
            key,
            expected: stored.version,
        })
        .await?;
    agent.reconcile_once().await?;
    assert!(!credential_directory.exists());
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_rejects_node_api_without_an_explicit_user_or_services()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut missing_user_deployment = deployment();
    missing_user_deployment.spec.service.node_api = NodeApiAccess::IdentityAndTelemetry;
    world.seed(&missing_user_deployment, &assignment()).await?;
    let report = world.agent().reconcile_once().await?;
    assert_eq!(report.unresolved, 1);
    let rejected = world.load_assignment().await?;
    assert_eq!(rejected.status.phase, AssignmentPhase::Pending);
    assert!(
        rejected
            .status
            .conditions
            .first()
            .is_some_and(|condition| condition.message.contains("explicit numeric user"))
    );

    let world = World::new();
    let owner = std::fs::metadata(world.node_api.path())?;
    let mut deployment = deployment();
    deployment.spec.service.node_api = NodeApiAccess::IdentityAndTelemetry;
    deployment.spec.service.user = Some(WorkloadUserSpec {
        user_id: owner.uid(),
        group_id: owner.gid(),
    });
    world.seed(&deployment, &assignment()).await?;
    world.agent().reconcile_once().await?;
    let rejected = world.load_assignment().await?;
    assert_eq!(rejected.status.phase, AssignmentPhase::Pending);
    assert!(
        rejected
            .status
            .conditions
            .first()
            .is_some_and(|condition| condition.message.contains("no agent services"))
    );
    Ok(())
}
