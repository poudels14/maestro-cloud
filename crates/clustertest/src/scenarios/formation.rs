use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BootstrapDecision, FormationCluster, FormationMemberRole, LeadershipAgreement,
    MembershipAgreement, ReservationState, ResourceAvailability, ScenarioError,
};

/// Proves designated-seed recovery, serial learner promotion, and lease cleanup.
pub async fn designated_seed_and_learners_form_registered_cluster<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: FormationCluster,
{
    let nodes = cluster.nodes();
    let [seed, middle, later] = nodes.as_slice() else {
        return Err(ScenarioError::Assertion(format!(
            "formation scenario requires exactly three voters, found {}",
            nodes.len()
        )));
    };

    assert_seed_decision(cluster, BootstrapDecision::BootstrapSeed, "initial seed").await?;
    cluster
        .mark_seed_starting()
        .await
        .map_err(|error| driver_error("mark seed starting", error))?;
    assert_seed_decision(cluster, BootstrapDecision::ResumeSeed, "interrupted seed").await?;
    cluster
        .mark_seed_joined()
        .await
        .map_err(|error| driver_error("mark seed joined", error))?;
    assert_seed_decision(cluster, BootstrapDecision::Restart, "joined seed").await?;
    cluster
        .await_seed()
        .await
        .map_err(|error| driver_error("await designated seed", error))?;

    let mut admitted = BTreeSet::from([seed.clone()]);
    for node in [later, middle] {
        admitted.insert(node.clone());
        let observation = cluster
            .join_and_promote(node)
            .await
            .map_err(|error| driver_error("join and promote configured voter", error))?;
        if observation.node != *node
            || observation.decision_before_join != BootstrapDecision::Restart
            || observation.decision_after_join != BootstrapDecision::JoinExisting
            || observation.initial_members != admitted
            || observation.final_role != FormationMemberRole::Voter
        {
            return Err(ScenarioError::Assertion(format!(
                "unexpected join observation for {node:?}: {observation:?}"
            )));
        }
    }

    let expected_members = nodes
        .iter()
        .cloned()
        .map(|node| (node, FormationMemberRole::Voter))
        .collect::<BTreeMap<_, _>>();
    let formed = cluster
        .formation_snapshot()
        .await
        .map_err(|error| driver_error("observe formed consensus group", error))?;
    if formed.members != expected_members
        || formed.membership != MembershipAgreement::Consistent
        || formed.leadership != LeadershipAgreement::Consistent
        || formed.writes != ResourceAvailability::Available
    {
        return Err(ScenarioError::Assertion(format!(
            "formed cluster did not converge: {formed:?}"
        )));
    }

    let registration = cluster
        .register_seed()
        .await
        .map_err(|error| driver_error("register designated seed", error))?;
    let seed_set = BTreeSet::from([seed.clone()]);
    if registration.registered_nodes != seed_set
        || registration.image_holders != seed_set
        || registration.reservation_node != *seed
        || registration.reservation_state != ReservationState::Active
        || registration.advertised_ports != registration.reserved_ports
    {
        return Err(ScenarioError::Assertion(format!(
            "seed registration did not match its reservation: {registration:?}"
        )));
    }

    let cleanup = cluster
        .deregister_seed()
        .await
        .map_err(|error| driver_error("deregister designated seed", error))?;
    if cleanup.registered_nodes.is_empty() && cleanup.image_holders.is_empty() {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "leased registration records survived deregistration: {cleanup:?}"
        )))
    }
}

async fn assert_seed_decision<Cluster>(
    cluster: &mut Cluster,
    expected: BootstrapDecision,
    operation: &'static str,
) -> Result<(), ScenarioError>
where
    Cluster: FormationCluster,
{
    let actual = cluster
        .seed_decision()
        .await
        .map_err(|error| driver_error(operation, error))?;
    if actual == expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "{operation} decision was {actual:?}, expected {expected:?}"
        )))
    }
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
