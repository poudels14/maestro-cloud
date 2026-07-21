use crate::{EgressCluster, EgressPolicyFixture, ScenarioError};

/// Proves egress apply and deletion publish complete gap-free per-node artifacts.
pub async fn egress_policy_applies_and_deletes_atomically<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: EgressCluster,
{
    let fixture = EgressPolicyFixture {
        cidr: "192.0.2.0/24".to_string(),
        port: 5_432,
    };
    let applied = cluster
        .apply_egress_policy(fixture.clone())
        .await
        .map_err(|error| driver_error("apply egress policy", error))?;
    let nodes = cluster.nodes();
    if !applied.policy_present
        || !applied.policy_acknowledged
        || applied.bundle_digest.is_none()
        || applied.rulesets.len() != nodes.len()
        || applied.rulesets.iter().any(|ruleset| {
            !ruleset.script.contains(&fixture.cidr)
                || !ruleset.script.contains(&fixture.port.to_string())
        })
    {
        return Err(ScenarioError::Assertion(format!(
            "egress policy did not produce an acknowledged complete bundle: {applied:?}"
        )));
    }

    let deleted = cluster
        .delete_egress_policy()
        .await
        .map_err(|error| driver_error("delete egress policy", error))?;
    if !deleted.policy_present
        && !deleted.policy_acknowledged
        && deleted.rulesets.len() == nodes.len()
        && deleted
            .rulesets
            .iter()
            .all(|ruleset| !ruleset.script.contains(&fixture.cidr))
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "egress policy deletion left stale effective rules: {deleted:?}"
        )))
    }
}

fn driver_error(operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation,
        message: error.to_string(),
    }
}
