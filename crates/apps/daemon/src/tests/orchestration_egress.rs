use kernel_api::{FirewallPolicy, ResourceKind, ResourceName, Timestamp};
use kernel_store::{CasOutcome, ExpectedVersion, PutRequest, Store};

use super::orchestration::{RolloutWorld, put};
use super::orchestration_fixture::egress_policy;

#[tokio::test]
async fn egress_policy_applies_exact_bundle_then_finalizes_without_a_rule_gap()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        let world = RolloutWorld::new(node_count).await?;
        world.converge().await?;
        let policy = egress_policy()?;
        put(&world.store, &world.keys, "FirewallPolicy", &policy).await?;
        world.converge().await?;

        let applied = world
            .list::<FirewallPolicy>("FirewallPolicy")
            .await?
            .into_iter()
            .next()
            .ok_or("firewall policy missing")?;
        let bundle = world.latest_firewall_bundle().await?;
        assert_eq!(applied.status.applied_generation, applied.meta.generation);
        assert_eq!(
            applied.status.ruleset_digest.as_deref(),
            Some(bundle.digest.as_str())
        );
        assert_eq!(bundle.rulesets.len(), usize::from(node_count));
        assert!(
            bundle
                .rulesets
                .iter()
                .all(|ruleset| ruleset.script.contains("192.0.2.0/24"))
        );

        mark_policy_deleting(&world, &applied, Timestamp(10_000)).await?;
        world.converge().await?;
        assert!(
            world
                .list::<FirewallPolicy>("FirewallPolicy")
                .await?
                .is_empty()
        );
        assert!(
            world
                .latest_firewall_bundle()
                .await?
                .rulesets
                .iter()
                .all(|ruleset| !ruleset.script.contains("192.0.2.0/24"))
        );
    }
    Ok(())
}

async fn mark_policy_deleting(
    world: &RolloutWorld,
    policy: &FirewallPolicy,
    at: Timestamp,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let key = world.keys.resource(
        &ResourceKind::new("FirewallPolicy")?,
        &ResourceName::from(policy.meta.id.clone()),
    );
    let stored = world.store.get(&key).await?.ok_or("policy missing")?;
    let mut policy: FirewallPolicy = serde_json::from_slice(&stored.value)?;
    policy.meta.deletion_timestamp = Some(at);
    let outcome = world
        .store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&policy)?,
            expected: ExpectedVersion::Exact(stored.version),
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("firewall policy deletion conflicted".into())
    }
}
