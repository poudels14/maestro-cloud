use kernel_api::{
    Deployment, DeploymentGoal, DeploymentId, Generation, ResourceKind, ResourceName,
};
use kernel_store::{CasOutcome, ExpectedVersion, PutRequest, Store};

use super::orchestration::{HarnessResult, RolloutWorld};

impl RolloutWorld {
    pub(super) async fn redeploy_service(&self) -> HarnessResult<()> {
        self.update_service(|service| {
            service.meta.generation = Generation(service.meta.generation.0.saturating_add(1));
        })
        .await
        .map(|_service| ())
    }

    pub(super) async fn restart_deployment(
        &self,
        deployment_id: &DeploymentId,
    ) -> HarnessResult<()> {
        let key = self.keys.resource(
            &ResourceKind::new("Deployment")?,
            &ResourceName::from(deployment_id.clone()),
        );
        let stored = self.store.get(&key).await?.ok_or("deployment missing")?;
        let mut deployment: Deployment = serde_json::from_slice(&stored.value)?;
        deployment.meta.generation = Generation(deployment.meta.generation.0.saturating_add(1));
        deployment.spec.restart_generation =
            Generation(deployment.spec.restart_generation.0.saturating_add(1));
        let outcome = self
            .store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&deployment)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err("deployment restart update conflicted".into())
        }
    }

    pub(super) async fn request_deployment_goal(
        &self,
        deployment_id: &DeploymentId,
        goal: DeploymentGoal,
    ) -> HarnessResult<()> {
        let key = self.keys.resource(
            &ResourceKind::new("Deployment")?,
            &ResourceName::from(deployment_id.clone()),
        );
        let stored = self.store.get(&key).await?.ok_or("deployment missing")?;
        let mut deployment: Deployment = serde_json::from_slice(&stored.value)?;
        deployment.meta.generation = Generation(deployment.meta.generation.0.saturating_add(1));
        deployment.spec.goal = goal;
        let outcome = self
            .store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&deployment)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err("deployment goal update conflicted".into())
        }
    }
}
