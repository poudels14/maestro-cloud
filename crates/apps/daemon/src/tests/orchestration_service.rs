use kernel_api::{
    Generation, PlacementConstraint, ResourceKind, ResourceName, RolloutState, Service, ServiceId,
    Timestamp,
};
use kernel_store::{CasOutcome, ExpectedVersion, PutRequest, Store};

use super::orchestration::{HarnessResult, RolloutWorld};

impl RolloutWorld {
    pub(super) async fn set_replica_override(&self, replicas: Option<u32>) -> HarnessResult<()> {
        self.update_service(|service| service.status.replica_override = replicas)
            .await
            .map(|_service| ())
    }

    pub(super) async fn mark_service_deleting(&self, at: Timestamp) -> HarnessResult<()> {
        self.update_service(|service| service.meta.deletion_timestamp = Some(at))
            .await
            .map(|_service| ())
    }

    pub(super) async fn set_rollout_state(&self, state: RolloutState) -> HarnessResult<()> {
        self.update_service(|service| service.status.rollout = state)
            .await
            .map(|_service| ())
    }

    pub(super) async fn set_placement(&self, placement: PlacementConstraint) -> HarnessResult<()> {
        self.update_service(|service| {
            service.meta.generation = Generation(service.meta.generation.0.saturating_add(1));
            service.spec.placement = placement;
        })
        .await
        .map(|_service| ())
    }

    pub(super) async fn update_service(
        &self,
        change: impl FnOnce(&mut Service),
    ) -> HarnessResult<Service> {
        self.update_service_by_id(&ServiceId::new("api")?, change)
            .await
    }

    pub(super) async fn update_service_by_id(
        &self,
        service_id: &ServiceId,
        change: impl FnOnce(&mut Service),
    ) -> HarnessResult<Service> {
        let key = self.keys.resource(
            &ResourceKind::new("Service")?,
            &ResourceName::from(service_id.clone()),
        );
        let stored = self.store.get(&key).await?.ok_or("service missing")?;
        let mut service: Service = serde_json::from_slice(&stored.value)?;
        change(&mut service);
        let outcome = self
            .store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&service)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(service)
        } else {
            Err("service update conflicted".into())
        }
    }
}
