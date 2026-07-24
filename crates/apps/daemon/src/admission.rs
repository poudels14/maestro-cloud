use std::sync::Arc;

use cluster::{
    AdmissionCoordinator, AdmissionCoordinatorError, ClusterCertificateAuthority, ClusterConfig,
    ClusterLaunchPolicy, StoreProvider,
};
use kernel_api::SecretValue;
use kernel_store::Store;

/// Secret material retained only by control-plane nodes that can admit members.
#[derive(Clone)]
pub struct AdmissionDependencies {
    /// Cluster trust-root signer delivered during bootstrap or control-plane join.
    pub authority: ClusterCertificateAuthority,
    /// Cluster-wide operator authentication key delivered to every node.
    pub operator_jwt_secret: SecretValue,
    /// Cluster-wide at-rest encryption key delivered to every node.
    pub store_encryption_secret: SecretValue,
    /// Optional production integrations delivered through encrypted join grants.
    pub launch_policy: ClusterLaunchPolicy,
}

impl AdmissionDependencies {
    pub(crate) fn coordinator(
        &self,
        config: ClusterConfig,
        provider: Arc<dyn StoreProvider>,
        store: Arc<dyn Store>,
    ) -> Result<Arc<AdmissionCoordinator>, AdmissionCoordinatorError> {
        AdmissionCoordinator::new(
            config,
            self.authority.clone(),
            self.operator_jwt_secret.clone(),
            self.store_encryption_secret.clone(),
            self.launch_policy.clone(),
            provider,
            store,
        )
        .map(Arc::new)
    }
}
