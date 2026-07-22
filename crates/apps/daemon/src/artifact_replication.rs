use std::sync::Arc;

use kernel_store::{SessionId, Store};
use node_agent::{ArtifactHolderRegistry, ArtifactReplicationAgent, ArtifactReplicationSettings};

use crate::cluster_query_clients;
use crate::control_plane::{DaemonRoleFactory, role_error};
use crate::{DaemonPlan, RoleError, RoleSpec};

pub(crate) fn build_artifact_replication_agent<
    MeshBackendType,
    FirewallBackendType,
    BridgeBackendType,
>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
    session_id: SessionId,
) -> Result<Arc<ArtifactReplicationAgent>, RoleError> {
    let peers = cluster_query_clients::artifact_source(plan, &factory.api_settings)?;
    let holders = ArtifactHolderRegistry::new(
        store.clone(),
        &spec.cluster_id,
        spec.node_id.clone(),
        session_id,
    );
    ArtifactReplicationAgent::new(
        store,
        factory.artifact_store.clone(),
        peers,
        holders,
        ArtifactReplicationSettings {
            cluster_id: spec.cluster_id.clone(),
            node_id: spec.node_id.clone(),
            resync_interval: factory.settings.artifact_resync_interval,
        },
        factory.monotonic_clock.clone(),
        factory.status_clock.clone(),
    )
    .map(Arc::new)
    .map_err(|error| role_error("construct artifact replication agent", error))
}
