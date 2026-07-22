use kernel_api::{ClusterId, NodeId, RequestId, ResourceKind, ResourceName};

use crate::Keyspace;

#[test]
fn keyspace_owns_every_canonical_cluster_key_shape() {
    let cluster_id = ClusterId::new("production").expect("cluster id");
    let node_id = NodeId::new("node-1").expect("node id");
    let request_id = RequestId::new("request-42").expect("request id");
    let kind = ResourceKind::new("Service").expect("resource kind");
    let resource_id = ResourceName::new("api").expect("resource name");
    let keys = Keyspace::new(&cluster_id);

    assert_eq!(keys.cluster().as_str(), "/maestro/clusters/production/");
    assert_eq!(
        keys.resources().as_str(),
        "/maestro/clusters/production/resources/"
    );
    assert_eq!(
        keys.resource_kind(&kind).as_str(),
        "/maestro/clusters/production/resources/Service/"
    );
    assert_eq!(
        keys.resource(&kind, &resource_id).as_str(),
        "/maestro/clusters/production/resources/Service/api"
    );
    assert_eq!(
        keys.node_liveness(&node_id).as_str(),
        "/maestro/clusters/production/liveness/nodes/node-1"
    );
    assert_eq!(
        keys.node_liveness_records().as_str(),
        "/maestro/clusters/production/liveness/nodes/"
    );
    let artifact_id = ResourceName::new("0123456789abcdef").expect("artifact id");
    assert_eq!(
        keys.artifact_holders(&artifact_id).as_str(),
        "/maestro/clusters/production/liveness/artifact-holders/by-digest/0123456789abcdef/"
    );
    assert_eq!(
        keys.artifact_holder(&artifact_id, &node_id).as_str(),
        "/maestro/clusters/production/liveness/artifact-holders/by-digest/0123456789abcdef/node-1"
    );
    assert_eq!(
        keys.node_artifact_holders(&node_id).as_str(),
        "/maestro/clusters/production/liveness/artifact-holders/by-node/node-1/"
    );
    assert_eq!(
        keys.node_artifact_holder(&node_id, &artifact_id).as_str(),
        "/maestro/clusters/production/liveness/artifact-holders/by-node/node-1/0123456789abcdef"
    );
    assert_eq!(
        keys.node_upgrade_command(&node_id).as_str(),
        "/maestro/clusters/production/control/node-upgrades/node-1"
    );
    assert_eq!(
        keys.request_claim(&request_id).as_str(),
        "/maestro/clusters/production/control/requests/request-42"
    );
    assert_eq!(
        keys.join_approvals().as_str(),
        "/maestro/clusters/production/control/join-approvals/"
    );
    assert_eq!(
        keys.join_approval(&node_id).as_str(),
        "/maestro/clusters/production/control/join-approvals/node-1"
    );
    assert_eq!(
        keys.migration_marker(&ResourceName::new("legacy-v1").expect("migration id"))
            .as_str(),
        "/maestro/clusters/production/control/migrations/legacy-v1"
    );
    assert_eq!(
        keys.leader().as_str(),
        "/maestro/clusters/production/control/leader"
    );
    assert_eq!(
        keys.scheduler_generation().as_str(),
        "/maestro/clusters/production/control/scheduler-generation"
    );
    assert_eq!(
        keys.scheduler_observation().as_str(),
        "/maestro/clusters/production/observations/scheduler"
    );
    assert_eq!(
        keys.traefik().as_str(),
        "/maestro/clusters/production/integrations/traefik/"
    );
    assert_eq!(
        keys.traefik_entry("http/routers/api/rule")
            .expect("Traefik entry")
            .as_str(),
        "/maestro/clusters/production/integrations/traefik/http/routers/api/rule"
    );
    assert_eq!(
        keys.traefik_prefix("http/services/api")
            .expect("Traefik prefix")
            .as_str(),
        "/maestro/clusters/production/integrations/traefik/http/services/api/"
    );
    assert!(keys.traefik_entry("../control/leader").is_err());
    assert!(keys.traefik_prefix("/http/routers").is_err());
}
