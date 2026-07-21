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
        keys.request_claim(&request_id).as_str(),
        "/maestro/clusters/production/control/requests/request-42"
    );
    assert_eq!(
        keys.leader().as_str(),
        "/maestro/clusters/production/control/leader"
    );
    assert_eq!(
        keys.scheduler_generation().as_str(),
        "/maestro/clusters/production/control/scheduler-generation"
    );
}
