use prost::Message;

use crate::proto::{MutationOperation, ResourceMutation, WorkloadIdentity};

#[test]
fn generated_identity_and_control_messages_round_trip() {
    let identity = WorkloadIdentity {
        workload_id: "workload-1".to_string(),
        assignment_id: "assignment-1".to_string(),
        node_id: "node-1".to_string(),
        service_id: "api".to_string(),
        deployment_id: "deployment-1".to_string(),
        labels: [("environment".to_string(), "test".to_string())]
            .into_iter()
            .collect(),
    };
    let mutation = ResourceMutation {
        request_id: "request-1".to_string(),
        kind: "Service".to_string(),
        resource_id: "api".to_string(),
        operation: MutationOperation::Apply.into(),
        resource_json: identity.encode_to_vec(),
    };

    let decoded = ResourceMutation::decode(mutation.encode_to_vec().as_slice())
        .expect("decode control mutation");

    assert_eq!(decoded, mutation);
}
