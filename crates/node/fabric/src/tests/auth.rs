use std::collections::BTreeMap;

use kernel_api::{AssignmentId, DeploymentId, NodeId, ServiceId, WorkloadId};

use crate::{AuthorizationError, SocketPeer, WorkloadAuthorization, WorkloadClaims, WorkloadToken};

#[test]
fn authorization_requires_both_constant_time_token_and_kernel_peer_identity() {
    let token = WorkloadToken::from_bytes([7; 32]);
    let authorization = WorkloadAuthorization::new(token, 1_000, claims());
    let peer = SocketPeer {
        process_id: 42,
        user_id: 1_000,
        group_id: 1_000,
    };

    assert_eq!(
        authorization
            .authorize(&[7; 32], peer)
            .expect("authorized workload")
            .workload_id,
        WorkloadId::new("workload-1").expect("workload id")
    );
    assert_eq!(
        authorization.authorize(&[8; 32], peer),
        Err(AuthorizationError::InvalidToken)
    );
    assert_eq!(
        authorization.authorize(
            &[7; 32],
            SocketPeer {
                user_id: 2_000,
                ..peer
            }
        ),
        Err(AuthorizationError::PeerUserMismatch {
            expected: 1_000,
            actual: 2_000
        })
    );
    assert!(!format!("{authorization:?}").contains("7, 7"));
}

fn claims() -> WorkloadClaims {
    WorkloadClaims {
        workload_id: WorkloadId::new("workload-1").expect("workload id"),
        assignment_id: AssignmentId::new("assignment-1").expect("assignment id"),
        node_id: NodeId::new("node-1").expect("node id"),
        service_id: ServiceId::new("api").expect("service id"),
        deployment_id: DeploymentId::new("deployment-1").expect("deployment id"),
        labels: BTreeMap::from([("environment".to_string(), "test".to_string())]),
    }
}
