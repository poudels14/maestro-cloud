use crate::{
    ClusterCertificateAuthority, JoinPayload, JoinPrivateKey, JoinProtocolError, JoinRequest,
    JoinResponseStatus, StoreJoinTicket, decrypt_join_response, encrypt_join_response,
};
use kernel_api::SecretValue;

use super::fixtures::{valid_config, validity};

#[test]
fn response_is_bound_to_request_key_and_status() -> Result<(), Box<dyn std::error::Error>> {
    let config = valid_config()?;
    let node_id = config.nodes.keys().next().ok_or("missing fixture node")?;
    let node = config.nodes.get(node_id).ok_or("missing fixture node")?;
    let key = JoinPrivateKey::generate();
    let request = JoinRequest::from_config(&key, &config, node_id, 1_000)?;
    let authority = ClusterCertificateAuthority::generate(&config.name, validity()?)?;
    let certificates = authority.issue_node_certificate(
        node_id,
        &node.hostname,
        node.endpoint.host_address,
        node.role,
        validity()?,
    )?;
    let payload = JoinPayload {
        cluster_id: config.cluster_id.clone(),
        cluster_name: config.name.clone(),
        cluster_cidr: config.cluster_cidr,
        node_limit: config.node_limit,
        node_prefix: config.node_prefix,
        nodes: config.nodes.clone(),
        control_allow_cidrs: config.control_allow_cidrs.clone(),
        ports: config.ports,
        tailscale: config.tailscale.clone(),
        cloudflare: config.cloudflare.clone(),
        certificates,
        operator_jwt_secret: SecretValue::new("operator-test-secret-with-at-least-32-characters"),
        store_encryption_secret: SecretValue::new("store-test-secret-with-at-least-32-characters"),
        store_join_ticket: Some(StoreJoinTicket::from_provider_data(
            node_id.clone(),
            b"test-ticket",
        )),
        certificate_issuer: Some(authority),
    };
    let envelope = encrypt_join_response(
        &config.join_secret,
        &request,
        &payload,
        JoinResponseStatus::ACCEPTED,
    )?;
    assert_eq!(
        decrypt_join_response(
            &config.join_secret,
            &key,
            &request,
            &envelope,
            JoinResponseStatus::ACCEPTED,
        )?,
        payload
    );

    let other_key = JoinPrivateKey::generate();
    assert!(
        decrypt_join_response(
            &config.join_secret,
            &other_key,
            &request,
            &envelope,
            JoinResponseStatus::ACCEPTED,
        )
        .is_err()
    );
    let wrong_status = JoinResponseStatus::new(201)?;
    assert!(
        decrypt_join_response(&config.join_secret, &key, &request, &envelope, wrong_status,)
            .is_err()
    );

    let mut missing_issuer = payload.clone();
    missing_issuer.certificate_issuer = None;
    assert!(matches!(
        encrypt_join_response(
            &config.join_secret,
            &request,
            &missing_issuer,
            JoinResponseStatus::ACCEPTED,
        ),
        Err(JoinProtocolError::ResponseIssuerGrantMismatch)
    ));

    let mut weak_secrets = payload;
    weak_secrets.operator_jwt_secret = SecretValue::new("too-short");
    assert!(matches!(
        encrypt_join_response(
            &config.join_secret,
            &request,
            &weak_secrets,
            JoinResponseStatus::ACCEPTED,
        ),
        Err(JoinProtocolError::ResponseSecretGrantMismatch)
    ));
    Ok(())
}
