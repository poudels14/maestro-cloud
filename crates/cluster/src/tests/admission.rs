use std::net::Ipv4Addr;

use kernel_api::NodeRole;

use crate::{AdmissionError, JoinPrivateKey, JoinRequest, admit_join_request, sign_join_request};

use super::fixtures::valid_config;

#[test]
fn admits_an_authenticated_declared_node() -> Result<(), Box<dyn std::error::Error>> {
    let config = valid_config()?;
    let node_id = config.nodes.keys().next().ok_or("missing fixture node")?;
    let key = JoinPrivateKey::generate();
    let request = JoinRequest::from_config(&key, &config, node_id, 1_000)?;
    let signature = sign_join_request(&config.join_secret, &request)?;
    let admission = admit_join_request(
        &config,
        &request,
        &signature,
        request.endpoint.host_address,
        1_000,
    )?;

    assert_eq!(&admission.node_id, node_id);
    assert_eq!(admission.request_nonce, request.nonce);
    assert_eq!(admission.public_key_sha256.len(), 64);
    Ok(())
}

#[test]
fn rejects_source_and_declaration_mismatches() -> Result<(), Box<dyn std::error::Error>> {
    let config = valid_config()?;
    let node_id = config.nodes.keys().next().ok_or("missing fixture node")?;
    let key = JoinPrivateKey::generate();
    let mut request = JoinRequest::from_config(&key, &config, node_id, 1_000)?;
    let signature = sign_join_request(&config.join_secret, &request)?;
    assert!(matches!(
        admit_join_request(
            &config,
            &request,
            &signature,
            Ipv4Addr::new(10, 20, 0, 99),
            1_000,
        ),
        Err(AdmissionError::SourceAddressMismatch { .. })
    ));

    request.role = NodeRole::Worker;
    let signature = sign_join_request(&config.join_secret, &request)?;
    assert!(matches!(
        admit_join_request(
            &config,
            &request,
            &signature,
            request.endpoint.host_address,
            1_000,
        ),
        Err(AdmissionError::DeclaredNodeMismatch { .. })
    ));
    Ok(())
}
