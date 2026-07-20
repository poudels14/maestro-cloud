use kernel_api::{ClusterId, SecretValue};

use crate::{
    CaDiscoveryRequest, ClusterCertificateAuthority, JoinPrivateKey, JoinProtocolError,
    create_ca_discovery_response, sign_join_request, verify_ca_discovery_response,
    verify_join_request_signature,
};

use super::fixtures::{valid_config, validity};

#[test]
fn canonical_signature_authenticates_the_complete_request() -> Result<(), Box<dyn std::error::Error>>
{
    let config = valid_config()?;
    let key = JoinPrivateKey::generate();
    let mut request = crate::JoinRequest::from_config(
        &key,
        &config,
        config.nodes.keys().next().ok_or("missing fixture node")?,
        1_000,
    )?;
    let signature = sign_join_request(&config.join_secret, &request)?;
    verify_join_request_signature(&config.join_secret, &request, &signature)?;

    request.hostname.push_str("-tampered");
    assert!(verify_join_request_signature(&config.join_secret, &request, &signature).is_err());
    Ok(())
}

#[test]
fn rejects_a_weak_bootstrap_secret() -> Result<(), Box<dyn std::error::Error>> {
    let config = valid_config()?;
    let key = JoinPrivateKey::generate();
    let request = crate::JoinRequest::from_config(
        &key,
        &config,
        config.nodes.keys().next().ok_or("missing fixture node")?,
        1_000,
    )?;

    assert!(matches!(
        sign_join_request(&SecretValue::new("too-short"), &request),
        Err(JoinProtocolError::WeakSharedSecret)
    ));
    Ok(())
}

#[test]
fn shared_secret_authenticates_ca_discovery() -> Result<(), Box<dyn std::error::Error>> {
    let authority = ClusterCertificateAuthority::generate("test-cluster", validity()?)?;
    let secret = SecretValue::new("a sufficiently long shared join secret");
    let request = CaDiscoveryRequest::new("test-cluster");
    let cluster_id = ClusterId::new("test-cluster")?;
    let response = create_ca_discovery_response(
        &secret,
        "test-cluster",
        &cluster_id,
        &authority.certificate_pem,
        &request,
    )?;
    verify_ca_discovery_response(&secret, "test-cluster", &request, &response)?;

    let other_secret = SecretValue::new("a different sufficiently long secret");
    assert!(
        verify_ca_discovery_response(&other_secret, "test-cluster", &request, &response).is_err()
    );
    Ok(())
}
