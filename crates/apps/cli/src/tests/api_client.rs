use kernel_api::SecretValue;

use crate::api_client::ApiClient;
use crate::contexts::Context;

#[test]
fn api_client_builds_origin_scoped_endpoints_and_rejects_bad_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let client = ApiClient::new(Context {
        host: "https://maestro.example.test".to_string(),
        token: None,
        ca_certificate_pem: None,
    })?;
    assert_eq!(
        client.endpoint("/api/services")?.as_str(),
        "https://maestro.example.test/api/services"
    );
    assert!(client.endpoint("api/services").is_err());
    assert!(
        ApiClient::new(Context {
            host: "https://maestro.example.test".to_string(),
            token: Some(SecretValue::new("invalid\nheader")),
            ca_certificate_pem: None,
        })
        .is_err()
    );
    Ok(())
}
