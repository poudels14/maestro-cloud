use cluster::Ipv4Cidr;
use kernel_api::SecretValue;

use crate::{ServerSettings, ServerSettingsError};

#[test]
fn managed_plaintext_requires_one_exact_bridge_address() -> Result<(), Box<dyn std::error::Error>> {
    let settings = ServerSettings::new(
        "10.50.0.5:80".parse()?,
        Some(SecretValue::new(
            "operator-test-secret-with-at-least-32-characters",
        )),
    )
    .with_managed_operator_plaintext()
    .with_operator_proxy_cidrs([Ipv4Cidr::new("10.50.0.0".parse()?, 24)?]);
    assert!(settings.validate().is_ok());

    let unscoped = ServerSettings::new(
        "0.0.0.0:80".parse()?,
        Some(SecretValue::new(
            "operator-test-secret-with-at-least-32-characters",
        )),
    )
    .with_managed_operator_plaintext()
    .with_operator_proxy_cidrs([Ipv4Cidr::new("10.50.0.0".parse()?, 24)?]);
    assert!(matches!(
        unscoped.validate(),
        Err(ServerSettingsError::UnscopedManagedPlaintext { .. })
    ));
    Ok(())
}
