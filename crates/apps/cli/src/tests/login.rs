use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use kernel_api::SecretValue;

use crate::login::{
    AccessLevel, OperatorClaims, TokenLifetime, issue_token, issue_token_for_lifetime,
};

#[test]
fn login_token_carries_required_operator_claims() -> Result<(), Box<dyn std::error::Error>> {
    let secret = SecretValue::new("operator-test-secret-with-32-characters");
    let token = issue_token(&secret, "maestro-cli/dev", 7, 1_000)?;
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = false;
    let claims = jsonwebtoken::decode::<OperatorClaims>(
        token.expose(),
        &DecodingKey::from_secret(secret.expose().as_bytes()),
        &validation,
    )?
    .claims;
    assert_eq!(claims.sub, "maestro-cli/dev");
    assert_eq!(claims.scope, "operator");
    assert_eq!(claims.iat, 1_000);
    assert_eq!(claims.exp, 605_800);
    Ok(())
}

#[test]
fn login_rejects_weak_secrets_and_invalid_lifetimes() {
    assert!(issue_token(&SecretValue::new("short"), "operator", 7, 1_000).is_err());
    assert!(
        issue_token(
            &SecretValue::new("operator-test-secret-with-32-characters"),
            "operator",
            0,
            1_000,
        )
        .is_err()
    );
}

#[test]
fn token_lifetimes_and_access_levels_are_explicit() -> Result<(), Box<dyn std::error::Error>> {
    let secret = SecretValue::new("operator-test-secret-with-32-characters");
    let lifetime = "15m".parse::<TokenLifetime>()?;
    let token = issue_token_for_lifetime(
        &secret,
        "workstation",
        lifetime.seconds(),
        AccessLevel::ReadOnly,
        1_000,
    )?;
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = false;
    let claims = jsonwebtoken::decode::<OperatorClaims>(
        token.expose(),
        &DecodingKey::from_secret(secret.expose().as_bytes()),
        &validation,
    )?
    .claims;
    assert_eq!(claims.scope, "read-only");
    assert_eq!(claims.exp, 1_000 + 15 * 60);
    assert!("0m".parse::<TokenLifetime>().is_err());
    assert!("1w".parse::<TokenLifetime>().is_err());
    assert!("hour".parse::<TokenLifetime>().is_err());
    assert!("".parse::<TokenLifetime>().is_err());
    assert!("é".parse::<TokenLifetime>().is_err());
    Ok(())
}
