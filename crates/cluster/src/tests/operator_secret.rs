use crate::OperatorJwtSecretSource;

#[test]
fn accepts_names_and_arns_but_rejects_non_aws_sources() -> Result<(), Box<dyn std::error::Error>> {
    let named =
        OperatorJwtSecretSource::new("aws-secret://maestro/production/operator-jwt-secret")?;
    assert_eq!(named.secret_id(), "maestro/production/operator-jwt-secret");
    assert!(
        OperatorJwtSecretSource::new(
            "aws-secret://arn:aws:secretsmanager:us-west-2:123456789012:secret:maestro/key"
        )
        .is_ok()
    );
    assert!(OperatorJwtSecretSource::new("file:///run/operator-secret").is_err());
    assert!(OperatorJwtSecretSource::new("aws-secret://").is_err());
    assert!(OperatorJwtSecretSource::new("aws-secret://secret?version=1").is_err());
    Ok(())
}
