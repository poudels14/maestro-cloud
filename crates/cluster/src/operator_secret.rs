use serde::{Deserialize, Serialize};

const AWS_SECRET_PREFIX: &str = "aws-secret://";
const MAXIMUM_SECRET_ID_BYTES: usize = 2_048;

/// Stable AWS Secrets Manager reference for the cluster-wide operator JWT key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OperatorJwtSecretSource {
    uri: String,
    secret_id: String,
}

impl OperatorJwtSecretSource {
    /// Validates one `aws-secret://` source without fetching its value.
    pub fn new(source: impl Into<String>) -> Result<Self, OperatorJwtSecretSourceError> {
        let source = source.into();
        let secret_id = source
            .strip_prefix(AWS_SECRET_PREFIX)
            .ok_or(OperatorJwtSecretSourceError::UnsupportedScheme)?;
        if secret_id.is_empty()
            || secret_id.len() > MAXIMUM_SECRET_ID_BYTES
            || secret_id.chars().any(|character| {
                character.is_whitespace()
                    || character.is_control()
                    || matches!(character, '?' | '#')
            })
        {
            return Err(OperatorJwtSecretSourceError::InvalidSecretId);
        }
        let secret_id = secret_id.to_owned();
        Ok(Self {
            uri: source,
            secret_id,
        })
    }

    /// Returns the complete source URI safe to persist in a launch document.
    pub fn as_str(&self) -> &str {
        &self.uri
    }

    /// Returns the exact identifier accepted by AWS Secrets Manager.
    pub fn secret_id(&self) -> &str {
        &self.secret_id
    }
}

impl TryFrom<String> for OperatorJwtSecretSource {
    type Error = OperatorJwtSecretSourceError;

    fn try_from(source: String) -> Result<Self, Self::Error> {
        Self::new(source)
    }
}

impl From<OperatorJwtSecretSource> for String {
    fn from(source: OperatorJwtSecretSource) -> Self {
        source.uri
    }
}

/// Invalid or unsupported operator signing-key source.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OperatorJwtSecretSourceError {
    /// Operator signing keys must use AWS Secrets Manager as their source of truth.
    #[error("operator JWT secret source must use the aws-secret:// scheme")]
    UnsupportedScheme,
    /// The AWS secret identifier was empty, unsafe, or too large.
    #[error("operator JWT secret source contains an invalid AWS secret identifier")]
    InvalidSecretId,
}

#[cfg(test)]
mod tests {
    use super::OperatorJwtSecretSource;

    #[test]
    fn accepts_names_and_arns_but_rejects_non_aws_sources() -> Result<(), Box<dyn std::error::Error>>
    {
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
}
