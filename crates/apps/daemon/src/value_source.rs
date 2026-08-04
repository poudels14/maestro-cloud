use std::collections::BTreeMap;

use async_trait::async_trait;
use aws_sdk_secretsmanager::error::DisplayErrorContext;
use kernel_api::{EnvironmentName, ExternalValueSource, SecretValue};
use runtime::{ValueSourceError, ValueSourceResolver};

pub(crate) struct AwsValueSourceResolver {
    client: aws_sdk_secretsmanager::Client,
}

impl AwsValueSourceResolver {
    pub(crate) fn new(config: &aws_config::SdkConfig) -> Self {
        Self {
            client: aws_sdk_secretsmanager::Client::new(config),
        }
    }
}

#[async_trait]
impl ValueSourceResolver for AwsValueSourceResolver {
    async fn resolve(
        &self,
        source: &str,
    ) -> Result<BTreeMap<String, SecretValue>, ValueSourceError> {
        let parsed =
            ExternalValueSource::parse(source).map_err(|_| ValueSourceError::Rejected {
                message: format!("unsupported external value source `{source}`"),
            })?;
        let secret_id = parsed
            .aws_secret_id()
            .ok_or_else(|| ValueSourceError::Rejected {
                message: format!("unsupported external value source `{source}`"),
            })?;
        let response = self
            .client
            .get_secret_value()
            .secret_id(secret_id)
            .send()
            .await
            .map_err(|error| ValueSourceError::Rejected {
                message: format!(
                    "failed to fetch AWS secret `{secret_id}`: {}",
                    DisplayErrorContext(error)
                ),
            })?;
        let raw = response
            .secret_string()
            .ok_or_else(|| ValueSourceError::Rejected {
                message: format!("AWS secret `{secret_id}` does not contain a string value"),
            })?;
        parse_key_values(source, raw)
    }
}

fn parse_key_values(
    source: &str,
    raw: &str,
) -> Result<BTreeMap<String, SecretValue>, ValueSourceError> {
    if let Ok(object) = serde_json::from_str::<BTreeMap<String, serde_json::Value>>(raw) {
        let values = object
            .into_iter()
            .map(|(key, value)| {
                let value = value
                    .as_str()
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| value.to_string());
                (key, SecretValue::new(value))
            })
            .collect();
        validate_values(source, &values)?;
        return Ok(values);
    }
    let values = dotenvy::Iter::new(raw.as_bytes())
        .map(|entry| entry.map(|(key, value)| (key, SecretValue::new(value))))
        .collect::<dotenvy::Result<BTreeMap<_, _>>>()
        .map_err(|_| ValueSourceError::Rejected {
            message: format!("external value source `{source}` contains invalid dotenv syntax"),
        })?;
    validate_values(source, &values)?;
    Ok(values)
}

fn validate_values(
    source: &str,
    values: &BTreeMap<String, SecretValue>,
) -> Result<(), ValueSourceError> {
    for (key, value) in values {
        if EnvironmentName::parse(key).is_err() {
            return Err(ValueSourceError::Rejected {
                message: format!(
                    "external value source `{source}` key `{key}` must match [A-Za-z_][A-Za-z0-9_]*"
                ),
            });
        }
        if value.expose().contains('\0') {
            return Err(ValueSourceError::Rejected {
                message: format!("external value source `{source}` value `{key}` contains NUL"),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use kernel_api::SecretValue;

    use super::parse_key_values;

    #[test]
    fn parses_json_and_dotenv_secret_strings() -> Result<(), Box<dyn std::error::Error>> {
        let json = parse_key_values("aws-secret://json", r#"{"TOKEN":"current"}"#)?;
        assert_eq!(json.get("TOKEN").map(SecretValue::expose), Some("current"));

        let dotenv = parse_key_values(
            "aws-secret://dotenv",
            "# comment\nexport TOKEN=\"rotated value\" # current token\nMODE='production mode'",
        )?;
        assert_eq!(
            dotenv.get("TOKEN").map(SecretValue::expose),
            Some("rotated value")
        );
        assert_eq!(
            dotenv.get("MODE").map(SecretValue::expose),
            Some("production mode")
        );
        Ok(())
    }

    #[test]
    fn dotenv_parse_errors_do_not_expose_secret_contents() {
        let error = parse_key_values("aws-secret://dotenv", "TOKEN='super-secret")
            .expect_err("unterminated quote must be rejected");
        assert!(error.to_string().contains("invalid dotenv syntax"));
        assert!(!error.to_string().contains("super-secret"));
    }
}
