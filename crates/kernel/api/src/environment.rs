use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const TEMPLATE_OPEN: &str = "${{";
const TEMPLATE_CLOSE: &str = "}}";

/// Preview hostname available to runtime environment templates.
pub const MAESTRO_PREVIEW_HOST: &str = "MAESTRO_PREVIEW_HOST";

/// A borrowed environment-variable name that has passed Maestro's portable name policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EnvironmentName<'a>(&'a str);

impl<'a> EnvironmentName<'a> {
    pub fn parse(value: &'a str) -> Result<Self, InvalidEnvironmentName> {
        let mut bytes = value.bytes();
        if bytes
            .next()
            .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
            && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        {
            Ok(Self(value))
        } else {
            Err(InvalidEnvironmentName)
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'a str {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("environment name must match [A-Za-z_][A-Za-z0-9_]*")]
pub struct InvalidEnvironmentName;

/// Non-secret values captured with a Deployment for resolving environment templates on its node.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EnvironmentTemplateContext {
    /// Canonical ingress hostname assigned to a pull-request preview.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_host: Option<String>,
}

impl EnvironmentTemplateContext {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.preview_host.is_none()
    }

    /// Resolves every supported Maestro expression without exposing the input in errors.
    pub fn resolve(&self, value: &str) -> Result<String, EnvironmentTemplateError> {
        let mut remaining = value;
        let mut output = String::with_capacity(value.len());
        while let Some(open) = remaining.find(TEMPLATE_OPEN) {
            output.push_str(&remaining[..open]);
            let expression = &remaining[open + TEMPLATE_OPEN.len()..];
            let Some(close) = expression.find(TEMPLATE_CLOSE) else {
                return Err(EnvironmentTemplateError::Invalid {
                    message: "template is missing its closing `}}`".to_owned(),
                });
            };
            let variable = expression[..close].trim();
            if variable.is_empty()
                || !variable
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            {
                return Err(EnvironmentTemplateError::Invalid {
                    message: "template variable is not a valid Maestro variable name".to_owned(),
                });
            }
            output.push_str(self.resolve_variable(variable)?.as_str());
            remaining = &expression[close + TEMPLATE_CLOSE.len()..];
        }
        output.push_str(remaining);
        Ok(output)
    }

    fn resolve_variable(&self, variable: &str) -> Result<String, EnvironmentTemplateError> {
        match variable {
            MAESTRO_PREVIEW_HOST => {
                self.preview_host
                    .clone()
                    .ok_or_else(|| EnvironmentTemplateError::Unavailable {
                        variable: variable.to_owned(),
                        message: "the deployment has no preview host".to_owned(),
                    })
            }
            _ => Err(EnvironmentTemplateError::Unavailable {
                variable: variable.to_owned(),
                message: "the variable is not supported".to_owned(),
            }),
        }
    }
}

/// Safe failure produced while resolving an environment template.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvironmentTemplateError {
    #[error("{message}")]
    Invalid { message: String },
    #[error("{message}")]
    Unavailable { variable: String, message: String },
}

#[cfg(test)]
mod tests {
    use super::{EnvironmentName, EnvironmentTemplateContext};

    #[test]
    fn validates_portable_environment_names() {
        for valid in ["A", "_", "MAESTRO_TOKEN", "value2"] {
            assert!(EnvironmentName::parse(valid).is_ok());
        }
        for invalid in ["", "2VALUE", "WITH-DASH", "NON_ASCII_é"] {
            assert!(EnvironmentName::parse(invalid).is_err());
        }
    }

    #[test]
    fn resolves_preview_templates_without_exposing_input_in_errors() {
        let context = EnvironmentTemplateContext {
            preview_host: Some("api-pr-42.preview.example.test".to_owned()),
        };
        assert_eq!(
            context
                .resolve("https://${{ MAESTRO_PREVIEW_HOST }}/")
                .expect("resolve preview host"),
            "https://api-pr-42.preview.example.test/"
        );
        assert!(
            EnvironmentTemplateContext::default()
                .resolve("secret-${{ MAESTRO_PREVIEW_HOST }}")
                .expect_err("missing context must fail")
                .to_string()
                .contains("deployment has no preview host")
        );
        assert!(
            context
                .resolve("secret-${{ MAESTRO_PREVIEW_HOST")
                .expect_err("malformed template must fail")
                .to_string()
                .contains("missing its closing")
        );
        let error = context
            .resolve("secret-${{ DO-NOT-EXPOSE }}")
            .expect_err("invalid variable must fail")
            .to_string();
        assert!(error.contains("not a valid Maestro variable name"));
        assert!(!error.contains("DO-NOT-EXPOSE"));
        let error = context
            .resolve("secret-${{ PRIVATE_TOKEN }}")
            .expect_err("unsupported variable must fail")
            .to_string();
        assert_eq!(error, "the variable is not supported");
        assert!(!error.contains("PRIVATE_TOKEN"));
    }
}
