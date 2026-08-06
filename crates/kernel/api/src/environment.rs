use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const TEMPLATE_OPEN: &str = "${{";
const TEMPLATE_CLOSE: &str = "}}";

/// Canonical ingress hostname available to runtime environment templates.
pub const MAESTRO_INGRESS_HOST: &str = "MAESTRO_INGRESS_HOST";

/// Ingress target port available to runtime environment templates.
pub const MAESTRO_INGRESS_PORT: &str = "MAESTRO_INGRESS_PORT";

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
    /// Canonical ingress hostname assigned to the service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress_host: Option<String>,
    /// Ingress target port assigned to the service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress_port: Option<u16>,
}

impl EnvironmentTemplateContext {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.ingress_host.is_none() && self.ingress_port.is_none()
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
            MAESTRO_INGRESS_HOST => {
                self.ingress_host
                    .clone()
                    .ok_or_else(|| EnvironmentTemplateError::Unavailable {
                        variable: variable.to_owned(),
                        message: "the deployment has no ingress host".to_owned(),
                    })
            }
            MAESTRO_INGRESS_PORT => {
                self.ingress_port
                    .map(|port| port.to_string())
                    .ok_or_else(|| EnvironmentTemplateError::Unavailable {
                        variable: variable.to_owned(),
                        message: "the deployment has no ingress port".to_owned(),
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
    fn resolves_ingress_templates_without_exposing_input_in_errors() {
        let context = EnvironmentTemplateContext {
            ingress_host: Some("api.example.test".to_owned()),
            ingress_port: Some(8080),
        };
        assert_eq!(
            context
                .resolve("https://${{ MAESTRO_INGRESS_HOST }}:${{ MAESTRO_INGRESS_PORT }}/")
                .as_deref(),
            Ok("https://api.example.test:8080/")
        );
        assert_eq!(
            EnvironmentTemplateContext::default()
                .resolve("secret-${{ MAESTRO_INGRESS_HOST }}")
                .map_err(|error| error.to_string()),
            Err("the deployment has no ingress host".to_owned())
        );
        assert_eq!(
            context
                .resolve("secret-${{ MAESTRO_INGRESS_HOST")
                .map_err(|error| error.to_string()),
            Err("template is missing its closing `}}`".to_owned())
        );
        assert_eq!(
            context
                .resolve("secret-${{ DO-NOT-EXPOSE }}")
                .map_err(|error| error.to_string()),
            Err("template variable is not a valid Maestro variable name".to_owned())
        );
        assert_eq!(
            context
                .resolve("secret-${{ PRIVATE_TOKEN }}")
                .map_err(|error| error.to_string()),
            Err("the variable is not supported".to_owned())
        );
    }
}
