use std::fmt::{Debug, Formatter};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A secret-bearing wire value whose debug representation is always redacted.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Zeroize, ZeroizeOnDrop)]
#[serde(transparent)]
pub struct SecretValue(String);

impl SecretValue {
    /// Wraps a secret received at a trusted API or configuration boundary.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes plaintext only to a component that must deliver the secret.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Produces the only API-safe representation of this value.
    pub fn masked(&self) -> MaskedSecret {
        let suffix = if self.0.chars().count() > 4 {
            let mut suffix = self.0.chars().rev().take(4).collect::<Vec<_>>();
            suffix.reverse();
            suffix.into_iter().collect()
        } else {
            String::new()
        };
        MaskedSecret(format!("••••{suffix}"))
    }
}

impl Debug for SecretValue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SecretValue([REDACTED])")
    }
}

/// A safe display form that reveals at most the last four characters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct MaskedSecret(String);

impl MaskedSecret {
    /// Produces a fully redacted display value without a visible suffix.
    pub fn redacted() -> Self {
        Self("••••".to_string())
    }

    /// Returns the masked display text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for MaskedSecret {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let Some(suffix) = value.strip_prefix("••••") else {
            return Err(serde::de::Error::custom(
                "masked secret must begin with the redaction prefix",
            ));
        };
        if suffix.chars().count() > 4 {
            return Err(serde::de::Error::custom(
                "masked secret suffix cannot exceed four characters",
            ));
        }
        Ok(Self(value))
    }
}
