use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::ValueEnum;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use kernel_api::SecretValue;
use serde::{Deserialize, Serialize};

use crate::CliError;
use crate::config_source::{ConfigSourceReader, SystemConfigSourceReader};
use crate::contexts::ContextStore;

pub(crate) const DEFAULT_LOGIN_DAYS: u64 = 7;
const SECONDS_PER_DAY: u64 = 86_400;
const JWT_SECRET_ENV: &str = "JWT_SECRET_KEY";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum AccessLevel {
    ReadOnly,
    Operator,
}

impl AccessLevel {
    fn scope(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::Operator => "operator",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TokenLifetime(u64);

impl TokenLifetime {
    pub(crate) fn seconds(self) -> u64 {
        self.0
    }
}

impl FromStr for TokenLifetime {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        let (number, unit) = value.split_at(value.len().saturating_sub(1));
        let multiplier = match unit {
            "s" => 1,
            "m" => 60,
            "h" => 60 * 60,
            "d" => SECONDS_PER_DAY,
            _ => return Err("lifetime must end in s, m, h, or d (for example 15m or 1h)".into()),
        };
        let number = number
            .parse::<u64>()
            .map_err(|_| "lifetime must start with a positive integer".to_string())?;
        let seconds = number
            .checked_mul(multiplier)
            .ok_or_else(|| "requested token lifetime is too large".to_string())?;
        if seconds == 0 {
            return Err("token lifetime must be greater than zero".into());
        }
        Ok(Self(seconds))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct OperatorClaims {
    pub(crate) sub: String,
    pub(crate) scope: String,
    pub(crate) iat: u64,
    pub(crate) exp: u64,
}

pub(crate) fn login(store: &ContextStore, days: u64) -> Result<(), CliError> {
    let subject = format!("maestro-cli/{}", store.active_name()?);
    let secret = read_secret()?;
    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CliError::invalid_input(format!("system clock is before epoch: {error}")))?
        .as_secs();
    let token = issue_token(&secret, &subject, days, issued_at)?;
    store.save_active_token(token)
}

pub(crate) fn issue_token(
    secret: &SecretValue,
    subject: &str,
    days: u64,
    issued_at: u64,
) -> Result<SecretValue, CliError> {
    let lifetime = days
        .checked_mul(SECONDS_PER_DAY)
        .ok_or_else(|| CliError::invalid_input("requested token lifetime is too large"))?;
    issue_token_for_lifetime(secret, subject, lifetime, AccessLevel::Operator, issued_at)
}

pub(crate) async fn mint_token(
    secret_source: &str,
    lifetime: TokenLifetime,
    access_level: AccessLevel,
    subject: &str,
) -> Result<SecretValue, CliError> {
    let secret = SystemConfigSourceReader.read(secret_source).await?;
    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CliError::invalid_input(format!("system clock is before epoch: {error}")))?
        .as_secs();
    issue_token_for_lifetime(
        &SecretValue::new(secret),
        subject,
        lifetime.seconds(),
        access_level,
        issued_at,
    )
}

pub(crate) fn issue_token_for_lifetime(
    secret: &SecretValue,
    subject: &str,
    lifetime: u64,
    access_level: AccessLevel,
    issued_at: u64,
) -> Result<SecretValue, CliError> {
    if secret.expose().len() < 32 {
        return Err(CliError::invalid_input(
            "JWT secret key must contain at least 32 bytes",
        ));
    }
    if subject.trim().is_empty() {
        return Err(CliError::invalid_input("operator subject cannot be empty"));
    }
    if lifetime == 0 {
        return Err(CliError::invalid_input(
            "token lifetime must be greater than zero",
        ));
    }
    let expires_at = issued_at
        .checked_add(lifetime)
        .ok_or_else(|| CliError::invalid_input("requested token lifetime is too large"))?;
    let claims = OperatorClaims {
        sub: subject.to_string(),
        scope: access_level.scope().to_string(),
        iat: issued_at,
        exp: expires_at,
    };
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.expose().as_bytes()),
    )
    .map_err(CliError::Token)?;
    Ok(SecretValue::new(token))
}

fn read_secret() -> Result<SecretValue, CliError> {
    match std::env::var(JWT_SECRET_ENV) {
        Ok(secret) => Ok(SecretValue::new(secret)),
        Err(std::env::VarError::NotPresent) => {
            rpassword::prompt_password(format!("{JWT_SECRET_ENV}: "))
                .map(SecretValue::new)
                .map_err(|source| CliError::io("failed to read JWT secret", source))
        }
        Err(std::env::VarError::NotUnicode(_)) => Err(CliError::invalid_input(format!(
            "{JWT_SECRET_ENV} is not valid UTF-8"
        ))),
    }
}
