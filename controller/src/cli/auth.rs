use std::time::{SystemTime, UNIX_EPOCH};

use crate::cli::contexts;
use crate::error::{Error, Result};

const JWT_SECRET_ENV: &str = "JWT_SECRET_KEY";
const DEFAULT_DAYS: u64 = 7;

pub fn run_auth(days: Option<u64>) -> Result<()> {
    let days = days.unwrap_or(DEFAULT_DAYS);
    if days == 0 {
        return Err(Error::invalid_input("--days must be greater than 0"));
    }
    let secret = read_secret()?;
    if secret.trim().is_empty() {
        return Err(Error::invalid_input("JWT secret key cannot be empty"));
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| Error::internal(format!("failed to read system time: {err}")))?
        .as_secs();
    let exp = now + days * 86_400;

    let claims = serde_json::json!({ "iat": now, "exp": exp });
    let key = jsonwebtoken::EncodingKey::from_secret(secret.as_bytes());
    let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    let token = jsonwebtoken::encode(&header, &claims, &key)
        .map_err(|err| Error::internal(format!("failed to encode jwt: {err}")))?;

    contexts::save_active_token(&token)?;
    println!("[maestro]: token saved to active context (expires in {days} days)");
    Ok(())
}

fn read_secret() -> Result<String> {
    if let Ok(value) = std::env::var(JWT_SECRET_ENV) {
        return Ok(value);
    }
    rpassword::prompt_password(format!("{JWT_SECRET_ENV}: "))
        .map_err(|err| Error::internal(format!("failed to read secret: {err}")))
}
