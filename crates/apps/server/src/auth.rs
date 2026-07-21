use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::Response;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use kernel_api::SecretValue;
use serde_json::Value;

use crate::ApiError;

const OPERATOR_SCOPE: &str = "operator";

/// Authenticated operator identity attached to protected requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorIdentity(pub String);

#[derive(Clone)]
pub(crate) struct AuthPolicy {
    secret: Option<SecretValue>,
}

impl AuthPolicy {
    pub(crate) fn new(secret: Option<SecretValue>) -> Self {
        Self { secret }
    }
}

pub(crate) async fn require_operator(
    State(policy): State<AuthPolicy>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let identity = match &policy.secret {
        None => OperatorIdentity("loopback-operator".to_string()),
        Some(secret) => authenticate(&request, secret)?,
    };
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

fn authenticate(request: &Request, secret: &SecretValue) -> Result<OperatorIdentity, ApiError> {
    let token = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| !token.is_empty())
        .ok_or_else(|| ApiError::unauthorized("missing or invalid Authorization header"))?;
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_required_spec_claims(&["exp", "scope", "sub"]);
    let claims = jsonwebtoken::decode::<Value>(
        token,
        &DecodingKey::from_secret(secret.expose().as_bytes()),
        &validation,
    )
    .map_err(|_| ApiError::unauthorized("invalid or expired operator token"))?
    .claims;
    let scopes = claims
        .get("scope")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::forbidden("operator token has no scope claim"))?;
    if !scopes
        .split_ascii_whitespace()
        .any(|scope| scope == OPERATOR_SCOPE)
    {
        return Err(ApiError::forbidden(
            "operator token does not grant the operator scope",
        ));
    }
    let subject = claims
        .get("sub")
        .and_then(Value::as_str)
        .filter(|subject| !subject.trim().is_empty())
        .ok_or_else(|| ApiError::unauthorized("operator token has no subject"))?;
    Ok(OperatorIdentity(subject.to_string()))
}
