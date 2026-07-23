use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::Response;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use kernel_api::SecretValue;
use serde_json::Value;

use crate::settings::NodeCertificateRequirement;
use crate::{ApiError, VerifiedNodeCertificate};

const OPERATOR_SCOPE: &str = "operator";
const NODE_SCOPE: &str = "node";

/// Authenticated operator identity attached to protected requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorIdentity(pub String);

#[derive(Clone)]
pub(crate) struct AuthPolicy {
    secret: Option<SecretValue>,
    node_certificate_requirement: NodeCertificateRequirement,
}

impl AuthPolicy {
    pub(crate) fn new(
        secret: Option<SecretValue>,
        node_certificate_requirement: NodeCertificateRequirement,
    ) -> Self {
        Self {
            secret,
            node_certificate_requirement,
        }
    }
}

pub(crate) async fn require_operator(
    State(policy): State<AuthPolicy>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let identity = match &policy.secret {
        None => OperatorIdentity("loopback-operator".to_string()),
        Some(secret) => OperatorIdentity(authenticate(&request, secret, OPERATOR_SCOPE)?),
    };
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

pub(crate) async fn require_node(
    State(policy): State<AuthPolicy>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if matches!(
        policy.node_certificate_requirement,
        NodeCertificateRequirement::Required
    ) && request
        .extensions()
        .get::<VerifiedNodeCertificate>()
        .is_none()
    {
        return Err(ApiError::forbidden(
            "node endpoint requires a verified cluster client certificate",
        ));
    }
    if let Some(secret) = &policy.secret {
        authenticate(&request, secret, NODE_SCOPE)?;
    }
    Ok(next.run(request).await)
}

fn authenticate(
    request: &Request,
    secret: &SecretValue,
    required_scope: &str,
) -> Result<String, ApiError> {
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
        .ok_or_else(|| ApiError::forbidden("token has no scope claim"))?;
    if !scopes
        .split_ascii_whitespace()
        .any(|scope| scope == required_scope)
    {
        return Err(ApiError::forbidden(format!(
            "token does not grant the `{required_scope}` scope"
        )));
    }
    let subject = claims
        .get("sub")
        .and_then(Value::as_str)
        .filter(|subject| !subject.trim().is_empty())
        .ok_or_else(|| ApiError::unauthorized("token has no subject"))?;
    Ok(subject.to_string())
}
