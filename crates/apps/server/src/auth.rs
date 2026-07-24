use std::time::SystemTime;

use axum::extract::{Request, State};
use axum::http::{Method, Uri, header};
use axum::middleware::Next;
use axum::response::Response;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use kernel_api::SecretValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::settings::NodeCertificateRequirement;
use crate::{ApiError, VerifiedNodeCertificate};

const OPERATOR_SCOPE: &str = "operator";
const NODE_SCOPE: &str = "node";
const BROWSER_SESSION_AUDIENCE: &str = "maestro-panel";
const BROWSER_SESSION_KIND: &str = "browser-session";
const BROWSER_SESSION_SECONDS: u64 = 8 * 60 * 60;

pub(crate) const BROWSER_SESSION_COOKIE: &str = "__Host-maestro-session";

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

    pub(crate) fn create_browser_session(&self, request: &Request) -> Result<String, ApiError> {
        let secret = self.secret.as_ref().ok_or_else(|| {
            ApiError::service_unavailable(
                "browser sessions are unavailable when loopback authentication is disabled",
            )
        })?;
        let subject = authenticate_bearer(request, secret, OPERATOR_SCOPE)?;
        issue_browser_session(secret, &subject)
    }
}

pub(crate) async fn require_operator(
    State(policy): State<AuthPolicy>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let identity = match &policy.secret {
        None => OperatorIdentity("loopback-operator".to_string()),
        Some(secret) => OperatorIdentity(authenticate_operator(&request, secret)?),
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
        authenticate_bearer(&request, secret, NODE_SCOPE)?;
    }
    Ok(next.run(request).await)
}

fn authenticate_operator(request: &Request, secret: &SecretValue) -> Result<String, ApiError> {
    if request.headers().contains_key(header::AUTHORIZATION) {
        return authenticate_bearer(request, secret, OPERATOR_SCOPE);
    }
    let token = cookie(request, BROWSER_SESSION_COOKIE)
        .ok_or_else(|| ApiError::unauthorized("missing operator authorization"))?;
    validate_cookie_origin(request)?;
    authenticate_browser_session(token, secret)
}

fn authenticate_bearer(
    request: &Request,
    secret: &SecretValue,
    required_scope: &str,
) -> Result<String, ApiError> {
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
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
    if claims.get("kind").and_then(Value::as_str) == Some(BROWSER_SESSION_KIND) {
        return Err(ApiError::unauthorized(
            "browser session credentials are accepted only as cookies",
        ));
    }
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

fn issue_browser_session(secret: &SecretValue, subject: &str) -> Result<String, ApiError> {
    let issued_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| ApiError::internal("system clock is before the Unix epoch"))?
        .as_secs();
    let claims = BrowserSessionClaims {
        subject: subject.to_string(),
        scope: OPERATOR_SCOPE.to_string(),
        issued_at,
        expires_at: issued_at.saturating_add(BROWSER_SESSION_SECONDS),
        audience: BROWSER_SESSION_AUDIENCE.to_string(),
        token_kind: BROWSER_SESSION_KIND.to_string(),
    };
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.expose().as_bytes()),
    )
    .map_err(|_| ApiError::internal("failed to issue browser session"))?;
    Ok(format!(
        "{BROWSER_SESSION_COOKIE}={token}; Path=/; Max-Age={BROWSER_SESSION_SECONDS}; \
         HttpOnly; Secure; SameSite=Strict"
    ))
}

fn authenticate_browser_session(token: &str, secret: &SecretValue) -> Result<String, ApiError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_required_spec_claims(&["aud", "exp", "kind", "scope", "sub"]);
    validation.set_audience(&[BROWSER_SESSION_AUDIENCE]);
    let claims = jsonwebtoken::decode::<BrowserSessionClaims>(
        token,
        &DecodingKey::from_secret(secret.expose().as_bytes()),
        &validation,
    )
    .map_err(|_| ApiError::unauthorized("invalid or expired browser session"))?
    .claims;
    if claims.token_kind != BROWSER_SESSION_KIND
        || !claims
            .scope
            .split_ascii_whitespace()
            .any(|scope| scope == OPERATOR_SCOPE)
    {
        return Err(ApiError::forbidden(
            "browser session does not grant operator access",
        ));
    }
    if claims.subject.trim().is_empty() {
        return Err(ApiError::unauthorized("browser session has no subject"));
    }
    Ok(claims.subject)
}

fn cookie<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request
        .headers()
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find_map(|(cookie_name, value)| {
            (cookie_name == name && !value.is_empty()).then_some(value)
        })
}

fn validate_cookie_origin(request: &Request) -> Result<(), ApiError> {
    if matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) && !request.headers().contains_key(header::UPGRADE)
    {
        return Ok(());
    }
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError::forbidden("cookie-authenticated request has no valid Host"))?;
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<Uri>().ok())
        .ok_or_else(|| ApiError::forbidden("cookie-authenticated request has no valid Origin"))?;
    if origin.authority().map(|authority| authority.as_str()) != Some(host) {
        return Err(ApiError::forbidden(
            "cookie-authenticated request Origin does not match Host",
        ));
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct BrowserSessionClaims {
    #[serde(rename = "sub")]
    subject: String,
    scope: String,
    #[serde(rename = "iat")]
    issued_at: u64,
    #[serde(rename = "exp")]
    expires_at: u64,
    #[serde(rename = "aud")]
    audience: String,
    #[serde(rename = "kind")]
    token_kind: String,
}

pub(crate) fn clear_browser_session() -> String {
    format!(
        "{BROWSER_SESSION_COOKIE}=; Path=/; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT; \
         HttpOnly; Secure; SameSite=Strict"
    )
}
