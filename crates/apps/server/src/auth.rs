use std::net::{IpAddr, SocketAddr};
use std::time::SystemTime;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{Method, Uri, header};
use axum::middleware::Next;
use axum::response::Response;
use cluster::Ipv4Cidr;
use cookie::Cookie;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use kernel_api::SecretValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::settings::NodeCertificateRequirement;
use crate::{ApiError, VerifiedNodeCertificate};

const OPERATOR_SCOPE: &str = "operator";
const READ_ONLY_SCOPE: &str = "read-only";
const NODE_SCOPE: &str = "node";
const BROWSER_SESSION_AUDIENCE: &str = "maestro-panel";
const BROWSER_SESSION_KIND: &str = "browser-session";

pub(crate) const BROWSER_SESSION_COOKIE: &str = "__Host-maestro-session";
pub(crate) const TAILNET_BROWSER_SESSION_COOKIE: &str = "maestro-session";

/// Authenticated operator identity attached to protected requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorIdentity(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatorAccess {
    ReadOnly,
    Operator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatorChannel {
    Local,
    TailnetProxy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthenticatedOperator {
    subject: String,
    access: OperatorAccess,
    expires_at: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct AuthPolicy {
    secret: Option<SecretValue>,
    node_certificate_requirement: NodeCertificateRequirement,
    operator_proxy_cidrs: Vec<Ipv4Cidr>,
    local_operator_access: bool,
}

impl AuthPolicy {
    pub(crate) fn new(
        secret: Option<SecretValue>,
        node_certificate_requirement: NodeCertificateRequirement,
        operator_proxy_cidrs: Vec<Ipv4Cidr>,
        local_operator_access: bool,
    ) -> Self {
        Self {
            secret,
            node_certificate_requirement,
            operator_proxy_cidrs,
            local_operator_access,
        }
    }

    pub(crate) fn create_browser_session(&self, request: &Request) -> Result<String, ApiError> {
        let channel = self.operator_channel(request)?;
        let secret = self.secret.as_ref().ok_or_else(|| {
            ApiError::service_unavailable(
                "browser sessions are unavailable when loopback authentication is disabled",
            )
        })?;
        let operator = authenticate_operator_bearer(request, secret)?;
        issue_browser_session(secret, &operator, channel)
    }

    pub(crate) fn clear_browser_session(&self, request: &Request) -> Result<String, ApiError> {
        let channel = self.operator_channel(request)?;
        Ok(clear_browser_session(channel))
    }

    fn operator_channel(&self, request: &Request) -> Result<OperatorChannel, ApiError> {
        let source = request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|source| source.0.ip());
        if self.local_operator_access && source.is_none_or(|source| source.is_loopback()) {
            return Ok(OperatorChannel::Local);
        }
        if source.is_some_and(|source| {
            let IpAddr::V4(source) = source else {
                return false;
            };
            self.operator_proxy_cidrs
                .iter()
                .any(|network| network.contains(source))
        }) {
            return Ok(OperatorChannel::TailnetProxy);
        }
        Err(ApiError::forbidden(
            "operator endpoints are available only through a managed Tailscale gateway",
        ))
    }
}

pub(crate) async fn require_operator(
    State(policy): State<AuthPolicy>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    policy.operator_channel(&request)?;
    let operator = match &policy.secret {
        None => AuthenticatedOperator {
            subject: "loopback-operator".to_string(),
            access: OperatorAccess::Operator,
            expires_at: None,
        },
        Some(secret) => authenticate_operator(&request, secret)?,
    };
    authorize_operator_request(&request, operator.access)?;
    let identity = OperatorIdentity(operator.subject);
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

pub(crate) async fn require_operator_source(
    State(policy): State<AuthPolicy>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    policy.operator_channel(&request)?;
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

fn authenticate_operator(
    request: &Request,
    secret: &SecretValue,
) -> Result<AuthenticatedOperator, ApiError> {
    if request.headers().contains_key(header::AUTHORIZATION) {
        return authenticate_operator_bearer(request, secret);
    }
    let token = cookie(request, BROWSER_SESSION_COOKIE)
        .or_else(|| cookie(request, TAILNET_BROWSER_SESSION_COOKIE))
        .ok_or_else(|| ApiError::unauthorized("missing operator authorization"))?;
    validate_cookie_origin(request)?;
    authenticate_browser_session(&token, secret)
}

fn authenticate_bearer(
    request: &Request,
    secret: &SecretValue,
    required_scope: &str,
) -> Result<String, ApiError> {
    let claims = bearer_claims(request, secret)?;
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
    token_subject(&claims)
}

fn authenticate_operator_bearer(
    request: &Request,
    secret: &SecretValue,
) -> Result<AuthenticatedOperator, ApiError> {
    let claims = bearer_claims(request, secret)?;
    let scopes = claims
        .get("scope")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::forbidden("token has no scope claim"))?;
    let access = operator_access(scopes).ok_or_else(|| {
        ApiError::forbidden("token does not grant the `operator` or `read-only` scope")
    })?;
    let expires_at = claims
        .get("exp")
        .and_then(Value::as_u64)
        .ok_or_else(|| ApiError::unauthorized("operator token has no valid expiration"))?;
    Ok(AuthenticatedOperator {
        subject: token_subject(&claims)?,
        access,
        expires_at: Some(expires_at),
    })
}

fn bearer_claims(request: &Request, secret: &SecretValue) -> Result<Value, ApiError> {
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
    if claims.get("kind").and_then(Value::as_str) == Some(BROWSER_SESSION_KIND) {
        return Err(ApiError::unauthorized(
            "browser session credentials are accepted only as cookies",
        ));
    }
    Ok(claims)
}

fn token_subject(claims: &Value) -> Result<String, ApiError> {
    let subject = claims
        .get("sub")
        .and_then(Value::as_str)
        .filter(|subject| !subject.trim().is_empty())
        .ok_or_else(|| ApiError::unauthorized("token has no subject"))?;
    Ok(subject.to_string())
}

fn operator_access(scopes: &str) -> Option<OperatorAccess> {
    let scopes = scopes.split_ascii_whitespace().collect::<Vec<_>>();
    if scopes.contains(&OPERATOR_SCOPE) {
        Some(OperatorAccess::Operator)
    } else if scopes.contains(&READ_ONLY_SCOPE) {
        Some(OperatorAccess::ReadOnly)
    } else {
        None
    }
}

fn authorize_operator_request(request: &Request, access: OperatorAccess) -> Result<(), ApiError> {
    if access == OperatorAccess::Operator
        || (matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        ) && !request.headers().contains_key(header::UPGRADE))
    {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "read-only credentials cannot execute commands or mutate cluster state",
        ))
    }
}

fn issue_browser_session(
    secret: &SecretValue,
    operator: &AuthenticatedOperator,
    channel: OperatorChannel,
) -> Result<String, ApiError> {
    let issued_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| ApiError::internal("system clock is before the Unix epoch"))?
        .as_secs();
    let expires_at = operator
        .expires_at
        .ok_or_else(|| ApiError::internal("browser session source has no expiration"))?;
    let max_age = expires_at.saturating_sub(issued_at);
    if max_age == 0 {
        return Err(ApiError::unauthorized(
            "operator token expires before a browser session can be issued",
        ));
    }
    let claims = BrowserSessionClaims {
        subject: operator.subject.clone(),
        scope: match operator.access {
            OperatorAccess::ReadOnly => READ_ONLY_SCOPE,
            OperatorAccess::Operator => OPERATOR_SCOPE,
        }
        .to_string(),
        issued_at,
        expires_at,
        audience: BROWSER_SESSION_AUDIENCE.to_string(),
        token_kind: BROWSER_SESSION_KIND.to_string(),
    };
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.expose().as_bytes()),
    )
    .map_err(|_| ApiError::internal("failed to issue browser session"))?;
    let cookie = browser_session_cookie(channel);
    let secure = match channel {
        OperatorChannel::Local => "; Secure",
        OperatorChannel::TailnetProxy => "",
    };
    Ok(format!(
        "{cookie}={token}; Path=/; Max-Age={max_age}; HttpOnly{secure}; SameSite=Strict"
    ))
}

fn authenticate_browser_session(
    token: &str,
    secret: &SecretValue,
) -> Result<AuthenticatedOperator, ApiError> {
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
    if claims.token_kind != BROWSER_SESSION_KIND {
        return Err(ApiError::forbidden("credential is not a browser session"));
    }
    let access = operator_access(&claims.scope).ok_or_else(|| {
        ApiError::forbidden("browser session does not grant operator or read-only access")
    })?;
    if claims.subject.trim().is_empty() {
        return Err(ApiError::unauthorized("browser session has no subject"));
    }
    Ok(AuthenticatedOperator {
        subject: claims.subject,
        access,
        expires_at: Some(claims.expires_at),
    })
}

fn cookie(request: &Request, name: &str) -> Option<String> {
    request
        .headers()
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(Cookie::split_parse)
        .filter_map(Result::ok)
        .find(|cookie| cookie.name() == name && !cookie.value().is_empty())
        .map(|cookie| cookie.value().to_owned())
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

fn browser_session_cookie(channel: OperatorChannel) -> &'static str {
    match channel {
        OperatorChannel::Local => BROWSER_SESSION_COOKIE,
        OperatorChannel::TailnetProxy => TAILNET_BROWSER_SESSION_COOKIE,
    }
}

fn clear_browser_session(channel: OperatorChannel) -> String {
    let cookie = browser_session_cookie(channel);
    let secure = match channel {
        OperatorChannel::Local => "; Secure",
        OperatorChannel::TailnetProxy => "",
    };
    format!(
        "{cookie}=; Path=/; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT; \
         HttpOnly{secure}; SameSite=Strict"
    )
}

#[cfg(test)]
mod cookie_tests {
    use axum::body::Body;
    use axum::http::{Request, header};

    use super::cookie;

    #[test]
    fn parses_request_cookie_headers_with_the_cookie_crate()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = Request::builder()
            .header(header::COOKIE, "malformed; other=first")
            .header(header::COOKIE, "session=token=with=equals; empty=")
            .body(Body::empty())?;

        assert_eq!(
            cookie(&request, "session").as_deref(),
            Some("token=with=equals")
        );
        assert_eq!(cookie(&request, "empty"), None);
        Ok(())
    }
}
