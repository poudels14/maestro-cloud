use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use serde_json::json;

const HEALTH_PATH: &str = "/api/health";
pub const MAX_TOKEN_TTL: Duration = Duration::from_secs(24 * 60 * 60);

pub struct ApiAuth {
    encoding: EncodingKey,
    decoding: DecodingKey,
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    iat: Option<u64>,
    exp: u64,
}

impl ApiAuth {
    pub fn new(secret: &[u8]) -> Self {
        Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
        }
    }

    pub fn issue(&self, subject: &str, ttl: Duration) -> String {
        let issued_at = unix_now();
        let claims = Claims {
            sub: subject.to_string(),
            iat: Some(issued_at),
            exp: issued_at + ttl.min(MAX_TOKEN_TTL).as_secs(),
        };
        jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &self.encoding)
            .unwrap_or_default()
    }

    pub fn verify(&self, token: &str) -> bool {
        jsonwebtoken::decode::<Claims>(token, &self.decoding, &Validation::new(Algorithm::HS256))
            .is_ok_and(|decoded| {
                let issued_at = decoded.claims.iat.unwrap_or_else(unix_now);
                decoded.claims.exp.saturating_sub(issued_at) <= MAX_TOKEN_TTL.as_secs()
            })
    }
}

pub async fn require_token(
    State(auth): State<Arc<ApiAuth>>,
    request: Request,
    next: Next,
) -> Response {
    let authorized = request.uri().path() == HEALTH_PATH
        || request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|token| auth.verify(token));
    match authorized {
        true => next.run(request).await,
        false => (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({ "error": "missing or invalid API token" })),
        )
            .into_response(),
    }
}

pub async fn require_token_for_config(
    State(auth): State<Arc<ApiAuth>>,
    request: Request,
    next: Next,
) -> Response {
    match changes_service_config(request.method(), request.uri().path()) {
        true => require_token(State(auth), request, next).await,
        false => next.run(request).await,
    }
}

fn changes_service_config(method: &Method, path: &str) -> bool {
    let service_path = path
        .strip_prefix("/api/services/")
        .is_some_and(|rest| !rest.is_empty() && !rest.contains('/'));
    match *method {
        Method::PUT => service_path,
        Method::POST => path == "/api/rollout" || path == "/api/services/up",
        _ => false,
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifies_tokens_signed_with_the_same_secret() {
        let auth = ApiAuth::new(b"cluster-secret-for-tests-0000000");
        let token = auth.issue("cli", Duration::from_secs(60));
        assert!(auth.verify(&token));
        assert!(!ApiAuth::new(b"another-secret-for-tests-0000000").verify(&token));
        assert!(!auth.verify("not-a-token"));
    }

    #[test]
    fn identifies_service_config_changes() {
        assert!(changes_service_config(&Method::PUT, "/api/services/web"));
        assert!(changes_service_config(&Method::POST, "/api/rollout"));
        assert!(changes_service_config(&Method::POST, "/api/services/up"));
        assert!(!changes_service_config(&Method::GET, "/api/services/web"));
        assert!(!changes_service_config(
            &Method::POST,
            "/api/services/web/redeploy"
        ));
        assert!(!changes_service_config(
            &Method::PATCH,
            "/api/services/web/replicas"
        ));
    }

    #[test]
    fn rejects_tokens_issued_for_longer_than_a_day() {
        let auth = ApiAuth::new(b"cluster-secret-for-tests-0000000");
        let now = unix_now();
        let long_lived = Claims {
            sub: "cli".to_string(),
            iat: Some(now),
            exp: now + 30 * 24 * 60 * 60,
        };
        let token =
            jsonwebtoken::encode(&Header::new(Algorithm::HS256), &long_lived, &auth.encoding)
                .unwrap();
        assert!(!auth.verify(&token));
        let capped = auth.issue("cli", Duration::from_secs(30 * 24 * 60 * 60));
        assert!(auth.verify(&capped));
    }

    #[test]
    fn rejects_expired_tokens() {
        let auth = ApiAuth::new(b"cluster-secret-for-tests-0000000");
        let claims = Claims {
            sub: "cli".to_string(),
            iat: Some(1),
            exp: 2,
        };
        let token =
            jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &auth.encoding).unwrap();
        assert!(!auth.verify(&token));
    }

    #[test]
    fn accepts_external_tokens_without_issued_at() {
        let secret = "cluster-secret-for-tests-0000000";
        let auth = ApiAuth::new(secret.as_bytes());
        let sign = |exp: u64| {
            jsonwebtoken::encode(
                &Header::new(Algorithm::HS256),
                &json!({ "sub": "ci", "exp": exp }),
                &EncodingKey::from_secret(secret.as_bytes()),
            )
            .unwrap()
        };
        assert!(auth.verify(&sign(unix_now() + 60 * 60)));
        assert!(!auth.verify(&sign(unix_now() + 30 * 24 * 60 * 60)));
    }
}
