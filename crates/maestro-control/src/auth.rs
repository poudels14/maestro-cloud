use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use serde_json::json;

const HEALTH_PATH: &str = "/api/health";

pub struct ApiAuth {
    encoding: EncodingKey,
    decoding: DecodingKey,
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    iat: u64,
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
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs());
        let claims = Claims {
            sub: subject.to_string(),
            iat: issued_at,
            exp: issued_at + ttl.as_secs(),
        };
        jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &self.encoding)
            .unwrap_or_default()
    }

    pub fn verify(&self, token: &str) -> bool {
        jsonwebtoken::decode::<Claims>(token, &self.decoding, &Validation::new(Algorithm::HS256))
            .is_ok()
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
    fn rejects_expired_tokens() {
        let auth = ApiAuth::new(b"cluster-secret-for-tests-0000000");
        let claims = Claims {
            sub: "cli".to_string(),
            iat: 1,
            exp: 2,
        };
        let token =
            jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &auth.encoding).unwrap();
        assert!(!auth.verify(&token));
    }
}
