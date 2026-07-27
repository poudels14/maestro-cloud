use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use kernel_api::SecretValue;
use kernel_store::{InMemoryStore, TokioClock};
use tower::ServiceExt;

use super::{seeded_store, token};
use crate::auth::BROWSER_SESSION_COOKIE;
use crate::{ApiServer, ServerSettings};

#[tokio::test]
async fn bearer_exchange_issues_a_scoped_secure_browser_session()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let secret = "browser-session-test-secret-32-bytes";
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, Some(SecretValue::new(secret))),
    )?;
    let operator = token(secret, "operator")?;

    let response = server
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/session")
                .header(header::AUTHORIZATION, format!("Bearer {operator}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let set_cookie = response
        .headers()
        .get(header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .ok_or("session response did not set a cookie")?;
    assert!(set_cookie.starts_with(&format!("{BROWSER_SESSION_COOKIE}=")));
    assert!(set_cookie.contains("; Path=/;"));
    assert!(set_cookie.contains("; Max-Age=28800;"));
    assert!(set_cookie.contains("; HttpOnly;"));
    assert!(set_cookie.contains("; Secure;"));
    assert!(set_cookie.ends_with("; SameSite=Strict"));
    let cookie = set_cookie
        .split(';')
        .next()
        .ok_or("session cookie has no value")?;

    let authenticated = server
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/cluster/nodes")
                .header(header::COOKIE, cookie)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(authenticated.status(), StatusCode::OK);

    let replayed_as_bearer = server
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/cluster/nodes")
                .header(
                    header::AUTHORIZATION,
                    format!(
                        "Bearer {}",
                        cookie
                            .split_once('=')
                            .map(|(_, value)| value)
                            .ok_or("cookie has no token")?
                    ),
                )
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(replayed_as_bearer.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn browser_sessions_fail_closed_for_scope_csrf_and_logout()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let cluster_id = kernel_api::ClusterId::new("session-test")?;
    let secret = "browser-session-test-secret-32-bytes";
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, Some(SecretValue::new(secret))),
    )?;

    for bearer in [None, Some(token(secret, "viewer")?)] {
        let mut request = Request::builder().method("POST").uri("/api/auth/session");
        if let Some(bearer) = bearer {
            request = request.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
        }
        let response = server
            .router()
            .oneshot(request.body(Body::empty())?)
            .await?;
        assert!(matches!(
            response.status(),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
        ));
    }

    let operator = token(secret, "operator")?;
    let response = server
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/session")
                .header(header::AUTHORIZATION, format!("Bearer {operator}"))
                .body(Body::empty())?,
        )
        .await?;
    let cookie = response
        .headers()
        .get(header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .ok_or("session response did not set a cookie")?;
    let mutation = server
        .router()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/services/api")
                .header(header::COOKIE, cookie)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(mutation.status(), StatusCode::FORBIDDEN);

    let logout = server
        .router()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/auth/session")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    let cleared = logout
        .headers()
        .get(header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .ok_or("logout did not clear the cookie")?;
    assert!(cleared.starts_with(&format!("{BROWSER_SESSION_COOKIE}=;")));
    assert!(cleared.contains("; Max-Age=0;"));
    Ok(())
}

#[tokio::test]
async fn read_only_bearers_create_read_only_browser_sessions()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let secret = "browser-session-test-secret-32-bytes";
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, Some(SecretValue::new(secret))),
    )?;
    let read_only = token(secret, "read-only")?;
    let response = server
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/session")
                .header(header::AUTHORIZATION, format!("Bearer {read_only}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let cookie = response
        .headers()
        .get(header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .ok_or("session response did not set a cookie")?;

    let read = server
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/cluster/nodes")
                .header(header::COOKIE, cookie)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(read.status(), StatusCode::OK);

    let mutation = server
        .router()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/services/api")
                .header(header::COOKIE, cookie)
                .header(header::HOST, "localhost")
                .header(header::ORIGIN, "https://localhost")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(mutation.status(), StatusCode::FORBIDDEN);
    Ok(())
}
