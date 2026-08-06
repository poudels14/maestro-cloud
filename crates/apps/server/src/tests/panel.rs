use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use kernel_api::{ClusterId, SecretValue};
use kernel_store::{InMemoryStore, TokioClock};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

#[tokio::test]
async fn panel_assets_and_spa_routes_share_the_api_origin() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    std::fs::create_dir(directory.path().join("assets"))?;
    std::fs::write(
        directory.path().join("index.html"),
        "<main>panel shell</main>",
    )?;
    std::fs::write(
        directory.path().join("assets/app.js"),
        "export const ready = true;",
    )?;
    let server = test_server(directory.path().to_path_buf())?;

    assert_body(&server, "/", "panel shell").await?;
    assert_body(&server, "/cluster/logs", "panel shell").await?;
    assert_body(&server, "/assets/app.js", "ready = true").await?;
    assert_cache_control(&server, "/", "no-store").await?;
    assert_cache_control(&server, "/cluster/logs", "no-store").await?;
    assert_cache_control(
        &server,
        "/assets/app.js",
        "public, max-age=31536000, immutable",
    )
    .await?;

    let conditional = server
        .router()
        .oneshot(
            Request::builder()
                .uri("/cluster/logs")
                .header(header::IF_MODIFIED_SINCE, "Thu, 01 Jan 1970 00:00:01 GMT")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(conditional.status(), StatusCode::OK);
    assert_eq!(
        conditional.headers().get(header::CACHE_CONTROL),
        Some(&header::HeaderValue::from_static("no-store"))
    );
    let body = conditional.into_body().collect().await?.to_bytes();
    assert!(String::from_utf8(body.to_vec())?.contains("panel shell"));

    let missing_asset = server
        .router()
        .oneshot(
            Request::builder()
                .uri("/assets/removed-release.js")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(missing_asset.status(), StatusCode::NOT_FOUND);
    let body = missing_asset.into_body().collect().await?.to_bytes();
    assert!(!String::from_utf8(body.to_vec())?.contains("panel shell"));

    for path in ["/api", "/api/not-a-route"] {
        let response = server
            .router()
            .oneshot(Request::builder().uri(path).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response.into_body().collect().await?.to_bytes();
        assert!(!String::from_utf8(body.to_vec())?.contains("panel shell"));
    }

    let mut outside = Request::builder().uri("/").body(Body::empty())?;
    outside
        .extensions_mut()
        .insert(ConnectInfo("10.1.0.20:40000".parse::<SocketAddr>()?));
    assert_eq!(
        server.router().oneshot(outside).await?.status(),
        StatusCode::FORBIDDEN
    );

    let mut tailnet_proxy = Request::builder().uri("/").body(Body::empty())?;
    tailnet_proxy
        .extensions_mut()
        .insert(ConnectInfo("10.42.1.4:40000".parse::<SocketAddr>()?));
    assert_eq!(
        server.router().oneshot(tailnet_proxy).await?.status(),
        StatusCode::OK
    );
    Ok(())
}

#[test]
fn panel_settings_require_an_absolute_directory_with_an_index()
-> Result<(), Box<dyn std::error::Error>> {
    let relative = ServerSettings::new("127.0.0.1:3000".parse()?, None)
        .with_panel_directory(PathBuf::from("panel"));
    assert!(relative.validate().is_err());

    let directory = tempfile::tempdir()?;
    let missing = ServerSettings::new("127.0.0.1:3000".parse()?, None)
        .with_panel_directory(directory.path().to_path_buf());
    assert!(missing.validate().is_err());
    Ok(())
}

fn test_server(directory: PathBuf) -> Result<ApiServer, Box<dyn std::error::Error>> {
    Ok(ApiServer::new(
        Arc::new(InMemoryStore::new(Arc::new(TokioClock::new()))),
        ClusterId::new("panel-test")?,
        ServerSettings::new(
            "127.0.0.1:3000".parse()?,
            Some(SecretValue::new("s".repeat(32))),
        )
        .with_operator_proxy_cidrs(["10.42.0.0/16".parse()?])
        .with_panel_directory(directory),
    )?)
}

async fn assert_body(
    server: &ApiServer,
    path: &str,
    expected: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = server
        .router()
        .oneshot(
            Request::builder()
                .uri(path)
                .header(header::ACCEPT, "text/html")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await?.to_bytes();
    assert!(String::from_utf8(body.to_vec())?.contains(expected));
    Ok(())
}

async fn assert_cache_control(
    server: &ApiServer,
    path: &str,
    expected: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = server
        .router()
        .oneshot(Request::builder().uri(path).body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some(expected)
    );
    Ok(())
}
