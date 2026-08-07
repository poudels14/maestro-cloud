use crate::{DEFAULT_MAX_CONCURRENT_PREVIEWS, PreviewLaunchConfig};

#[test]
fn preview_concurrency_defaults_to_fifty_and_accepts_an_override()
-> Result<(), Box<dyn std::error::Error>> {
    let defaulted: PreviewLaunchConfig = serde_json::from_value(serde_json::json!({
        "domain": "preview.example.test",
        "githubToken": "github-secret"
    }))?;
    assert_eq!(
        defaulted.max_concurrent_previews,
        DEFAULT_MAX_CONCURRENT_PREVIEWS
    );

    let overridden: PreviewLaunchConfig = serde_json::from_value(serde_json::json!({
        "domain": "preview.example.test",
        "githubToken": "github-secret",
        "maxConcurrentPreviews": 75
    }))?;
    assert_eq!(overridden.max_concurrent_previews, 75);
    Ok(())
}
