use crate::error::{Error, Result};

pub async fn run_config(host: &str) -> Result<()> {
    let base = normalize_base_url(host)?;
    let endpoint = format!("{base}/api/config");
    let response = reqwest::Client::new()
        .get(&endpoint)
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call config endpoint: {err}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "config request failed with status {status}: {body}"
        )));
    }

    let body = response
        .json::<serde_json::Value>()
        .await
        .map_err(|err| Error::external(format!("failed to decode config response: {err}")))?;
    let pretty = serde_json::to_string_pretty(&body)
        .map_err(|err| Error::internal(format!("failed to format config: {err}")))?;
    println!("{pretty}");
    Ok(())
}

fn normalize_base_url(host: &str) -> Result<String> {
    let host = host.trim();
    if host.is_empty() {
        return Err(Error::invalid_input("host cannot be empty"));
    }

    let base = if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    };

    Ok(base.trim_end_matches('/').to_string())
}
