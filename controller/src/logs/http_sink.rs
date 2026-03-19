use std::io::Write;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use flate2::{Compression, write::GzEncoder};
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE};
use serde::Serialize;

use super::sink::LogSink;
use super::store::LogEntry;

pub struct HttpSink {
    id: String,
    endpoint: String,
    client: reqwest::Client,
}

impl HttpSink {
    pub fn new(id: &str, endpoint: &str) -> Self {
        Self {
            id: id.to_string(),
            endpoint: endpoint.to_string(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build http client"),
        }
    }
}

#[async_trait]
impl LogSink for HttpSink {
    fn id(&self) -> &str {
        &self.id
    }

    async fn send(&self, entries: &[LogEntry]) -> Result<()> {
        let compressed_body = gzip_json(entries)?;
        let response = self
            .client
            .post(&self.endpoint)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_ENCODING, "gzip")
            .body(compressed_body)
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("log sink POST failed with {status}: {body}");
        }
        Ok(())
    }
}

fn gzip_json<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(value)?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&payload)?;
    let compressed = encoder
        .finish()
        .map_err(|err| anyhow::anyhow!("failed to finish gzip stream: {err}"))?;
    Ok(compressed)
}
