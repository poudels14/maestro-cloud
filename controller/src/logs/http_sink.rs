use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

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
        let response = self
            .client
            .post(&self.endpoint)
            .json(entries)
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
