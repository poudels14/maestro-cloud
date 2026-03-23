use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{Result, anyhow};
use tokio::sync::Mutex;

use crate::logs::Logger;

pub struct SecretProvider {
    source: String,
    reference: String,
    logger: Logger,
}

impl SecretProvider {
    pub fn new(source: &str, logger: &Logger) -> Result<Self> {
        if let Some(reference) = source.strip_prefix("aws-secret://") {
            Ok(Self {
                source: source.to_string(),
                reference: reference.to_string(),
                logger: logger.clone(),
            })
        } else {
            Err(anyhow!("unsupported secret source: {source}"))
        }
    }

    pub async fn fetch_raw(&self) -> Result<String> {
        if self.reference.is_empty() {
            return Err(anyhow!("empty AWS secret reference"));
        }
        let client = aws_client().await;
        let result = client
            .get_secret_value()
            .secret_id(&self.reference)
            .send()
            .await
            .map_err(|err| anyhow!("failed to fetch AWS secret `{}`: {err}", self.reference))?;
        result
            .secret_string()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("AWS secret `{}` has no string value", self.reference))
    }

    pub async fn fetch_kv(&self) -> Result<HashMap<String, String>> {
        let raw = self.fetch_raw().await?;
        let items = parse_kv(&raw);
        self.logger.emit(
            "info",
            &format!("loaded {} keys from `{}`", items.len(), self.source),
        );
        Ok(items)
    }
}

fn parse_kv(raw: &str) -> HashMap<String, String> {
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(raw) {
        if let Some(object) = parsed.as_object() {
            let mut result = HashMap::new();
            for (key, value) in object {
                if let Some(string_value) = value.as_str() {
                    result.insert(key.clone(), string_value.to_string());
                } else {
                    result.insert(key.clone(), value.to_string());
                }
            }
            return result;
        }
    }
    dotenvy::from_read_iter(raw.as_bytes())
        .filter_map(|item| item.ok())
        .collect()
}

static AWS_CLIENT: OnceLock<Mutex<Option<aws_sdk_secretsmanager::Client>>> = OnceLock::new();

async fn aws_client() -> aws_sdk_secretsmanager::Client {
    let mutex = AWS_CLIENT.get_or_init(|| Mutex::new(None));
    let mut guard = mutex.lock().await;
    if let Some(client) = guard.as_ref() {
        client.clone()
    } else {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_secretsmanager::Client::new(&config);
        *guard = Some(client.clone());
        client
    }
}
