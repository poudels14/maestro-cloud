use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{Result, anyhow};
use tokio::sync::Mutex;

pub enum SecretProvider {
    Aws,
}

impl SecretProvider {
    fn from_source(source: &str) -> Result<(Self, &str)> {
        if let Some(reference) = source.strip_prefix("aws-secret://") {
            Ok((SecretProvider::Aws, reference))
        } else {
            Err(anyhow!("unsupported secret source: {source}"))
        }
    }

    pub async fn fetch_from_source(source: &str) -> Result<String> {
        let (provider, reference) = Self::from_source(source)?;
        provider.fetch(reference).await
    }

    pub async fn fetch_json_from_source(source: &str) -> Result<HashMap<String, String>> {
        let (provider, reference) = Self::from_source(source)?;
        provider.fetch_json(reference).await
    }

    async fn fetch(&self, reference: &str) -> Result<String> {
        match self {
            SecretProvider::Aws => {
                if reference.is_empty() {
                    return Err(anyhow!("empty AWS secret reference"));
                }
                let client = aws_client().await;
                let result = client
                    .get_secret_value()
                    .secret_id(reference)
                    .send()
                    .await
                    .map_err(|err| anyhow!("failed to fetch AWS secret `{reference}`: {err}"))?;
                let secret_string = result
                    .secret_string()
                    .ok_or_else(|| anyhow!("AWS secret `{reference}` has no string value"))?;
                Ok(secret_string.to_string())
            }
        }
    }

    async fn fetch_json(&self, reference: &str) -> Result<HashMap<String, String>> {
        let raw = self.fetch(reference).await?;
        let parsed: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|err| anyhow!("secret `{reference}` is not valid JSON: {err}"))?;
        let object = parsed
            .as_object()
            .ok_or_else(|| anyhow!("secret `{reference}` is not a JSON object"))?;
        let mut result = HashMap::new();
        for (key, value) in object {
            if let Some(string_value) = value.as_str() {
                result.insert(key.clone(), string_value.to_string());
            } else {
                result.insert(key.clone(), value.to_string());
            }
        }
        Ok(result)
    }
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
