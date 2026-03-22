use std::collections::HashMap;

use anyhow::{Result, anyhow};
use async_trait::async_trait;

#[async_trait]
pub trait SecretProvider: Send + Sync {
    fn prefix(&self) -> &str;
    async fn fetch(&self, reference: &str) -> Result<String>;
}

pub async fn resolve_source(
    providers: &[Box<dyn SecretProvider>],
    source: &str,
) -> Result<HashMap<String, String>> {
    let (provider_idx, reference) = providers
        .iter()
        .enumerate()
        .find_map(|(idx, p)| {
            source
                .strip_prefix(p.prefix())
                .map(|rest| (idx, rest.to_string()))
        })
        .ok_or_else(|| anyhow!("no secret provider matched source `{source}`"))?;

    let raw = providers[provider_idx].fetch(&reference).await?;
    let parsed: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| anyhow!("secret source `{source}` is not valid JSON: {err}"))?;
    let object = parsed
        .as_object()
        .ok_or_else(|| anyhow!("secret source `{source}` is not a JSON object"))?;

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

pub async fn resolve_secrets(
    providers: &[Box<dyn SecretProvider>],
    deployment: &mut crate::deployment::types::ServiceDeployment,
) -> Result<()> {
    if let Some(source) = deployment.config.deploy.env.source.take() {
        let source_items = resolve_source(providers, &source).await?;
        for (key, value) in source_items {
            deployment
                .config
                .deploy
                .env
                .items
                .entry(key)
                .or_insert(value);
        }
    }
    if let Some(build) = &mut deployment.config.build {
        if let Some(source) = build.env.source.take() {
            let source_items = resolve_source(providers, &source).await?;
            for (key, value) in source_items {
                build.env.items.entry(key).or_insert(value);
            }
        }
    }
    if let Some(secrets) = &mut deployment.config.deploy.secrets {
        if let Some(source) = secrets.source.take() {
            let source_items = resolve_source(providers, &source).await?;
            for (key, value) in source_items {
                secrets.items.entry(key).or_insert(value);
            }
        }
    }
    Ok(())
}

pub struct AwsSecretProvider;

#[async_trait]
impl SecretProvider for AwsSecretProvider {
    fn prefix(&self) -> &str {
        "aws-secret://"
    }

    async fn fetch(&self, reference: &str) -> Result<String> {
        if reference.is_empty() {
            return Err(anyhow!("empty AWS secret reference"));
        }
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_secretsmanager::Client::new(&config);
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
