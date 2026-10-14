use std::collections::BTreeMap;
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_secretsmanager::Client;
use maestro_core::{ConfigError, EnvSpec, ServiceSpec};
use tokio::sync::OnceCell;

const AWS_SECRET_PREFIX: &str = "aws-secret://";
const FETCH_ATTEMPTS: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(500);

static CLIENT: OnceCell<Client> = OnceCell::const_new();

pub async fn resolve_spec(mut spec: ServiceSpec) -> Result<ServiceSpec, String> {
    resolve_env(&mut spec.deploy.env).await?;
    if let Some(secrets) = spec.deploy.secrets.as_mut()
        && let Some(source) = secrets.source.take()
    {
        merge(&mut secrets.items, fetch_values(&source).await?);
    }
    if let Some(build) = spec.build.as_mut() {
        resolve_env(&mut build.env).await?;
        resolve_env(&mut build.secrets).await?;
    }
    Ok(spec)
}

pub fn read_config_source(source: &str) -> Result<String, ConfigError> {
    match source.strip_prefix(AWS_SECRET_PREFIX) {
        Some(_) => tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(fetch_raw(source))
        })
        .map_err(ConfigError::Invalid),
        None => maestro_core::read_file_source(source),
    }
}

async fn resolve_env(env: &mut EnvSpec) -> Result<(), String> {
    if let Some(source) = env.source.take() {
        merge(&mut env.items, fetch_values(&source).await?);
    }
    Ok(())
}

fn merge(items: &mut BTreeMap<String, String>, fetched: BTreeMap<String, String>) {
    fetched.into_iter().for_each(|(key, value)| {
        items.entry(key).or_insert(value);
    });
}

async fn fetch_values(source: &str) -> Result<BTreeMap<String, String>, String> {
    fetch_raw(source).await.map(|text| parse_values(&text))
}

async fn fetch_raw(source: &str) -> Result<String, String> {
    let secret_id = source
        .strip_prefix(AWS_SECRET_PREFIX)
        .filter(|secret_id| !secret_id.is_empty())
        .ok_or_else(|| format!("unsupported secret source `{source}`"))?;
    let client = CLIENT
        .get_or_init(|| async {
            Client::new(&aws_config::load_defaults(BehaviorVersion::latest()).await)
        })
        .await;
    let fetch = || client.get_secret_value().secret_id(secret_id).send();
    let mut result = fetch().await;
    let mut attempt = 1;
    while attempt < FETCH_ATTEMPTS
        && let Err(error) = &result
    {
        tracing::warn!(%secret_id, attempt, error = %error, "secret fetch failed; retrying");
        tokio::time::sleep(RETRY_DELAY).await;
        attempt += 1;
        result = fetch().await;
    }
    result
        .map_err(|error| {
            format!(
                "failed to fetch secret `{secret_id}`: {}",
                aws_sdk_secretsmanager::error::DisplayErrorContext(error)
            )
        })
        .and_then(|output| {
            output
                .secret_string
                .ok_or_else(|| format!("secret `{secret_id}` has no string value"))
        })
}

fn parse_values(text: &str) -> BTreeMap<String, String> {
    match serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(text) {
        Ok(object) => object
            .into_iter()
            .map(|(key, value)| match value {
                serde_json::Value::String(text) => (key, text),
                other => (key, other.to_string()),
            })
            .collect(),
        Err(_) => dotenvy::from_read_iter(text.as_bytes()).flatten().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_objects_and_dotenv_text() {
        assert_eq!(
            parse_values(r#"{ "TOKEN": "abc", "PORT": 8080, "ON": true }"#),
            BTreeMap::from([
                ("ON".to_string(), "true".to_string()),
                ("PORT".to_string(), "8080".to_string()),
                ("TOKEN".to_string(), "abc".to_string()),
            ])
        );
        assert_eq!(
            parse_values("# comment\nTOKEN=abc\nQUOTED=\"a b\"\n"),
            BTreeMap::from([
                ("QUOTED".to_string(), "a b".to_string()),
                ("TOKEN".to_string(), "abc".to_string()),
            ])
        );
    }

    #[test]
    fn explicit_items_win_over_fetched_values() {
        let mut items = BTreeMap::from([("TOKEN".to_string(), "explicit".to_string())]);
        merge(
            &mut items,
            BTreeMap::from([
                ("TOKEN".to_string(), "fetched".to_string()),
                ("OTHER".to_string(), "fetched".to_string()),
            ]),
        );
        assert_eq!(items["TOKEN"], "explicit");
        assert_eq!(items["OTHER"], "fetched");
    }
}
