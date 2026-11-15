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
        let fetched = fetch_values(&source).await?;
        merge(&mut secrets.items, &mut secrets.comments, fetched);
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
        let fetched = fetch_values(&source).await?;
        merge(&mut env.items, &mut env.comments, fetched);
    }
    Ok(())
}

/// Values set in the spec win over fetched ones, and so do their (absent) comments.
fn merge(
    items: &mut BTreeMap<String, String>,
    comments: &mut BTreeMap<String, String>,
    fetched: Fetched,
) {
    fetched.values.into_iter().for_each(|(key, value)| {
        if let std::collections::btree_map::Entry::Vacant(entry) = items.entry(key) {
            if let Some(comment) = fetched.comments.get(entry.key()) {
                comments.insert(entry.key().clone(), comment.clone());
            }
            entry.insert(value);
        }
    });
}

struct Fetched {
    values: BTreeMap<String, String>,
    comments: BTreeMap<String, String>,
}

async fn fetch_values(source: &str) -> Result<Fetched, String> {
    fetch_raw(source).await.map(|text| Fetched {
        values: parse_values(&text),
        comments: parse_comments(&text),
    })
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

/// The comment after a value on the same line, such as `KEY=value # v3`; JSON sources have none.
/// Like the dotenv parser, `#` only starts a comment after whitespace or a closing quote.
fn parse_comments(text: &str) -> BTreeMap<String, String> {
    match serde_json::from_str::<serde_json::Value>(text) {
        Ok(_) => BTreeMap::new(),
        Err(_) => text.lines().filter_map(inline_comment).collect(),
    }
}

fn inline_comment(line: &str) -> Option<(String, String)> {
    let line = line.trim_start();
    let (key, value) = line
        .strip_prefix("export ")
        .unwrap_or(line)
        .split_once('=')?;
    let key = key.trim();
    let valid_key = !key.is_empty()
        && key
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_.-".contains(character));
    let value = value.trim_start();
    let after_value = match value.chars().next() {
        Some(quote @ ('"' | '\'')) => closing_quote(value, quote).map(|end| &value[end + 1..]),
        _ => value
            .char_indices()
            .find(|(index, character)| {
                *character == '#'
                    && value[..*index].ends_with(|previous: char| previous.is_whitespace())
            })
            .map(|(index, _)| &value[index..]),
    };
    let comment = after_value?.trim_start().strip_prefix('#')?.trim();
    (valid_key && !comment.is_empty()).then(|| (key.to_string(), comment.to_string()))
}

fn closing_quote(value: &str, quote: char) -> Option<usize> {
    let mut escaped = false;
    value.char_indices().skip(1).find_map(|(index, character)| {
        let closes = character == quote && !(escaped && quote == '"');
        escaped = character == '\\' && !escaped;
        closes.then_some(index)
    })
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
    fn explicit_items_win_over_fetched_values_and_their_comments() {
        let mut items = BTreeMap::from([("TOKEN".to_string(), "explicit".to_string())]);
        let mut comments = BTreeMap::new();
        let text = "TOKEN=fetched # v2\nOTHER=fetched # v7\n";
        merge(
            &mut items,
            &mut comments,
            Fetched {
                values: parse_values(text),
                comments: parse_comments(text),
            },
        );
        assert_eq!(items["TOKEN"], "explicit");
        assert_eq!(items["OTHER"], "fetched");
        assert_eq!(
            comments,
            BTreeMap::from([("OTHER".to_string(), "v7".to_string())])
        );
    }

    #[test]
    fn reads_the_comment_after_each_value() {
        let text = [
            "# Slack alerts",
            "WEBHOOK=https://hooks.slack.com/abc # v3, rotated 2026-11",
            "export POOL=20\t# max connections",
            "QUOTED=\"a # not a comment\" # v2",
            "SINGLE='x#y' #v1",
            "FRAGMENT=https://example.com/#anchor",
            "PLAIN=value",
            "EMPTY=value #",
        ]
        .join("\n");
        assert_eq!(
            parse_comments(&text),
            BTreeMap::from([
                ("POOL".to_string(), "max connections".to_string()),
                ("QUOTED".to_string(), "v2".to_string()),
                ("SINGLE".to_string(), "v1".to_string()),
                ("WEBHOOK".to_string(), "v3, rotated 2026-11".to_string()),
            ])
        );
        assert_eq!(parse_values(&text)["QUOTED"], "a # not a comment");
        assert_eq!(
            parse_values(&text)["WEBHOOK"],
            "https://hooks.slack.com/abc"
        );
        assert_eq!(
            parse_values(&text)["FRAGMENT"],
            "https://example.com/#anchor"
        );
        assert!(parse_comments(r#"{ "TOKEN": "abc # v1" }"#).is_empty());
    }
}
