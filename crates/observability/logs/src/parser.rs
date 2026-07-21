use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime};
use kernel_api::Timestamp;

use crate::LogBody;

mod access_log;
mod structured_text;

pub use structured_text::{DatePrefixedLogParser, LogrusLogParser, Rfc3339PrefixedLogParser};

use access_log::{is_traefik_access_log, normalize_traefik_attributes, sanitize_request_path};

/// Parser output before node ownership and replay identity are attached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLog {
    /// Producer event time when the format carries a Unix millisecond timestamp.
    pub event_at: Option<Timestamp>,
    /// Producer severity when present.
    pub severity: Option<String>,
    /// Parsed or byte-preserving record body.
    pub body: LogBody,
    /// Structured producer fields not consumed as canonical fields.
    pub attributes: BTreeMap<String, String>,
}

/// One ordered parser in a deterministic log normalization chain.
pub trait LogParser: Send + Sync {
    /// Returns `None` when this parser does not recognize the payload.
    fn parse(&self, payload: &[u8]) -> Option<ParsedLog>;
}

/// Parser for JSON object logs with conventional message, level, and timestamp fields.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonLogParser;

impl LogParser for JsonLogParser {
    fn parse(&self, payload: &[u8]) -> Option<ParsedLog> {
        let text = ansi_stripped_text(payload)?;
        let mut object =
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&text).ok()?;
        let is_access_log = is_traefik_access_log(&object);
        if is_access_log {
            sanitize_request_path(&mut object);
        }
        let fallback_body = if is_access_log {
            serde_json::to_string(&object).unwrap_or_else(|_| text.to_string())
        } else {
            text.to_string()
        };

        let body = take_string(&mut object, &["msg", "message", "text"])
            .filter(|message| !message.is_empty())
            .map_or_else(|| LogBody::Text(fallback_body), LogBody::Text);
        let severity = take_string(&mut object, &["level", "severity"])
            .map(|severity| normalize_severity(&severity));
        let event_at = take_timestamp(&mut object, &["ts", "timestamp", "time"]);
        let mut attributes = object
            .into_iter()
            .map(|(key, value)| (key, attribute_value(value)))
            .collect();
        if is_access_log {
            normalize_traefik_attributes(&mut attributes);
        }
        Some(ParsedLog {
            event_at,
            severity,
            body,
            attributes,
        })
    }
}

/// Byte-preserving final parser that recognizes every payload.
#[derive(Debug, Clone, Copy, Default)]
pub struct PlainTextLogParser;

impl LogParser for PlainTextLogParser {
    fn parse(&self, payload: &[u8]) -> Option<ParsedLog> {
        let body = ansi_stripped_text(payload).map_or_else(
            || LogBody::Bytes(payload.to_vec()),
            |text| LogBody::Text(text.into_owned()),
        );
        Some(ParsedLog {
            event_at: None,
            severity: None,
            body,
            attributes: BTreeMap::new(),
        })
    }
}

/// Returns the baseline parser chain in strict first-match order.
pub fn standard_parsers() -> Vec<Arc<dyn LogParser>> {
    vec![
        Arc::new(JsonLogParser),
        Arc::new(Rfc3339PrefixedLogParser),
        Arc::new(LogrusLogParser),
        Arc::new(DatePrefixedLogParser),
        Arc::new(PlainTextLogParser),
    ]
}

pub(super) fn ansi_stripped_text(payload: &[u8]) -> Option<Cow<'_, str>> {
    let text = std::str::from_utf8(payload).ok()?;
    if payload.contains(&0x1b) {
        String::from_utf8(strip_ansi_escapes::strip(text))
            .ok()
            .map(Cow::Owned)
    } else {
        Some(Cow::Borrowed(text))
    }
}

pub(super) fn parse_rfc3339_timestamp(value: &str) -> Option<Timestamp> {
    DateTime::parse_from_rfc3339(value.trim_matches('"'))
        .map(|time| Timestamp(time.timestamp_millis()))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|time| Timestamp(time.and_utc().timestamp_millis()))
        })
        .ok()
}

pub(super) fn normalize_severity(value: &str) -> String {
    match value.to_ascii_lowercase().as_str() {
        "err" | "error" | "fatal" | "panic" => "error".to_owned(),
        "warn" | "warning" => "warn".to_owned(),
        "debug" | "dbg" => "debug".to_owned(),
        "info" => "info".to_owned(),
        "trace" => "trace".to_owned(),
        other => other.to_owned(),
    }
}

pub(super) fn normalize_known_severity(value: &str) -> Option<String> {
    let normalized = normalize_severity(value);
    matches!(
        normalized.as_str(),
        "error" | "warn" | "info" | "debug" | "trace"
    )
    .then_some(normalized)
}

fn take_string(
    object: &mut serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<String> {
    let key = keys.iter().find(|key| object.contains_key(**key))?;
    object
        .remove(*key)
        .and_then(|value| value.as_str().map(str::to_owned))
}

fn take_timestamp(
    object: &mut serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<Timestamp> {
    let key = keys.iter().find(|key| object.contains_key(**key))?;
    object.remove(*key).and_then(|value| match value {
        serde_json::Value::Number(number) => number.as_i64().map(Timestamp),
        serde_json::Value::String(value) => parse_rfc3339_timestamp(&value),
        _ => None,
    })
}

fn attribute_value(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value,
        value => value.to_string(),
    }
}
