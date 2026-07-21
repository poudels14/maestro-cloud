use std::collections::BTreeMap;
use std::sync::Arc;

use kernel_api::Timestamp;

use crate::LogBody;

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
        let text = std::str::from_utf8(payload).ok()?;
        let mut object =
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(text).ok()?;
        let body = take_string(&mut object, &["message", "msg", "text"])
            .map_or_else(|| LogBody::Text(text.to_owned()), LogBody::Text);
        let severity = take_string(&mut object, &["level", "severity"]);
        let event_at = take_timestamp(&mut object, &["timestamp", "ts"]);
        let attributes = object
            .into_iter()
            .map(|(key, value)| (key, attribute_value(value)))
            .collect();
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
        let body = match std::str::from_utf8(payload) {
            Ok(text) => LogBody::Text(text.to_owned()),
            Err(_) => LogBody::Bytes(payload.to_vec()),
        };
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
    vec![Arc::new(JsonLogParser), Arc::new(PlainTextLogParser)]
}

fn take_string(
    object: &mut serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<String> {
    keys.iter().find_map(|key| {
        object
            .get(*key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .inspect(|_| {
                object.remove(*key);
            })
    })
}

fn take_timestamp(
    object: &mut serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<Timestamp> {
    keys.iter().find_map(|key| {
        object
            .get(*key)
            .and_then(serde_json::Value::as_i64)
            .map(Timestamp)
            .inspect(|_| {
                object.remove(*key);
            })
    })
}

fn attribute_value(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value,
        value => value.to_string(),
    }
}
