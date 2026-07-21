use std::collections::BTreeMap;

use chrono::NaiveDateTime;
use kernel_api::Timestamp;

use super::{
    LogParser, ParsedLog, ansi_stripped_text, normalize_known_severity, normalize_severity,
    parse_rfc3339_timestamp,
};
use crate::LogBody;

/// Parser for `RFC3339 LEVEL message` records.
#[derive(Debug, Clone, Copy, Default)]
pub struct Rfc3339PrefixedLogParser;

impl LogParser for Rfc3339PrefixedLogParser {
    fn parse(&self, payload: &[u8]) -> Option<ParsedLog> {
        let text = ansi_stripped_text(payload)?;
        let (timestamp, remainder) = text.split_once(' ')?;
        let event_at = parse_rfc3339_timestamp(timestamp)?;
        let remainder = remainder.trim_start();
        let (severity, body) = remainder
            .split_once(' ')
            .and_then(|(candidate, body)| {
                normalize_known_severity(candidate)
                    .map(|severity| (Some(severity), body.trim_start()))
            })
            .unwrap_or((None, remainder));
        Some(ParsedLog {
            event_at: Some(event_at),
            severity,
            body: LogBody::Text(body.to_owned()),
            attributes: BTreeMap::new(),
        })
    }
}

/// Parser for logrus `key=value` records beginning with a quoted time field.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogrusLogParser;

impl LogParser for LogrusLogParser {
    fn parse(&self, payload: &[u8]) -> Option<ParsedLog> {
        let text = ansi_stripped_text(payload)?;
        if !text.starts_with("time=\"") {
            return None;
        }

        let mut remainder = text.as_ref();
        let mut event_at = None;
        let mut severity = None;
        let mut message = None;
        let mut attributes = BTreeMap::new();
        while !remainder.trim_start().is_empty() {
            let ((key, value), rest) = take_field(remainder)?;
            remainder = rest;
            match key {
                "time" => event_at = parse_rfc3339_timestamp(&value),
                "level" => severity = Some(normalize_severity(&value)),
                "msg" => message = Some(value),
                _ => {
                    attributes.insert(key.to_owned(), value);
                }
            }
        }

        Some(ParsedLog {
            event_at,
            severity,
            body: LogBody::Text(message.unwrap_or_else(|| text.into_owned())),
            attributes,
        })
    }
}

/// Parser for `YYYY/MM/DD HH:MM:SS message` records.
#[derive(Debug, Clone, Copy, Default)]
pub struct DatePrefixedLogParser;

impl LogParser for DatePrefixedLogParser {
    fn parse(&self, payload: &[u8]) -> Option<ParsedLog> {
        let text = ansi_stripped_text(payload)?;
        let timestamp = text.get(..19)?;
        let time = NaiveDateTime::parse_from_str(timestamp, "%Y/%m/%d %H:%M:%S").ok()?;
        let body = text.get(19..)?.trim_start();
        Some(ParsedLog {
            event_at: Some(Timestamp(time.and_utc().timestamp_millis())),
            severity: None,
            body: LogBody::Text(body.to_owned()),
            attributes: BTreeMap::new(),
        })
    }
}

fn take_field(line: &str) -> Option<((&str, String), &str)> {
    let line = line.trim_start();
    let (key, value_and_rest) = line.split_once('=')?;
    if key.is_empty() || key.chars().any(char::is_whitespace) {
        return None;
    }

    if let Some(quoted) = value_and_rest.strip_prefix('"') {
        let mut escaped = false;
        for (offset, character) in quoted.char_indices() {
            if character == '"' && !escaped {
                let value = quoted.get(..offset)?.replace("\\\"", "\"");
                let rest = quoted.get(offset + character.len_utf8()..)?;
                return Some(((key, value), rest));
            }
            escaped = character == '\\' && !escaped;
            if character != '\\' {
                escaped = false;
            }
        }
        None
    } else {
        let end = value_and_rest
            .find(char::is_whitespace)
            .unwrap_or(value_and_rest.len());
        Some((
            (key, value_and_rest.get(..end)?.to_owned()),
            value_and_rest.get(end..)?,
        ))
    }
}
