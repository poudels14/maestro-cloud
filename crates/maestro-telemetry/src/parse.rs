use serde_json::{Map, Value};

const DEFAULT_LEVEL: &str = "info";
const RESERVED_JSON_KEYS: &[&str] = &["ts", "timestamp", "time", "level", "msg", "message"];

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ParsedLine {
    pub level: String,
    pub message: String,
    pub attributes: Map<String, Value>,
}

pub(crate) fn parse_log_line(line: &str) -> ParsedLine {
    let stripped = strip_ansi(line);
    let parsed = match serde_json::from_str::<Value>(&stripped) {
        Ok(Value::Object(object)) => Some(parse_json_line(&stripped, object)),
        _ => parse_iso_prefixed(&stripped)
            .or_else(|| parse_logrus_line(&stripped))
            .or_else(|| parse_slash_prefixed(&stripped)),
    };
    parsed.unwrap_or_else(|| ParsedLine {
        level: DEFAULT_LEVEL.to_string(),
        message: stripped.clone(),
        attributes: Map::new(),
    })
}

fn parse_json_line(line: &str, object: Map<String, Value>) -> ParsedLine {
    let level = object
        .get("level")
        .and_then(Value::as_str)
        .map_or_else(|| DEFAULT_LEVEL.to_string(), normalize_level);
    let message = object
        .get("msg")
        .or_else(|| object.get("message"))
        .and_then(Value::as_str)
        .filter(|message| !message.is_empty())
        .map_or_else(|| line.to_string(), str::to_string);
    let attributes = object
        .into_iter()
        .filter(|(key, _)| !RESERVED_JSON_KEYS.contains(&key.as_str()))
        .collect();
    ParsedLine {
        level,
        message,
        attributes,
    }
}

fn parse_iso_prefixed(line: &str) -> Option<ParsedLine> {
    let looks_like_iso =
        line.len() > 20 && line.as_bytes()[4] == b'-' && line.as_bytes()[10] == b'T';
    let space_index = looks_like_iso
        .then(|| line.get(..35.min(line.len()))?.find(' '))
        .flatten()?;
    is_iso_timestamp(&line[..space_index]).then(|| {
        let rest = line[space_index + 1..].trim_start();
        let leveled = rest.split_once(' ').and_then(|(level, message)| {
            normalize_known_level(level).map(|level| (level, message.trim_start()))
        });
        match leveled {
            Some((level, message)) => ParsedLine {
                level,
                message: message.to_string(),
                attributes: Map::new(),
            },
            None => ParsedLine {
                level: DEFAULT_LEVEL.to_string(),
                message: rest.to_string(),
                attributes: Map::new(),
            },
        }
    })
}

fn parse_logrus_line(line: &str) -> Option<ParsedLine> {
    line.starts_with("time=\"").then(|| {
        let mut level = None;
        let mut message = None;
        let mut attributes = Map::new();
        let bytes = line.as_bytes();
        let mut position = 0;
        while position < bytes.len() {
            while position < bytes.len() && bytes[position] == b' ' {
                position += 1;
            }
            let key_start = position;
            while position < bytes.len() && bytes[position] != b'=' {
                position += 1;
            }
            if position >= bytes.len() {
                break;
            }
            let key = &line[key_start..position];
            position += 1;
            let (value, next) = read_logrus_value(line, position);
            position = next;
            match key {
                "time" => {}
                "level" => level = Some(normalize_level(&value)),
                "msg" => message = Some(value),
                _ => {
                    attributes.insert(key.to_string(), Value::String(value));
                }
            }
        }
        ParsedLine {
            level: level.unwrap_or_else(|| DEFAULT_LEVEL.to_string()),
            message: message.unwrap_or_else(|| line.to_string()),
            attributes,
        }
    })
}

fn read_logrus_value(line: &str, mut position: usize) -> (String, usize) {
    let bytes = line.as_bytes();
    if position < bytes.len() && bytes[position] == b'"' {
        position += 1;
        let value_start = position;
        while position < bytes.len()
            && !(bytes[position] == b'"'
                && (position == value_start || bytes[position - 1] != b'\\'))
        {
            position += 1;
        }
        let value = line[value_start..position].replace("\\\"", "\"");
        (value, (position + 1).min(bytes.len()))
    } else {
        let value_start = position;
        while position < bytes.len() && bytes[position] != b' ' {
            position += 1;
        }
        (line[value_start..position].to_string(), position)
    }
}

fn parse_slash_prefixed(line: &str) -> Option<ParsedLine> {
    let looks_like_slash =
        line.len() > 19 && line.as_bytes()[4] == b'/' && line.as_bytes()[7] == b'/';
    (looks_like_slash
        && line.is_char_boundary(19)
        && chrono::NaiveDateTime::parse_from_str(&line[..19], "%Y/%m/%d %H:%M:%S").is_ok())
    .then(|| ParsedLine {
        level: DEFAULT_LEVEL.to_string(),
        message: line[19..].trim_start().to_string(),
        attributes: Map::new(),
    })
}

fn is_iso_timestamp(value: &str) -> bool {
    let value = value.trim_matches('"');
    chrono::DateTime::parse_from_rfc3339(value).is_ok()
        || chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
}

fn normalize_level(level: &str) -> String {
    match level.to_ascii_lowercase().as_str() {
        "err" | "error" | "fatal" | "panic" => "error".to_string(),
        "warn" | "warning" => "warn".to_string(),
        "info" => "info".to_string(),
        "debug" | "dbg" => "debug".to_string(),
        "trace" => "trace".to_string(),
        other => other.to_string(),
    }
}

fn normalize_known_level(level: &str) -> Option<String> {
    let normalized = normalize_level(level);
    matches!(
        normalized.as_str(),
        "error" | "warn" | "info" | "debug" | "trace"
    )
    .then_some(normalized)
}

fn strip_ansi(line: &str) -> String {
    let bytes = strip_ansi_escapes::strip(line);
    String::from_utf8(bytes)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_with_level_and_msg() {
        let parsed = parse_log_line(
            r#"{"ts":"2026-03-15T20:28:36Z","level":"error","msg":"connection refused"}"#,
        );
        assert_eq!(parsed.message, "connection refused");
        assert_eq!(parsed.level, "error");
        assert!(parsed.attributes.is_empty());
    }

    #[test]
    fn json_with_message_field() {
        let parsed = parse_log_line(r#"{"level":"WARNING","message":"disk full"}"#);
        assert_eq!(parsed.message, "disk full");
        assert_eq!(parsed.level, "warn");
    }

    #[test]
    fn json_keeps_extra_fields_as_typed_attributes() {
        let parsed = parse_log_line(
            r#"{"level":"info","http.method":"GET","http":{"status_code":200},"duration":933354417,"msg":"request"}"#,
        );
        assert_eq!(parsed.message, "request");
        assert_eq!(parsed.attributes.len(), 3);
        assert_eq!(parsed.attributes["http.method"], "GET");
        assert_eq!(parsed.attributes["http"]["status_code"], 200);
        assert_eq!(parsed.attributes["duration"], 933354417);
    }

    #[test]
    fn json_without_msg_uses_full_line() {
        let line = r#"{"level":"debug","data":"something"}"#;
        let parsed = parse_log_line(line);
        assert_eq!(parsed.message, line);
        assert_eq!(parsed.level, "debug");
    }

    #[test]
    fn json_scalars_are_plain_text() {
        let parsed = parse_log_line("42");
        assert_eq!(parsed.message, "42");
        assert!(parsed.attributes.is_empty());
    }

    #[test]
    fn iso_prefixed_with_level() {
        let parsed = parse_log_line("2026-03-15T20:28:36Z ERR Provider error, retrying in 5s");
        assert_eq!(parsed.message, "Provider error, retrying in 5s");
        assert_eq!(parsed.level, "error");
    }

    #[test]
    fn iso_prefixed_unknown_token_is_message_not_level() {
        let parsed =
            parse_log_line("2026-03-15T20:28:36Z HTTPS://HTTPBIN.ORG/STATUS/200 error <urlopen>");
        assert_eq!(
            parsed.message,
            "HTTPS://HTTPBIN.ORG/STATUS/200 error <urlopen>"
        );
        assert_eq!(parsed.level, "info");
    }

    #[test]
    fn logrus_fields_become_attributes() {
        let parsed = parse_log_line(
            r#"time="2026-03-20T05:35:46Z" level=fatal msg="boom \"quoted\"" component=db"#,
        );
        assert_eq!(parsed.level, "error");
        assert_eq!(parsed.message, r#"boom "quoted""#);
        assert_eq!(parsed.attributes["component"], "db");
    }

    #[test]
    fn slash_timestamp_is_stripped() {
        let parsed = parse_log_line("2026/03/18 08:39:04   Starting up on port 80");
        assert_eq!(parsed.message, "Starting up on port 80");
    }

    #[test]
    fn plain_text_defaults_to_info() {
        let parsed = parse_log_line("\x1b[31mjust a plain log message\x1b[0m");
        assert_eq!(parsed.message, "just a plain log message");
        assert_eq!(parsed.level, "info");
    }

    #[test]
    fn normalize_level_aliases() {
        assert_eq!(normalize_level("ERR"), "error");
        assert_eq!(normalize_level("PANIC"), "error");
        assert_eq!(normalize_level("warning"), "warn");
        assert_eq!(normalize_level("DBG"), "debug");
        assert_eq!(normalize_level("custom"), "custom");
    }
}
