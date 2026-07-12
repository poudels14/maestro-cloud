use super::{LogEntry, LogOrigin};

const HEALTHCHECK_PATH_TAG_PREFIX: &str = "maestro.internal.healthcheck-path:";
const HTTP_METHOD_KEYS: &[&str] = &["http.method", "method", "req.method", "request.method"];
const HTTP_STATUS_KEYS: &[&str] = &[
    "http.status_code",
    "status_code",
    "statuscode",
    "status",
    "http.status",
    "response.status_code",
];
const HTTP_PATH_KEYS: &[&str] = &[
    "http.url_details.path",
    "http.path",
    "path",
    "url",
    "route",
    "uri",
    "request.path",
    "target",
];

pub(crate) fn healthcheck_path_tag(path: &str) -> String {
    format!("{HEALTHCHECK_PATH_TAG_PREFIX}{path}")
}

pub(super) fn is_internal_tag(tag: &str) -> bool {
    tag.starts_with("maestro.internal.")
}

pub(super) trait LogFilter: Send + Sync {
    fn matches(&self, entry: &LogEntry) -> bool;
}

#[derive(Default)]
pub(super) struct LogFilterSet {
    filters: Vec<Box<dyn LogFilter>>,
}

impl LogFilterSet {
    pub(super) fn excluding_successful_healthchecks() -> Self {
        Self {
            filters: vec![Box::new(SuccessfulHealthcheckFilter)],
        }
    }

    pub(super) fn excludes(&self, entry: &LogEntry) -> bool {
        self.filters.iter().any(|filter| filter.matches(entry))
    }
}

struct SuccessfulHealthcheckFilter;

impl LogFilter for SuccessfulHealthcheckFilter {
    fn matches(&self, entry: &LogEntry) -> bool {
        if entry.origin != LogOrigin::Service {
            return false;
        }
        let Some(healthcheck_path) = tag_value(&entry.tags, HEALTHCHECK_PATH_TAG_PREFIX) else {
            return false;
        };
        let Some(method) = attr_value(entry, HTTP_METHOD_KEYS) else {
            return false;
        };
        let Some(status) = attr_value(entry, HTTP_STATUS_KEYS) else {
            return false;
        };
        let Some(request_path) = attr_value(entry, HTTP_PATH_KEYS) else {
            return false;
        };

        method.eq_ignore_ascii_case("GET")
            && status.trim() == "200"
            && normalized_path(request_path) == normalized_path(healthcheck_path)
    }
}

fn attr_value<'a>(entry: &'a LogEntry, keys: &[&str]) -> Option<&'a str> {
    entry.attrs.iter().find_map(|(key, value)| {
        keys.iter()
            .any(|candidate| key.eq_ignore_ascii_case(candidate))
            .then_some(value.as_str())
    })
}

fn tag_value<'a>(tags: &'a serde_json::Value, prefix: &str) -> Option<&'a str> {
    tags.as_array()?.iter().find_map(|tag| {
        tag.as_str()
            .and_then(|tag| tag.strip_prefix(prefix))
            .filter(|value| !value.is_empty())
    })
}

fn normalized_path(value: &str) -> String {
    let value = value.trim();
    let value = reqwest::Url::parse(value)
        .ok()
        .map(|url| url.path().to_string())
        .unwrap_or_else(|| value.split(['?', '#']).next().unwrap_or(value).to_string());
    if value.len() > 1 {
        value.trim_end_matches('/').to_string()
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn access_log(method: &str, status: &str, path: &str) -> LogEntry {
        LogEntry {
            seq: 1,
            ts: 1_700_000_000_000,
            level: Arc::from("info"),
            stream: Arc::from("stdout"),
            text: "request".to_string(),
            source: Arc::from("api/dep/replica0"),
            origin: LogOrigin::Service,
            tags: Arc::new(serde_json::json!([
                "service:api",
                healthcheck_path_tag("/health")
            ])),
            attrs: vec![
                ("request.method".to_string(), method.to_string()),
                ("response.status_code".to_string(), status.to_string()),
                ("http.url_details.path".to_string(), path.to_string()),
            ],
        }
    }

    #[test]
    fn successful_healthcheck_filter_matches_canonical_http_aliases() {
        let filters = LogFilterSet::excluding_successful_healthchecks();
        assert!(filters.excludes(&access_log("GET", "200", "/health?full=1")));
        assert!(filters.excludes(&access_log("get", "200", "/health/")));
        assert!(filters.excludes(&access_log(
            "GET",
            "200",
            "http://api.internal/health?full=1"
        )));
    }

    #[test]
    fn successful_healthcheck_filter_keeps_failures_and_other_requests() {
        let filters = LogFilterSet::excluding_successful_healthchecks();
        assert!(!filters.excludes(&access_log("GET", "503", "/health")));
        assert!(!filters.excludes(&access_log("GET", "200", "/api/users")));
        assert!(!filters.excludes(&access_log("POST", "200", "/health")));
    }
}
