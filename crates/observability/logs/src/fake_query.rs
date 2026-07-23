use std::collections::BTreeMap;

use async_trait::async_trait;
use logql::{Comparison, Expression, Field, FieldValue, MatchCase, Predicate};

use crate::{
    InMemoryLogStore, IngestLogEntry, LogBody, LogHistogramBucket, LogHistogramGroupBy,
    LogHistogramQuery, LogOrigin, LogQueryScope, LogQueryStore, LogQueryStoreError, LogReadCursor,
    LogReadOrder, LogReadQuery, SequencedLogEntry,
};

const HTTP_STATUS_KEYS: &[&str] = &[
    "http.status_code",
    "http.response.status_code",
    "response.status_code",
    "status_code",
    "statusCode",
    "StatusCode",
    "statuscode",
    "DownstreamStatus",
    "downstreamstatus",
    "http.status",
    "response.status",
    "status",
];

#[async_trait]
impl LogQueryStore for InMemoryLogStore {
    async fn query_logs(
        &self,
        query: &LogReadQuery,
    ) -> Result<Vec<SequencedLogEntry>, LogQueryStoreError> {
        let mut entries = self
            .query_entries()?
            .into_iter()
            .filter(|entry| matches_query(entry, query))
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.sequence);
        if query.order() == LogReadOrder::NewestFirst {
            entries.reverse();
        }
        entries.truncate(query.limit());
        Ok(entries)
    }

    async fn query_log_histogram(
        &self,
        query: &LogHistogramQuery,
    ) -> Result<Vec<LogHistogramBucket>, LogQueryStoreError> {
        let mut buckets = BTreeMap::<i64, LogHistogramBucket>::new();
        for stored in self.query_entries()?.into_iter().filter(|stored| {
            matches_scope(&stored.entry, query.scope())
                && stored.entry.event_at.0 >= query.from().0
                && stored.entry.event_at.0 < query.to().0
                && query
                    .search()
                    .is_none_or(|search| matches_expression(&stored.entry, search.expression()))
        }) {
            let group = match query.group_by() {
                LogHistogramGroupBy::Level => stored.entry.severity.to_lowercase(),
                LogHistogramGroupBy::HttpStatusClass => {
                    let Some(status) = canonical_status(&stored.entry)
                        .and_then(|status| status.parse::<u16>().ok())
                        .filter(|status| (100..=599).contains(status))
                    else {
                        continue;
                    };
                    format!("{}xx", status / 100)
                }
            };
            let bucket_at = stored
                .entry
                .event_at
                .0
                .div_euclid(query.bucket_ms())
                .saturating_mul(query.bucket_ms());
            let bucket = buckets
                .entry(bucket_at)
                .or_insert_with(|| LogHistogramBucket {
                    bucket_at: kernel_api::Timestamp(bucket_at),
                    count: 0,
                    groups: BTreeMap::new(),
                });
            bucket.count = bucket.count.saturating_add(1);
            let count = bucket.groups.entry(group).or_default();
            *count = count.saturating_add(1);
        }
        Ok(buckets.into_values().collect())
    }
}

fn matches_query(stored: &SequencedLogEntry, query: &LogReadQuery) -> bool {
    matches_scope(&stored.entry, query.scope())
        && query
            .from()
            .is_none_or(|from| stored.entry.event_at.0 >= from.0)
        && query.to().is_none_or(|to| stored.entry.event_at.0 < to.0)
        && match query.cursor() {
            None => true,
            Some(LogReadCursor::After(cursor)) => stored.sequence > cursor,
            Some(LogReadCursor::Before(cursor)) => stored.sequence < cursor,
        }
        && query
            .search()
            .is_none_or(|search| matches_expression(&stored.entry, search.expression()))
}

fn matches_scope(entry: &IngestLogEntry, scope: &LogQueryScope) -> bool {
    match (scope, &entry.origin) {
        (LogQueryScope::All, _) => true,
        (LogQueryScope::Service(expected), LogOrigin::Workload { metadata }) => {
            expected == &metadata.service_id
        }
        (LogQueryScope::Deployment(expected), LogOrigin::Workload { metadata }) => {
            expected == &metadata.deployment_id
        }
        (LogQueryScope::System, LogOrigin::System { .. }) => true,
        (LogQueryScope::SystemComponent(expected), LogOrigin::System { component, .. }) => {
            expected == component
        }
        (LogQueryScope::Build(expected), LogOrigin::Build { build_id, .. }) => expected == build_id,
        _ => false,
    }
}

fn matches_expression(entry: &IngestLogEntry, expression: &Expression) -> bool {
    match expression {
        Expression::And(left, right) => {
            matches_expression(entry, left) && matches_expression(entry, right)
        }
        Expression::Or(left, right) => {
            matches_expression(entry, left) || matches_expression(entry, right)
        }
        Expression::Not(expression) => !matches_expression(entry, expression),
        Expression::Predicate(predicate) => matches_predicate(entry, predicate),
    }
}

fn matches_predicate(entry: &IngestLogEntry, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Text(pattern) => text_body(entry).is_some_and(|value| {
            let pattern = if contains_wildcard(pattern) {
                pattern.clone()
            } else {
                format!("*{pattern}*")
            };
            wildcard_matches(value, &pattern, MatchCase::Insensitive)
        }),
        Predicate::Field { field, value } => field_value(entry, field)
            .is_some_and(|candidate| matches_field_value(candidate, value, field)),
    }
}

fn field_value<'a>(entry: &'a IngestLogEntry, field: &Field) -> Option<&'a str> {
    match field {
        Field::Level => Some(&entry.severity),
        Field::Message => text_body(entry),
        Field::Source => match &entry.origin {
            LogOrigin::Workload { metadata } => Some(metadata.workload_id.as_str()),
            LogOrigin::System { component, .. } => Some(component),
            LogOrigin::Build { build_id, .. } => Some(build_id.as_str()),
        },
        Field::Service => match &entry.origin {
            LogOrigin::Workload { metadata } => Some(metadata.service_id.as_str()),
            LogOrigin::System { .. } | LogOrigin::Build { .. } => None,
        },
        Field::HttpStatus => canonical_status(entry),
        Field::Attribute(attribute) => entry.attributes.get(attribute).map(String::as_str),
    }
}

fn matches_field_value(candidate: &str, value: &FieldValue, field: &Field) -> bool {
    match value {
        FieldValue::Match(pattern) => {
            if pattern == "*" {
                true
            } else {
                wildcard_matches(candidate, pattern, field.match_case())
            }
        }
        FieldValue::Range { start, end } => candidate
            .parse::<f64>()
            .is_ok_and(|candidate| candidate >= *start && candidate <= *end),
        FieldValue::Compare { operator, value } => {
            candidate
                .parse::<f64>()
                .is_ok_and(|candidate| match operator {
                    Comparison::Greater => candidate > *value,
                    Comparison::GreaterOrEqual => candidate >= *value,
                    Comparison::Less => candidate < *value,
                    Comparison::LessOrEqual => candidate <= *value,
                })
        }
    }
}

fn text_body(entry: &IngestLogEntry) -> Option<&str> {
    match &entry.body {
        LogBody::Text(body) => Some(body),
        LogBody::Bytes(_) => None,
    }
}

fn canonical_status(entry: &IngestLogEntry) -> Option<&str> {
    HTTP_STATUS_KEYS
        .iter()
        .find_map(|key| entry.attributes.get(*key).map(String::as_str))
}

fn contains_wildcard(value: &str) -> bool {
    value.contains(['*', '?'])
}

fn wildcard_matches(value: &str, pattern: &str, match_case: MatchCase) -> bool {
    let (value, pattern) = match match_case {
        MatchCase::Insensitive => (value.to_lowercase(), pattern.to_lowercase()),
        MatchCase::Sensitive => (value.to_owned(), pattern.to_owned()),
    };
    let value = value.chars().collect::<Vec<_>>();
    let pattern = pattern.chars().collect::<Vec<_>>();
    let (mut value_index, mut pattern_index) = (0, 0);
    let (mut star, mut retry) = (None, 0);
    while let Some(character) = value.get(value_index) {
        match pattern.get(pattern_index) {
            Some('?') => {
                value_index += 1;
                pattern_index += 1;
            }
            Some(candidate) if candidate == character => {
                value_index += 1;
                pattern_index += 1;
            }
            Some('*') => {
                star = Some(pattern_index);
                pattern_index += 1;
                retry = value_index;
            }
            _ if star.is_some() => {
                pattern_index = star.unwrap_or_default() + 1;
                retry += 1;
                value_index = retry;
            }
            _ => return false,
        }
    }
    pattern
        .get(pattern_index..)
        .is_some_and(|remaining| remaining.iter().all(|character| *character == '*'))
}
