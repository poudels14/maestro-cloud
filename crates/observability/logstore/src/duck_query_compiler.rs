use std::convert::Infallible;

use logql::{Comparison, Expression, Field, FieldValue, MatchCase, Predicate, QueryBackend};

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

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QueryValue {
    Text(String),
    Integer(i64),
    Number(f64),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CompiledPredicate {
    pub(crate) sql: String,
    pub(crate) values: Vec<QueryValue>,
}

pub(crate) struct DuckDbLogQlCompiler;

impl QueryBackend for DuckDbLogQlCompiler {
    type Output = CompiledPredicate;
    type Error = Infallible;

    fn compile(&self, expression: &Expression) -> Result<Self::Output, Self::Error> {
        let mut values = Vec::new();
        let sql = compile_expression(expression, &mut values);
        Ok(CompiledPredicate { sql, values })
    }
}

pub(crate) fn compile_histogram_status() -> CompiledPredicate {
    let mut values = Vec::new();
    let sql = canonical_status_expression(&mut values);
    CompiledPredicate { sql, values }
}

pub(crate) fn compile_json_value(path: &str) -> CompiledPredicate {
    CompiledPredicate {
        sql: "json_extract_string(entry_json, ?)".to_owned(),
        values: vec![QueryValue::Text(path.to_owned())],
    }
}

fn compile_expression(expression: &Expression, values: &mut Vec<QueryValue>) -> String {
    match expression {
        Expression::And(left, right) => format!(
            "({} AND {})",
            compile_expression(left, values),
            compile_expression(right, values)
        ),
        Expression::Or(left, right) => format!(
            "({} OR {})",
            compile_expression(left, values),
            compile_expression(right, values)
        ),
        Expression::Not(expression) => {
            format!("(NOT {})", compile_expression(expression, values))
        }
        Expression::Predicate(predicate) => compile_predicate(predicate, values),
    }
}

fn compile_predicate(predicate: &Predicate, values: &mut Vec<QueryValue>) -> String {
    match predicate {
        Predicate::Text(value) => {
            let message = message_expression(values);
            let value = if contains_wildcard(value) {
                value.clone()
            } else {
                format!("*{value}*")
            };
            compile_match(&message, &value, MatchCase::Insensitive, values)
        }
        Predicate::Field { field, value } => {
            let expression = field_expression(field, values);
            compile_field_value(&expression, value, field.match_case(), values)
        }
    }
}

fn field_expression(field: &Field, values: &mut Vec<QueryValue>) -> String {
    match field {
        Field::Level => json_expression("$.severity", values),
        Field::Message => message_expression(values),
        Field::Source => coalesce_expression(
            &[
                "$.origin.metadata.workloadId",
                "$.origin.component",
                "$.origin.buildId",
            ],
            values,
        ),
        Field::Service => json_expression("$.origin.metadata.serviceId", values),
        Field::HttpStatus => canonical_status_expression(values),
        Field::Attribute(attribute) => {
            json_expression(&format!("$.attributes.\"{attribute}\""), values)
        }
    }
}

fn message_expression(values: &mut Vec<QueryValue>) -> String {
    let body_type = json_expression("$.body.type", values);
    values.push(QueryValue::Text("text".to_owned()));
    let body_value = json_expression("$.body.value", values);
    format!("CASE WHEN {body_type} = ? THEN {body_value} END")
}

fn canonical_status_expression(values: &mut Vec<QueryValue>) -> String {
    coalesce_expression(
        &HTTP_STATUS_KEYS
            .iter()
            .map(|key| format!("$.attributes.\"{key}\""))
            .collect::<Vec<_>>(),
        values,
    )
}

fn coalesce_expression(paths: &[impl AsRef<str>], values: &mut Vec<QueryValue>) -> String {
    let expressions = paths
        .iter()
        .map(|path| json_expression(path.as_ref(), values))
        .collect::<Vec<_>>();
    format!("COALESCE({})", expressions.join(", "))
}

fn json_expression(path: &str, values: &mut Vec<QueryValue>) -> String {
    values.push(QueryValue::Text(path.to_owned()));
    "json_extract_string(entry_json, ?)".to_owned()
}

fn compile_field_value(
    expression: &str,
    value: &FieldValue,
    match_case: MatchCase,
    values: &mut Vec<QueryValue>,
) -> String {
    match value {
        FieldValue::Match(value) => compile_match(expression, value, match_case, values),
        FieldValue::Range { start, end } => {
            values.extend([QueryValue::Number(*start), QueryValue::Number(*end)]);
            format!("(TRY_CAST({expression} AS DOUBLE) BETWEEN ? AND ?)")
        }
        FieldValue::Compare { operator, value } => {
            values.push(QueryValue::Number(*value));
            let operator = match operator {
                Comparison::Greater => ">",
                Comparison::GreaterOrEqual => ">=",
                Comparison::Less => "<",
                Comparison::LessOrEqual => "<=",
            };
            format!("(TRY_CAST({expression} AS DOUBLE) {operator} ?)")
        }
    }
}

fn compile_match(
    expression: &str,
    value: &str,
    match_case: MatchCase,
    values: &mut Vec<QueryValue>,
) -> String {
    if value == "*" {
        return format!("({expression} IS NOT NULL)");
    }
    if contains_wildcard(value) {
        values.push(QueryValue::Text(sql_pattern(value)));
        match match_case {
            MatchCase::Insensitive => {
                format!("(LOWER({expression}) LIKE LOWER(?) ESCAPE '\\')")
            }
            MatchCase::Sensitive => format!("({expression} LIKE ? ESCAPE '\\')"),
        }
    } else {
        values.push(QueryValue::Text(value.to_owned()));
        match match_case {
            MatchCase::Insensitive => format!("(LOWER({expression}) = LOWER(?))"),
            MatchCase::Sensitive => format!("({expression} = ?)"),
        }
    }
}

fn contains_wildcard(value: &str) -> bool {
    value.contains(['*', '?'])
}

fn sql_pattern(value: &str) -> String {
    let mut pattern = String::new();
    for character in value.chars() {
        match character {
            '*' => pattern.push('%'),
            '?' => pattern.push('_'),
            '%' | '_' | '\\' => {
                pattern.push('\\');
                pattern.push(character);
            }
            _ => pattern.push(character),
        }
    }
    pattern
}
