const MAX_QUERY_BYTES: usize = 4_096;
const MAX_QUERY_TOKENS: usize = 256;
const MESSAGE_EXPRESSION: &str = "coalesce(message, line)";
const RESERVED_FIELDS: &[&str] = &[
    "level",
    "status",
    "message",
    "service",
    "deployment",
    "replica",
    "source",
    "stream",
];
const STATUS_ATTRIBUTE_KEYS: &[&str] = &[
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
pub(crate) enum LogSearchValue {
    Text(String),
    Number(f64),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CompiledLogSearch {
    pub sql: String,
    pub values: Vec<LogSearchValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogSearchQuery {
    expression: Expression,
}

#[derive(Debug, Clone, PartialEq)]
enum Expression {
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
    Predicate(Predicate),
}

#[derive(Debug, Clone, PartialEq)]
enum Predicate {
    Text(String),
    Field { name: String, value: FieldValue },
}

#[derive(Debug, Clone, PartialEq)]
enum FieldValue {
    Match(String),
    Range { start: f64, end: f64 },
    Compare { operator: Comparison, value: f64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Comparison {
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),
    Phrase(String),
    Colon,
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    Minus,
    And,
    Or,
    Not,
    To,
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
}

impl std::str::FromStr for LogSearchQuery {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > MAX_QUERY_BYTES {
            return Err(format!("log query cannot exceed {MAX_QUERY_BYTES} bytes"));
        }
        let tokens = tokenize(value)?;
        match tokens.len() {
            0 => Err("log query cannot be empty".to_string()),
            count if count > MAX_QUERY_TOKENS => Err(format!(
                "log query cannot exceed {MAX_QUERY_TOKENS} terms and operators"
            )),
            _ => {
                let mut parser = Parser { tokens, cursor: 0 };
                let expression = parser.parse_or()?;
                match parser.peek() {
                    Some(token) => Err(format!("unexpected token {}", token_label(token))),
                    None => Ok(Self { expression }),
                }
            }
        }
    }
}

impl LogSearchQuery {
    pub(crate) fn compile(&self) -> CompiledLogSearch {
        let mut values = Vec::new();
        let sql = compile_expression(&self.expression, &mut values);
        CompiledLogSearch { sql, values }
    }
}

impl Parser {
    fn parse_or(&mut self) -> Result<Expression, String> {
        let mut expression = self.parse_and()?;
        while matches!(self.peek(), Some(Token::Or)) {
            self.cursor += 1;
            expression = Expression::Or(Box::new(expression), Box::new(self.parse_and()?));
        }
        Ok(expression)
    }

    fn parse_and(&mut self) -> Result<Expression, String> {
        let mut expression = self.parse_unary()?;
        loop {
            if matches!(self.peek(), Some(Token::And)) {
                self.cursor += 1;
            } else if !self.peek().is_some_and(token_starts_expression) {
                break;
            }
            expression = Expression::And(Box::new(expression), Box::new(self.parse_unary()?));
        }
        Ok(expression)
    }

    fn parse_unary(&mut self) -> Result<Expression, String> {
        if matches!(self.peek(), Some(Token::Not | Token::Minus)) {
            self.cursor += 1;
            Ok(Expression::Not(Box::new(self.parse_unary()?)))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<Expression, String> {
        if matches!(self.peek(), Some(Token::LeftParen)) {
            self.cursor += 1;
            let expression = self.parse_or()?;
            self.expect(|token| matches!(token, Token::RightParen), "`)`")?;
            return Ok(expression);
        }

        let token = self
            .next()
            .ok_or_else(|| "expected a log query term".to_string())?;
        match token {
            Token::Word(name) if matches!(self.peek(), Some(Token::Colon)) => {
                self.cursor += 1;
                validate_field_name(&name)?;
                let value = self.parse_field_value()?;
                match name.starts_with('@') || matches!(value, FieldValue::Match(_)) {
                    true => Ok(Expression::Predicate(Predicate::Field { name, value })),
                    false => Err(format!(
                        "reserved field `{name}` does not support numeric comparisons"
                    )),
                }
            }
            Token::Word(value) | Token::Phrase(value) => {
                Ok(Expression::Predicate(Predicate::Text(value)))
            }
            other => Err(format!(
                "expected a log query term, found {}",
                token_label(&other)
            )),
        }
    }

    fn parse_field_value(&mut self) -> Result<FieldValue, String> {
        if matches!(self.peek(), Some(Token::LeftBracket)) {
            self.cursor += 1;
            let start = self.parse_number("range start")?;
            self.expect(|token| matches!(token, Token::To), "`TO`")?;
            let end = self.parse_number("range end")?;
            self.expect(|token| matches!(token, Token::RightBracket), "`]`")?;
            return match start > end {
                true => Err("log query range start cannot exceed its end".to_string()),
                false => Ok(FieldValue::Range { start, end }),
            };
        }

        let value = match self.next() {
            Some(Token::Word(value) | Token::Phrase(value)) => value,
            Some(token) => {
                return Err(format!(
                    "expected a field value, found {}",
                    token_label(&token)
                ));
            }
            None => return Err("expected a field value".to_string()),
        };
        let comparison = [
            (">=", Comparison::GreaterOrEqual),
            ("<=", Comparison::LessOrEqual),
            (">", Comparison::Greater),
            ("<", Comparison::Less),
        ]
        .into_iter()
        .find_map(|(prefix, operator)| value.strip_prefix(prefix).map(|raw| (raw, operator)));
        match comparison {
            Some((raw, operator)) => finite_number(raw)
                .map(|number| FieldValue::Compare {
                    operator,
                    value: number,
                })
                .map_err(|_| format!("invalid numeric comparison `{value}`")),
            None => Ok(FieldValue::Match(value)),
        }
    }

    fn parse_number(&mut self, label: &str) -> Result<f64, String> {
        match self.next() {
            Some(Token::Word(value) | Token::Phrase(value)) => {
                finite_number(&value).map_err(|_| format!("invalid {label} `{value}`"))
            }
            Some(token) => Err(format!("expected {label}, found {}", token_label(&token))),
            None => Err(format!("expected {label}")),
        }
    }

    fn expect(
        &mut self,
        predicate: impl FnOnce(&Token) -> bool,
        expected: &str,
    ) -> Result<(), String> {
        let token = self
            .next()
            .ok_or_else(|| format!("expected {expected}, found end of query"))?;
        match predicate(&token) {
            true => Ok(()),
            false => Err(format!(
                "expected {expected}, found {}",
                token_label(&token)
            )),
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.cursor)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.cursor).cloned();
        self.cursor += usize::from(token.is_some());
        token
    }
}

fn tokenize(value: &str) -> Result<Vec<Token>, String> {
    let chars = value.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut cursor = 0;
    while cursor < chars.len() {
        let single = match chars[cursor] {
            ':' => Some(Token::Colon),
            '(' => Some(Token::LeftParen),
            ')' => Some(Token::RightParen),
            '[' => Some(Token::LeftBracket),
            ']' => Some(Token::RightBracket),
            '-' => Some(Token::Minus),
            _ => None,
        };
        if chars[cursor].is_whitespace() {
            cursor += 1;
        } else if let Some(token) = single {
            tokens.push(token);
            cursor += 1;
        } else if chars[cursor] == '"' {
            let (phrase, next) = read_quoted(&chars, cursor + 1)?;
            tokens.push(Token::Phrase(phrase));
            cursor = next;
        } else {
            let (word, next) = read_word(&chars, cursor)?;
            tokens.push(match word.as_str() {
                "AND" => Token::And,
                "OR" => Token::Or,
                "NOT" => Token::Not,
                "TO" => Token::To,
                _ => Token::Word(word),
            });
            cursor = next;
        }
    }
    Ok(tokens)
}

fn read_word(chars: &[char], mut cursor: usize) -> Result<(String, usize), String> {
    let mut word = String::new();
    while cursor < chars.len()
        && !chars[cursor].is_whitespace()
        && !matches!(chars[cursor], ':' | '(' | ')' | '[' | ']')
    {
        if chars[cursor] == '\\' {
            cursor += 1;
            let escaped = chars
                .get(cursor)
                .ok_or_else(|| "log query cannot end with an escape".to_string())?;
            word.push(*escaped);
        } else {
            word.push(chars[cursor]);
        }
        cursor += 1;
    }
    match word.is_empty() {
        true => Err("log query contains an empty term".to_string()),
        false => Ok((word, cursor)),
    }
}

fn read_quoted(chars: &[char], mut cursor: usize) -> Result<(String, usize), String> {
    let mut value = String::new();
    while cursor < chars.len() {
        match chars[cursor] {
            '"' => return Ok((value, cursor + 1)),
            '\\' => {
                cursor += 1;
                let escaped = chars
                    .get(cursor)
                    .ok_or_else(|| "quoted log query cannot end with an escape".to_string())?;
                value.push(*escaped);
            }
            character => value.push(character),
        }
        cursor += 1;
    }
    Err("unterminated quoted phrase in log query".to_string())
}

fn validate_field_name(name: &str) -> Result<(), String> {
    let field = name.strip_prefix('@').unwrap_or(name);
    let valid_characters = !field.is_empty()
        && field.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        });
    if !valid_characters {
        Err(format!("invalid log field `{name}`"))
    } else if !name.starts_with('@') && !RESERVED_FIELDS.contains(&name) {
        Err(format!(
            "unknown reserved log field `{name}`; prefix custom attributes with `@`"
        ))
    } else {
        Ok(())
    }
}

fn token_starts_expression(token: &Token) -> bool {
    matches!(
        token,
        Token::Word(_) | Token::Phrase(_) | Token::LeftParen | Token::Minus | Token::Not
    )
}

fn token_label(token: &Token) -> String {
    match token {
        Token::Word(value) | Token::Phrase(value) => format!("`{value}`"),
        Token::Colon => "`:`".to_string(),
        Token::LeftParen => "`(`".to_string(),
        Token::RightParen => "`)`".to_string(),
        Token::LeftBracket => "`[`".to_string(),
        Token::RightBracket => "`]`".to_string(),
        Token::Minus => "`-`".to_string(),
        Token::And => "`AND`".to_string(),
        Token::Or => "`OR`".to_string(),
        Token::Not => "`NOT`".to_string(),
        Token::To => "`TO`".to_string(),
    }
}

fn compile_expression(expression: &Expression, values: &mut Vec<LogSearchValue>) -> String {
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
        Expression::Not(inner) => format!("(NOT {})", compile_expression(inner, values)),
        Expression::Predicate(predicate) => compile_predicate(predicate, values),
    }
}

fn compile_predicate(predicate: &Predicate, values: &mut Vec<LogSearchValue>) -> String {
    match predicate {
        Predicate::Text(value) => {
            let pattern = match contains_wildcard(value) {
                true => value.clone(),
                false => format!("*{value}*"),
            };
            compile_match(MESSAGE_EXPRESSION, &pattern, true, values)
        }
        Predicate::Field { name, value } => {
            let expression = match name.as_str() {
                "level" | "status" => "level".to_string(),
                "message" => MESSAGE_EXPRESSION.to_string(),
                "service" => "service_id".to_string(),
                "deployment" => "deployment_id".to_string(),
                "replica" | "source" => "replica_id".to_string(),
                "stream" => "stream".to_string(),
                "@http.status_code" | "@http.response.status_code" => canonical_status_expression(),
                attribute => attribute_expression(attribute.trim_start_matches('@')),
            };
            let case_insensitive = matches!(name.as_str(), "level" | "status" | "message");
            compile_field_value(&expression, value, case_insensitive, values)
        }
    }
}

fn compile_field_value(
    expression: &str,
    value: &FieldValue,
    case_insensitive: bool,
    values: &mut Vec<LogSearchValue>,
) -> String {
    match value {
        FieldValue::Match(value) => compile_match(expression, value, case_insensitive, values),
        FieldValue::Range { start, end } => {
            values.push(LogSearchValue::Number(*start));
            values.push(LogSearchValue::Number(*end));
            format!("({} BETWEEN ? AND ?)", numeric_expression(expression))
        }
        FieldValue::Compare { operator, value } => {
            values.push(LogSearchValue::Number(*value));
            let operator = match operator {
                Comparison::Greater => ">",
                Comparison::GreaterOrEqual => ">=",
                Comparison::Less => "<",
                Comparison::LessOrEqual => "<=",
            };
            format!("({} {operator} ?)", numeric_expression(expression))
        }
    }
}

fn compile_match(
    expression: &str,
    value: &str,
    case_insensitive: bool,
    values: &mut Vec<LogSearchValue>,
) -> String {
    if value == "*" {
        format!("({expression} IS NOT NULL)")
    } else if contains_wildcard(value) {
        values.push(LogSearchValue::Text(sql_pattern(value)));
        match case_insensitive {
            true => format!("(LOWER({expression}) LIKE LOWER(?) ESCAPE '\\')"),
            false => format!("({expression} LIKE ? ESCAPE '\\')"),
        }
    } else {
        values.push(LogSearchValue::Text(value.to_string()));
        match case_insensitive {
            true => format!("(LOWER({expression}) = LOWER(?))"),
            false => format!("({expression} = ?)"),
        }
    }
}

fn contains_wildcard(value: &str) -> bool {
    value.contains(['*', '?'])
}

fn sql_pattern(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '*' => "%".to_string(),
            '?' => "_".to_string(),
            '%' | '_' | '\\' => format!("\\{character}"),
            _ => character.to_string(),
        })
        .collect()
}

fn numeric_expression(expression: &str) -> String {
    format!("TRY_CAST({expression} AS DOUBLE)")
}

fn finite_number(value: &str) -> Result<f64, ()> {
    let number = value.parse::<f64>().map_err(|_| ())?;
    number.is_finite().then_some(number).ok_or(())
}

fn attribute_expression(attribute: &str) -> String {
    let flat = format!("json_extract_string(attributes, '$.\"{attribute}\"')");
    match attribute.contains('.') {
        true => {
            let nested = attribute
                .split('.')
                .map(|segment| format!("\"{segment}\""))
                .collect::<Vec<_>>()
                .join(".");
            format!("COALESCE({flat}, json_extract_string(attributes, '$.{nested}'))")
        }
        false => flat,
    }
}

fn canonical_status_expression() -> String {
    format!(
        "COALESCE({})",
        STATUS_ATTRIBUTE_KEYS
            .iter()
            .map(|key| attribute_expression(key))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn parses_boolean_fields_ranges_and_implicit_and() {
        let query = LogSearchQuery::from_str(
            r#"level:error (@http.status_code:[500 TO 599] OR @http.status_code:429) -message:"health check""#,
        )
        .expect("query");
        let compiled = query.compile();
        assert!(compiled.sql.contains("level"));
        assert!(compiled.sql.contains("BETWEEN ? AND ?"));
        assert!(compiled.sql.contains("NOT"));
        assert_eq!(compiled.values.len(), 5);
    }

    #[test]
    fn status_alias_includes_traefik_and_standard_attributes() {
        let query = "@http.status_code:503"
            .parse::<LogSearchQuery>()
            .expect("query");
        let compiled = query.compile();
        assert!(compiled.sql.contains("http.status_code"));
        assert!(compiled.sql.contains("DownstreamStatus"));
        assert_eq!(compiled.values, vec![LogSearchValue::Text("503".into())]);
    }

    #[test]
    fn dotted_attributes_match_flat_and_nested_json_keys() {
        assert_eq!(
            attribute_expression("user.id"),
            "COALESCE(json_extract_string(attributes, '$.\"user.id\"'), \
             json_extract_string(attributes, '$.\"user\".\"id\"'))"
        );
        assert_eq!(
            attribute_expression("user_id"),
            "json_extract_string(attributes, '$.\"user_id\"')"
        );
    }

    #[test]
    fn free_text_is_a_case_insensitive_contains_match() {
        let compiled = "Timeout"
            .parse::<LogSearchQuery>()
            .expect("query")
            .compile();
        assert_eq!(
            compiled.sql,
            "(LOWER(coalesce(message, line)) LIKE LOWER(?) ESCAPE '\\')"
        );
        assert_eq!(
            compiled.values,
            vec![LogSearchValue::Text("%Timeout%".into())]
        );
    }

    #[test]
    fn wildcards_escape_sql_pattern_characters() {
        let compiled = "service:api_*"
            .parse::<LogSearchQuery>()
            .expect("query")
            .compile();
        assert_eq!(compiled.sql, "(service_id LIKE ? ESCAPE '\\')");
        assert_eq!(
            compiled.values,
            vec![LogSearchValue::Text(r"api\_%".into())]
        );
    }

    #[test]
    fn reserved_fields_map_to_v2_columns() {
        for (query, column) in [
            ("service:api", "service_id"),
            ("deployment:dep1", "deployment_id"),
            ("replica:dep1-0", "replica_id"),
            ("source:dep1-0", "replica_id"),
            ("stream:stderr", "stream"),
        ] {
            let compiled = query.parse::<LogSearchQuery>().expect("query").compile();
            assert!(compiled.sql.contains(column), "{query} -> {}", compiled.sql);
        }
    }

    #[test]
    fn numeric_comparisons_cast_attributes() {
        let compiled = "@duration_ms:>=250"
            .parse::<LogSearchQuery>()
            .expect("query")
            .compile();
        assert!(compiled.sql.contains("TRY_CAST("));
        assert!(compiled.sql.contains(">= ?"));
        assert_eq!(compiled.values, vec![LogSearchValue::Number(250.0)]);
    }

    #[test]
    fn rejects_malformed_or_unsupported_queries() {
        for query in [
            "",
            "level:",
            "level:error OR",
            "@http.status_code:[500 599]",
            "@http.status_code:[599 TO 500]",
            "level:>3",
            "unknown:value",
            "@bad'field:x",
            "(error",
            r#"message:"unterminated"#,
        ] {
            assert!(
                query.parse::<LogSearchQuery>().is_err(),
                "accepted `{query}`"
            );
        }
    }
}
