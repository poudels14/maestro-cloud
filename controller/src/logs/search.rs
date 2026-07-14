#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlDialect {
    Sqlite,
    DuckDb,
}

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

impl std::str::FromStr for LogSearchQuery {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > 4_096 {
            return Err("log query cannot exceed 4096 bytes".to_string());
        }
        let tokens = tokenize(value)?;
        if tokens.is_empty() {
            return Err("log query cannot be empty".to_string());
        }
        if tokens.len() > 256 {
            return Err("log query cannot exceed 256 terms and operators".to_string());
        }
        let mut parser = Parser { tokens, cursor: 0 };
        let expression = parser.parse_or()?;
        if let Some(token) = parser.peek() {
            return Err(format!("unexpected token {}", token_label(token)));
        }
        Ok(Self { expression })
    }
}

impl LogSearchQuery {
    pub(crate) fn compile(&self, dialect: SqlDialect) -> CompiledLogSearch {
        let mut values = Vec::new();
        let sql = compile_expression(&self.expression, dialect, &mut values);
        CompiledLogSearch { sql, values }
    }
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
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
            return Ok(Expression::Not(Box::new(self.parse_unary()?)));
        }
        self.parse_primary()
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
                if !name.starts_with('@') && !matches!(value, FieldValue::Match(_)) {
                    return Err(format!(
                        "reserved field `{name}` does not support numeric comparisons"
                    ));
                }
                Ok(Expression::Predicate(Predicate::Field { name, value }))
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
            if start > end {
                return Err("log query range start cannot exceed its end".to_string());
            }
            return Ok(FieldValue::Range { start, end });
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
        for (prefix, operator) in [
            (">=", Comparison::GreaterOrEqual),
            ("<=", Comparison::LessOrEqual),
            (">", Comparison::Greater),
            ("<", Comparison::Less),
        ] {
            if let Some(raw) = value.strip_prefix(prefix) {
                let value = finite_number(raw)
                    .map_err(|_| format!("invalid numeric comparison `{value}`"))?;
                return Ok(FieldValue::Compare { operator, value });
            }
        }
        Ok(FieldValue::Match(value))
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
        if predicate(&token) {
            Ok(())
        } else {
            Err(format!(
                "expected {expected}, found {}",
                token_label(&token)
            ))
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
        if chars[cursor].is_whitespace() {
            cursor += 1;
            continue;
        }
        let single = match chars[cursor] {
            ':' => Some(Token::Colon),
            '(' => Some(Token::LeftParen),
            ')' => Some(Token::RightParen),
            '[' => Some(Token::LeftBracket),
            ']' => Some(Token::RightBracket),
            '-' => Some(Token::Minus),
            _ => None,
        };
        if let Some(token) = single {
            tokens.push(token);
            cursor += 1;
            continue;
        }
        if chars[cursor] == '"' {
            let (phrase, next) = read_quoted(&chars, cursor + 1)?;
            tokens.push(Token::Phrase(phrase));
            cursor = next;
            continue;
        }

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
        if word.is_empty() {
            return Err("log query contains an empty term".to_string());
        }
        tokens.push(match word.as_str() {
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "TO" => Token::To,
            _ => Token::Word(word),
        });
    }
    Ok(tokens)
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
    if field.is_empty()
        || !field.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        })
    {
        return Err(format!("invalid log field `{name}`"));
    }
    if !name.starts_with('@')
        && !matches!(name, "level" | "status" | "message" | "source" | "service")
    {
        return Err(format!(
            "unknown reserved log field `{name}`; prefix custom attributes with `@`"
        ));
    }
    Ok(())
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

fn compile_expression(
    expression: &Expression,
    dialect: SqlDialect,
    values: &mut Vec<LogSearchValue>,
) -> String {
    match expression {
        Expression::And(left, right) => format!(
            "({} AND {})",
            compile_expression(left, dialect, values),
            compile_expression(right, dialect, values)
        ),
        Expression::Or(left, right) => format!(
            "({} OR {})",
            compile_expression(left, dialect, values),
            compile_expression(right, dialect, values)
        ),
        Expression::Not(inner) => {
            format!("(NOT {})", compile_expression(inner, dialect, values))
        }
        Expression::Predicate(predicate) => compile_predicate(predicate, dialect, values),
    }
}

fn compile_predicate(
    predicate: &Predicate,
    dialect: SqlDialect,
    values: &mut Vec<LogSearchValue>,
) -> String {
    match predicate {
        Predicate::Text(value) => {
            let value = if contains_wildcard(value) {
                value.clone()
            } else {
                format!("*{value}*")
            };
            compile_match("text", &value, true, values)
        }
        Predicate::Field { name, value } => {
            let expression = match name.as_str() {
                "level" | "status" => "level".to_string(),
                "message" => "text".to_string(),
                "source" => "source".to_string(),
                "service" => "source".to_string(),
                "@http.status_code" | "@http.response.status_code" => {
                    canonical_status_expression(dialect)
                }
                attribute if attribute.starts_with('@') => {
                    attribute_expression(&attribute[1..], dialect)
                }
                _ => unreachable!("field names are validated by the parser"),
            };
            if name == "service" {
                return compile_service_match(value, values);
            }
            let case_insensitive = matches!(name.as_str(), "level" | "status" | "message");
            compile_field_value(&expression, value, case_insensitive, dialect, values)
        }
    }
}

fn compile_service_match(value: &FieldValue, values: &mut Vec<LogSearchValue>) -> String {
    match value {
        FieldValue::Match(value) if !contains_wildcard(value) => {
            values.push(LogSearchValue::Text(value.clone()));
            values.push(LogSearchValue::Text(sql_like_prefix(&format!("{value}/"))));
            "(source = ? OR source LIKE ?)".to_string()
        }
        FieldValue::Match(value) => compile_match("source", value, false, values),
        FieldValue::Range { .. } | FieldValue::Compare { .. } => "false".to_string(),
    }
}

fn compile_field_value(
    expression: &str,
    value: &FieldValue,
    case_insensitive: bool,
    dialect: SqlDialect,
    values: &mut Vec<LogSearchValue>,
) -> String {
    match value {
        FieldValue::Match(value) => compile_match(expression, value, case_insensitive, values),
        FieldValue::Range { start, end } => {
            values.push(LogSearchValue::Number(*start));
            values.push(LogSearchValue::Number(*end));
            format!(
                "({} BETWEEN ? AND ?)",
                numeric_expression(expression, dialect)
            )
        }
        FieldValue::Compare { operator, value } => {
            values.push(LogSearchValue::Number(*value));
            let operator = match operator {
                Comparison::Greater => ">",
                Comparison::GreaterOrEqual => ">=",
                Comparison::Less => "<",
                Comparison::LessOrEqual => "<=",
            };
            format!("({} {operator} ?)", numeric_expression(expression, dialect))
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
        return format!("({expression} IS NOT NULL)");
    }
    if contains_wildcard(value) {
        values.push(LogSearchValue::Text(sql_pattern(value)));
        if case_insensitive {
            format!("(LOWER({expression}) LIKE LOWER(?) ESCAPE '\\')")
        } else {
            format!("({expression} LIKE ? ESCAPE '\\')")
        }
    } else {
        values.push(LogSearchValue::Text(value.to_string()));
        if case_insensitive {
            format!("(LOWER({expression}) = LOWER(?))")
        } else {
            format!("({expression} = ?)")
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

pub(crate) fn sql_like_prefix(value: &str) -> String {
    let mut pattern = String::new();
    for character in value.chars() {
        if matches!(character, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(character);
    }
    pattern.push('%');
    pattern
}

fn numeric_expression(expression: &str, dialect: SqlDialect) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("CAST({expression} AS REAL)"),
        SqlDialect::DuckDb => format!("TRY_CAST({expression} AS DOUBLE)"),
    }
}

fn finite_number(value: &str) -> Result<f64, ()> {
    let value = value.parse::<f64>().map_err(|_| ())?;
    value.is_finite().then_some(value).ok_or(())
}

fn attribute_expression(attribute: &str, dialect: SqlDialect) -> String {
    let path = format!("$.\"{attribute}\"");
    match dialect {
        SqlDialect::Sqlite => format!(
            "(SELECT json_extract(attribute.value, '$[1]') \
             FROM json_each(attributes_json) attribute \
             WHERE json_extract(attribute.value, '$[0]') = '{attribute}' LIMIT 1)"
        ),
        SqlDialect::DuckDb => format!("json_extract_string(attributes_json, '{path}')"),
    }
}

fn canonical_status_expression(dialect: SqlDialect) -> String {
    const KEYS: &[&str] = &[
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
    format!(
        "COALESCE({})",
        KEYS.iter()
            .map(|key| attribute_expression(key, dialect))
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
        let compiled = query.compile(SqlDialect::DuckDb);
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
        for dialect in [SqlDialect::Sqlite, SqlDialect::DuckDb] {
            let compiled = query.compile(dialect);
            assert!(compiled.sql.contains("http.status_code"));
            assert!(compiled.sql.contains("DownstreamStatus"));
            assert_eq!(compiled.values, vec![LogSearchValue::Text("503".into())]);
        }
    }

    #[test]
    fn literal_scope_prefixes_do_not_expand_sql_wildcards() {
        assert_eq!(sql_like_prefix("api_v2/%"), r"api\_v2/\%%");
        let query = "service:api_v2".parse::<LogSearchQuery>().expect("query");
        assert_eq!(
            query.compile(SqlDialect::Sqlite).values,
            vec![
                LogSearchValue::Text("api_v2".into()),
                LogSearchValue::Text(r"api\_v2/%".into()),
            ]
        );
    }

    #[test]
    fn rejects_malformed_or_unsupported_queries() {
        for query in [
            "level:",
            "level:error OR",
            "@http.status_code:[500 599]",
            "@http.status_code:[599 TO 500]",
            "unknown:value",
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
