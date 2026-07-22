use std::str::FromStr;

use crate::ast::{Comparison, Expression, Field, FieldValue, LogQuery, Predicate};
use crate::token::{Token, label, starts_expression, tokenize};

const MAXIMUM_QUERY_BYTES: usize = 4_096;
const MAXIMUM_QUERY_TOKENS: usize = 256;

/// Invalid or unbounded LogQL expression.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct LogQueryParseError {
    message: String,
}

impl LogQueryParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl FromStr for LogQuery {
    type Err = LogQueryParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > MAXIMUM_QUERY_BYTES {
            return Err(LogQueryParseError::new(
                "log query cannot exceed 4096 bytes",
            ));
        }
        let tokens = tokenize(value).map_err(LogQueryParseError::new)?;
        if tokens.is_empty() {
            return Err(LogQueryParseError::new("log query cannot be empty"));
        }
        if tokens.len() > MAXIMUM_QUERY_TOKENS {
            return Err(LogQueryParseError::new(
                "log query cannot exceed 256 terms and operators",
            ));
        }
        let mut parser = Parser { tokens, cursor: 0 };
        let expression = parser.parse_or().map_err(LogQueryParseError::new)?;
        if let Some(token) = parser.peek() {
            return Err(LogQueryParseError::new(format!(
                "unexpected token {}",
                label(token)
            )));
        }
        Ok(Self {
            source: value.to_owned(),
            expression,
        })
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
            } else if !self.peek().is_some_and(starts_expression) {
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
                let field = parse_field(&name)?;
                let value = self.parse_field_value()?;
                if !field.supports_numeric_comparison() && !matches!(value, FieldValue::Match(_)) {
                    return Err(format!(
                        "reserved field `{name}` does not support numeric comparisons"
                    ));
                }
                Ok(Expression::Predicate(Predicate::Field { field, value }))
            }
            Token::Word(value) | Token::Phrase(value) => {
                Ok(Expression::Predicate(Predicate::Text(value)))
            }
            other => Err(format!(
                "expected a log query term, found {}",
                label(&other)
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
                return Err(format!("expected a field value, found {}", label(&token)));
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
                let number = finite_number(raw)
                    .map_err(|_| format!("invalid numeric comparison `{value}`"))?;
                return Ok(FieldValue::Compare {
                    operator,
                    value: number,
                });
            }
        }
        Ok(FieldValue::Match(value))
    }

    fn parse_number(&mut self, name: &str) -> Result<f64, String> {
        match self.next() {
            Some(Token::Word(value) | Token::Phrase(value)) => {
                finite_number(&value).map_err(|_| format!("invalid {name} `{value}`"))
            }
            Some(token) => Err(format!("expected {name}, found {}", label(&token))),
            None => Err(format!("expected {name}")),
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
            Err(format!("expected {expected}, found {}", label(&token)))
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

fn parse_field(name: &str) -> Result<Field, String> {
    let attribute = name.strip_prefix('@');
    let field = attribute.unwrap_or(name);
    if field.is_empty()
        || !field.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        })
    {
        return Err(format!("invalid log field `{name}`"));
    }
    match (attribute, name) {
        (Some("http.status_code" | "http.response.status_code"), _) => Ok(Field::HttpStatus),
        (Some(attribute), _) => Ok(Field::Attribute(attribute.to_string())),
        (None, "level" | "status") => Ok(Field::Level),
        (None, "message") => Ok(Field::Message),
        (None, "source") => Ok(Field::Source),
        (None, "service") => Ok(Field::Service),
        (None, _) => Err(format!(
            "unknown reserved log field `{name}`; prefix custom attributes with `@`"
        )),
    }
}

fn finite_number(value: &str) -> Result<f64, ()> {
    let value = value.parse::<f64>().map_err(|_| ())?;
    value.is_finite().then_some(value).ok_or(())
}
