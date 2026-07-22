/// Parsed log query with boolean precedence and validated field names.
#[derive(Debug, Clone)]
pub struct LogQuery {
    pub(crate) source: String,
    pub(crate) expression: Expression,
}

impl PartialEq for LogQuery {
    fn eq(&self, other: &Self) -> bool {
        self.expression == other.expression
    }
}

impl LogQuery {
    /// Returns the validated source text for forwarding to another query endpoint.
    pub fn as_str(&self) -> &str {
        &self.source
    }

    /// Returns the backend-neutral expression tree.
    pub fn expression(&self) -> &Expression {
        &self.expression
    }

    /// Compiles this query through a storage adapter without embedding backend syntax here.
    pub fn compile_with<Backend: QueryBackend>(
        &self,
        backend: &Backend,
    ) -> Result<Backend::Output, Backend::Error> {
        backend.compile(self.expression())
    }
}

/// Storage-specific compilation boundary for one validated expression.
pub trait QueryBackend {
    /// Backend operation produced from the expression tree.
    type Output;
    /// Compilation failure type.
    type Error;

    /// Compiles an expression using parameters or another injection-safe representation.
    fn compile(&self, expression: &Expression) -> Result<Self::Output, Self::Error>;
}

/// Boolean log-query expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// Both operands must match.
    And(Box<Self>, Box<Self>),
    /// Either operand may match.
    Or(Box<Self>, Box<Self>),
    /// The operand must not match.
    Not(Box<Self>),
    /// One text or field predicate.
    Predicate(Predicate),
}

/// Searchable predicate over normalized log fields.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Full-text body match.
    Text(String),
    /// Match or compare one validated field.
    Field { field: Field, value: FieldValue },
}

/// Canonical normalized log field selected by a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Field {
    /// Normalized severity; the retained `status` alias maps here too.
    Level,
    /// Text log body.
    Message,
    /// Complete normalized producer source.
    Source,
    /// Service source including its deployment descendants.
    Service,
    /// Canonical HTTP status across standard and Traefik aliases.
    HttpStatus,
    /// Named structured attribute without the query's `@` prefix.
    Attribute(String),
}

impl Field {
    /// Whether numeric comparisons are valid for this field.
    pub fn supports_numeric_comparison(&self) -> bool {
        matches!(self, Self::HttpStatus | Self::Attribute(_))
    }
}

/// Field operand retained without storage-specific wildcard translation.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// Exact or wildcard textual match.
    Match(String),
    /// Inclusive numeric range.
    Range { start: f64, end: f64 },
    /// One-sided numeric comparison.
    Compare { operator: Comparison, value: f64 },
}

/// Numeric comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}
