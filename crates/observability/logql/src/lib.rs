//! Backend-neutral parsing contracts for Maestro's retained log query language.
//!
//! This crate owns syntax, validation, and the query AST. Storage adapters compile that AST into
//! parameterized backend operations; this crate never emits SQL or depends on a log store.

mod ast;
mod parse;
mod token;

pub use ast::{Comparison, Expression, Field, FieldValue, LogQuery, Predicate, QueryBackend};
pub use parse::LogQueryParseError;

#[cfg(test)]
mod tests;
