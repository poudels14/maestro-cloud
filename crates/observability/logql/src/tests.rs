use std::convert::Infallible;

use proptest::prelude::*;

use crate::{Comparison, Expression, Field, FieldValue, LogQuery, Predicate, QueryBackend};

#[test]
fn parses_boolean_fields_ranges_and_implicit_and() -> Result<(), Box<dyn std::error::Error>> {
    let query = r#"level:error (@http.status_code:[500 TO 599] OR @http.status_code:429) -message:"health check""#
        .parse::<LogQuery>()?;
    assert!(matches!(query.expression(), Expression::And(_, _)));
    let mut fields = Vec::new();
    collect_fields(query.expression(), &mut fields);
    assert_eq!(
        fields,
        [
            Field::Level,
            Field::HttpStatus,
            Field::HttpStatus,
            Field::Message
        ]
    );
    Ok(())
}

#[test]
fn retains_numeric_and_wildcard_operands_for_backends() -> Result<(), Box<dyn std::error::Error>> {
    let query = "@duration:>=12.5 service:api_*".parse::<LogQuery>()?;
    let Expression::And(left, right) = query.expression() else {
        return Err("expected implicit conjunction".into());
    };
    assert!(matches!(
        left.as_ref(),
        Expression::Predicate(Predicate::Field {
            field: Field::Attribute(attribute),
            value: FieldValue::Compare {
                operator: Comparison::GreaterOrEqual,
                value: 12.5,
            },
        }) if attribute == "duration"
    ));
    assert!(matches!(
        right.as_ref(),
        Expression::Predicate(Predicate::Field {
            field: Field::Service,
            value: FieldValue::Match(value),
        }) if value == "api_*"
    ));
    Ok(())
}

#[test]
fn backend_compilation_receives_only_a_validated_ast() -> Result<(), Box<dyn std::error::Error>> {
    struct PredicateCounter;

    impl QueryBackend for PredicateCounter {
        type Output = usize;
        type Error = Infallible;

        fn compile(&self, expression: &Expression) -> Result<Self::Output, Self::Error> {
            Ok(count_predicates(expression))
        }
    }

    let query = "error OR (level:warn -@retry:true)".parse::<LogQuery>()?;
    assert_eq!(query.compile_with(&PredicateCounter)?, 3);
    Ok(())
}

#[test]
fn retains_validated_source_for_transport_without_recompiling_the_ast()
-> Result<(), Box<dyn std::error::Error>> {
    let source = r#"message:\"x & y\" AND @attempt:>=2"#;
    let query = source.parse::<LogQuery>()?;

    assert_eq!(query.as_str(), source);
    assert_eq!("error warn".parse::<LogQuery>()?, "error AND warn".parse()?);
    Ok(())
}

#[test]
fn rejects_malformed_unsupported_and_unbounded_queries() {
    for query in [
        "",
        "level:",
        "level:error OR",
        "@http.status_code:[500 599]",
        "@http.status_code:[599 TO 500]",
        "unknown:value",
        "(error",
        r#"message:"unterminated"#,
    ] {
        assert!(
            query.parse::<LogQuery>().is_err(),
            "accepted malformed query `{query}`"
        );
    }
    assert!("x".repeat(4_097).parse::<LogQuery>().is_err());
    let too_many_terms = std::iter::repeat_n("error", 257)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(too_many_terms.parse::<LogQuery>().is_err());
}

fn collect_fields(expression: &Expression, fields: &mut Vec<Field>) {
    match expression {
        Expression::And(left, right) | Expression::Or(left, right) => {
            collect_fields(left, fields);
            collect_fields(right, fields);
        }
        Expression::Not(expression) => collect_fields(expression, fields),
        Expression::Predicate(Predicate::Field { field, .. }) => fields.push(field.clone()),
        Expression::Predicate(Predicate::Text(_)) => {}
    }
}

fn count_predicates(expression: &Expression) -> usize {
    match expression {
        Expression::And(left, right) | Expression::Or(left, right) => {
            count_predicates(left).saturating_add(count_predicates(right))
        }
        Expression::Not(expression) => count_predicates(expression),
        Expression::Predicate(_) => 1,
    }
}

proptest! {
    #[test]
    fn parser_is_total_over_arbitrary_unicode(input in any::<String>()) {
        let _result = input.parse::<LogQuery>();
    }
}
