use logql::LogQuery;

use crate::duck_query_compiler::{DuckDbLogQlCompiler, QueryValue, compile_histogram_status};

#[test]
fn compiler_binds_every_user_value_and_attribute_path() -> Result<(), Box<dyn std::error::Error>> {
    let query = r#"message:"x' OR true" @custom:>=12 service:api_*"#.parse::<LogQuery>()?;
    let compiled = query.compile_with(&DuckDbLogQlCompiler)?;
    assert!(!compiled.sql.contains("x' OR true"));
    assert!(!compiled.sql.contains("custom"));
    assert!(
        compiled
            .values
            .contains(&QueryValue::Text("x' OR true".to_owned()))
    );
    assert!(
        compiled
            .values
            .contains(&QueryValue::Text("$.attributes.\"custom\"".to_owned()))
    );
    assert!(compiled.values.contains(&QueryValue::Number(12.0)));
    Ok(())
}

#[test]
fn status_alias_compiles_all_twelve_paths_as_parameters() {
    let compiled = compile_histogram_status();
    assert_eq!(compiled.sql.matches("json_extract_string").count(), 12);
    assert_eq!(compiled.values.len(), 12);
    assert!(compiled.values.contains(&QueryValue::Text(
        "$.attributes.\"DownstreamStatus\"".to_owned()
    )));
}
