use crate::DuckLogStoreSettings;

#[test]
fn settings_require_an_absolute_path_and_positive_queue() {
    assert!(DuckLogStoreSettings::new("relative.duckdb".into(), 16).is_err());
    assert!(DuckLogStoreSettings::new("/tmp/logs.duckdb".into(), 0).is_err());
    assert!(DuckLogStoreSettings::new("/tmp/logs.duckdb".into(), 16).is_ok());
}
