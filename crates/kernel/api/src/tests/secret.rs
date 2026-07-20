use crate::SecretValue;

#[test]
fn secret_debug_and_masking_never_reveal_short_values() {
    let short = SecretValue::new("key");
    let long = SecretValue::new("production-token");

    assert_eq!(format!("{short:?}"), "SecretValue([REDACTED])");
    assert_eq!(short.masked().as_str(), "••••");
    assert_eq!(long.masked().as_str(), "••••oken");
}
