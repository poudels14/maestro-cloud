use std::collections::BTreeMap;

use kernel_api::Timestamp;

use crate::{JsonLogParser, LogBody, LogParser, PlainTextLogParser};

#[test]
fn json_parser_extracts_canonical_fields_and_retains_attributes() {
    let parsed = JsonLogParser
        .parse(br#"{"ts":1750000000123,"level":"warn","msg":"slow","attempt":2}"#)
        .expect("JSON object should parse");

    assert_eq!(parsed.event_at, Some(Timestamp(1_750_000_000_123)));
    assert_eq!(parsed.severity.as_deref(), Some("warn"));
    assert_eq!(parsed.body, LogBody::Text("slow".to_owned()));
    assert_eq!(
        parsed.attributes,
        BTreeMap::from([("attempt".to_owned(), "2".to_owned())])
    );
}

#[test]
fn plain_text_parser_preserves_invalid_utf8() {
    let payload = [0xff, b'a'];
    let parsed = PlainTextLogParser.parse(&payload).expect("fallback parser");

    assert_eq!(parsed.body, LogBody::Bytes(payload.to_vec()));
}
