use kernel_api::{ServiceId, Timestamp};

use crate::{
    LogHistogramGroupBy, LogHistogramQuery, LogQueryScope, LogReadCursor, LogReadOrder,
    LogReadQuery, LogSequence, MAXIMUM_LOG_QUERY_LIMIT,
};

#[test]
fn read_queries_preserve_typed_filters_and_exclusive_cursors()
-> Result<(), Box<dyn std::error::Error>> {
    let query = LogReadQuery::new(
        LogQueryScope::Service(ServiceId::new("api")?),
        LogReadOrder::NewestFirst,
        50,
    )?
    .within(Some(Timestamp(100)), Some(Timestamp(200)))?
    .with_search("level:error -message:healthcheck".parse()?)
    .with_cursor(LogReadCursor::Before(LogSequence(42)));

    assert_eq!(query.from(), Some(Timestamp(100)));
    assert_eq!(query.to(), Some(Timestamp(200)));
    assert_eq!(query.cursor(), Some(LogReadCursor::Before(LogSequence(42))));
    assert_eq!(query.order(), LogReadOrder::NewestFirst);
    assert!(query.search().is_some());
    Ok(())
}

#[test]
fn query_bounds_are_rejected_before_backend_execution() -> Result<(), Box<dyn std::error::Error>> {
    assert!(LogReadQuery::new(LogQueryScope::All, LogReadOrder::OldestFirst, 0).is_err());
    assert!(
        LogReadQuery::new(
            LogQueryScope::All,
            LogReadOrder::OldestFirst,
            MAXIMUM_LOG_QUERY_LIMIT + 1,
        )
        .is_err()
    );
    assert!(
        LogReadQuery::new(LogQueryScope::All, LogReadOrder::OldestFirst, 1)?
            .within(Some(Timestamp(2)), Some(Timestamp(2)))
            .is_err()
    );
    assert!(
        LogReadQuery::new(
            LogQueryScope::SystemComponent(String::new()),
            LogReadOrder::OldestFirst,
            1,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn histograms_reject_invalid_or_excessive_bucket_counts() {
    assert!(
        LogHistogramQuery::new(
            LogQueryScope::System,
            Timestamp(10),
            Timestamp(10),
            1,
            LogHistogramGroupBy::Level,
        )
        .is_err()
    );
    assert!(
        LogHistogramQuery::new(
            LogQueryScope::System,
            Timestamp(0),
            Timestamp(2_001),
            1,
            LogHistogramGroupBy::HttpStatusClass,
        )
        .is_err()
    );
}
