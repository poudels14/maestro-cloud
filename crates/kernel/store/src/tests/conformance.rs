use std::sync::Arc;

use kernel_api::ClusterId;

use super::memory::{ManualClock, test_store};
use crate::conformance::{ConformanceReport, run};

#[tokio::test]
async fn memory_backend_passes_the_exported_conformance_battery()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, _clock): (_, ManualClock) = test_store();
    let report = run(Arc::new(store), ClusterId::new("memory-conformance")?).await?;
    assert_eq!(
        report,
        ConformanceReport {
            watch_events: 2,
            conflicts: 1,
            expired_session_keys: 1,
        }
    );
    Ok(())
}
