use std::sync::Arc;

use kernel_api::Timestamp;
use logs::{DeadLetterStore, InMemoryDeadLetterStore, LogSequence, LogSinkId, SinkDeadLetter};
use logstore::{DuckLogStoreRuntime, DuckStoreSettings};

use crate::dead_letter_admin::{
    DeadLetterAdminCommand, DeadLetterAdminError, DeadLetterAdminOutput, administer_database,
    execute,
};

#[tokio::test]
async fn list_export_and_purge_are_ordered_bounded_and_no_clobber()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryDeadLetterStore::default());
    let sink_id = LogSinkId::new("datadog")?;
    for sequence in 1..=257_u64 {
        let payload = if sequence == 257 {
            vec![255]
        } else {
            format!(r#"{{"sequence":{sequence}}}"#).into_bytes()
        };
        store
            .record(&SinkDeadLetter {
                sink_id: sink_id.clone(),
                source_sequence: LogSequence(sequence),
                status_code: Some(400),
                reason: "rejected".to_owned(),
                payload,
                recorded_at: Timestamp(i64::try_from(sequence)?),
            })
            .await?;
    }

    let listed = execute(
        store.clone(),
        DeadLetterAdminCommand::List {
            sink_id: sink_id.clone(),
            limit: 2,
        },
    )
    .await?;
    let DeadLetterAdminOutput::Listed(document) = listed else {
        return Err("list returned the wrong output kind".into());
    };
    let document: serde_json::Value = serde_json::from_str(&document)?;
    assert_eq!(document.pointer("/count"), Some(&serde_json::json!(257)));
    assert_eq!(
        document.pointer("/entries/0/sourceSequence"),
        Some(&serde_json::json!(1))
    );
    assert!(document.pointer("/entries/0/payload").is_none());

    let directory = tempfile::tempdir()?;
    let output = directory.path().join("dead-letters.jsonl");
    let exported = execute(
        store.clone(),
        DeadLetterAdminCommand::Export {
            sink_id: sink_id.clone(),
            output: output.clone(),
        },
    )
    .await?;
    assert!(matches!(
        exported,
        DeadLetterAdminOutput::Exported { count: 257, .. }
    ));
    let contents = tokio::fs::read_to_string(&output).await?;
    let lines = contents.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 257);
    let first: serde_json::Value =
        serde_json::from_str(lines.first().ok_or("export has no first record")?)?;
    let last: serde_json::Value =
        serde_json::from_str(lines.last().ok_or("export has no last record")?)?;
    assert_eq!(
        first.pointer("/payload/sequence"),
        Some(&serde_json::json!(1))
    );
    assert_eq!(
        last.pointer("/sourceSequence"),
        Some(&serde_json::json!(257))
    );
    assert_eq!(
        last.pointer("/payloadEncoding"),
        Some(&serde_json::json!("bytes"))
    );
    assert_eq!(last.pointer("/payload"), Some(&serde_json::json!([255])));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(&output)?.permissions().mode() & 0o077, 0);
    }

    assert!(matches!(
        execute(
            store.clone(),
            DeadLetterAdminCommand::Export {
                sink_id: sink_id.clone(),
                output: output.clone(),
            },
        )
        .await,
        Err(DeadLetterAdminError::OutputExists { .. })
    ));
    assert_eq!(tokio::fs::read_to_string(&output).await?, contents);

    let purged = execute(
        store.clone(),
        DeadLetterAdminCommand::Purge {
            sink_id: sink_id.clone(),
            through: Some(LogSequence(256)),
        },
    )
    .await?;
    assert!(matches!(
        purged,
        DeadLetterAdminOutput::Purged { count: 256, .. }
    ));
    assert_eq!(store.stats(&sink_id).await?.count, 1);
    Ok(())
}

#[tokio::test]
async fn existing_duck_store_is_opened_and_missing_store_is_not_created()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("logs.duckdb");
    assert!(matches!(
        administer_database(
            &database,
            DeadLetterAdminCommand::List {
                sink_id: LogSinkId::new("datadog")?,
                limit: 1,
            },
        )
        .await,
        Err(DeadLetterAdminError::DatabaseMissing { .. })
    ));
    assert!(!database.exists());

    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(database.clone(), 8)?).await?;
    let store = runtime.store();
    store
        .record(&SinkDeadLetter {
            sink_id: LogSinkId::new("datadog")?,
            source_sequence: LogSequence(9),
            status_code: Some(413),
            reason: "too large".to_owned(),
            payload: br#"[{"message":"large"}]"#.to_vec(),
            recorded_at: Timestamp(9),
        })
        .await?;
    drop(store);
    runtime.shutdown().await?;

    let output = administer_database(
        &database,
        DeadLetterAdminCommand::List {
            sink_id: LogSinkId::new("datadog")?,
            limit: 1,
        },
    )
    .await?;
    let DeadLetterAdminOutput::Listed(document) = output else {
        return Err("DuckDB list returned the wrong output kind".into());
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&document)?.pointer("/count"),
        Some(&serde_json::json!(1))
    );
    Ok(())
}
