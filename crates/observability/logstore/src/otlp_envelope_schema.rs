use duckdb::{Connection, OptionalExt, params};
use logs::{OtlpEnvelope, OtlpEnvelopeAppendReport, OtlpEnvelopeStoreError};

pub(crate) fn append(
    connection: &mut Connection,
    envelopes: &[OtlpEnvelope],
) -> Result<OtlpEnvelopeAppendReport, OtlpEnvelopeStoreError> {
    for envelope in envelopes {
        envelope
            .validate()
            .map_err(|error| OtlpEnvelopeStoreError::Rejected {
                message: error.to_string(),
            })?;
    }
    let transaction = connection
        .transaction()
        .map_err(unavailable("begin OTLP envelope append"))?;
    let mut report = OtlpEnvelopeAppendReport::default();
    for envelope in envelopes {
        let metadata_json = serde_json::to_string(&envelope.metadata).map_err(|error| {
            OtlpEnvelopeStoreError::Rejected {
                message: format!("OTLP envelope ownership could not be encoded: {error}"),
            }
        })?;
        let existing = transaction
            .query_row(
                "SELECT metadata_json, payload
                 FROM otlp_envelopes
                 WHERE node_id = ?1 AND workload_id = ?2 AND signal = ?3 AND digest = ?4",
                params![
                    envelope.id.node_id.as_str(),
                    envelope.id.workload_id.as_str(),
                    envelope.id.signal.as_str(),
                    envelope.id.digest.as_bytes().as_slice(),
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .map_err(unavailable("read OTLP envelope replay identity"))?;
        match existing {
            Some((existing_metadata, existing_payload))
                if existing_metadata == metadata_json && existing_payload == envelope.payload =>
            {
                report.deduplicated = report.deduplicated.saturating_add(1);
            }
            Some(_) => {
                return Err(OtlpEnvelopeStoreError::Rejected {
                    message: "OTLP envelope identity was reused with different ownership"
                        .to_owned(),
                });
            }
            None => {
                transaction
                    .execute(
                        "INSERT INTO otlp_envelopes
                         (node_id, workload_id, signal, digest, observed_at_ms, metadata_json, payload)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            envelope.id.node_id.as_str(),
                            envelope.id.workload_id.as_str(),
                            envelope.id.signal.as_str(),
                            envelope.id.digest.as_bytes().as_slice(),
                            envelope.observed_at.0,
                            metadata_json.as_str(),
                            envelope.payload.as_slice(),
                        ],
                    )
                    .map_err(unavailable("insert OTLP envelope"))?;
                report.committed = report.committed.saturating_add(1);
            }
        }
    }
    transaction
        .commit()
        .map_err(unavailable("commit OTLP envelope append"))?;
    Ok(report)
}

pub(crate) fn migrate_v5_to_v6(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE otlp_envelopes (
                 node_id VARCHAR NOT NULL,
                 workload_id VARCHAR NOT NULL,
                 signal VARCHAR NOT NULL CHECK (signal IN ('metrics', 'traces')),
                 digest BLOB NOT NULL,
                 observed_at_ms BIGINT NOT NULL,
                 metadata_json VARCHAR NOT NULL,
                 payload BLOB NOT NULL,
                 PRIMARY KEY (node_id, workload_id, signal, digest)
             );
             CREATE INDEX otlp_envelopes_observed
                 ON otlp_envelopes(observed_at_ms, signal);
             UPDATE schema_version SET version = 6;",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> OtlpEnvelopeStoreError {
    move |error| OtlpEnvelopeStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}
