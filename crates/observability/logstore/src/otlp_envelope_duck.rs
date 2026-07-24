use async_trait::async_trait;
use logs::{OtlpEnvelope, OtlpEnvelopeAppendReport, OtlpEnvelopeStore, OtlpEnvelopeStoreError};
use tokio::sync::oneshot;

use crate::duck::{Command, DuckLogStore};
use crate::duck_worker::otlp_worker_stopped;

#[async_trait]
impl OtlpEnvelopeStore for DuckLogStore {
    async fn append_otlp_envelopes(
        &self,
        envelopes: &[OtlpEnvelope],
    ) -> Result<OtlpEnvelopeAppendReport, OtlpEnvelopeStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::AppendOtlpEnvelopes {
                envelopes: envelopes.to_vec(),
                response,
            })
            .await
            .map_err(|_| otlp_worker_stopped("accepting OTLP envelope append"))?;
        result
            .await
            .map_err(|_| otlp_worker_stopped("completing OTLP envelope append"))?
    }
}
