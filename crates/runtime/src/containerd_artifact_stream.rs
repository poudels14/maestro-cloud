use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use containerd::services::v1::{StreamInit, streaming_client::StreamingClient};
use containerd::tonic::{Streaming, transport::Channel};
use containerd::types::transfer::{AuthRequest, AuthResponse, AuthType, Data, WindowUpdate};
use futures_util::stream;
use prost::{Message, Name};
use prost_types::Any;
use tokio::sync::mpsc;

use crate::containerd_artifact_support::{artifact_request, operation_error, stream_error};
use crate::{ArtifactByteStream, ArtifactStoreError, RegistryCredential};

const DATA_BYTES: usize = 32 * 1_024;
const WINDOW_BYTES: usize = 2 * DATA_BYTES;
const OUTBOUND_QUEUE: usize = 4;

/// Owned containerd transfer operation.
///
/// The operation is polled only while its import or export owner is active,
/// so dropping the owner cancels the gRPC request instead of detaching a
/// background task. Containerd's expiring transfer lease bounds server-side
/// cleanup when cancellation prevents the normal explicit lease deletion.
pub(crate) struct TransferTask {
    future: Pin<Box<dyn Future<Output = Result<(), ArtifactStoreError>> + Send>>,
}

impl TransferTask {
    pub(crate) fn new(
        future: impl Future<Output = Result<(), ArtifactStoreError>> + Send + 'static,
    ) -> Self {
        Self {
            future: Box::pin(future),
        }
    }
}

impl Future for TransferTask {
    type Output = Result<(), ArtifactStoreError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(context)
    }
}

pub(crate) struct ArtifactDuplex {
    sender: Option<mpsc::Sender<Any>>,
    incoming: Streaming<Any>,
}

pub(crate) async fn open_stream(
    channel: Channel,
    namespace: &str,
    stream_id: &str,
    lease_id: Option<&str>,
) -> Result<ArtifactDuplex, ArtifactStoreError> {
    let (sender, receiver) = mpsc::channel(OUTBOUND_QUEUE);
    sender
        .send(containerd::to_any(&StreamInit {
            id: stream_id.to_owned(),
        }))
        .await
        .map_err(|_| stream_error("open stream", "initialization channel closed"))?;
    let outbound = stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|message| (message, receiver))
    });
    let mut incoming = StreamingClient::new(channel)
        .stream(artifact_request(outbound, namespace, lease_id)?)
        .await
        .map_err(|error| operation_error("open artifact stream", None, error))?
        .into_inner();
    incoming
        .message()
        .await
        .map_err(|error| stream_error("open stream acknowledgement", error))?
        .ok_or_else(|| stream_error("open stream acknowledgement", "server closed the stream"))?;
    Ok(ArtifactDuplex {
        sender: Some(sender),
        incoming,
    })
}

pub(crate) async fn serve_registry_auth(
    mut duplex: ArtifactDuplex,
    mut transfer: TransferTask,
    registry_host: &str,
    credential: &RegistryCredential,
) -> Result<(), ArtifactStoreError> {
    loop {
        let message = tokio::select! {
            result = &mut transfer => {
                duplex.sender.take();
                return result;
            }
            result = duplex.incoming.message() => result
                .map_err(|error| stream_error("registry authentication", error))?,
        }
        .ok_or_else(|| {
            stream_error(
                "registry authentication",
                "server closed the callback stream before transfer completion",
            )
        })?;
        let request = decode::<AuthRequest>(&message, "registry authentication request")?;
        let response = registry_auth_response(&request, registry_host, credential)?;
        duplex
            .sender
            .as_ref()
            .ok_or_else(|| stream_error("registry authentication", "callback stream is closed"))?
            .send(containerd::to_any(&response))
            .await
            .map_err(|_| {
                stream_error(
                    "registry authentication",
                    "callback response channel closed",
                )
            })?;
    }
}

pub(crate) fn registry_auth_response(
    request: &AuthRequest,
    registry_host: &str,
    credential: &RegistryCredential,
) -> Result<AuthResponse, ArtifactStoreError> {
    if request.host != registry_host {
        return Err(stream_error(
            "registry authentication",
            format!(
                "registry `{registry_host}` requested credentials for unexpected host `{}`",
                request.host
            ),
        ));
    }
    Ok(AuthResponse {
        auth_type: AuthType::Credentials as i32,
        secret: credential.secret().expose().to_owned(),
        username: credential.username().to_owned(),
        expire_at: None,
    })
}

pub(crate) struct ContainerdArtifactStream {
    duplex: Option<ArtifactDuplex>,
    transfer: Option<TransferTask>,
    completion: Option<Result<(), ArtifactStoreError>>,
    pending_credit: usize,
    initialized: bool,
    finished: bool,
}

impl ContainerdArtifactStream {
    pub(crate) fn new(duplex: ArtifactDuplex, transfer: TransferTask) -> Self {
        Self {
            duplex: Some(duplex),
            transfer: Some(transfer),
            completion: None,
            pending_credit: 0,
            initialized: false,
            finished: false,
        }
    }

    async fn advertise_credit(&mut self) -> Result<(), ArtifactStoreError> {
        let update = if self.initialized {
            std::mem::take(&mut self.pending_credit)
        } else {
            self.initialized = true;
            WINDOW_BYTES
        };
        if update == 0 {
            return Ok(());
        }
        let update = i32::try_from(update)
            .map_err(|_| stream_error("export", "window credit exceeded protocol bounds"))?;
        self.duplex
            .as_ref()
            .ok_or_else(|| stream_error("export", "stream is already closed"))?
            .sender
            .as_ref()
            .ok_or_else(|| stream_error("export", "window channel is already closed"))?
            .send(containerd::to_any(&WindowUpdate { update }))
            .await
            .map_err(|_| stream_error("export", "window update channel closed"))
    }

    async fn finish(&mut self) -> Result<(), ArtifactStoreError> {
        if let Some(duplex) = self.duplex.as_mut() {
            duplex.sender.take();
        }
        self.finished = true;
        let completion = match self.completion.take() {
            Some(completion) => completion,
            None => {
                let transfer = self
                    .transfer
                    .take()
                    .ok_or_else(|| stream_error("export", "transfer completion was lost"))?;
                transfer.await
            }
        };
        self.duplex.take();
        completion
    }

    fn record_completion(
        &mut self,
        result: Result<(), ArtifactStoreError>,
    ) -> Result<(), ArtifactStoreError> {
        self.transfer.take();
        if let Some(duplex) = self.duplex.as_mut() {
            duplex.sender.take();
        }
        let completion = result;
        if let Err(error) = completion {
            self.finished = true;
            self.duplex.take();
            return Err(error);
        }
        self.completion = Some(completion);
        Ok(())
    }
}

#[async_trait]
impl ArtifactByteStream for ContainerdArtifactStream {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, ArtifactStoreError> {
        if self.finished {
            return Ok(None);
        }
        if self.completion.is_none() {
            self.advertise_credit().await?;
        }
        loop {
            let message = if let Some(transfer) = self.transfer.as_mut() {
                let duplex = self
                    .duplex
                    .as_mut()
                    .ok_or_else(|| stream_error("export", "stream is already closed"))?;
                tokio::select! {
                    result = transfer => {
                        self.record_completion(result)?;
                        continue;
                    }
                    result = duplex.incoming.message() => result,
                }
            } else {
                self.duplex
                    .as_mut()
                    .ok_or_else(|| stream_error("export", "stream is already closed"))?
                    .incoming
                    .message()
                    .await
            }
            .map_err(|error| stream_error("export receive", error))?;
            let Some(message) = message else {
                self.finish().await?;
                return Ok(None);
            };
            let data = decode::<Data>(&message, "export data")?.data;
            if data.len() > DATA_BYTES {
                return Err(stream_error(
                    "export",
                    format!(
                        "server emitted {} bytes; maximum is {DATA_BYTES}",
                        data.len()
                    ),
                ));
            }
            if data.is_empty() {
                continue;
            }
            self.pending_credit = data.len();
            return Ok(Some(data));
        }
    }
}

pub(crate) async fn upload_stream(
    mut source: Box<dyn ArtifactByteStream>,
    mut duplex: ArtifactDuplex,
    mut transfer: TransferTask,
) -> Result<(), ArtifactStoreError> {
    let mut credit = 0_usize;
    loop {
        let chunk = tokio::select! {
            result = &mut transfer => {
                result?;
                duplex.sender.take();
                return clean_source_end(source).await;
            }
            result = source.next() => result?,
        };
        let Some(chunk) = chunk else {
            duplex.sender.take();
            return transfer.await;
        };
        if chunk.len() > WINDOW_BYTES {
            return Err(stream_error(
                "import",
                format!(
                    "source emitted {} bytes; maximum is {WINDOW_BYTES}",
                    chunk.len()
                ),
            ));
        }
        let mut offset = 0;
        while offset < chunk.len() {
            while credit == 0 {
                let message = tokio::select! {
                    result = &mut transfer => {
                        return Err(match result {
                            Ok(()) => stream_error(
                                "import",
                                "server completed before pending archive bytes were sent",
                            ),
                            Err(error) => error,
                        });
                    }
                    result = duplex.incoming.message() => result
                        .map_err(|error| stream_error("import window", error))?,
                }
                .ok_or_else(|| stream_error("import", "server closed before granting credit"))?;
                let update = decode::<WindowUpdate>(&message, "import window")?.update;
                let update = usize::try_from(update)
                    .ok()
                    .filter(|update| *update > 0 && *update <= WINDOW_BYTES)
                    .ok_or_else(|| {
                        stream_error("import", "server granted invalid window credit")
                    })?;
                credit = credit
                    .checked_add(update)
                    .filter(|credit| *credit <= WINDOW_BYTES)
                    .ok_or_else(|| stream_error("import", "server window credit overflowed"))?;
            }
            let count = DATA_BYTES.min(credit).min(chunk.len() - offset);
            let end = offset
                .checked_add(count)
                .ok_or_else(|| stream_error("import", "chunk boundary overflowed"))?;
            let data = chunk
                .get(offset..end)
                .ok_or_else(|| stream_error("import", "chunk boundary was invalid"))?
                .to_vec();
            duplex
                .sender
                .as_ref()
                .ok_or_else(|| stream_error("import", "data channel is already closed"))?
                .send(containerd::to_any(&Data { data }))
                .await
                .map_err(|_| stream_error("import", "data channel closed"))?;
            offset = end;
            credit -= count;
        }
    }
}

pub(crate) fn decode<MessageType>(
    message: &Any,
    operation: &str,
) -> Result<MessageType, ArtifactStoreError>
where
    MessageType: Message + Name + Default,
{
    let actual = message.type_url.rsplit('/').next().unwrap_or_default();
    if actual != MessageType::full_name() {
        return Err(stream_error(
            operation,
            format!("unexpected message type `{}`", message.type_url),
        ));
    }
    MessageType::decode(message.value.as_slice())
        .map_err(|error| stream_error(operation, format!("invalid protobuf: {error}")))
}

async fn clean_source_end(
    mut source: Box<dyn ArtifactByteStream>,
) -> Result<(), ArtifactStoreError> {
    loop {
        match source.next().await? {
            Some(chunk) if chunk.is_empty() => {}
            Some(_) => {
                return Err(stream_error(
                    "import",
                    "source emitted trailing bytes after containerd committed the archive",
                ));
            }
            None => return Ok(()),
        }
    }
}
