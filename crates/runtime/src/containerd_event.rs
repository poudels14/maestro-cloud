use async_trait::async_trait;
use containerd::events::{ContainerCreate, ContainerDelete, TaskExit, TaskStart};
use containerd::services::v1::{GetContainerRequest, SubscribeRequest};
use containerd::tonic::{Streaming, transport::Channel};
use containerd::types::Envelope;
use kernel_api::WorkloadId;
use prost::Message;

use crate::containerd_support::{container_metadata, namespaced};
use crate::{
    EventCursor, EventRequest, RuntimeError, RuntimeEvent, RuntimeEventKind, RuntimeEventStream,
};

pub(crate) struct ContainerdEventStream {
    events: Streaming<Envelope>,
    channel: Channel,
    namespace: String,
    request: EventRequest,
}

impl ContainerdEventStream {
    pub(crate) async fn subscribe(
        channel: Channel,
        namespace: String,
        request: EventRequest,
    ) -> Result<Box<dyn RuntimeEventStream>, RuntimeError> {
        let response = containerd::services::v1::events_client::EventsClient::new(channel.clone())
            .subscribe(SubscribeRequest {
                filters: vec![format!("namespace=={namespace}")],
            })
            .await
            .map_err(|error| RuntimeError::Unavailable {
                message: format!("failed to subscribe to containerd events: {error}"),
            })?;
        Ok(Box::new(Self {
            events: response.into_inner(),
            channel,
            namespace,
            request,
        }))
    }

    async fn metadata_matches(&self, container_id: &str) -> Result<bool, RuntimeError> {
        let response = containerd::services::v1::containers_client::ContainersClient::new(
            self.channel.clone(),
        )
        .get(namespaced(
            GetContainerRequest {
                id: container_id.to_owned(),
            },
            &self.namespace,
        )?)
        .await;
        match response {
            Ok(response) => {
                let container =
                    response
                        .into_inner()
                        .container
                        .ok_or_else(|| RuntimeError::Stream {
                            message: "containerd event lookup omitted container metadata"
                                .to_owned(),
                        })?;
                let metadata = container_metadata(&container)?;
                Ok(metadata.cluster_id == self.request.cluster_id
                    && metadata.node_id == self.request.node_id)
            }
            Err(error) if error.code() == containerd::tonic::Code::NotFound => Ok(false),
            Err(error) => Err(RuntimeError::Stream {
                message: format!("containerd event ownership lookup failed: {error}"),
            }),
        }
    }
}

#[async_trait]
impl RuntimeEventStream for ContainerdEventStream {
    async fn next(&mut self) -> Result<Option<RuntimeEvent>, RuntimeError> {
        while let Some(envelope) =
            self.events
                .message()
                .await
                .map_err(|error| RuntimeError::Stream {
                    message: format!("containerd event stream failed: {error}"),
                })?
        {
            let Some(native) = decode_event(&envelope)? else {
                continue;
            };
            if native.kind != RuntimeEventKind::Removed
                && !self.metadata_matches(&native.container_id).await?
            {
                continue;
            }
            let Some(workload_value) = native.container_id.strip_prefix("maestro-") else {
                continue;
            };
            let workload_id = WorkloadId::new(workload_value.to_owned()).map_err(|error| {
                RuntimeError::Stream {
                    message: format!("containerd event has invalid workload identity: {error}"),
                }
            })?;
            return Ok(Some(RuntimeEvent {
                cursor: event_cursor(&envelope, &native.container_id),
                workload_id,
                kind: native.kind,
                exit_code: native.exit_code,
            }));
        }
        Ok(None)
    }
}

pub(crate) struct NativeEvent {
    pub(crate) container_id: String,
    pub(crate) kind: RuntimeEventKind,
    pub(crate) exit_code: Option<i32>,
}

pub(crate) fn decode_event(envelope: &Envelope) -> Result<Option<NativeEvent>, RuntimeError> {
    let Some(payload) = envelope.event.as_ref() else {
        return Ok(None);
    };
    match envelope.topic.as_str() {
        "/containers/create" => {
            let event = ContainerCreate::decode(payload.value.as_slice()).map_err(event_decode)?;
            Ok(Some(NativeEvent {
                container_id: event.id,
                kind: RuntimeEventKind::Created,
                exit_code: None,
            }))
        }
        "/containers/delete" => {
            let event = ContainerDelete::decode(payload.value.as_slice()).map_err(event_decode)?;
            Ok(Some(NativeEvent {
                container_id: event.id,
                kind: RuntimeEventKind::Removed,
                exit_code: None,
            }))
        }
        "/tasks/start" => {
            let event = TaskStart::decode(payload.value.as_slice()).map_err(event_decode)?;
            Ok(Some(NativeEvent {
                container_id: event.container_id,
                kind: RuntimeEventKind::Started,
                exit_code: None,
            }))
        }
        "/tasks/exit" => {
            let event = TaskExit::decode(payload.value.as_slice()).map_err(event_decode)?;
            if event.id.is_empty() {
                Ok(Some(NativeEvent {
                    container_id: event.container_id,
                    kind: RuntimeEventKind::Exited,
                    exit_code: i32::try_from(event.exit_status).ok(),
                }))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

fn event_cursor(envelope: &Envelope, container_id: &str) -> EventCursor {
    let timestamp = envelope.timestamp.as_ref();
    let seconds = timestamp.map_or(0, |timestamp| timestamp.seconds);
    let nanoseconds = timestamp.map_or(0, |timestamp| timestamp.nanos);
    EventCursor::new(format!(
        "{seconds}.{nanoseconds:09}:{}:{container_id}",
        envelope.topic
    ))
}

fn event_decode(error: prost::DecodeError) -> RuntimeError {
    RuntimeError::Stream {
        message: format!("containerd event payload is invalid: {error}"),
    }
}
