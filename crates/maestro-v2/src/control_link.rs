use maestro_agent::ControlLink;
use maestro_core::{AgentMessage, ControlMessage};
use maestro_proto::NodeCredentials;
use maestro_proto::pb::cluster_client::ClusterClient;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::cluster_service::{decode, encode};

const LINK_BUFFER: usize = 64;

pub struct GrpcControlLink {
    credentials: NodeCredentials,
}

impl GrpcControlLink {
    pub fn new(credentials: NodeCredentials) -> Self {
        Self { credentials }
    }
}

impl ControlLink for GrpcControlLink {
    async fn connect(
        &self,
        target: &str,
    ) -> Result<(mpsc::Sender<AgentMessage>, mpsc::Receiver<ControlMessage>), String> {
        let channel = Channel::from_shared(format!("https://{target}"))
            .map_err(|error| error.to_string())?
            .tls_config(self.credentials.client_tls())
            .map_err(|error| error.to_string())?
            .connect()
            .await
            .map_err(|error| error.to_string())?;
        let (agent_sender, agent_receiver) = mpsc::channel::<AgentMessage>(LINK_BUFFER);
        let outbound =
            ReceiverStream::new(agent_receiver).filter_map(|message| encode(&message).ok());
        let mut inbound = ClusterClient::new(channel)
            .attach(outbound)
            .await
            .map_err(|status| status.to_string())?
            .into_inner();
        let (control_sender, control_receiver) = mpsc::channel::<ControlMessage>(LINK_BUFFER);
        tokio::spawn(async move {
            while let Some(Ok(envelope)) = inbound.next().await {
                let delivered = match decode::<ControlMessage>(&envelope) {
                    Ok(message) => control_sender.send(message).await.is_ok(),
                    Err(status) => {
                        tracing::warn!(%status, "ignoring malformed control message");
                        true
                    }
                };
                if !delivered {
                    break;
                }
            }
        });
        Ok((agent_sender, control_receiver))
    }
}
