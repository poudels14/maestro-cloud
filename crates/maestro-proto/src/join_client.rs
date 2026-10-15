use std::sync::Arc;

use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tonic::transport::{Channel, Endpoint, Uri};

use crate::pki::{VOTER_SERVER_NAME, pinned_client_config};

pub async fn connect_pinned(
    address: &str,
    ca_fingerprint: &str,
) -> Result<Channel, tonic::transport::Error> {
    let connector = TlsConnector::from(Arc::new(pinned_client_config(ca_fingerprint)));
    let target = address.to_string();
    Endpoint::from_shared(format!("http://{address}"))?
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let connector = connector.clone();
            let target = target.clone();
            async move {
                let stream = TcpStream::connect(&target).await?;
                let server_name = ServerName::try_from(VOTER_SERVER_NAME)
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                let tls = connector.connect(server_name, stream).await?;
                Ok::<_, std::io::Error>(TokioIo::new(tls))
            }
        }))
        .await
}
