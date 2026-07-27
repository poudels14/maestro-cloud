use std::net::Ipv4Addr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const OPERATOR_SECRET: &str = "real-cluster-operator-secret-with-at-least-32-characters";
const MAX_REQUEST_BYTES: usize = 64 * 1024;

pub(super) struct LocalSecretsManager {
    endpoint: Option<String>,
    task: Option<JoinHandle<()>>,
}

impl LocalSecretsManager {
    pub(super) fn pending() -> Self {
        Self {
            endpoint: None,
            task: None,
        }
    }

    pub(super) async fn start(
        bridge_address: Ipv4Addr,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        let port = listener.local_addr()?.port();
        let task = tokio::spawn(async move {
            while let Ok((stream, _peer)) = listener.accept().await {
                tokio::spawn(serve_secret(stream));
            }
        });
        Ok(Self {
            endpoint: Some(format!("http://{bridge_address}:{port}")),
            task: Some(task),
        })
    }

    pub(super) fn endpoint(&self) -> Result<&str, &'static str> {
        self.endpoint
            .as_deref()
            .ok_or("Secrets Manager fixture must start before nodes launch")
    }
}

impl Drop for LocalSecretsManager {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn serve_secret(mut stream: TcpStream) {
    if read_request(&mut stream).await.is_err() {
        return;
    }
    let body = format!(
        concat!(
            r#"{{"ARN":"arn:aws:secretsmanager:us-east-1:000000000000:secret:maestro/test/operator-jwt-secret","#,
            r#""Name":"maestro/test/operator-jwt-secret","SecretString":"{}","#,
            r#""VersionId":"00000000-0000-0000-0000-000000000001"}}"#
        ),
        OPERATOR_SECRET
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/x-amz-json-1.1\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

async fn read_request(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        let bytes = chunk.get(..read).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Secrets Manager fixture read exceeded its buffer",
            )
        })?;
        request.extend_from_slice(bytes);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            return Ok(());
        }
        if request.len() >= MAX_REQUEST_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Secrets Manager fixture request exceeded its size limit",
            ));
        }
    }
}
