use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;

/// One node-local readiness target derived from a scheduled workload address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthProbeTarget {
    /// HTTP GET requiring a successful status code.
    Http {
        /// Cluster-routable workload address.
        address: IpAddr,
        /// Workload port accepting the request.
        port: u16,
        /// Absolute HTTP request path.
        path: String,
    },
    /// TCP connect requiring a completed handshake.
    Tcp {
        /// Cluster-routable workload address.
        address: IpAddr,
        /// Workload port accepting the connection.
        port: u16,
    },
}

/// Probe adapter used by deterministic health reconciliation.
#[async_trait]
pub trait HealthProber: Send + Sync {
    /// Performs one bounded probe; cancellation closes only this attempt.
    async fn probe(&self, target: &HealthProbeTarget) -> Result<(), HealthProbeError>;
}

/// Production HTTP and TCP health adapter with one explicit IO deadline.
#[derive(Clone)]
pub struct NetworkHealthProber {
    client: reqwest::Client,
    timeout: Duration,
}

impl NetworkHealthProber {
    /// Creates a prober whose individual network attempts cannot outlive `timeout`.
    pub fn new(timeout: Duration) -> Result<Self, HealthProbeError> {
        if timeout.is_zero() {
            return Err(HealthProbeError::InvalidTarget {
                message: "health probe timeout must be positive".to_owned(),
            });
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| HealthProbeError::Unavailable {
                message: format!("failed to create health HTTP client: {error}"),
            })?;
        Ok(Self { client, timeout })
    }
}

#[async_trait]
impl HealthProber for NetworkHealthProber {
    async fn probe(&self, target: &HealthProbeTarget) -> Result<(), HealthProbeError> {
        match target {
            HealthProbeTarget::Http {
                address,
                port,
                path,
            } => {
                if !path.starts_with('/') {
                    return Err(HealthProbeError::InvalidTarget {
                        message: format!("HTTP health path `{path}` is not absolute"),
                    });
                }
                let host = match address {
                    IpAddr::V4(address) => address.to_string(),
                    IpAddr::V6(address) => format!("[{address}]"),
                };
                let response = self
                    .client
                    .get(format!("http://{host}:{port}{path}"))
                    .timeout(self.timeout)
                    .send()
                    .await
                    .map_err(|error| HealthProbeError::Unhealthy {
                        message: error.to_string(),
                    })?;
                if response.status().is_success() {
                    Ok(())
                } else {
                    Err(HealthProbeError::Unhealthy {
                        message: format!("HTTP status {}", response.status()),
                    })
                }
            }
            HealthProbeTarget::Tcp { address, port } => {
                let target = SocketAddr::new(*address, *port);
                match tokio::time::timeout(self.timeout, tokio::net::TcpStream::connect(target))
                    .await
                {
                    Ok(Ok(_stream)) => Ok(()),
                    Ok(Err(error)) => Err(HealthProbeError::Unhealthy {
                        message: error.to_string(),
                    }),
                    Err(_elapsed) => Err(HealthProbeError::Unhealthy {
                        message: format!("TCP connection exceeded {:?}", self.timeout),
                    }),
                }
            }
        }
    }
}

/// Why one health attempt could not establish readiness.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HealthProbeError {
    /// Desired probe settings cannot form a safe network request.
    #[error("invalid health target: {message}")]
    InvalidTarget {
        /// Stable validation detail.
        message: String,
    },
    /// The adapter could not be initialized.
    #[error("health probe adapter is unavailable: {message}")]
    Unavailable {
        /// Initialization detail safe to log.
        message: String,
    },
    /// A valid probe completed without establishing health.
    #[error("health probe failed: {message}")]
    Unhealthy {
        /// Protocol or transport detail safe for a status condition.
        message: String,
    },
}
