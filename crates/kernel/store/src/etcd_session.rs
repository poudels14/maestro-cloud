use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use etcd_client::{Client, LeaseKeepAliveStream, LeaseKeeper};
use tokio::sync::Mutex;

use crate::etcd_support::{operation_error, session_i64};
use crate::{Session, SessionBinding, SessionId, StoreError};

pub(crate) struct EtcdSession {
    client: Client,
    id: SessionId,
    ttl: Duration,
    closed: AtomicBool,
    keepalive: Mutex<Option<(LeaseKeeper, LeaseKeepAliveStream)>>,
}

impl EtcdSession {
    pub(crate) fn new(client: Client, id: SessionId, ttl: Duration) -> Self {
        Self {
            client,
            id,
            ttl,
            closed: AtomicBool::new(false),
            keepalive: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Session for EtcdSession {
    fn id(&self) -> SessionId {
        self.id
    }

    fn ttl(&self) -> Duration {
        self.ttl
    }

    async fn keep_alive(&self) -> Result<(), StoreError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StoreError::SessionExpired {
                session_id: self.id,
            });
        }
        let mut keepalive = self.keepalive.lock().await;
        if keepalive.is_none() {
            let pair = self
                .client
                .clone()
                .lease_keep_alive(session_i64(self.id)?)
                .await
                .map_err(|error| {
                    operation_error(
                        error,
                        Some(SessionBinding {
                            session_id: self.id,
                        }),
                    )
                })?;
            *keepalive = Some(pair);
        }
        let (keeper, responses) = keepalive.as_mut().ok_or_else(|| StoreError::Contract {
            message: "etcd keepalive stream was not initialized".to_string(),
        })?;
        keeper.keep_alive().await.map_err(|error| {
            operation_error(
                error,
                Some(SessionBinding {
                    session_id: self.id,
                }),
            )
        })?;
        let response = responses
            .message()
            .await
            .map_err(|error| {
                operation_error(
                    error,
                    Some(SessionBinding {
                        session_id: self.id,
                    }),
                )
            })?
            .ok_or(StoreError::SessionExpired {
                session_id: self.id,
            })?;
        if response.ttl() <= 0 {
            self.closed.store(true, Ordering::Release);
            Err(StoreError::SessionExpired {
                session_id: self.id,
            })
        } else {
            Ok(())
        }
    }

    async fn close(&self) -> Result<(), StoreError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Err(StoreError::SessionExpired {
                session_id: self.id,
            });
        }
        self.client
            .clone()
            .lease_revoke(session_i64(self.id)?)
            .await
            .map_err(|error| {
                operation_error(
                    error,
                    Some(SessionBinding {
                        session_id: self.id,
                    }),
                )
            })?;
        Ok(())
    }
}
