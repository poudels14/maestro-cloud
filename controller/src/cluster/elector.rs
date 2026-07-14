use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use etcd_client::{Client, ConnectOptions, ResignOptions, TlsOptions};
use tokio::sync::{Mutex, broadcast, watch};

use crate::cluster::types::{LeaderInfo, LeadershipState, LeadershipToken, NodeId};
use crate::logs::Logger;

const ELECTION_NAME: &str = "/maetro/cluster/leader";

#[async_trait]
pub trait LeaderElector: Send + Sync {
    fn state(&self) -> LeadershipState;
    fn watch(&self) -> watch::Receiver<LeadershipState>;
    async fn resign(&self) -> Result<()>;
}

pub struct EtcdLeaderElector {
    client: Client,
    node_id: NodeId,
    can_campaign: bool,
    state_tx: watch::Sender<LeadershipState>,
    current: Mutex<Option<LeadershipToken>>,
}

impl EtcdLeaderElector {
    pub async fn connect(
        endpoints: &[String],
        tls: Option<TlsOptions>,
        node_id: NodeId,
        can_campaign: bool,
    ) -> Result<Self> {
        let options = tls.map(|tls| ConnectOptions::new().with_tls(tls));
        let mut delay = Duration::from_millis(250);
        let client = loop {
            if let Ok(mut client) = Client::connect(endpoints, options.clone()).await
                && client.status().await.is_ok()
            {
                break client;
            }
            if delay <= Duration::from_secs(4) {
                tokio::time::sleep(delay).await;
                delay *= 2;
            } else {
                bail!("timed out connecting leader elector to etcd");
            }
        };
        let (state_tx, _) = watch::channel(LeadershipState::Unknown);
        Ok(Self {
            client,
            node_id,
            can_campaign,
            state_tx,
            current: Mutex::new(None),
        })
    }

    pub fn spawn(
        self: Arc<Self>,
        shutdown: broadcast::Receiver<crate::signal::ShutdownEvent>,
        logger: Logger,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let observer = tokio::spawn(
            self.clone()
                .observe_loop(shutdown.resubscribe(), logger.clone()),
        );
        let mut handles = vec![observer];
        if self.can_campaign {
            handles.push(tokio::spawn(self.campaign_loop(shutdown, logger)));
        }
        handles
    }

    pub async fn wait_until_leading(&self, timeout: Duration) -> Result<LeadershipToken> {
        let mut receiver = self.watch();
        tokio::time::timeout(timeout, async {
            loop {
                if let LeadershipState::Leading(token) = receiver.borrow().clone() {
                    return Ok(token);
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| anyhow!("leader elector stopped"))?;
            }
        })
        .await
        .map_err(|_| anyhow!("timed out waiting to acquire initial cluster leadership"))?
    }

    async fn campaign_loop(
        self: Arc<Self>,
        mut shutdown: broadcast::Receiver<crate::signal::ShutdownEvent>,
        logger: Logger,
    ) {
        loop {
            let mut attempt_shutdown = shutdown.resubscribe();
            tokio::select! {
                result = self.campaign_once(&mut attempt_shutdown) => {
                    if let Err(err) = result {
                        logger.emit("warn", &format!("leader campaign interrupted: {err}"));
                    }
                }
                _ = shutdown.recv() => break,
            }
            self.demote().await;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = shutdown.recv() => break,
            }
        }
        let _ = self.resign().await;
    }

    async fn campaign_once(
        &self,
        shutdown: &mut broadcast::Receiver<crate::signal::ShutdownEvent>,
    ) -> Result<()> {
        let mut lease_client = self.client.clone();
        let lease_id = lease_client.lease_grant(10, None).await?.id();
        let (mut keeper, mut stream) = lease_client.lease_keep_alive(lease_id).await?;
        let heartbeat = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                keeper.keep_alive().await?;
                let response = tokio::time::timeout(Duration::from_secs(5), stream.message())
                    .await
                    .map_err(|_| anyhow!("leader lease response timed out"))??;
                if response.is_none_or(|response| response.ttl() <= 0) {
                    bail!("leader lease expired");
                }
            }
        };
        tokio::pin!(heartbeat);
        let mut election_client = self.client.clone();
        let campaign = election_client.campaign(ELECTION_NAME, self.node_id.clone(), lease_id);
        tokio::pin!(campaign);
        let mut response = tokio::select! {
            response = &mut campaign => response?,
            result = &mut heartbeat => return result,
            _ = shutdown.recv() => return Ok(()),
        };
        let leader = response
            .take_leader()
            .ok_or_else(|| anyhow!("campaign response omitted the leader key"))?;
        let token = LeadershipToken {
            info: LeaderInfo {
                node_id: self.node_id.clone(),
            },
            election_key: leader.key().to_vec(),
            create_revision: leader.rev(),
            lease_id: leader.lease(),
        };
        *self.current.lock().await = Some(token.clone());
        self.state_tx.send_replace(LeadershipState::Leading(token));
        tokio::select! {
            result = &mut heartbeat => result,
            _ = shutdown.recv() => Ok(()),
        }
    }

    async fn observe_loop(
        self: Arc<Self>,
        mut shutdown: broadcast::Receiver<crate::signal::ShutdownEvent>,
        logger: Logger,
    ) {
        loop {
            let mut client = self.client.clone();
            match client.observe(ELECTION_NAME).await {
                Ok(mut stream) => loop {
                    tokio::select! {
                        message = stream.message() => match message {
                            Ok(Some(response)) => {
                                let leader = response.kv().and_then(|entry| {
                                    std::str::from_utf8(entry.value()).ok().map(|node_id| LeaderInfo {
                                        node_id: node_id.to_string(),
                                    })
                                });
                                if !matches!(self.state(), LeadershipState::Leading(_)) {
                                    self.state_tx.send_replace(LeadershipState::Following(leader));
                                }
                            }
                            Ok(None) => break,
                            Err(err) => {
                                logger.emit("warn", &format!("leader observer interrupted: {err}"));
                                break;
                            }
                        },
                        _ = shutdown.recv() => return,
                    }
                },
                Err(err) => {
                    logger.emit("warn", &format!("failed to observe cluster leader: {err}"))
                }
            }
            if !matches!(self.state(), LeadershipState::Leading(_)) {
                self.state_tx.send_replace(LeadershipState::Unknown);
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = shutdown.recv() => return,
            }
        }
    }

    async fn demote(&self) {
        *self.current.lock().await = None;
        self.state_tx.send_replace(LeadershipState::Following(None));
    }
}

#[async_trait]
impl LeaderElector for EtcdLeaderElector {
    fn state(&self) -> LeadershipState {
        self.state_tx.borrow().clone()
    }

    fn watch(&self) -> watch::Receiver<LeadershipState> {
        self.state_tx.subscribe()
    }

    async fn resign(&self) -> Result<()> {
        let Some(token) = self.current.lock().await.take() else {
            return Ok(());
        };
        let leader = etcd_client::LeaderKey::new()
            .with_name(ELECTION_NAME)
            .with_key(token.election_key)
            .with_rev(token.create_revision)
            .with_lease(token.lease_id);
        let mut client = self.client.clone();
        client
            .resign(Some(ResignOptions::new().with_leader(leader)))
            .await?;
        let _ = client.lease_revoke(token.lease_id).await;
        self.state_tx.send_replace(LeadershipState::Following(None));
        Ok(())
    }
}

#[cfg(test)]
pub struct InMemoryLeaderElector {
    node_id: NodeId,
    state_tx: watch::Sender<LeadershipState>,
}

#[cfg(test)]
impl InMemoryLeaderElector {
    pub fn new(node_id: NodeId) -> Self {
        let (state_tx, _) = watch::channel(LeadershipState::Following(None));
        Self { node_id, state_tx }
    }

    async fn campaign(&self) {
        self.state_tx
            .send_replace(LeadershipState::Leading(LeadershipToken {
                info: LeaderInfo {
                    node_id: self.node_id.clone(),
                },
                election_key: format!("{ELECTION_NAME}/memory").into_bytes(),
                create_revision: 1,
                lease_id: 1,
            }));
    }
}

#[cfg(test)]
#[async_trait]
impl LeaderElector for InMemoryLeaderElector {
    fn state(&self) -> LeadershipState {
        self.state_tx.borrow().clone()
    }

    fn watch(&self) -> watch::Receiver<LeadershipState> {
        self.state_tx.subscribe()
    }

    async fn resign(&self) -> Result<()> {
        self.state_tx.send_replace(LeadershipState::Following(None));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_campaign_retains_a_fencing_token() {
        let elector = InMemoryLeaderElector::new("node00000001".to_string());
        elector.campaign().await;
        let LeadershipState::Leading(token) = elector.state() else {
            panic!("expected leading state");
        };
        assert_eq!(token.info.node_id, "node00000001");
        assert_eq!(token.create_revision, 1);
        elector.resign().await.expect("resign");
        assert_eq!(elector.state(), LeadershipState::Following(None));
    }
}
