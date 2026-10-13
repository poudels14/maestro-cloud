use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use hickory_proto::op::{Message, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA};
use hickory_proto::rr::{RData, Record, RecordType};
use tokio::net::UdpSocket;

const INTERNAL_SUFFIX: &str = "maestro.internal";
const RECORD_TTL: u32 = 5;
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(2);
const BIND_RETRY: Duration = Duration::from_secs(2);
const MAX_PACKET: usize = 4096;

#[derive(Clone, Default)]
pub struct DnsRecords {
    names: Arc<RwLock<HashMap<String, Vec<IpAddr>>>>,
}

pub struct DnsServer {
    records: DnsRecords,
    upstreams: Vec<SocketAddr>,
}

impl DnsRecords {
    pub fn replace(&self, names: HashMap<String, Vec<IpAddr>>) {
        let normalized = names
            .into_iter()
            .map(|(name, addresses)| (normalize(&name), addresses))
            .collect();
        *self
            .names
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = normalized;
    }

    pub fn lookup(&self, name: &str) -> Option<Vec<IpAddr>> {
        self.names
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&normalize(name))
            .cloned()
    }
}

impl DnsServer {
    pub fn new(records: DnsRecords, upstreams: Vec<SocketAddr>) -> Self {
        Self { records, upstreams }
    }

    pub async fn run(self, bind: SocketAddr) {
        let socket = loop {
            match UdpSocket::bind(bind).await {
                Ok(socket) => break socket,
                Err(error) => {
                    tracing::debug!(%bind, %error, "DNS bind failed; retrying");
                    tokio::time::sleep(BIND_RETRY).await;
                }
            }
        };
        tracing::info!(%bind, "DNS server listening");
        self.serve(Arc::new(socket)).await
    }

    pub async fn serve(self, socket: Arc<UdpSocket>) {
        let server = Arc::new(self);
        let mut buffer = vec![0u8; MAX_PACKET];
        loop {
            match socket.recv_from(&mut buffer).await {
                Ok((length, client)) => {
                    let request = buffer[..length].to_vec();
                    let server = server.clone();
                    let socket = socket.clone();
                    tokio::spawn(async move {
                        if let Some(response) = server.answer(&request).await {
                            let _ = socket.send_to(&response, client).await;
                        }
                    });
                }
                Err(error) => tracing::warn!(%error, "DNS receive failed"),
            }
        }
    }

    async fn answer(&self, request: &[u8]) -> Option<Vec<u8>> {
        let message = Message::from_vec(request).ok()?;
        let query = message.queries.first()?.clone();
        let name = normalize(&query.name().to_ascii());
        match is_internal(&name) {
            true => {
                let mut response = Message::response(message.metadata.id, message.metadata.op_code);
                response.metadata.authoritative = true;
                response.metadata.recursion_desired = message.metadata.recursion_desired;
                response.metadata.recursion_available = true;
                response.add_query(query.clone());
                match self.records.lookup(&name) {
                    None => response.metadata.response_code = ResponseCode::NXDomain,
                    Some(addresses) => {
                        let answers = addresses.into_iter().filter_map(|address| {
                            let rdata = match (query.query_type(), address) {
                                (RecordType::A, IpAddr::V4(ipv4)) => Some(RData::A(A(ipv4))),
                                (RecordType::AAAA, IpAddr::V6(ipv6)) => {
                                    Some(RData::AAAA(AAAA(ipv6)))
                                }
                                _ => None,
                            };
                            rdata.map(|rdata| {
                                Record::from_rdata(query.name().clone(), RECORD_TTL, rdata)
                            })
                        });
                        response.add_answers(answers);
                    }
                }
                response.to_vec().ok()
            }
            false => {
                let forwarded = self.forward(request).await;
                match forwarded {
                    Some(response) => Some(response),
                    None => Message::error_msg(
                        message.metadata.id,
                        message.metadata.op_code,
                        ResponseCode::ServFail,
                    )
                    .to_vec()
                    .ok(),
                }
            }
        }
    }

    async fn forward(&self, request: &[u8]) -> Option<Vec<u8>> {
        for upstream in &self.upstreams {
            let local = match upstream {
                SocketAddr::V4(_) => "0.0.0.0:0",
                SocketAddr::V6(_) => "[::]:0",
            };
            let exchange = async {
                let socket = UdpSocket::bind(local).await.ok()?;
                socket.send_to(request, upstream).await.ok()?;
                let mut buffer = vec![0u8; MAX_PACKET];
                let (length, _) = socket.recv_from(&mut buffer).await.ok()?;
                buffer.truncate(length);
                Some(buffer)
            };
            if let Ok(Some(response)) = tokio::time::timeout(UPSTREAM_TIMEOUT, exchange).await {
                return Some(response);
            }
        }
        None
    }
}

pub fn upstream_resolvers(resolv_conf: &str) -> Vec<SocketAddr> {
    resolv_conf
        .lines()
        .filter_map(|line| line.trim().strip_prefix("nameserver"))
        .filter_map(|address| address.trim().parse::<IpAddr>().ok())
        .filter(|address| !address.is_loopback())
        .map(|address| SocketAddr::new(address, 53))
        .collect()
}

fn is_internal(name: &str) -> bool {
    name == INTERNAL_SUFFIX || name.ends_with(&format!(".{INTERNAL_SUFFIX}"))
}

fn normalize(name: &str) -> String {
    name.trim_end_matches('.').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use hickory_proto::op::{MessageType, OpCode, Query};
    use hickory_proto::rr::Name;

    use super::*;

    fn query_bytes(name: &str, record_type: RecordType) -> Vec<u8> {
        let mut message = Message::new(42, MessageType::Query, OpCode::Query);
        message.add_query(Query::query(Name::from_str(name).unwrap(), record_type));
        message.to_vec().unwrap()
    }

    fn records() -> DnsRecords {
        let records = DnsRecords::default();
        records.replace(HashMap::from([(
            "Web.maestro.internal".to_string(),
            vec!["10.200.1.2".parse().unwrap(), "10.200.1.3".parse().unwrap()],
        )]));
        records
    }

    #[tokio::test]
    async fn answers_internal_names_authoritatively() {
        let server = DnsServer::new(records(), vec![]);
        let response = server
            .answer(&query_bytes("web.maestro.internal.", RecordType::A))
            .await
            .unwrap();
        let message = Message::from_vec(&response).unwrap();
        assert_eq!(message.metadata.id, 42);
        assert!(message.metadata.authoritative);
        let addresses = message
            .answers
            .iter()
            .map(|record| record.data.to_string())
            .collect::<Vec<_>>();
        assert_eq!(addresses, ["10.200.1.2", "10.200.1.3"]);
    }

    #[tokio::test]
    async fn returns_nxdomain_for_unknown_internal_names() {
        let server = DnsServer::new(records(), vec![]);
        let response = server
            .answer(&query_bytes("missing.maestro.internal.", RecordType::A))
            .await
            .unwrap();
        let message = Message::from_vec(&response).unwrap();
        assert_eq!(message.metadata.response_code, ResponseCode::NXDomain);
    }

    #[tokio::test]
    async fn forwards_external_names_to_upstream() {
        let upstream = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buffer = vec![0u8; MAX_PACKET];
            let (length, client) = upstream.recv_from(&mut buffer).await.unwrap();
            let request = Message::from_vec(&buffer[..length]).unwrap();
            let mut reply = Message::response(request.metadata.id, OpCode::Query);
            reply.add_answer(Record::from_rdata(
                Name::from_str("example.com.").unwrap(),
                60,
                RData::A(A("93.184.216.34".parse().unwrap())),
            ));
            upstream
                .send_to(&reply.to_vec().unwrap(), client)
                .await
                .unwrap();
        });
        let server = DnsServer::new(records(), vec![upstream_address]);
        let response = server
            .answer(&query_bytes("example.com.", RecordType::A))
            .await
            .unwrap();
        let message = Message::from_vec(&response).unwrap();
        assert_eq!(message.answers[0].data.to_string(), "93.184.216.34");
    }

    #[test]
    fn parses_non_loopback_upstreams() {
        let resolv = "nameserver 127.0.0.53\nnameserver 10.0.0.2\n# comment\nnameserver fd00::1\n";
        assert_eq!(
            upstream_resolvers(resolv),
            [
                "10.0.0.2:53".parse::<SocketAddr>().unwrap(),
                "[fd00::1]:53".parse().unwrap()
            ]
        );
    }
}
