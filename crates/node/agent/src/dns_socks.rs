use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use hickory_server::proto::op::{Message, MessageType, Query, ResponseCode};
use hickory_server::proto::rr::{DNSClass, Name, RData, Record, RecordType};
use kernel_api::DnsRecordValue;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::dns_plugin::DnsResolverPluginError;
use crate::{DnsAnswer, DnsLookup, DnsQueryType, DnsResponseCode};

const SOCKS_VERSION: u8 = 5;
const SOCKS_CONNECT: u8 = 1;
const SOCKS_IPV4: u8 = 1;
const SOCKS_IPV6: u8 = 4;
const SOCKS_DOMAIN: u8 = 3;

#[async_trait]
pub(crate) trait DnsForwardClient: Send + Sync {
    async fn lookup(
        &self,
        proxy: SocketAddr,
        nameserver: SocketAddr,
        name: &str,
        query_type: DnsQueryType,
        timeout: Duration,
    ) -> Result<DnsLookup, DnsResolverPluginError>;
}

#[derive(Debug, Default)]
pub(crate) struct Socks5DnsForwardClient {
    next_query_id: AtomicU16,
}

#[async_trait]
impl DnsForwardClient for Socks5DnsForwardClient {
    async fn lookup(
        &self,
        proxy: SocketAddr,
        nameserver: SocketAddr,
        name: &str,
        query_type: DnsQueryType,
        timeout: Duration,
    ) -> Result<DnsLookup, DnsResolverPluginError> {
        tokio::time::timeout(
            timeout,
            self.lookup_inner(proxy, nameserver, name, query_type),
        )
        .await
        .map_err(|_| DnsResolverPluginError::new(format!("DNS path through {proxy} timed out")))?
    }
}

impl Socks5DnsForwardClient {
    async fn lookup_inner(
        &self,
        proxy: SocketAddr,
        nameserver: SocketAddr,
        name: &str,
        query_type: DnsQueryType,
    ) -> Result<DnsLookup, DnsResolverPluginError> {
        let mut stream = TcpStream::connect(proxy)
            .await
            .map_err(|error| transport("connect to Tailscale SOCKS gateway", error))?;
        negotiate_socks(&mut stream, nameserver).await?;
        let query_id = self.next_query_id.fetch_add(1, Ordering::Relaxed);
        let request = query_message(query_id, name, query_type)?;
        let request_length = u16::try_from(request.len())
            .map_err(|_| DnsResolverPluginError::new("DNS request exceeds TCP wire limit"))?;
        stream
            .write_all(&request_length.to_be_bytes())
            .await
            .map_err(|error| transport("write DNS request length", error))?;
        stream
            .write_all(&request)
            .await
            .map_err(|error| transport("write DNS request", error))?;
        let response_length = stream
            .read_u16()
            .await
            .map_err(|error| transport("read DNS response length", error))?;
        let mut response = vec![0_u8; usize::from(response_length)];
        stream
            .read_exact(&mut response)
            .await
            .map_err(|error| transport("read DNS response", error))?;
        parse_response(query_id, &response)
    }
}

async fn negotiate_socks(
    stream: &mut TcpStream,
    nameserver: SocketAddr,
) -> Result<(), DnsResolverPluginError> {
    stream
        .write_all(&[SOCKS_VERSION, 1, 0])
        .await
        .map_err(|error| transport("write SOCKS greeting", error))?;
    let mut greeting = [0_u8; 2];
    stream
        .read_exact(&mut greeting)
        .await
        .map_err(|error| transport("read SOCKS greeting", error))?;
    if greeting != [SOCKS_VERSION, 0] {
        return Err(DnsResolverPluginError::new(
            "Tailscale SOCKS gateway rejected unauthenticated access",
        ));
    }

    let mut request = vec![SOCKS_VERSION, SOCKS_CONNECT, 0];
    match nameserver.ip() {
        std::net::IpAddr::V4(address) => {
            request.push(SOCKS_IPV4);
            request.extend_from_slice(&address.octets());
        }
        std::net::IpAddr::V6(address) => {
            request.push(SOCKS_IPV6);
            request.extend_from_slice(&address.octets());
        }
    }
    request.extend_from_slice(&nameserver.port().to_be_bytes());
    stream
        .write_all(&request)
        .await
        .map_err(|error| transport("write SOCKS connect request", error))?;
    let mut header = [0_u8; 4];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|error| transport("read SOCKS connect response", error))?;
    if header[0] != SOCKS_VERSION || header[1] != 0 {
        return Err(DnsResolverPluginError::new(format!(
            "Tailscale SOCKS connect failed with status {}",
            header[1]
        )));
    }
    consume_bound_address(stream, header[3]).await
}

async fn consume_bound_address(
    stream: &mut TcpStream,
    address_type: u8,
) -> Result<(), DnsResolverPluginError> {
    let address_length = match address_type {
        SOCKS_IPV4 => 4,
        SOCKS_IPV6 => 16,
        SOCKS_DOMAIN => usize::from(
            stream
                .read_u8()
                .await
                .map_err(|error| transport("read SOCKS bound domain length", error))?,
        ),
        other => {
            return Err(DnsResolverPluginError::new(format!(
                "Tailscale SOCKS response used unknown address type {other}"
            )));
        }
    };
    let mut discarded = vec![0_u8; address_length + 2];
    stream
        .read_exact(&mut discarded)
        .await
        .map_err(|error| transport("read SOCKS bound address", error))?;
    Ok(())
}

pub(crate) fn query_message(
    query_id: u16,
    name: &str,
    query_type: DnsQueryType,
) -> Result<Vec<u8>, DnsResolverPluginError> {
    let mut message = Message::query();
    message.metadata.id = query_id;
    message.add_query(Query::query(
        Name::from_ascii(name)
            .map_err(|error| DnsResolverPluginError::new(format!("invalid DNS name: {error}")))?,
        record_type(query_type),
    ));
    message
        .to_vec()
        .map_err(|error| DnsResolverPluginError::new(format!("encode DNS request: {error}")))
}

pub(crate) fn parse_response(
    query_id: u16,
    response: &[u8],
) -> Result<DnsLookup, DnsResolverPluginError> {
    let message = Message::from_vec(response)
        .map_err(|error| DnsResolverPluginError::new(format!("decode DNS response: {error}")))?;
    if message.metadata.id != query_id || message.metadata.message_type != MessageType::Response {
        return Err(DnsResolverPluginError::new(
            "DNS response does not match the forwarded query",
        ));
    }
    let response_code = match message.metadata.response_code {
        ResponseCode::NoError => DnsResponseCode::NoError,
        ResponseCode::NXDomain => DnsResponseCode::NameError,
        ResponseCode::Refused => DnsResponseCode::Refused,
        code => {
            return Err(DnsResolverPluginError::new(format!(
                "remote DNS resolver returned {code}"
            )));
        }
    };
    let answers = message
        .answers
        .iter()
        .filter(|record| record.dns_class == DNSClass::IN)
        .map(answer)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DnsLookup {
        authoritative: message.metadata.authoritative,
        recursion_available: message.metadata.recursion_available,
        response_code,
        answers,
    })
}

fn answer(record: &Record) -> Result<DnsAnswer, DnsResolverPluginError> {
    let value = match &record.data {
        RData::A(value) => DnsRecordValue::A(value.0),
        RData::AAAA(value) => DnsRecordValue::Aaaa(value.0),
        RData::CNAME(value) => DnsRecordValue::Cname(value.0.to_lowercase().to_string()),
        RData::TXT(value) => DnsRecordValue::Txt(
            String::from_utf8(value.txt_data.iter().flatten().copied().collect())
                .map_err(|_| DnsResolverPluginError::new("remote TXT answer is not UTF-8"))?,
        ),
        RData::SRV(value) => DnsRecordValue::Srv {
            priority: value.priority,
            weight: value.weight,
            port: value.port,
            target: value.target.to_lowercase().to_string(),
        },
        other => {
            return Err(DnsResolverPluginError::new(format!(
                "remote DNS answer type {} is unsupported",
                other.record_type()
            )));
        }
    };
    Ok(DnsAnswer {
        name: record.name.to_lowercase().to_string(),
        ttl_secs: record.ttl,
        value,
    })
}

const fn record_type(query_type: DnsQueryType) -> RecordType {
    match query_type {
        DnsQueryType::A => RecordType::A,
        DnsQueryType::Aaaa => RecordType::AAAA,
        DnsQueryType::Cname => RecordType::CNAME,
        DnsQueryType::Txt => RecordType::TXT,
        DnsQueryType::Srv => RecordType::SRV,
        DnsQueryType::Any => RecordType::ANY,
        DnsQueryType::Other(value) => RecordType::Unknown(value),
    }
}

fn transport(action: &'static str, error: std::io::Error) -> DnsResolverPluginError {
    DnsResolverPluginError::new(format!("{action}: {error}"))
}
