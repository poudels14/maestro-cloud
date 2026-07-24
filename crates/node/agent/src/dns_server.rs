use std::iter;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;
use hickory_server::Server;
use hickory_server::net::runtime::Time;
use hickory_server::proto::op::{
    Edns, Header, HeaderCounts, MessageType, Metadata, OpCode, ResponseCode,
};
use hickory_server::proto::rr::rdata::{A, AAAA, CNAME, SRV, TXT};
use hickory_server::proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_server::server::{Request, RequestHandler, ResponseHandler, ResponseInfo};
use hickory_server::zone_handler::MessageResponseBuilder;
use kernel_api::DnsRecordValue;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::watch;

use crate::{AuthoritativeDnsResolver, DnsAnswer, DnsLookup, DnsQueryType, DnsResponseCode};

const TCP_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_RESPONSE_BUFFER_BYTES: usize = 64 * 1024;
const MAX_EDNS_PAYLOAD_BYTES: u16 = 4096;
const EPHEMERAL_BIND_ATTEMPTS: usize = 32;

/// Stable port exposed only on each node's workload bridge.
pub const AUTHORITATIVE_DNS_PORT: u16 = 53;

/// Validated bridge-only DNS listener configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DnsServerSettings {
    bind_address: SocketAddr,
}

impl DnsServerSettings {
    /// Accepts one concrete, non-loopback bridge address and nonzero port.
    pub fn new(bind_address: SocketAddr) -> Result<Self, DnsServerError> {
        if bind_address.port() == 0 {
            return Err(DnsServerError::ZeroPort);
        }
        let ip = bind_address.ip();
        if ip.is_unspecified()
            || ip.is_loopback()
            || ip.is_multicast()
            || ip == IpAddr::V4(Ipv4Addr::BROADCAST)
        {
            return Err(DnsServerError::UnsafeBindAddress { address: ip });
        }
        Ok(Self { bind_address })
    }

    /// Returns the exact bridge socket address to bind for both UDP and TCP.
    pub const fn bind_address(self) -> SocketAddr {
        self.bind_address
    }
}

/// Hickory UDP/TCP server bound to one node bridge address.
pub struct BoundDnsServer {
    server: Server<HickoryDnsHandler>,
    local_address: SocketAddr,
}

/// Owned DNS serving task returned by a listener binder.
#[async_trait]
pub trait DnsServerRuntime: Send {
    /// Serves until shutdown is requested or a listener fails.
    async fn serve(self: Box<Self>, shutdown: watch::Receiver<bool>) -> Result<(), DnsServerError>;
}

/// Socket-binding boundary used by the daemon composition root.
#[async_trait]
pub trait DnsServerBinder: Send + Sync {
    /// Binds UDP and TCP listeners for one authoritative resolver.
    async fn bind(
        &self,
        settings: DnsServerSettings,
        resolver: AuthoritativeDnsResolver,
    ) -> Result<Box<dyn DnsServerRuntime>, DnsServerError>;
}

/// Production binder backed by Hickory UDP and TCP listeners.
#[derive(Debug, Clone, Copy, Default)]
pub struct HickoryDnsServerBinder;

#[async_trait]
impl DnsServerBinder for HickoryDnsServerBinder {
    async fn bind(
        &self,
        settings: DnsServerSettings,
        resolver: AuthoritativeDnsResolver,
    ) -> Result<Box<dyn DnsServerRuntime>, DnsServerError> {
        Ok(Box::new(BoundDnsServer::bind(settings, resolver).await?))
    }
}

#[async_trait]
impl DnsServerRuntime for BoundDnsServer {
    async fn serve(self: Box<Self>, shutdown: watch::Receiver<bool>) -> Result<(), DnsServerError> {
        BoundDnsServer::serve(*self, shutdown).await
    }
}

impl BoundDnsServer {
    /// Binds UDP first, then TCP on the same validated bridge address.
    pub async fn bind(
        settings: DnsServerSettings,
        resolver: AuthoritativeDnsResolver,
    ) -> Result<Self, DnsServerError> {
        Self::bind_address(settings.bind_address(), resolver).await
    }

    pub(crate) async fn bind_address(
        bind_address: SocketAddr,
        resolver: AuthoritativeDnsResolver,
    ) -> Result<Self, DnsServerError> {
        if bind_address.port() == 0 {
            return Self::bind_ephemeral(bind_address, resolver).await;
        }
        let udp = UdpSocket::bind(bind_address)
            .await
            .map_err(|source| bind_error("UDP", bind_address, source))?;
        let tcp = TcpListener::bind(bind_address)
            .await
            .map_err(|source| bind_error("TCP", bind_address, source))?;
        Self::from_sockets(udp, tcp, resolver)
    }

    async fn bind_ephemeral(
        bind_address: SocketAddr,
        resolver: AuthoritativeDnsResolver,
    ) -> Result<Self, DnsServerError> {
        let mut last_address = bind_address;
        for _ in 0..EPHEMERAL_BIND_ATTEMPTS {
            let tcp = TcpListener::bind(bind_address)
                .await
                .map_err(|source| bind_error("TCP", bind_address, source))?;
            let shared_address = tcp.local_addr().map_err(|source| DnsServerError::Io {
                operation: "inspect TCP listener",
                source,
            })?;
            last_address = shared_address;
            match UdpSocket::bind(shared_address).await {
                Ok(udp) => return Self::from_sockets(udp, tcp, resolver),
                Err(source) if source.kind() == std::io::ErrorKind::AddrInUse => {}
                Err(source) => return Err(bind_error("UDP", shared_address, source)),
            }
        }
        Err(bind_error(
            "UDP",
            last_address,
            std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                "no shared ephemeral UDP/TCP port was available",
            ),
        ))
    }

    /// Returns the shared UDP/TCP listener address.
    pub const fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Serves until shutdown is requested or a listener fails.
    pub async fn serve(
        mut self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), DnsServerError> {
        let cancellation = self.server.shutdown_token().clone();
        loop {
            if *shutdown.borrow() {
                cancellation.cancel();
                return self.server.block_until_done().await.map_err(Into::into);
            }
            tokio::select! {
                result = self.server.block_until_done() => return result.map_err(Into::into),
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        cancellation.cancel();
                        return self.server.block_until_done().await.map_err(Into::into);
                    }
                }
            }
        }
    }

    fn from_sockets(
        udp: UdpSocket,
        tcp: TcpListener,
        resolver: AuthoritativeDnsResolver,
    ) -> Result<Self, DnsServerError> {
        let udp_address = udp.local_addr().map_err(|source| DnsServerError::Io {
            operation: "inspect UDP listener",
            source,
        })?;
        let tcp_address = tcp.local_addr().map_err(|source| DnsServerError::Io {
            operation: "inspect TCP listener",
            source,
        })?;
        if udp_address != tcp_address {
            return Err(DnsServerError::ListenerAddressMismatch {
                udp_address,
                tcp_address,
            });
        }
        let mut server = Server::new(HickoryDnsHandler { resolver });
        server.register_socket(udp);
        server.register_listener(tcp, TCP_TIMEOUT, TCP_RESPONSE_BUFFER_BYTES);
        Ok(Self {
            server,
            local_address: udp_address,
        })
    }
}

#[derive(Clone)]
struct HickoryDnsHandler {
    resolver: AuthoritativeDnsResolver,
}

#[async_trait]
impl RequestHandler for HickoryDnsHandler {
    async fn handle_request<R: ResponseHandler, T: Time>(
        &self,
        request: &Request,
        response_handle: R,
    ) -> ResponseInfo {
        if request.metadata.message_type != MessageType::Query
            || request.metadata.op_code != OpCode::Query
        {
            return send_error(request, ResponseCode::NotImp, response_handle).await;
        }
        let Ok(request_info) = request.request_info() else {
            return send_error(request, ResponseCode::FormErr, response_handle).await;
        };
        if request_info.query.query_class() != DNSClass::IN
            || matches!(
                request_info.query.query_type(),
                RecordType::AXFR | RecordType::IXFR
            )
        {
            return send_error(request, ResponseCode::Refused, response_handle).await;
        }
        if request
            .edns
            .as_ref()
            .is_some_and(|request_edns| request_edns.version() > 0)
        {
            return send_error(request, ResponseCode::BADVERS, response_handle).await;
        }
        let lookup = match self
            .resolver
            .lookup(
                &request_info.query.name().to_string(),
                query_type(request_info.query.query_type()),
            )
            .await
        {
            Ok(lookup) => lookup,
            Err(_) => return send_error(request, ResponseCode::ServFail, response_handle).await,
        };
        let answers = match lookup
            .answers
            .iter()
            .map(answer_record)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(answers) => answers,
            Err(_) => return send_error(request, ResponseCode::ServFail, response_handle).await,
        };
        send_lookup(request, lookup, &answers, response_handle).await
    }
}

async fn send_lookup(
    request: &Request,
    lookup: DnsLookup,
    answers: &[Record],
    mut response_handle: impl ResponseHandler,
) -> ResponseInfo {
    let mut metadata = Metadata::response_from_request(&request.metadata);
    metadata.authoritative = lookup.authoritative;
    metadata.recursion_available = false;
    metadata.response_code = response_code(lookup.response_code);
    let response_edns = response_edns(request);
    let response = MessageResponseBuilder::new(&request.queries, response_edns.as_ref()).build(
        metadata,
        answers.iter(),
        iter::empty(),
        iter::empty(),
        iter::empty(),
    );
    match response_handle.send_response(response).await {
        Ok(info) => info,
        Err(_) => failed_response_info(metadata),
    }
}

async fn send_error(
    request: &Request,
    code: ResponseCode,
    mut response_handle: impl ResponseHandler,
) -> ResponseInfo {
    let mut response_edns = response_edns(request);
    if code.high() != 0
        && let Some(edns) = response_edns.as_mut()
    {
        edns.set_rcode_high(code.high());
    }
    let response = MessageResponseBuilder::new(&request.queries, response_edns.as_ref())
        .error_msg(&request.metadata, code);
    match response_handle.send_response(response).await {
        Ok(info) => info,
        Err(_) => {
            let mut metadata = Metadata::response_from_request(&request.metadata);
            metadata.response_code = ResponseCode::ServFail;
            failed_response_info(metadata)
        }
    }
}

fn response_edns(request: &Request) -> Option<Edns> {
    request.edns.as_ref().map(|request_edns| {
        let mut response = Edns::new();
        response.set_version(0);
        response.set_max_payload(
            request_edns
                .max_payload()
                .clamp(512, MAX_EDNS_PAYLOAD_BYTES),
        );
        response
    })
}

fn failed_response_info(metadata: Metadata) -> ResponseInfo {
    ResponseInfo::from(Header {
        metadata,
        counts: HeaderCounts::default(),
    })
}

fn query_type(record_type: RecordType) -> DnsQueryType {
    match record_type {
        RecordType::A => DnsQueryType::A,
        RecordType::AAAA => DnsQueryType::Aaaa,
        RecordType::CNAME => DnsQueryType::Cname,
        RecordType::TXT => DnsQueryType::Txt,
        RecordType::SRV => DnsQueryType::Srv,
        RecordType::ANY => DnsQueryType::Any,
        other => DnsQueryType::Other(u16::from(other)),
    }
}

const fn response_code(code: DnsResponseCode) -> ResponseCode {
    match code {
        DnsResponseCode::NoError => ResponseCode::NoError,
        DnsResponseCode::NameError => ResponseCode::NXDomain,
        DnsResponseCode::Refused => ResponseCode::Refused,
    }
}

fn answer_record(answer: &DnsAnswer) -> Result<Record, String> {
    let name = Name::from_ascii(&answer.name).map_err(|error| error.to_string())?;
    let data = match &answer.value {
        DnsRecordValue::A(address) => RData::A(A(*address)),
        DnsRecordValue::Aaaa(address) => RData::AAAA(AAAA(*address)),
        DnsRecordValue::Cname(target) => RData::CNAME(CNAME(
            Name::from_ascii(target).map_err(|error| error.to_string())?,
        )),
        DnsRecordValue::Txt(text) => RData::TXT(TXT::new(vec![text.clone()])),
        DnsRecordValue::Srv {
            priority,
            weight,
            port,
            target,
        } => RData::SRV(SRV::new(
            *priority,
            *weight,
            *port,
            Name::from_ascii(target).map_err(|error| error.to_string())?,
        )),
    };
    Ok(Record::from_rdata(name, answer.ttl_secs, data))
}

fn bind_error(
    transport: &'static str,
    address: SocketAddr,
    source: std::io::Error,
) -> DnsServerError {
    DnsServerError::Bind {
        transport,
        address,
        source,
    }
}

/// Invalid listener policy, socket failure, or Hickory server failure.
#[derive(Debug, thiserror::Error)]
pub enum DnsServerError {
    /// DNS must not bind an ephemeral production port.
    #[error("DNS server port must be nonzero")]
    ZeroPort,
    /// DNS may bind only a concrete non-loopback bridge address.
    #[error("DNS server cannot bind unsafe address `{address}`")]
    UnsafeBindAddress { address: IpAddr },
    /// UDP or TCP could not bind the requested bridge address.
    #[error("failed to bind DNS {transport} listener `{address}`: {source}")]
    Bind {
        transport: &'static str,
        address: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    /// Bound sockets unexpectedly used different addresses.
    #[error("DNS UDP `{udp_address}` and TCP `{tcp_address}` listeners differ")]
    ListenerAddressMismatch {
        udp_address: SocketAddr,
        tcp_address: SocketAddr,
    },
    /// A listener inspection or test binding operation failed.
    #[error("failed to {operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    /// Hickory listener processing failed.
    #[error(transparent)]
    Server(#[from] hickory_server::net::NetError),
}
