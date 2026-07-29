use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use hickory_server::proto::op::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};

use crate::dns_socks::{parse_response, query_message};
use crate::{
    AuthoritativeDnsResolver, DnsLookup, DnsQueryType, DnsResolverPlugin, DnsResolverPluginError,
    MAESTRO_DNS_ZONE,
};

const DNS_PORT: u16 = 53;
const MAX_UDP_RESPONSE_BYTES: usize = 4_096;

/// Validated host resolver configuration used for public DNS forwarding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemDnsPluginSettings {
    nameservers: Vec<SocketAddr>,
    timeout: Duration,
}

impl SystemDnsPluginSettings {
    /// Loads bounded upstream resolver addresses from one resolv.conf file.
    pub fn from_resolv_conf_file(
        path: &Path,
        timeout: Duration,
    ) -> Result<Self, DnsResolverPluginError> {
        let contents = std::fs::read_to_string(path).map_err(|error| {
            DnsResolverPluginError::new(format!(
                "failed to read upstream DNS configuration `{}`: {error}",
                path.display()
            ))
        })?;
        Self::from_resolv_conf(&contents, timeout)
    }

    /// Parses nameserver entries without inheriting search domains or resolver options.
    pub fn from_resolv_conf(
        contents: &str,
        timeout: Duration,
    ) -> Result<Self, DnsResolverPluginError> {
        if timeout.is_zero() {
            return Err(DnsResolverPluginError::new(
                "upstream DNS timeout must be nonzero",
            ));
        }
        let mut nameservers = BTreeSet::new();
        for line in contents.lines() {
            let line = line.split('#').next().unwrap_or_default().trim();
            let mut fields = line.split_whitespace();
            if fields.next() != Some("nameserver") {
                continue;
            }
            let value = fields.next().ok_or_else(|| {
                DnsResolverPluginError::new("upstream DNS nameserver address is missing")
            })?;
            let address = value.parse::<IpAddr>().map_err(|error| {
                DnsResolverPluginError::new(format!(
                    "invalid upstream DNS nameserver `{value}`: {error}"
                ))
            })?;
            if address.is_unspecified() || address.is_multicast() {
                return Err(DnsResolverPluginError::new(format!(
                    "unsafe upstream DNS nameserver `{address}`"
                )));
            }
            nameservers.insert(SocketAddr::new(address, DNS_PORT));
        }
        if nameservers.is_empty() {
            return Err(DnsResolverPluginError::new(
                "upstream DNS configuration has no nameservers",
            ));
        }
        Ok(Self {
            nameservers: nameservers.into_iter().collect(),
            timeout,
        })
    }

    /// Appends public DNS forwarding after scoped Maestro plugins.
    pub fn attach(&self, resolver: AuthoritativeDnsResolver) -> AuthoritativeDnsResolver {
        resolver.with_plugin(Arc::new(SystemDnsResolverPlugin::with_client(
            self.nameservers.clone(),
            self.timeout,
            Arc::new(DirectDnsForwardClient::default()),
        )))
    }
}

#[async_trait]
pub(crate) trait UpstreamDnsClient: Send + Sync {
    async fn lookup(
        &self,
        nameserver: SocketAddr,
        name: &str,
        query_type: DnsQueryType,
        timeout: Duration,
    ) -> Result<DnsLookup, DnsResolverPluginError>;
}

pub(crate) struct SystemDnsResolverPlugin {
    nameservers: Vec<SocketAddr>,
    timeout: Duration,
    client: Arc<dyn UpstreamDnsClient>,
}

impl SystemDnsResolverPlugin {
    pub(crate) fn with_client(
        nameservers: Vec<SocketAddr>,
        timeout: Duration,
        client: Arc<dyn UpstreamDnsClient>,
    ) -> Self {
        Self {
            nameservers,
            timeout,
            client,
        }
    }
}

#[async_trait]
impl DnsResolverPlugin for SystemDnsResolverPlugin {
    async fn lookup(
        &self,
        name: &str,
        query_type: DnsQueryType,
    ) -> Result<Option<DnsLookup>, DnsResolverPluginError> {
        if name == MAESTRO_DNS_ZONE || name.ends_with(&format!(".{MAESTRO_DNS_ZONE}")) {
            return Ok(None);
        }
        let mut failures = Vec::new();
        for nameserver in &self.nameservers {
            match self
                .client
                .lookup(*nameserver, name, query_type, self.timeout)
                .await
            {
                Ok(lookup) => return Ok(Some(lookup)),
                Err(error) => failures.push(error.to_string()),
            }
        }
        Err(DnsResolverPluginError::new(format!(
            "every upstream DNS resolver failed: {}",
            failures.join("; ")
        )))
    }
}

#[derive(Debug, Default)]
pub(crate) struct DirectDnsForwardClient {
    next_query_id: AtomicU16,
}

#[async_trait]
impl UpstreamDnsClient for DirectDnsForwardClient {
    async fn lookup(
        &self,
        nameserver: SocketAddr,
        name: &str,
        query_type: DnsQueryType,
        timeout: Duration,
    ) -> Result<DnsLookup, DnsResolverPluginError> {
        tokio::time::timeout(timeout, self.lookup_inner(nameserver, name, query_type))
            .await
            .map_err(|_| {
                DnsResolverPluginError::new(format!("upstream DNS resolver {nameserver} timed out"))
            })?
    }
}

impl DirectDnsForwardClient {
    async fn lookup_inner(
        &self,
        nameserver: SocketAddr,
        name: &str,
        query_type: DnsQueryType,
    ) -> Result<DnsLookup, DnsResolverPluginError> {
        let query_id = self.next_query_id.fetch_add(1, Ordering::Relaxed);
        let request = query_message(query_id, name, query_type)?;
        let bind_address = match nameserver.ip() {
            IpAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
            IpAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
        };
        let socket = UdpSocket::bind(bind_address)
            .await
            .map_err(|error| transport("bind upstream DNS socket", error))?;
        socket
            .send_to(&request, nameserver)
            .await
            .map_err(|error| transport("send upstream DNS request", error))?;
        let mut response = vec![0_u8; MAX_UDP_RESPONSE_BYTES];
        let (length, source) = socket
            .recv_from(&mut response)
            .await
            .map_err(|error| transport("receive upstream DNS response", error))?;
        if source != nameserver {
            return Err(DnsResolverPluginError::new(format!(
                "upstream DNS response came from unexpected source {source}"
            )));
        }
        response.truncate(length);
        let message = Message::from_vec(&response).map_err(|error| {
            DnsResolverPluginError::new(format!("decode DNS response: {error}"))
        })?;
        if message.metadata.truncation {
            return tcp_lookup(nameserver, query_id, &request).await;
        }
        parse_response(query_id, &response)
    }
}

async fn tcp_lookup(
    nameserver: SocketAddr,
    query_id: u16,
    request: &[u8],
) -> Result<DnsLookup, DnsResolverPluginError> {
    let mut stream = TcpStream::connect(nameserver)
        .await
        .map_err(|error| transport("connect to upstream DNS resolver", error))?;
    let request_length = u16::try_from(request.len())
        .map_err(|_| DnsResolverPluginError::new("DNS request exceeds TCP wire limit"))?;
    stream
        .write_all(&request_length.to_be_bytes())
        .await
        .map_err(|error| transport("write upstream DNS request length", error))?;
    stream
        .write_all(request)
        .await
        .map_err(|error| transport("write upstream DNS request", error))?;
    let response_length = stream
        .read_u16()
        .await
        .map_err(|error| transport("read upstream DNS response length", error))?;
    let mut response = vec![0_u8; usize::from(response_length)];
    stream
        .read_exact(&mut response)
        .await
        .map_err(|error| transport("read upstream DNS response", error))?;
    parse_response(query_id, &response)
}

fn transport(action: &'static str, error: std::io::Error) -> DnsResolverPluginError {
    DnsResolverPluginError::new(format!("{action}: {error}"))
}
