use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId, SecretValue};
use kernel_store::{EtcdStore, EtcdTlsConfig, Store, TokioClock, derive_key};
use node_agent::{
    AUTHORITATIVE_DNS_PORT, AuthoritativeDnsResolver, BoundDnsServer, DnsResourceAgent,
    DnsServerSettings, SystemDnsPluginSettings, TailscaleDnsPluginSettings,
};
use tokio::sync::watch;
use url::Url;

/// Files and store coordinates needed by the delegated-network DNS role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsResolverLaunchConfig {
    /// Cluster whose authoritative `DnsRecord` resources are served.
    pub cluster_id: ClusterId,
    /// Node identity acknowledged by the resolver agent.
    pub node_id: NodeId,
    /// Mutually authenticated HTTPS endpoints for control-plane store members.
    pub endpoints: Vec<String>,
    /// PEM trust root mounted into the resolver workload.
    pub certificate_authority: PathBuf,
    /// PEM client certificate mounted into the resolver workload.
    pub client_certificate: PathBuf,
    /// PEM client private key mounted into the resolver workload.
    pub client_private_key: PathBuf,
    /// Cluster store-encryption secret mounted into the resolver workload.
    pub store_encryption_secret: PathBuf,
    /// UDP and TCP port bound on all interfaces inside the workload namespace.
    pub port: u16,
    /// Periodic full-snapshot interval in addition to store watches.
    pub resync_interval: Duration,
    /// Optional scoped forwarding through managed Tailscale gateway workloads.
    pub dns_plugin_settings: Option<TailscaleDnsPluginSettings>,
}

impl DnsResolverLaunchConfig {
    /// Checks network and secret-file boundaries before contacting the store.
    pub fn validate(&self) -> Result<(), DnsResolverLaunchError> {
        if self.endpoints.is_empty() {
            return Err(invalid("at least one store endpoint is required"));
        }
        if self
            .endpoints
            .iter()
            .any(|endpoint| !secure_store_endpoint(endpoint))
        {
            return Err(invalid("store endpoints must use HTTPS"));
        }
        for path in [
            &self.certificate_authority,
            &self.client_certificate,
            &self.client_private_key,
            &self.store_encryption_secret,
        ] {
            if !path.is_absolute() {
                return Err(invalid(format!(
                    "resolver credential path `{}` is not absolute",
                    path.display()
                )));
            }
        }
        DnsServerSettings::container(self.port)?;
        if self.resync_interval.is_zero() {
            return Err(invalid("DNS resync interval must be nonzero"));
        }
        Ok(())
    }
}

fn secure_store_endpoint(endpoint: &str) -> bool {
    Url::parse(endpoint).is_ok_and(|url| {
        url.scheme() == "https"
            && url.has_host()
            && url.username().is_empty()
            && url.password().is_none()
    })
}

/// Runs the store-fed Hickory resolver inside an isolated runtime network.
pub async fn run_dns_resolver(
    config: DnsResolverLaunchConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<(), DnsResolverLaunchError> {
    config.validate()?;
    let certificate_authority =
        read_file(&config.certificate_authority, "read certificate authority").await?;
    let client_certificate =
        read_file(&config.client_certificate, "read client certificate").await?;
    let client_private_key =
        read_file(&config.client_private_key, "read client private key").await?;
    let store_encryption_secret = read_secret(
        &config.store_encryption_secret,
        "read store encryption secret",
    )
    .await?;
    let tls = EtcdTlsConfig::for_endpoints(
        certificate_authority,
        client_certificate,
        client_private_key,
    );
    let encryption_key = derive_key(store_encryption_secret.expose())?;
    let store = Arc::new(
        EtcdStore::connect_with_tls_and_encryption(config.endpoints, tls, encryption_key).await?,
    ) as Arc<dyn Store>;
    let resolver = AuthoritativeDnsResolver::new()?;
    let resolver = match &config.dns_plugin_settings {
        Some(settings) => settings.attach(resolver),
        None => resolver,
    };
    let upstream = SystemDnsPluginSettings::from_resolv_conf_file(
        Path::new("/etc/resolv.conf"),
        Duration::from_secs(5),
    )
    .map_err(|error| invalid(format!("invalid upstream DNS settings: {error}")))?;
    let resolver = upstream.attach(resolver);
    let agent = DnsResourceAgent::new(
        store,
        &config.cluster_id,
        config.node_id,
        resolver.clone(),
        Arc::new(TokioClock::new()),
        config.resync_interval,
    )?;
    agent.reconcile_once().await?;
    let server = BoundDnsServer::bind(DnsServerSettings::container(config.port)?, resolver).await?;
    let resource_shutdown = shutdown.clone();
    let resource_task = async move {
        agent
            .run(resource_shutdown)
            .await
            .map_err(DnsResolverLaunchError::Resource)
    };
    let server_task = async move {
        server
            .serve(shutdown)
            .await
            .map_err(DnsResolverLaunchError::Server)
    };
    tokio::try_join!(resource_task, server_task)?;
    Ok(())
}

async fn read_file(path: &Path, action: &'static str) -> Result<Vec<u8>, DnsResolverLaunchError> {
    tokio::fs::read(path)
        .await
        .map_err(|source| DnsResolverLaunchError::Io {
            action,
            path: path.to_path_buf(),
            source,
        })
}

async fn read_secret(
    path: &Path,
    action: &'static str,
) -> Result<SecretValue, DnsResolverLaunchError> {
    let bytes = read_file(path, action).await?;
    String::from_utf8(bytes)
        .map(SecretValue::new)
        .map_err(|source| DnsResolverLaunchError::InvalidText {
            path: path.to_path_buf(),
            source,
        })
}

fn invalid(detail: impl Into<String>) -> DnsResolverLaunchError {
    DnsResolverLaunchError::InvalidConfiguration {
        detail: detail.into(),
    }
}

/// Why the delegated-network authoritative resolver failed to start or run.
#[derive(Debug, thiserror::Error)]
pub enum DnsResolverLaunchError {
    /// Static resolver settings were unsafe or incomplete.
    #[error("invalid DNS resolver configuration: {detail}")]
    InvalidConfiguration { detail: String },
    /// A mounted credential file could not be read.
    #[error("failed to {action} `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The store encryption secret was not UTF-8.
    #[error("store encryption secret `{}` is not UTF-8: {source}", path.display())]
    InvalidText {
        path: PathBuf,
        #[source]
        source: std::string::FromUtf8Error,
    },
    /// The cluster store-encryption secret could not derive a key.
    #[error(transparent)]
    Encryption(#[from] kernel_store::EncryptionError),
    /// The resolver could not connect to or read the cluster store.
    #[error(transparent)]
    Store(#[from] kernel_store::StoreError),
    /// The in-memory authoritative zone could not be constructed.
    #[error(transparent)]
    Resolver(#[from] node_agent::DnsResolverError),
    /// Store resources could not be projected into the authoritative zone.
    #[error(transparent)]
    Resource(#[from] node_agent::DnsResourceError),
    /// UDP or TCP listener setup or serving failed.
    #[error(transparent)]
    Server(#[from] node_agent::DnsServerError),
}

/// Standard authoritative DNS port used by the managed resolver workload.
pub const DEFAULT_DNS_RESOLVER_PORT: u16 = AUTHORITATIVE_DNS_PORT;
