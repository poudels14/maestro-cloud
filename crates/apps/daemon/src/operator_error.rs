/// Construction failure from one concrete operator contract.
#[derive(Debug, thiserror::Error)]
pub enum OperatorSuiteError {
    /// The shared retry policy was invalid.
    #[error(transparent)]
    Backoff(#[from] kernel_controller::BackoffError),
    /// The shared watch resync policy was invalid.
    #[error(transparent)]
    RuntimeConfig(#[from] kernel_controller::RuntimeConfigError),
    /// Deployment lifecycle settings or identifiers were invalid.
    #[error(transparent)]
    Deployment(#[from] deployment::DeploymentError),
    /// Scheduler settings or identifiers were invalid.
    #[error(transparent)]
    Scheduler(#[from] scheduler::SchedulerError),
    /// Ingress settings or identifiers were invalid.
    #[error(transparent)]
    Ingress(#[from] ingress::IngressError),
    /// DNS settings or identifiers were invalid.
    #[error(transparent)]
    Dns(#[from] dns::DnsError),
    /// Firewall settings or identifiers were invalid.
    #[error(transparent)]
    Firewall(#[from] firewall::FirewallError),
    /// Build resource settings or identifiers were invalid.
    #[error(transparent)]
    Build(#[from] kernel_api::InvalidIdentifier),
    /// Git build-watch settings or identifiers were invalid.
    #[error(transparent)]
    BuildWatch(#[from] build::BuildWatchError),
    /// Preview derivation settings or resource state were invalid.
    #[error(transparent)]
    Preview(#[from] preview::PreviewError),
    /// Preview source polling or quota settings were invalid.
    #[error(transparent)]
    PreviewSource(#[from] preview::PreviewSourceError),
    /// Preview settings require an injected pull-request API.
    #[error("preview settings require a pull-request API backend")]
    PreviewBackendMissing,
    /// A pull-request API without preview settings would never be consumed.
    #[error("pull-request API backend was configured without preview settings")]
    PreviewBackendUnexpected,
    /// Upgrade settings require an injected node-maintenance backend.
    #[error("upgrade settings require a node-upgrade backend")]
    UpgradeBackendMissing,
    /// A node-upgrade backend without upgrade settings would never be consumed.
    #[error("node-upgrade backend was configured without upgrade settings")]
    UpgradeBackendUnexpected,
    /// Upgrade settings or resource identifiers were invalid.
    #[error(transparent)]
    Upgrade(#[from] upgrade::UpgradeError),
}
