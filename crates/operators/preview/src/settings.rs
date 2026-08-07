use crate::error::PreviewError;

/// Static derivation settings shared by all previews in one cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewSettings {
    pub(super) preview_domain: String,
    dashboard_origin: Option<url::Url>,
}

impl PreviewSettings {
    /// Validates a preview DNS suffix.
    pub fn new(preview_domain: impl Into<String>) -> Result<Self, PreviewError> {
        let preview_domain = preview_domain.into();
        let normalized = preview_domain.trim().trim_end_matches('.');
        if normalized.is_empty()
            || normalized.split('.').any(|label| {
                label.is_empty()
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '-')
            })
        {
            return Err(PreviewError::InvalidSettings {
                message: format!("preview domain `{preview_domain}` is not a DNS suffix"),
            });
        }
        Ok(Self {
            preview_domain: normalized.to_ascii_lowercase(),
            dashboard_origin: None,
        })
    }

    /// Attaches the Maestro dashboard origin used by GitHub deployment links.
    pub fn with_dashboard_origin(
        mut self,
        dashboard_origin: impl AsRef<str>,
    ) -> Result<Self, PreviewError> {
        let dashboard_origin = dashboard_origin.as_ref();
        let parsed =
            url::Url::parse(dashboard_origin).map_err(|_| PreviewError::InvalidSettings {
                message: format!("preview dashboard origin `{dashboard_origin}` is not a URL"),
            })?;
        let private_http = parsed.scheme() == "http"
            && matches!(
                parsed.host(),
                Some(url::Host::Ipv4(address)) if address.is_private()
            );
        if (parsed.scheme() != "https" && !private_http)
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(PreviewError::InvalidSettings {
                message: format!(
                    "preview dashboard origin `{dashboard_origin}` must be an HTTPS origin or a private IPv4 HTTP origin"
                ),
            });
        }
        self.dashboard_origin = Some(parsed);
        Ok(self)
    }

    /// DNS suffix used to expose derived preview services.
    pub fn preview_domain(&self) -> &str {
        &self.preview_domain
    }

    /// Maestro dashboard origin linked from native GitHub deployments.
    pub fn dashboard_origin(&self) -> Option<&url::Url> {
        self.dashboard_origin.as_ref()
    }
}
