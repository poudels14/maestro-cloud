use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use kernel_api::SecretValue;
use runtime::{ArtifactStoreError, RegistryCredential, RegistryCredentialProvider};
use zeroize::Zeroizing;

pub(crate) struct DaemonRegistryCredentialProvider {
    static_credentials: BTreeMap<String, RegistryCredential>,
    ecr: Arc<dyn EcrCredentialSource>,
}

impl DaemonRegistryCredentialProvider {
    pub(crate) fn new(
        static_credentials: BTreeMap<String, RegistryCredential>,
        sdk_config: &aws_config::SdkConfig,
    ) -> Self {
        Self {
            static_credentials,
            ecr: Arc::new(AwsEcrCredentialSource {
                sdk_config: sdk_config.clone(),
            }),
        }
    }

    #[cfg(test)]
    fn with_ecr_source(
        static_credentials: BTreeMap<String, RegistryCredential>,
        ecr: Arc<dyn EcrCredentialSource>,
    ) -> Self {
        Self {
            static_credentials,
            ecr,
        }
    }
}

#[async_trait]
impl RegistryCredentialProvider for DaemonRegistryCredentialProvider {
    async fn credential(
        &self,
        registry_host: &str,
    ) -> Result<Option<RegistryCredential>, ArtifactStoreError> {
        if let Some(credential) = self.static_credentials.get(registry_host) {
            return Ok(Some(credential.clone()));
        }
        let Some(registry) = EcrRegistry::parse(registry_host) else {
            return Ok(None);
        };
        self.ecr.credential(&registry).await.map(Some)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EcrRegistry {
    host: String,
    region: String,
}

impl EcrRegistry {
    fn parse(host: &str) -> Option<Self> {
        let stem = host
            .strip_suffix(".amazonaws.com")
            .or_else(|| host.strip_suffix(".amazonaws.com.cn"))?;
        let mut labels = stem.split('.');
        let account_id = labels.next()?;
        let dkr = labels.next()?;
        let ecr = labels.next()?;
        let region = labels.next()?;
        if labels.next().is_some()
            || account_id.len() != 12
            || !account_id.bytes().all(|byte| byte.is_ascii_digit())
            || dkr != "dkr"
            || ecr != "ecr"
            || region.is_empty()
            || !region
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return None;
        }
        Some(Self {
            host: host.to_owned(),
            region: region.to_owned(),
        })
    }
}

#[async_trait]
trait EcrCredentialSource: Send + Sync {
    async fn credential(
        &self,
        registry: &EcrRegistry,
    ) -> Result<RegistryCredential, ArtifactStoreError>;
}

struct AwsEcrCredentialSource {
    sdk_config: aws_config::SdkConfig,
}

#[async_trait]
impl EcrCredentialSource for AwsEcrCredentialSource {
    async fn credential(
        &self,
        registry: &EcrRegistry,
    ) -> Result<RegistryCredential, ArtifactStoreError> {
        let config = aws_sdk_ecr::config::Builder::from(&self.sdk_config)
            .region(aws_sdk_ecr::config::Region::new(registry.region.clone()))
            .build();
        let response = aws_sdk_ecr::Client::from_conf(config)
            .get_authorization_token()
            .send()
            .await
            .map_err(|error| ArtifactStoreError::Unavailable {
                message: format!(
                    "request ECR authorization for registry `{}`: {error}",
                    registry.host
                ),
            })?;
        let authorization = response
            .authorization_data()
            .iter()
            .find(|authorization| {
                authorization
                    .proxy_endpoint()
                    .and_then(normalize_proxy_endpoint)
                    .and_then(EcrRegistry::parse)
                    .is_some_and(|authorized| authorized.region == registry.region)
            })
            .ok_or_else(|| ArtifactStoreError::Unavailable {
                message: format!(
                    "ECR authorization response omitted registry `{}`",
                    registry.host
                ),
            })?;
        let token =
            authorization
                .authorization_token()
                .ok_or_else(|| ArtifactStoreError::Unavailable {
                    message: format!(
                        "ECR authorization response omitted credentials for registry `{}`",
                        registry.host
                    ),
                })?;
        decode_authorization_token(token, &registry.host)
    }
}

fn normalize_proxy_endpoint(endpoint: &str) -> Option<&str> {
    endpoint
        .strip_prefix("https://")
        .map(|endpoint| endpoint.trim_end_matches('/'))
        .filter(|endpoint| !endpoint.is_empty())
}

fn decode_authorization_token(
    token: &str,
    registry_host: &str,
) -> Result<RegistryCredential, ArtifactStoreError> {
    let decoded =
        Zeroizing::new(
            BASE64
                .decode(token)
                .map_err(|_| ArtifactStoreError::Unavailable {
                    message: format!(
                        "ECR returned malformed authorization for registry `{registry_host}`"
                    ),
                })?,
        );
    let decoded = std::str::from_utf8(&decoded).map_err(|_| ArtifactStoreError::Unavailable {
        message: format!("ECR returned malformed authorization for registry `{registry_host}`"),
    })?;
    let (username, password) = decoded
        .split_once(':')
        .filter(|(username, password)| *username == "AWS" && !password.is_empty())
        .ok_or_else(|| ArtifactStoreError::Unavailable {
            message: format!("ECR returned malformed authorization for registry `{registry_host}`"),
        })?;
    Ok(RegistryCredential::new(
        username,
        SecretValue::new(password),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn recognizes_only_exact_private_ecr_hosts() -> Result<(), ArtifactStoreError> {
        let registry = EcrRegistry::parse("761018876802.dkr.ecr.us-west-2.amazonaws.com")
            .ok_or_else(|| ArtifactStoreError::Rejected {
                message: "private ECR host was not recognized".to_owned(),
            })?;
        assert_eq!(registry.region, "us-west-2");
        assert!(EcrRegistry::parse("public.ecr.aws").is_none());
        assert!(
            EcrRegistry::parse("761018876802.dkr.ecr.us-west-2.amazonaws.com.evil.test").is_none()
        );
        assert!(EcrRegistry::parse("761018876802.dkr.ecr.us-west-2.amazonaws.com:443").is_none());
        assert!(EcrRegistry::parse("not-an-account.dkr.ecr.us-west-2.amazonaws.com").is_none());
        Ok(())
    }

    #[test]
    fn decodes_only_the_expected_ecr_basic_auth_shape() -> Result<(), ArtifactStoreError> {
        let token = BASE64.encode("AWS:short-lived-password");
        let credential = decode_authorization_token(&token, "registry.example")?;
        assert_eq!(credential.username(), "AWS");
        assert_eq!(credential.secret().expose(), "short-lived-password");
        assert!(
            decode_authorization_token(&BASE64.encode("other:password"), "registry.example")
                .is_err()
        );
        assert!(decode_authorization_token("not-base64", "registry.example").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn resolves_ecr_on_demand_without_caching_and_preserves_exact_static_credentials()
    -> Result<(), ArtifactStoreError> {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = DaemonRegistryCredentialProvider::with_ecr_source(
            BTreeMap::from([(
                "registry.depot.dev".to_owned(),
                RegistryCredential::new("x-token", SecretValue::new("depot-token")),
            )]),
            Arc::new(RecordingEcrSource {
                calls: calls.clone(),
            }),
        );

        let depot = provider
            .credential("registry.depot.dev")
            .await?
            .ok_or_else(|| ArtifactStoreError::Rejected {
                message: "static registry credential missing".to_owned(),
            })?;
        assert_eq!(depot.username(), "x-token");
        assert!(provider.credential("registry.example").await?.is_none());
        for _ in 0..2 {
            let ecr = provider
                .credential("761018876802.dkr.ecr.us-west-2.amazonaws.com")
                .await?
                .ok_or_else(|| ArtifactStoreError::Rejected {
                    message: "ECR credential missing".to_owned(),
                })?;
            assert_eq!(ecr.username(), "AWS");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    struct RecordingEcrSource {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl EcrCredentialSource for RecordingEcrSource {
        async fn credential(
            &self,
            registry: &EcrRegistry,
        ) -> Result<RegistryCredential, ArtifactStoreError> {
            assert_eq!(
                registry.host,
                "761018876802.dkr.ecr.us-west-2.amazonaws.com"
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(RegistryCredential::new(
                "AWS",
                SecretValue::new("ephemeral-password"),
            ))
        }
    }
}
