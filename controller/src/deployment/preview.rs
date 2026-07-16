use anyhow::{Result, anyhow};
use sha2::{Digest, Sha256};

use crate::deployment::types::{PreviewSource, ServiceConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewPullRequest {
    pub number: u64,
    pub title: String,
    pub head_ref: String,
    pub head_sha: String,
    pub created_at: u64,
}

pub fn derive_preview_config(
    base: &ServiceConfig,
    pull_request: &PreviewPullRequest,
    preview_domain: &str,
) -> Result<ServiceConfig> {
    let preview = base
        .preview
        .as_ref()
        .filter(|preview| preview.enabled)
        .ok_or_else(|| anyhow!("service `{}` does not have previews enabled", base.id))?;
    let id = format!("{}-pr-{}", base.id, pull_request.number);
    crate::validation::validate_service_id(&id, "derived preview id")
        .map_err(anyhow::Error::msg)?;
    if id.len() > 63
        || id.starts_with('-')
        || id.ends_with('-')
        || !id.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
    {
        return Err(anyhow!(
            "derived preview id `{id}` is not a valid DNS label"
        ));
    }

    let mut config = base.clone();
    config.id = id.clone();
    config.name = id.clone();
    config.preview = None;
    config.preview_source = Some(PreviewSource {
        base_service_id: base.id.clone(),
        pr_number: pull_request.number,
        head_ref: pull_request.head_ref.clone(),
        head_sha: pull_request.head_sha.clone(),
        title: pull_request.title.clone(),
        created_at: pull_request.created_at,
        volumes_stripped: !config.deploy.volumes.is_empty(),
        closed_at: None,
    });

    let build = config
        .build
        .as_mut()
        .ok_or_else(|| anyhow!("preview-enabled service `{}` has no build config", base.id))?;
    build.branch = Some(pull_request.head_ref.clone());
    build.watch = true;

    config.deploy.env.items.extend(preview.env.items.clone());
    config.deploy.replicas = preview.replicas;
    config.deploy.volumes.clear();

    let ingress = config.ingress.as_mut().ok_or_else(|| {
        anyhow!(
            "preview-enabled service `{}` has no ingress config",
            base.id
        )
    })?;
    ingress.host = Some(format!("{id}.{}", preview_domain.trim_end_matches('.')));
    ingress.hosts.clear();

    config.version.clear();
    let mut version_config = config.clone();
    if let Some(source) = &mut version_config.preview_source {
        source.title.clear();
        source.head_sha.clear();
        source.closed_at = None;
    }
    let encoded = serde_json::to_vec(&version_config)?;
    config.version = format!("preview-{:x}", Sha256::digest(encoded));
    Ok(config)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::deployment::types::{
        EnvConfig, IngressConfig, PreviewConfig, PreviewEnvConfig, ServiceBuildConfig,
        ServiceDeployConfig, VolumeMount,
    };
    use crate::utils::crypto::SecretString;

    fn base_config() -> ServiceConfig {
        ServiceConfig {
            id: "app".to_string(),
            name: "App".to_string(),
            version: "cfg-base".to_string(),
            build: Some(ServiceBuildConfig {
                repo: Some("git@github.com:Baton-AI/baton.git".to_string()),
                branch: Some("main".to_string()),
                dockerfile: "Dockerfile".to_string(),
                watch: false,
                registry: None,
                depot: None,
                env: EnvConfig::default(),
                secrets: EnvConfig::default(),
            }),
            image: None,
            deploy: ServiceDeployConfig {
                flags: Vec::new(),
                expose_ports: Vec::new(),
                command: None,
                healthcheck_path: None,
                healthcheck_interval: 60,
                replicas: 3,
                max_restarts: None,
                env: EnvConfig {
                    source: Some("aws-secret://base-env".to_string()),
                    items: HashMap::from([
                        ("SHARED".to_string(), SecretString::new("base".to_string())),
                        ("BASE".to_string(), SecretString::new("yes".to_string())),
                    ]),
                },
                secrets: None,
                volumes: vec![VolumeMount {
                    host_path: "/data".to_string(),
                    mount_path: "/data".to_string(),
                    read_only: false,
                    owner: None,
                }],
                node_affinity: Some(crate::cluster::NodeAffinity {
                    node_id: None,
                    labels: std::collections::BTreeMap::from([(
                        "zone".to_string(),
                        "west".to_string(),
                    )]),
                }),
                egress: crate::deployment::types::ServiceEgressConfig {
                    allow: vec![crate::deployment::types::ServiceEgressRule {
                        cidr: "10.0.10.0/24".to_string(),
                        ports: vec![443],
                    }],
                },
            },
            ingress: Some(IngressConfig {
                host: Some("app.example.com".to_string()),
                hosts: vec!["legacy.example.com".to_string()],
                port: Some(3000),
                session_affinity: None,
            }),
            preview: Some(PreviewConfig {
                enabled: true,
                close_grace_period: "1d".to_string(),
                replicas: 1,
                env: PreviewEnvConfig {
                    items: HashMap::from([
                        (
                            "SHARED".to_string(),
                            SecretString::new("preview".to_string()),
                        ),
                        ("PREVIEW".to_string(), SecretString::new("yes".to_string())),
                    ]),
                },
            }),
            preview_source: None,
        }
    }

    fn pull_request() -> PreviewPullRequest {
        PreviewPullRequest {
            number: 123,
            title: "Preview me".to_string(),
            head_ref: "feature/preview".to_string(),
            head_sha: "0123456789abcdef".to_string(),
            created_at: 1,
        }
    }

    #[test]
    fn derives_isolated_preview_shape() {
        let derived =
            derive_preview_config(&base_config(), &pull_request(), "preview.getbaton.ai").unwrap();
        assert_eq!(derived.id, "app-pr-123");
        assert_eq!(derived.name, "app-pr-123");
        assert_eq!(
            derived.build.as_ref().unwrap().branch.as_deref(),
            Some("feature/preview")
        );
        assert!(derived.build.as_ref().unwrap().watch);
        assert_eq!(derived.deploy.replicas, 1);
        assert!(derived.deploy.volumes.is_empty());
        assert_eq!(
            derived
                .deploy
                .node_affinity
                .as_ref()
                .and_then(|affinity| affinity.labels.get("zone"))
                .map(String::as_str),
            Some("west")
        );
        assert_eq!(derived.deploy.egress.allow[0].cidr, "10.0.10.0/24");
        assert_eq!(derived.deploy.egress.allow[0].ports, [443]);
        assert_eq!(derived.deploy.env.items["SHARED"].as_str(), "preview");
        assert_eq!(derived.deploy.env.items["BASE"].as_str(), "yes");
        assert_eq!(derived.deploy.env.items["PREVIEW"].as_str(), "yes");
        assert_eq!(
            derived.deploy.env.source.as_deref(),
            Some("aws-secret://base-env")
        );
        assert_eq!(
            derived.ingress.as_ref().unwrap().host.as_deref(),
            Some("app-pr-123.preview.getbaton.ai")
        );
        assert!(derived.ingress.as_ref().unwrap().hosts.is_empty());
        assert!(derived.preview.is_none());
        assert!(derived.preview_source.as_ref().unwrap().volumes_stripped);
    }

    #[test]
    fn derived_version_changes_with_base_or_pr_configuration() {
        let first =
            derive_preview_config(&base_config(), &pull_request(), "preview.example.com").unwrap();
        let mut changed_base = base_config();
        changed_base.build.as_mut().unwrap().dockerfile = "Dockerfile.preview".to_string();
        let second =
            derive_preview_config(&changed_base, &pull_request(), "preview.example.com").unwrap();
        assert_ne!(first.version, second.version);

        let mut changed_pr = pull_request();
        changed_pr.head_ref = "feature/renamed".to_string();
        let third =
            derive_preview_config(&base_config(), &changed_pr, "preview.example.com").unwrap();
        assert_ne!(first.version, third.version);
    }
}
