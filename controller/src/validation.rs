use crate::deployment::types::{
    EnvConfig, IngressConfig, MAX_HEALTHCHECK_INTERVAL_SECS, MIN_HEALTHCHECK_INTERVAL_SECS,
    PreviewConfig, ServiceBuildConfig, ServiceDeployConfig,
};

pub fn validate_service_id(service_id: &str, field_name: &str) -> Result<(), String> {
    if service_id.is_empty() {
        return Err(format!("{field_name} cannot be empty"));
    }
    let is_url_safe = service_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_');
    if !is_url_safe {
        return Err(format!(
            "{field_name} must be URL-safe and contain only letters, numbers, '-' or '_'"
        ));
    }
    Ok(())
}

pub fn validate_user_service_id(service_id: &str, field_name: &str) -> Result<(), String> {
    validate_service_id(service_id, field_name)?;
    if service_id.to_ascii_lowercase().contains("-pr-") {
        return Err(format!(
            "{field_name} cannot contain the reserved `-pr-` infix"
        ));
    }
    Ok(())
}

pub fn validate_preview_config(
    service_id: &str,
    preview: &Option<PreviewConfig>,
    build: &Option<ServiceBuildConfig>,
    ingress: &Option<IngressConfig>,
) -> Result<(), String> {
    let Some(preview) = preview else {
        return Ok(());
    };
    parse_duration(&preview.close_grace_period, "preview.closeGracePeriod")?;
    if preview.replicas != 1 {
        return Err("preview.replicas must be 1 in v1".to_string());
    }
    if !preview.enabled {
        return Ok(());
    }
    validate_dns_label(service_id, "service id")?;
    validate_dns_label(&format!("{service_id}-pr-99999"), "derived preview id")?;
    let repo = build
        .as_ref()
        .and_then(|config| config.repo.as_deref())
        .ok_or_else(|| "preview-enabled services require build.repo".to_string())?;
    parse_github_repo(repo)?;
    if ingress.is_none() {
        return Err("preview-enabled services require ingress configuration".to_string());
    }
    Ok(())
}

pub fn parse_duration(value: &str, field_name: &str) -> Result<std::time::Duration, String> {
    let value = value.trim();
    let split_at = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (amount, unit) = value.split_at(split_at);
    let amount = amount.parse::<u64>().map_err(|_| {
        format!("{field_name} must be a duration such as `1d`, `12h`, `30m`, or `60s`")
    })?;
    if amount == 0 {
        return Err(format!("{field_name} must be greater than zero"));
    }
    let seconds = match unit {
        "s" => Some(amount),
        "m" => amount.checked_mul(60),
        "h" => amount.checked_mul(60 * 60),
        "d" => amount.checked_mul(24 * 60 * 60),
        _ => None,
    }
    .ok_or_else(|| format!("{field_name} must use one of the units `s`, `m`, `h`, or `d`"))?;
    Ok(std::time::Duration::from_secs(seconds))
}

pub fn parse_github_repo(repo: &str) -> Result<(String, String), String> {
    let trimmed = repo.trim();
    let path = if let Some(path) = trimmed.strip_prefix("git@github.com:") {
        path.to_string()
    } else {
        let parsed = reqwest::Url::parse(trimmed)
            .map_err(|_| "preview build.repo must be a github.com repository URL".to_string())?;
        if !parsed
            .host_str()
            .is_some_and(|host| host.eq_ignore_ascii_case("github.com"))
        {
            return Err("preview build.repo must be hosted on github.com".to_string());
        }
        parsed.path().trim_start_matches('/').to_string()
    };
    let path = path.trim_end_matches('/').trim_end_matches(".git");
    let mut segments = path.split('/');
    let owner = segments.next().unwrap_or_default();
    let name = segments.next().unwrap_or_default();
    if owner.is_empty() || name.is_empty() || segments.next().is_some() {
        return Err("preview build.repo must identify a github.com owner/repository".to_string());
    }
    Ok((owner.to_string(), name.to_string()))
}

fn validate_dns_label(value: &str, field_name: &str) -> Result<(), String> {
    let valid = !value.is_empty()
        && value.len() <= 63
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        });
    if !valid {
        return Err(format!(
            "{field_name} must be a lowercase DNS label no longer than 63 characters"
        ));
    }
    Ok(())
}

pub fn validate_build_config(
    build: &Option<ServiceBuildConfig>,
    image: &Option<String>,
) -> Result<(Option<ServiceBuildConfig>, Option<String>), String> {
    match (build, image) {
        (Some(build), None) => {
            let repo = build
                .repo
                .as_ref()
                .map(|repo| repo.trim().to_string())
                .filter(|repo| !repo.is_empty());
            let dockerfile = build.dockerfile.trim();
            if dockerfile.is_empty() {
                return Err("build.dockerfile cannot be empty".to_string());
            }
            validate_env_config(&build.env, "build.env")?;
            validate_env_config(&build.secrets, "build.secrets")?;
            let depot = build
                .depot
                .as_ref()
                .map(|depot| {
                    let project = depot.project.trim();
                    if project.is_empty() {
                        Err("build.depot.project cannot be empty".to_string())
                    } else {
                        Ok(crate::deployment::types::DepotConfig {
                            project: project.to_string(),
                        })
                    }
                })
                .transpose()?;
            Ok((
                Some(ServiceBuildConfig {
                    repo,
                    branch: build.branch.clone(),
                    dockerfile: dockerfile.to_string(),
                    watch: build.watch,
                    registry: build.registry.clone(),
                    depot,
                    env: build.env.clone(),
                    secrets: build.secrets.clone(),
                }),
                None,
            ))
        }
        (None, Some(image)) => {
            let image = image.trim();
            if image.is_empty() {
                return Err("image cannot be empty".to_string());
            }
            Ok((None, Some(image.to_string())))
        }
        (Some(_), Some(_)) => Err("set either `build` or `image`, not both".to_string()),
        (None, None) => Err("set either `build` or `image`".to_string()),
    }
}

pub fn validate_service_provider_config(
    build: &Option<ServiceBuildConfig>,
    image: &Option<String>,
    deploy: &ServiceDeployConfig,
) -> Result<
    (
        Option<ServiceBuildConfig>,
        Option<String>,
        ServiceDeployConfig,
    ),
    String,
> {
    validate_env_config(&deploy.env, "deploy.env")?;
    if let Some(secrets) = &deploy.secrets
        && secrets.source.is_some()
        && !secrets.items.is_empty()
    {
        return Err(
            "deploy.secrets cannot have both `source` and `items`; use one or the other"
                .to_string(),
        );
    }
    for (index, volume) in deploy.volumes.iter().enumerate() {
        if volume.host_path.trim().is_empty() {
            return Err(format!("deploy.volumes[{index}].hostPath cannot be empty"));
        }
        if volume.mount_path.trim().is_empty() {
            return Err(format!("deploy.volumes[{index}].mountPath cannot be empty"));
        }
        if !volume.host_path.starts_with('/') {
            return Err(format!(
                "deploy.volumes[{index}].hostPath must be an absolute path"
            ));
        }
        if !volume.mount_path.starts_with('/') {
            return Err(format!(
                "deploy.volumes[{index}].mountPath must be an absolute path"
            ));
        }
        if let Some(reason) = sensitive_host_path_reason(&volume.host_path) {
            return Err(format!(
                "deploy.volumes[{index}].hostPath `{}` is not allowed ({reason})",
                volume.host_path
            ));
        }
    }
    if let Some(affinity) = &deploy.node_affinity {
        if let Some(node_id) = &affinity.node_id
            && (node_id.len() != 12
                || !node_id
                    .chars()
                    .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit()))
        {
            return Err(
                "deploy.nodeAffinity.node-id must be exactly 12 lowercase alphanumeric characters"
                    .to_string(),
            );
        }
        for (key, value) in &affinity.labels {
            if key.trim().is_empty() || value.trim().is_empty() {
                return Err(
                    "deploy.nodeAffinity.labels keys and values cannot be empty".to_string()
                );
            }
        }
    }
    if deploy.replicas > 1 && has_writable_volume(deploy) {
        return Err(format!(
            "deploy.replicas ({}) cannot exceed 1 while a writable volume is mounted; \
             multiple replicas would share the host path and risk data corruption. \
             Mark the volume `readOnly: true` or keep replicas: 1.",
            deploy.replicas
        ));
    }
    let mut resolved_deploy = deploy.clone();
    for (index, rule) in resolved_deploy.egress.allow.iter_mut().enumerate() {
        let cidr = rule.cidr.trim();
        if cidr.is_empty() {
            return Err(format!("deploy.egress.allow[{index}].cidr cannot be empty"));
        }
        let cidr = crate::cluster::network::Ipv4Cidr::parse(cidr)
            .map_err(|error| format!("deploy.egress.allow[{index}].cidr is invalid: {error}"))?;
        rule.cidr = cidr.to_string();
        if rule.ports.contains(&0) {
            return Err(format!(
                "deploy.egress.allow[{index}].ports must contain only ports from 1 to 65535"
            ));
        }
        rule.ports.sort_unstable();
        rule.ports.dedup();
    }
    resolved_deploy.egress.allow.sort();
    resolved_deploy.egress.allow.dedup();
    resolved_deploy.healthcheck_interval = resolved_deploy
        .healthcheck_interval
        .clamp(MIN_HEALTHCHECK_INTERVAL_SECS, MAX_HEALTHCHECK_INTERVAL_SECS);

    let (build, image) = validate_build_config(build, image)?;
    Ok((build, image, resolved_deploy))
}

pub fn has_writable_volume(deploy: &ServiceDeployConfig) -> bool {
    deploy.volumes.iter().any(|volume| !volume.read_only)
}

pub fn validate_ingress_config(ingress: &Option<IngressConfig>) -> Result<(), String> {
    let Some(ingress) = ingress else {
        return Ok(());
    };
    if let Some(port) = ingress.port
        && port == 0
    {
        return Err("ingress.port cannot be 0".to_string());
    }
    for (index, host) in ingress.hosts().iter().enumerate() {
        validate_ingress_host(host, index)?;
    }
    if let Some(affinity) = &ingress.session_affinity {
        let header = affinity.header.as_str();
        if header.is_empty() || header.trim() != header {
            return Err(
                "ingress.sessionAffinity.header must be a non-empty HTTP header name without surrounding whitespace"
                    .to_string(),
            );
        }
        if header.eq_ignore_ascii_case("host") {
            return Err(
                "ingress.sessionAffinity.header cannot be `Host`; configure ingress.host or ingress.hosts instead"
                    .to_string(),
            );
        }
        axum::http::HeaderName::from_bytes(header.as_bytes()).map_err(|_| {
            format!("ingress.sessionAffinity.header `{header}` is not a valid HTTP header name")
        })?;
    }
    Ok(())
}

fn validate_ingress_host(host: &str, index: usize) -> Result<(), String> {
    let trimmed = host.trim();
    if trimmed.is_empty() {
        return Err(format!("ingress.hosts[{index}] cannot be empty"));
    }
    if trimmed.contains('`') {
        return Err(format!(
            "ingress.hosts[{index}] `{trimmed}` cannot contain backtick (`)"
        ));
    }
    let looks_like_regex = trimmed
        .chars()
        .any(|c| !c.is_ascii_alphanumeric() && c != '.' && c != '-');
    if !looks_like_regex {
        return Ok(());
    }
    regex::Regex::new(trimmed)
        .map_err(|err| format!("ingress.hosts[{index}] `{trimmed}` is not a valid regex: {err}"))?;
    Ok(())
}

fn validate_env_config(config: &EnvConfig, field: &str) -> Result<(), String> {
    if config.source.is_some() && !config.items.is_empty() {
        return Err(format!(
            "{field} cannot have both `source` and `items`; use one or the other"
        ));
    }
    Ok(())
}

fn sensitive_host_path_reason(path: &str) -> Option<&'static str> {
    if path.split('/').any(|seg| seg == ".." || seg == ".") {
        return Some("path traversal segments (`..`, `.`) are not allowed");
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return Some("`/` is reserved");
    }
    const EXACT: &[&str] = &[
        "/var/run/docker.sock",
        "/run/docker.sock",
        "/var/run/containerd/containerd.sock",
        "/run/containerd/containerd.sock",
    ];
    for &deny in EXACT {
        if trimmed == deny {
            return Some("container runtime socket");
        }
    }
    const PREFIXES: &[(&str, &str)] = &[
        ("/etc", "host config directory"),
        ("/proc", "kernel /proc"),
        ("/sys", "kernel /sys"),
        ("/dev", "host /dev"),
        ("/boot", "host /boot"),
        ("/root", "root user home"),
        ("/var/lib/docker", "docker state directory"),
        ("/var/lib/containerd", "containerd state directory"),
    ];
    for &(deny, reason) in PREFIXES {
        if trimmed == deny || trimmed.starts_with(&format!("{deny}/")) {
            return Some(reason);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::types::{ServiceEgressConfig, ServiceEgressRule, SessionAffinityConfig};

    fn ingress_with_affinity_header(header: &str) -> Option<IngressConfig> {
        Some(IngressConfig {
            host: Some("api.example.com".to_string()),
            hosts: Vec::new(),
            port: Some(8080),
            session_affinity: Some(SessionAffinityConfig {
                header: header.to_string(),
            }),
        })
    }

    #[test]
    fn validates_session_affinity_header_names() {
        assert!(validate_ingress_config(&ingress_with_affinity_header("X-Session-Node")).is_ok());
        assert!(
            validate_ingress_config(&ingress_with_affinity_header("bad header"))
                .unwrap_err()
                .contains("not a valid HTTP header name")
        );
        assert!(
            validate_ingress_config(&ingress_with_affinity_header("Host"))
                .unwrap_err()
                .contains("cannot be `Host`")
        );
    }

    fn deploy_with_egress(allow: Vec<ServiceEgressRule>) -> ServiceDeployConfig {
        ServiceDeployConfig {
            flags: Vec::new(),
            expose_ports: Vec::new(),
            command: None,
            healthcheck_path: None,
            healthcheck_interval: crate::deployment::types::DEFAULT_HEALTHCHECK_INTERVAL_SECS,
            replicas: 1,
            exec: true,
            max_restarts: None,
            env: Default::default(),
            secrets: None,
            volumes: Vec::new(),
            node_affinity: None,
            egress: ServiceEgressConfig { allow },
        }
    }

    #[test]
    fn validates_and_normalizes_service_egress_allows() {
        let deploy = deploy_with_egress(vec![
            ServiceEgressRule {
                cidr: " 10.0.10.0/24 ".to_string(),
                ports: vec![5432, 443, 5432],
            },
            ServiceEgressRule {
                cidr: "10.0.10.0/24".to_string(),
                ports: vec![443, 5432],
            },
        ]);

        let (_, _, deploy) =
            validate_service_provider_config(&None, &Some("example/api".to_string()), &deploy)
                .unwrap();
        assert_eq!(
            deploy.egress.allow,
            [ServiceEgressRule {
                cidr: "10.0.10.0/24".to_string(),
                ports: vec![443, 5432],
            }]
        );
    }

    #[test]
    fn rejects_invalid_service_egress_allows_with_field_paths() {
        for (rule, expected) in [
            (
                ServiceEgressRule {
                    cidr: "10.0.10.1/24".to_string(),
                    ports: vec![5432],
                },
                "deploy.egress.allow[0].cidr is invalid",
            ),
            (
                ServiceEgressRule {
                    cidr: "2001:db8::/32".to_string(),
                    ports: vec![5432],
                },
                "deploy.egress.allow[0].cidr is invalid",
            ),
            (
                ServiceEgressRule {
                    cidr: "10.0.10.0/24".to_string(),
                    ports: vec![0],
                },
                "deploy.egress.allow[0].ports",
            ),
        ] {
            let error = validate_service_provider_config(
                &None,
                &Some("example/api".to_string()),
                &deploy_with_egress(vec![rule]),
            )
            .unwrap_err();
            assert!(error.contains(expected), "unexpected error: {error}");
        }
    }

    #[test]
    fn validates_preview_duration_and_github_repository_formats() {
        assert_eq!(parse_duration("1d", "duration").unwrap().as_secs(), 86_400);
        assert_eq!(parse_duration("30m", "duration").unwrap().as_secs(), 1_800);
        assert!(parse_duration("0h", "duration").is_err());
        assert!(parse_duration("one day", "duration").is_err());
        assert_eq!(
            parse_github_repo("git@github.com:Baton-AI/baton.git").unwrap(),
            ("Baton-AI".to_string(), "baton".to_string())
        );
        assert_eq!(
            parse_github_repo("https://github.com/Baton-AI/baton.git").unwrap(),
            ("Baton-AI".to_string(), "baton".to_string())
        );
        assert!(parse_github_repo("https://gitlab.com/Baton-AI/baton.git").is_err());
    }

    #[test]
    fn reserves_preview_ids_for_derived_services() {
        assert!(validate_user_service_id("app-pr-12", "id").is_err());
        assert!(validate_user_service_id("app", "id").is_ok());
        assert!(validate_dns_label("valid-service", "id").is_ok());
        assert!(validate_dns_label("not_valid", "id").is_err());
    }
}
