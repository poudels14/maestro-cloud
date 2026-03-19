use crate::deployment::types::{ServiceBuildConfig, ServiceDeployConfig, ServiceProvider};

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

pub fn validate_build_config(
    build: &Option<ServiceBuildConfig>,
    image: &Option<String>,
) -> Result<(Option<ServiceBuildConfig>, Option<String>), String> {
    match (build, image) {
        (Some(build), None) => {
            let repo = build.repo.trim();
            if repo.is_empty() {
                return Err("build.repo cannot be empty".to_string());
            }
            let dockerfile_path = build.dockerfile_path.trim();
            if dockerfile_path.is_empty() {
                return Err("build.dockerfilePath cannot be empty".to_string());
            }
            Ok((
                Some(ServiceBuildConfig {
                    repo: repo.to_string(),
                    branch: build.branch.clone(),
                    dockerfile_path: dockerfile_path.to_string(),
                    watch: build.watch,
                    env: build.env.clone(),
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
    provider: ServiceProvider,
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
    match provider {
        ServiceProvider::Docker => {
            let (build, image) = validate_build_config(build, image)?;
            Ok((build, image, deploy.clone()))
        }
        ServiceProvider::Shell => {
            if build.is_some() || image.is_some() {
                return Err(
                    "shell provider does not allow `build` or `image`; set deploy.command instead"
                        .to_string(),
                );
            }
            let Some(command) = deploy.command.as_ref() else {
                return Err("shell provider requires deploy.command".to_string());
            };
            if command.command.trim().is_empty() {
                return Err("shell provider requires non-empty deploy.command.command".to_string());
            }
            Ok((None, None, deploy.clone()))
        }
    }
}
