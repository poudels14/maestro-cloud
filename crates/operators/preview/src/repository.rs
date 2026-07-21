use kernel_api::{ArtifactTemplate, BuildSource, Service};

/// Parsed coordinates for one GitHub repository.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct GithubRepository {
    pub(crate) owner: String,
    pub(crate) name: String,
    pub(crate) full_name: String,
}

pub(crate) fn service_repository(service: &Service) -> Result<GithubRepository, String> {
    let ArtifactTemplate::Build { template } = &service.spec.artifact else {
        return Err("preview-enabled services must use a build artifact".to_string());
    };
    let BuildSource::Git { repository, .. } = &template.source else {
        return Err("preview-enabled services must use a Git build source".to_string());
    };
    parse_github_repository(repository)
}

fn parse_github_repository(repository: &str) -> Result<GithubRepository, String> {
    let trimmed = repository
        .trim()
        .trim_end_matches('/')
        .trim_end_matches(".git");
    let path = if let Some(path) = trimmed.strip_prefix("git@github.com:") {
        path
    } else {
        let without_scheme = trimmed
            .strip_prefix("https://")
            .or_else(|| trimmed.strip_prefix("http://"))
            .or_else(|| trimmed.strip_prefix("ssh://git@"))
            .ok_or_else(|| "GitHub repository must use HTTPS or SSH".to_string())?;
        without_scheme
            .strip_prefix("github.com/")
            .ok_or_else(|| "preview repositories must be hosted on github.com".to_string())?
    };
    let mut components = path.split('/');
    let owner = components.next().unwrap_or_default();
    let name = components.next().unwrap_or_default();
    if owner.is_empty() || name.is_empty() || components.next().is_some() {
        return Err("GitHub repository must identify exactly one owner and repository".to_string());
    }
    let owner = owner.to_ascii_lowercase();
    let name = name.to_ascii_lowercase();
    Ok(GithubRepository {
        full_name: format!("{owner}/{name}"),
        owner,
        name,
    })
}
