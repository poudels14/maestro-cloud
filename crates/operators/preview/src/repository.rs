use gix_url::Scheme;
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
    let repository = repository.trim();
    let parsed = gix_url::parse(repository.into())
        .map_err(|_| "GitHub repository is not a valid Git remote URL".to_string())?;
    match parsed.scheme {
        Scheme::Http | Scheme::Https if parsed.user().is_none() && parsed.password().is_none() => {}
        Scheme::Ssh if parsed.user() == Some("git") && parsed.password().is_none() => {}
        _ => return Err("GitHub repository must use HTTP(S) or Git SSH".to_string()),
    }
    if !parsed
        .host()
        .is_some_and(|host| host.eq_ignore_ascii_case("github.com"))
    {
        return Err("preview repositories must be hosted on github.com".to_string());
    }
    let path = std::str::from_utf8(parsed.path.as_ref())
        .map_err(|_| "GitHub repository path must be UTF-8".to_string())?
        .trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
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
