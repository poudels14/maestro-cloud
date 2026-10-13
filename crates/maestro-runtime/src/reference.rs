const DEFAULT_REGISTRY: &str = "docker.io";
const OFFICIAL_REPOSITORY_PREFIX: &str = "library/";
const DEFAULT_TAG: &str = "latest";

pub fn normalize(reference: &str) -> String {
    let (name, digest) = match reference.split_once('@') {
        Some((name, digest)) => (name, Some(digest)),
        None => (reference, None),
    };
    let last_segment_start = name.rfind('/').map_or(0, |index| index + 1);
    let (repository, tag) = match name[last_segment_start..].split_once(':') {
        Some((_, tag)) => (&name[..name.len() - tag.len() - 1], Some(tag)),
        None => (name, None),
    };
    let first_component = repository.split('/').next().unwrap_or_default();
    let has_registry = repository.contains('/')
        && (first_component.contains('.')
            || first_component.contains(':')
            || first_component == "localhost");
    let qualified = match (has_registry, repository.contains('/')) {
        (true, _) => repository.to_string(),
        (false, true) => format!("{DEFAULT_REGISTRY}/{repository}"),
        (false, false) => format!("{DEFAULT_REGISTRY}/{OFFICIAL_REPOSITORY_PREFIX}{repository}"),
    };
    match (tag, digest) {
        (Some(tag), Some(digest)) => format!("{qualified}:{tag}@{digest}"),
        (None, Some(digest)) => format!("{qualified}@{digest}"),
        (Some(tag), None) => format!("{qualified}:{tag}"),
        (None, None) => format!("{qualified}:{DEFAULT_TAG}"),
    }
}

#[cfg(test)]
mod tests {
    use super::normalize;

    #[test]
    fn normalizes_docker_references() {
        let cases = [
            ("nginx", "docker.io/library/nginx:latest"),
            ("nginx:1.27", "docker.io/library/nginx:1.27"),
            ("traefik/whoami", "docker.io/traefik/whoami:latest"),
            ("ghcr.io/acme/api:d1", "ghcr.io/acme/api:d1"),
            ("localhost:5000/api", "localhost:5000/api:latest"),
            (
                "registry.local:5000/team/api:v2",
                "registry.local:5000/team/api:v2",
            ),
            ("nginx@sha256:abc", "docker.io/library/nginx@sha256:abc"),
            (
                "ghcr.io/acme/api:v1@sha256:abc",
                "ghcr.io/acme/api:v1@sha256:abc",
            ),
        ];
        cases.iter().for_each(|(input, expected)| {
            assert_eq!(normalize(input), *expected, "input {input}");
        });
    }
}
