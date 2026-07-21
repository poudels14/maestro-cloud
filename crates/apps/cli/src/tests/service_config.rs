use std::collections::BTreeMap;

use kernel_api::{ArtifactTemplate, ExecPolicy, SecretValue};

use crate::CliError;
use crate::config_source::ConfigSourceReader;
use crate::service_config::load_services;

struct MemoryReader {
    sources: BTreeMap<String, String>,
}

impl ConfigSourceReader for MemoryReader {
    async fn read(&self, source: &str) -> Result<String, CliError> {
        self.sources
            .get(source)
            .cloned()
            .ok_or_else(|| CliError::not_found(format!("missing fixture `{source}`")))
    }
}

#[tokio::test]
async fn familiar_jsonc_shape_maps_to_typed_service_and_reports_ignored_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let source = "file:///config/maestro.services.jsonc";
    let reader = MemoryReader {
        sources: BTreeMap::from([(
            source.to_string(),
            r#"{
                $schema: "maestro.schema.json",
                futureRoot: true,
                services: {
                    api: {
                        name: "API",
                        image: "registry.example/api:1",
                        futureService: "ignored",
                        ingress: { host: "api.example.com", port: 8080 },
                        deploy: {
                            exposePorts: [8080],
                            replicas: 2,
                            exec: false,
                            healthcheckPath: "/health",
                            env: { items: { MODE: "production" } },
                            secrets: {
                                mountPath: "/run/secrets/api.env",
                                items: { TOKEN: "super-secret-token" }
                            }
                        }
                    }
                }
            }"#
            .to_string(),
        )]),
    };
    let loaded = load_services(source, &reader).await?;
    assert_eq!(
        loaded.ignored_fields,
        ["futureRoot", "services.api.futureService"]
    );
    let desired = loaded
        .services
        .values()
        .next()
        .ok_or("missing desired service")?;
    assert!(matches!(
        desired.spec.artifact,
        ArtifactTemplate::Image { .. }
    ));
    assert_eq!(desired.spec.replicas, 2);
    assert_eq!(desired.spec.exec, ExecPolicy::Denied);
    assert_eq!(
        desired.spec.environment.get("MODE").map(String::as_str),
        Some("production")
    );
    assert_eq!(
        desired
            .spec
            .secrets
            .as_ref()
            .and_then(|secrets| secrets.items.get("TOKEN")),
        Some(&SecretValue::new("super-secret-token"))
    );
    assert!(desired.spec.version.starts_with("cfg-"));
    assert!(desired.ingress.is_some());
    Ok(())
}

#[tokio::test]
async fn value_sources_resolve_relative_files_aws_secrets_and_environment_defaults()
-> Result<(), Box<dyn std::error::Error>> {
    let source = "file:///config/maestro.services.jsonc";
    let reader = MemoryReader {
        sources: BTreeMap::from([
            (
                source.to_string(),
                r#"{
                    services: {
                        api: {
                            name: "API",
                            build: {
                                repo: "https://github.com/example/api.git",
                                branch: "main",
                                dockerfile: "Dockerfile",
                                env: { source: "build.env" },
                                secrets: { source: "aws-secret://build-secrets" }
                            },
                            deploy: {
                                env: { items: { MODE: "${MAESTRO_TEST_MODE:-fallback}" } },
                                replicas: 1
                            }
                        }
                    }
                }"#
                .to_string(),
            ),
            (
                "file:///config/build.env".to_string(),
                "PROFILE=release\nFEATURES=default".to_string(),
            ),
            (
                "aws-secret://build-secrets".to_string(),
                r#"{"REGISTRY_TOKEN":"private-token"}"#.to_string(),
            ),
        ]),
    };
    let loaded = load_services(source, &reader).await?;
    let desired = loaded
        .services
        .values()
        .next()
        .ok_or("missing desired service")?;
    let ArtifactTemplate::Build { template } = &desired.spec.artifact else {
        return Err("expected build artifact".into());
    };
    assert_eq!(
        template.environment.get("PROFILE").map(String::as_str),
        Some("release")
    );
    assert_eq!(
        template.secrets.get("REGISTRY_TOKEN"),
        Some(&SecretValue::new("private-token"))
    );
    assert_eq!(
        desired.spec.environment.get("MODE").map(String::as_str),
        Some("fallback")
    );
    Ok(())
}

#[tokio::test]
async fn validation_errors_include_the_exact_service_field_path()
-> Result<(), Box<dyn std::error::Error>> {
    let source = "file:///config/maestro.services.jsonc";
    let reader = MemoryReader {
        sources: BTreeMap::from([(
            source.to_string(),
            r#"{
                services: {
                    api: {
                        name: "API",
                        image: "api:latest",
                        deploy: {
                            replicas: 1,
                            env: {
                                source: "aws-secret://env",
                                items: { MODE: "production" }
                            }
                        }
                    }
                }
            }"#
            .to_string(),
        )]),
    };
    let error = load_services(source, &reader)
        .await
        .expect_err("mixed source and items must fail");
    assert!(
        error
            .to_string()
            .contains("services.api.deploy.env: set either `source` or `items`")
    );
    Ok(())
}
