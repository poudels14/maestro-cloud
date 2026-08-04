use std::collections::BTreeMap;

use kernel_api::{
    ArtifactArchiveId, ArtifactTemplate, ExecPolicy, FirewallDirection, FirewallSubject,
    FirewallVerdict, ReplicaSpread, SecretValue, ServiceId, TransportProtocol, VolumeSource,
};

use crate::CliError;
use crate::config_source::ConfigSourceReader;
use crate::service_config::{load_services, load_uploaded_service};

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
                            replicaSpread: "bestEffort",
                            exec: false,
                            healthcheckPath: "/health",
                            env: { items: { MODE: "production" } },
                            secrets: {
                                mountPath: "/run/secrets/api.env",
                                items: { TOKEN: "super-secret-token" }
                            },
                            volumes: [{
                                managedVolume: "api-data",
                                mountPath: "/var/lib/api"
                            }, {
                                replicaManagedVolume: "worker-state",
                                mountPath: "/var/lib/worker"
                            }],
                            egress: {
                                allow: [{ cidr: "10.0.0.0/24", ports: [443, 443] }]
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
    assert_eq!(
        desired.spec.placement.replica_spread,
        ReplicaSpread::BestEffort
    );
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
            .and_then(|secrets| match secrets {
                kernel_api::SecretMountSpec::Dotenv { items, .. } => items.get("TOKEN"),
                kernel_api::SecretMountSpec::Files { .. } => None,
            }),
        Some(&SecretValue::new("super-secret-token"))
    );
    assert!(matches!(
        desired.spec.volumes.first().map(|volume| &volume.source),
        Some(VolumeSource::Managed { name }) if name == "api-data"
    ));
    assert!(matches!(
        desired.spec.volumes.get(1).map(|volume| &volume.source),
        Some(VolumeSource::ReplicaManaged { name }) if name == "worker-state"
    ));
    assert!(desired.spec.version.starts_with("cfg-"));
    let rollout = desired.rollout_spec(&ServiceId::new("api")?)?;
    assert_eq!(rollout.service.exposed_ports, [8080]);
    let ingress = rollout.ingress.ok_or("missing ingress rollout")?;
    assert_eq!(ingress.service_id, ServiceId::new("api")?);
    assert_eq!(ingress.hosts, ["api.example.com"]);
    assert_eq!(ingress.target_port, 8080);
    let egress = rollout.egress.ok_or("missing egress rollout")?;
    assert_eq!(egress.direction, FirewallDirection::Egress);
    assert_eq!(
        egress.subject,
        FirewallSubject::Service(ServiceId::new("api")?)
    );
    assert_eq!(egress.default_verdict, FirewallVerdict::Deny);
    assert_eq!(egress.rules.len(), 1);
    let rule = egress.rules.first().ok_or("missing egress rule")?;
    assert_eq!(rule.cidr, "10.0.0.0/24");
    assert_eq!(rule.protocol, TransportProtocol::Any);
    assert_eq!(rule.ports.len(), 1);
    let port = rule.ports.first().ok_or("missing egress port")?;
    assert_eq!(port.start, 443);
    assert_eq!(port.end, 443);
    assert_eq!(rule.verdict, FirewallVerdict::Allow);
    Ok(())
}

#[tokio::test]
async fn uploaded_service_requires_the_services_document_envelope()
-> Result<(), Box<dyn std::error::Error>> {
    let source = "file:///config/maestro.services.jsonc";
    let reader = MemoryReader {
        sources: BTreeMap::from([(
            source.to_string(),
            r#"{
                id: "my-service",
                name: "My Service",
                build: { dockerfile: "Dockerfile" },
                ingress: { host: "my-service.local", port: 80 },
                deploy: { healthcheckPath: "/health", replicas: 1 }
            }"#
            .to_string(),
        )]),
    };
    let archive_id = ArtifactArchiveId::from_sha256([7; 32]);
    let error = load_uploaded_service(source, None, archive_id, &reader)
        .await
        .expect_err("single-service compatibility document must be rejected");
    assert!(error.to_string().contains("missing field `services`"));
    Ok(())
}

#[tokio::test]
async fn value_sources_resolve_relative_files_and_preserve_expanded_aws_references()
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
                                registry: "registry.example/team/",
                                depot: { project: "project-123" },
                                env: { source: "build.env" },
                            },
                            deploy: {
                                env: { items: { MODE: "fallback" } },
                                secrets: {
                                    mountPath: "/run/secrets/app.env",
                                    source: "${MAESTRO_TEST_SECRET_SOURCE:-aws-secret://deploy-secrets}"
                                },
                                replicas: 1
                            }
                        }
                    }
                }"#
                .to_string(),
            ),
            (
                "file:///config/build.env".to_string(),
                "PROFILE=\"release candidate\" # build profile\nFEATURES=default\\ features\nexport TARGET=musl"
                    .to_string(),
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
        Some("release candidate")
    );
    assert_eq!(
        template.environment.get("FEATURES").map(String::as_str),
        Some("default features")
    );
    assert_eq!(
        template.environment.get("TARGET").map(String::as_str),
        Some("musl")
    );
    assert!(template.secrets.is_empty());
    assert_eq!(template.registry.as_deref(), Some("registry.example/team"));
    assert_eq!(
        template.depot.as_ref().map(|depot| depot.project.as_str()),
        Some("project-123")
    );
    assert_eq!(
        desired.spec.environment.get("MODE").map(String::as_str),
        Some("fallback")
    );
    let Some(kernel_api::SecretMountSpec::Dotenv { source, items, .. }) = &desired.spec.secrets
    else {
        return Err("expected dotenv secret mount".into());
    };
    assert_eq!(source.as_deref(), Some("aws-secret://deploy-secrets"));
    assert!(
        items.is_empty(),
        "AWS secret contents must not be loaded by the CLI"
    );
    Ok(())
}

#[tokio::test]
async fn preview_environment_preserves_maestro_templates_while_expanding_local_variables()
-> Result<(), Box<dyn std::error::Error>> {
    let source = "file:///config/maestro.services.jsonc";
    let reader = MemoryReader {
        sources: BTreeMap::from([(
            source.to_owned(),
            r#"{
                services: {
                    api: {
                        name: "API",
                        ingress: { host: "api.example.test", port: 8080 },
                        preview: {
                            enabled: true,
                            env: {
                                items: {
                                    PREVIEW_URL: "https://${{ MAESTRO_PREVIEW_HOST }}"
                                }
                            }
                        },
                        deploy: {
                            replicas: 1,
                            env: {
                                items: {
                                    MODE: "${MAESTRO_TEST_MODE:-fallback}"
                                }
                            }
                        },
                        build: {
                            repo: "https://github.com/example/api.git",
                            dockerfile: "Dockerfile"
                        }
                    }
                }
            }"#
            .to_owned(),
        )]),
    };

    let loaded = load_services(source, &reader).await?;
    let desired = loaded.services.values().next().ok_or("missing service")?;

    assert_eq!(
        desired.spec.environment.get("MODE").map(String::as_str),
        Some("fallback")
    );
    assert_eq!(
        desired
            .spec
            .preview
            .as_ref()
            .and_then(|preview| preview.environment.get("PREVIEW_URL"))
            .map(String::as_str),
        Some("https://${{ MAESTRO_PREVIEW_HOST }}")
    );
    Ok(())
}

#[tokio::test]
async fn deploy_environment_rejects_maestro_templates() -> Result<(), Box<dyn std::error::Error>> {
    let source = "file:///config/maestro.services.jsonc";
    let reader = MemoryReader {
        sources: BTreeMap::from([(
            source.to_owned(),
            r#"{
                services: {
                    api: {
                        name: "API",
                        image: "api:latest",
                        deploy: {
                            replicas: 1,
                            env: {
                                items: {
                                    URL: "https://${{ MAESTRO_PREVIEW_HOST }}"
                                }
                            }
                        }
                    }
                }
            }"#
            .to_owned(),
        )]),
    };

    let error = load_services(source, &reader)
        .await
        .expect_err("deploy environment must reject Maestro templates");
    assert!(
        error
            .to_string()
            .contains("Maestro templates are supported only in preview.env")
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

#[tokio::test]
async fn host_volumes_remain_compatible_and_ambiguous_sources_fail_exactly()
-> Result<(), Box<dyn std::error::Error>> {
    let source = "file:///config/maestro.services.jsonc";
    let host_reader = MemoryReader {
        sources: BTreeMap::from([(
            source.to_string(),
            r#"{
                services: {
                    api: {
                        name: "API",
                        image: "api:latest",
                        deploy: {
                            replicas: 1,
                            nodeAffinity: { "node-id": "node-1" },
                            volumes: [{
                                hostPath: "/srv/api",
                                mountPath: "/var/lib/api",
                                readOnly: true
                            }]
                        }
                    }
                }
            }"#
            .to_owned(),
        )]),
    };
    let loaded = load_services(source, &host_reader).await?;
    let service = loaded.services.values().next().ok_or("missing service")?;
    assert!(matches!(
        service.spec.volumes.first().map(|volume| &volume.source),
        Some(VolumeSource::HostPath { path, node_id })
            if path == "/srv/api" && node_id.as_str() == "node-1"
    ));

    let ambiguous_reader = MemoryReader {
        sources: BTreeMap::from([(
            source.to_string(),
            r#"{
                services: {
                    api: {
                        name: "API",
                        image: "api:latest",
                        deploy: {
                            replicas: 1,
                            nodeAffinity: { "node-id": "node-1" },
                            volumes: [{
                                hostPath: "/srv/api",
                                managedVolume: "api-data",
                                mountPath: "/var/lib/api"
                            }]
                        }
                    }
                }
            }"#
            .to_owned(),
        )]),
    };
    let error = load_services(source, &ambiguous_reader)
        .await
        .expect_err("ambiguous volume source must fail");
    assert!(error.to_string().contains(
        "services.api.deploy.volumes[0]: set exactly one of `hostPath`, `managedVolume`, or \
         `replicaManagedVolume`"
    ));
    Ok(())
}
