use std::collections::BTreeMap;

use crate::CliError;
use crate::config_source::{ConfigSourceReader, load_merged};

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
async fn extends_merges_objects_replaces_arrays_and_resolves_relative_files()
-> Result<(), Box<dyn std::error::Error>> {
    let reader = MemoryReader {
        sources: BTreeMap::from([
            (
                "file:///configs/services.jsonc".to_string(),
                r#"{
                    $extends: "base.jsonc",
                    services: { api: { deploy: { replicas: 3 }, tags: ["overlay"] } }
                }"#
                .to_string(),
            ),
            (
                "file:///configs/base.jsonc".to_string(),
                r#"{
                    services: {
                        api: {
                            name: "API",
                            deploy: { replicas: 1, exec: true },
                            tags: ["base"]
                        }
                    }
                }"#
                .to_string(),
            ),
        ]),
    };
    let merged = load_merged("file:///configs/services.jsonc", &reader).await?;
    assert_eq!(
        merged.pointer("/services/api/name"),
        Some(&serde_json::json!("API"))
    );
    assert_eq!(
        merged.pointer("/services/api/deploy/replicas"),
        Some(&serde_json::json!(3))
    );
    assert_eq!(
        merged.pointer("/services/api/deploy/exec"),
        Some(&serde_json::json!(true))
    );
    assert_eq!(
        merged.pointer("/services/api/tags"),
        Some(&serde_json::json!(["overlay"]))
    );
    Ok(())
}

#[tokio::test]
async fn extends_rejects_cycles_and_relative_remote_children()
-> Result<(), Box<dyn std::error::Error>> {
    let reader = MemoryReader {
        sources: BTreeMap::from([
            (
                "file:///configs/a.jsonc".to_string(),
                r#"{$extends: "b.jsonc"}"#.to_string(),
            ),
            (
                "file:///configs/b.jsonc".to_string(),
                r#"{$extends: "a.jsonc"}"#.to_string(),
            ),
            (
                "aws-secret://remote".to_string(),
                r#"{$extends: "base.jsonc"}"#.to_string(),
            ),
        ]),
    };
    let cycle = load_merged("file:///configs/a.jsonc", &reader)
        .await
        .expect_err("cycle must fail");
    assert!(cycle.to_string().contains("cycle detected"));
    let remote = load_merged("aws-secret://remote", &reader)
        .await
        .expect_err("relative remote child must fail");
    assert!(
        remote
            .to_string()
            .contains("cannot be resolved from remote")
    );
    Ok(())
}
