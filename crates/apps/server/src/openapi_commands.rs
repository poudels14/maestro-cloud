use serde_json::{Map, Value, json};

use crate::openapi::get_operation;

pub(crate) fn insert_command_schemas(schemas: &mut Map<String, Value>) {
    schemas.insert(
        "CommandRequest".to_string(),
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["expectedRevision"],
            "properties": {
                "expectedRevision": {"$ref": "#/components/schemas/ResourceRevision"}
            }
        }),
    );
    schemas.insert(
        "ReplicaOverrideRequest".to_string(),
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["expectedRevision", "replicas"],
            "properties": {
                "expectedRevision": {"$ref": "#/components/schemas/ResourceRevision"},
                "replicas": {
                    "type": "integer",
                    "format": "uint32",
                    "minimum": 0,
                    "nullable": true,
                    "description": "Temporary replica count, or null to clear the override"
                }
            }
        }),
    );
    schemas.insert(
        "ServiceCommandResponse".to_string(),
        json!({
            "type": "object",
            "required": ["serviceId", "generation", "rollout"],
            "properties": {
                "serviceId": {"$ref": "#/components/schemas/ServiceId"},
                "generation": {"$ref": "#/components/schemas/Generation"},
                "rollout": {"$ref": "#/components/schemas/RolloutState"},
                "replicaOverride": {
                    "type": "integer",
                    "format": "uint32",
                    "minimum": 0
                },
                "deletionTimestamp": {"$ref": "#/components/schemas/Timestamp"}
            }
        }),
    );
    schemas.insert(
        "DeploymentCommandResponse".to_string(),
        json!({
            "type": "object",
            "required": ["deploymentId", "generation", "restartGeneration", "goal"],
            "properties": {
                "deploymentId": {"$ref": "#/components/schemas/DeploymentId"},
                "generation": {"$ref": "#/components/schemas/Generation"},
                "restartGeneration": {"$ref": "#/components/schemas/Generation"},
                "goal": {"$ref": "#/components/schemas/DeploymentGoal"}
            }
        }),
    );
    schemas.insert(
        "NodeCommandResponse".to_string(),
        json!({
            "type": "object",
            "required": ["nodeId", "draining"],
            "properties": {
                "nodeId": {"$ref": "#/components/schemas/NodeId"},
                "draining": {"type": "boolean"}
            }
        }),
    );
    schemas.insert(
        "UpgradeCreateRequest".to_string(),
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["upgradeRunId", "spec"],
            "properties": {
                "upgradeRunId": {"$ref": "#/components/schemas/UpgradeRunId"},
                "spec": {"$ref": "#/components/schemas/UpgradeRunSpec"}
            }
        }),
    );
    schemas.insert(
        "UpgradeCommandResponse".to_string(),
        json!({
            "type": "object",
            "required": ["upgradeRunId", "generation", "phase"],
            "properties": {
                "upgradeRunId": {"$ref": "#/components/schemas/UpgradeRunId"},
                "generation": {"$ref": "#/components/schemas/Generation"},
                "phase": {"$ref": "#/components/schemas/UpgradePhase"},
                "deletionTimestamp": {"$ref": "#/components/schemas/Timestamp"}
            }
        }),
    );
    schemas.insert(
        "FirewallPolicyWriteRequest".to_string(),
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["spec"],
            "properties": {
                "expectedRevision": {
                    "$ref": "#/components/schemas/ResourceRevision",
                    "description": "Required current revision; omit only when creating"
                },
                "spec": {"$ref": "#/components/schemas/FirewallPolicySpec"}
            }
        }),
    );
    schemas.insert(
        "FirewallPolicyCommandResponse".to_string(),
        json!({
            "type": "object",
            "required": ["policyId", "generation"],
            "properties": {
                "policyId": {"$ref": "#/components/schemas/FirewallPolicyId"},
                "generation": {"$ref": "#/components/schemas/Generation"},
                "deletionTimestamp": {"$ref": "#/components/schemas/Timestamp"}
            }
        }),
    );
    schemas.insert(
        "FirewallDryRunRequest".to_string(),
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["spec"],
            "properties": {
                "spec": {"$ref": "#/components/schemas/FirewallPolicySpec"}
            }
        }),
    );
    schemas.insert(
        "FirewallDryRunRuleset".to_string(),
        json!({
            "type": "object",
            "required": ["nodeId", "tableName", "script", "digest"],
            "properties": {
                "nodeId": {"$ref": "#/components/schemas/NodeId"},
                "tableName": {"type": "string"},
                "script": {"type": "string"},
                "digest": {"type": "string"}
            }
        }),
    );
    schemas.insert(
        "FirewallDryRunResponse".to_string(),
        json!({
            "type": "object",
            "required": ["bundleDigest", "rulesets"],
            "properties": {
                "bundleDigest": {"type": "string"},
                "rulesets": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/FirewallDryRunRuleset"}
                }
            }
        }),
    );
    schemas.insert(
        "WebhookWriteRequest".to_string(),
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["endpoint", "events"],
            "properties": {
                "expectedRevision": {
                    "$ref": "#/components/schemas/ResourceRevision",
                    "description": "Required current revision; omit only when creating"
                },
                "endpoint": {"type": "string", "format": "uri", "maxLength": 2048},
                "events": {
                    "type": "array",
                    "minItems": 1,
                    "uniqueItems": true,
                    "items": {"$ref": "#/components/schemas/WebhookEvent"}
                },
                "signingSecret": {
                    "$ref": "#/components/schemas/SecretValue",
                    "description": "Required when creating; omit on update to preserve the current secret"
                }
            }
        }),
    );
    schemas.insert(
        "WebhookCommandResponse".to_string(),
        json!({
            "type": "object",
            "required": ["webhookId", "generation"],
            "properties": {
                "webhookId": {"$ref": "#/components/schemas/WebhookId"},
                "generation": {"$ref": "#/components/schemas/Generation"}
            }
        }),
    );
    schemas.insert(
        "WebhookTestRequest".to_string(),
        json!({"type": "object", "additionalProperties": false}),
    );
    schemas.insert(
        "WebhookTestResponse".to_string(),
        json!({
            "type": "object",
            "required": ["webhookId", "deliveryId", "testedAt"],
            "properties": {
                "webhookId": {"$ref": "#/components/schemas/WebhookId"},
                "deliveryId": {"type": "string"},
                "testedAt": {"$ref": "#/components/schemas/Timestamp"}
            }
        }),
    );
}

pub(crate) fn webhook_operation() -> Value {
    let mut operation = get_operation("getWebhook", "webhookId", "Webhook");
    if let Some(item) = operation.as_object_mut() {
        item.insert(
            "put".to_string(),
            command_operation(
                "putWebhook",
                &["webhookId"],
                "WebhookWriteRequest",
                "WebhookCommandResponse",
            ),
        );
        item.insert(
            "delete".to_string(),
            command_operation(
                "deleteWebhook",
                &["webhookId"],
                "CommandRequest",
                "WebhookCommandResponse",
            ),
        );
    }
    operation
}

pub(crate) fn webhook_test_path() -> Value {
    let mut operation = command_operation(
        "testWebhook",
        &["webhookId"],
        "WebhookTestRequest",
        "WebhookTestResponse",
    );
    if let Some(responses) = operation
        .get_mut("responses")
        .and_then(Value::as_object_mut)
        && let Some(mut response) = responses.remove("202")
    {
        if let Some(response) = response.as_object_mut() {
            response.insert("description".to_string(), json!("Test delivery completed"));
        }
        responses.insert("200".to_string(), response);
        responses.insert(
            "502".to_string(),
            json!({"description": "Webhook endpoint rejected or did not complete the delivery"}),
        );
        responses.insert(
            "503".to_string(),
            json!({"description": "Webhook delivery is not configured"}),
        );
    }
    Value::Object(Map::from_iter([("post".to_string(), operation)]))
}

pub(crate) fn firewall_policy_operation() -> Value {
    let mut operation = get_operation("getFirewallPolicy", "policyId", "FirewallPolicy");
    if let Some(item) = operation.as_object_mut() {
        item.insert(
            "put".to_string(),
            command_operation(
                "putFirewallPolicy",
                &["policyId"],
                "FirewallPolicyWriteRequest",
                "FirewallPolicyCommandResponse",
            ),
        );
        item.insert(
            "delete".to_string(),
            command_operation(
                "deleteFirewallPolicy",
                &["policyId"],
                "CommandRequest",
                "FirewallPolicyCommandResponse",
            ),
        );
    }
    operation
}

pub(crate) fn firewall_dry_run_path() -> Value {
    json!({
        "post": {
            "operationId": "dryRunFirewallPolicy",
            "security": [{"bearerAuth": []}],
            "parameters": [{
                "name": "policyId",
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            }],
            "requestBody": {
                "required": true,
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/FirewallDryRunRequest"}
                    }
                }
            },
            "responses": {
                "200": {
                    "description": "Deterministic effective rules without persistence",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/FirewallDryRunResponse"}
                        }
                    }
                },
                "400": {"description": "Invalid proposed policy"},
                "409": {"description": "Proposed policy conflicts with cluster state"},
                "413": {"description": "Request body exceeds the command limit"},
                "503": {"description": "Firewall planning is unavailable"}
            }
        }
    })
}

pub(crate) fn deployment_command_path(operation_id: &str) -> Value {
    command_path(
        "post",
        operation_id,
        &["serviceId", "deploymentId"],
        "CommandRequest",
        "DeploymentCommandResponse",
    )
}

pub(crate) fn command_path(
    method: &str,
    operation_id: &str,
    parameters: &[&str],
    request_schema: &str,
    response_schema: &str,
) -> Value {
    Value::Object(Map::from_iter([(
        method.to_string(),
        command_operation(operation_id, parameters, request_schema, response_schema),
    )]))
}

pub(crate) fn command_operation(
    operation_id: &str,
    parameters: &[&str],
    request_schema: &str,
    response_schema: &str,
) -> Value {
    let mut parameters = parameters
        .iter()
        .map(|parameter| {
            json!({
                "name": parameter,
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            })
        })
        .collect::<Vec<_>>();
    parameters.push(json!({
        "name": "Idempotency-Key",
        "in": "header",
        "required": true,
        "schema": {"type": "string"}
    }));
    json!({
        "operationId": operation_id,
        "security": [{"bearerAuth": []}],
        "parameters": parameters,
        "requestBody": {
            "required": true,
            "content": {
                "application/json": {
                    "schema": {"$ref": format!("#/components/schemas/{request_schema}")}
                }
            }
        },
        "responses": {
            "202": {
                "description": "Lifecycle command accepted for reconciliation",
                "content": {
                    "application/json": {
                        "schema": {"$ref": format!("#/components/schemas/{response_schema}")}
                    }
                }
            },
            "400": {"description": "Invalid command request"},
            "404": {"description": "Resource not found"},
            "409": {"description": "Revision, idempotency, or lifecycle conflict"},
            "413": {"description": "Request body exceeds the command limit"}
        }
    })
}
