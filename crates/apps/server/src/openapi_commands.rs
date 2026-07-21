use serde_json::{Map, Value, json};

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
