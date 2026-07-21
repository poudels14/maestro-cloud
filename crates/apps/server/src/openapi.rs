use serde_json::{Map, Value, json};

use crate::openapi_commands::{
    command_operation, command_path, deployment_command_path, insert_command_schemas,
};

/// Composes server path operations over the canonical kernel component schemas.
pub fn openapi_document() -> Value {
    let mut document = kernel_api::openapi_document();
    let paths = Value::Object(Map::from_iter([
        (
            "/api/cluster/nodes".to_string(),
            list_operation("listNodes", "Node"),
        ),
        (
            "/api/cluster/nodes/{nodeId}".to_string(),
            get_operation("getNode", "nodeId", "Node"),
        ),
        (
            "/api/cluster/nodes/{nodeId}/drain".to_string(),
            command_path(
                "post",
                "drainNode",
                &["nodeId"],
                "CommandRequest",
                "NodeCommandResponse",
            ),
        ),
        (
            "/api/cluster/nodes/{nodeId}/restore".to_string(),
            command_path(
                "post",
                "restoreNode",
                &["nodeId"],
                "CommandRequest",
                "NodeCommandResponse",
            ),
        ),
        (
            "/api/services".to_string(),
            list_operation("listServices", "Service"),
        ),
        ("/api/services/{serviceId}".to_string(), service_operation()),
        (
            "/api/services/{serviceId}/redeploy".to_string(),
            command_path(
                "post",
                "redeployService",
                &["serviceId"],
                "CommandRequest",
                "ServiceCommandResponse",
            ),
        ),
        (
            "/api/services/{serviceId}/freeze".to_string(),
            command_path(
                "post",
                "freezeService",
                &["serviceId"],
                "CommandRequest",
                "ServiceCommandResponse",
            ),
        ),
        (
            "/api/services/{serviceId}/unfreeze".to_string(),
            command_path(
                "post",
                "unfreezeService",
                &["serviceId"],
                "CommandRequest",
                "ServiceCommandResponse",
            ),
        ),
        (
            "/api/services/{serviceId}/replicas".to_string(),
            command_path(
                "put",
                "setServiceReplicas",
                &["serviceId"],
                "ReplicaOverrideRequest",
                "ServiceCommandResponse",
            ),
        ),
        (
            "/api/services/{serviceId}/deployments".to_string(),
            nested_list_operation("listDeployments", &["serviceId"], "Deployment"),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}".to_string(),
            deployment_operation(),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/assignments".to_string(),
            nested_list_operation(
                "listAssignments",
                &["serviceId", "deploymentId"],
                "Assignment",
            ),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/assignments/{assignmentId}"
                .to_string(),
            scoped_get_operation(
                "getAssignment",
                &["serviceId", "deploymentId", "assignmentId"],
                "Assignment",
            ),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/replicas".to_string(),
            nested_list_operation(
                "listReplicas",
                &["serviceId", "deploymentId"],
                "ReplicaState",
            ),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/replicas/{replicaId}".to_string(),
            scoped_get_operation(
                "getReplica",
                &["serviceId", "deploymentId", "replicaId"],
                "ReplicaState",
            ),
        ),
        (
            "/api/services/{serviceId}/builds".to_string(),
            nested_list_operation("listBuilds", &["serviceId"], "Build"),
        ),
        (
            "/api/services/{serviceId}/builds/{buildId}".to_string(),
            scoped_get_operation("getBuild", &["serviceId", "buildId"], "Build"),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/restart".to_string(),
            deployment_command_path("restartDeployment"),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/cancel".to_string(),
            deployment_command_path("cancelDeployment"),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/remove".to_string(),
            deployment_command_path("removeDeployment"),
        ),
        ("/healthz".to_string(), health_operation()),
        ("/openapi.json".to_string(), openapi_operation()),
    ]));
    let security_schemes = json!({
        "bearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "HS256 token with a non-empty subject and the operator scope"
        }
    });
    if let Some(root) = document.as_object_mut() {
        root.insert("paths".to_string(), paths);
        if let Some(components) = root.get_mut("components").and_then(Value::as_object_mut) {
            components.insert("securitySchemes".to_string(), security_schemes);
            if let Some(schemas) = components.get_mut("schemas").and_then(Value::as_object_mut) {
                schemas.insert(
                    "ServiceWriteRequest".to_string(),
                    json!({
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["spec"],
                        "properties": {
                            "expectedRevision": {
                                "$ref": "#/components/schemas/ResourceRevision",
                                "description": "Required current revision; omit only when creating"
                            },
                            "spec": {"$ref": "#/components/schemas/ServiceSpec"}
                        }
                    }),
                );
                schemas.insert(
                    "ServiceWriteResponse".to_string(),
                    json!({
                        "type": "object",
                        "required": ["serviceId", "generation"],
                        "properties": {
                            "serviceId": {"$ref": "#/components/schemas/ServiceId"},
                            "generation": {"$ref": "#/components/schemas/Generation"}
                        }
                    }),
                );
                insert_command_schemas(schemas);
            }
        }
    }
    document
}

fn service_operation() -> Value {
    let mut operation = get_operation("getService", "serviceId", "Service");
    if let Some(item) = operation.as_object_mut() {
        item.insert("put".to_string(), put_service_operation());
        item.insert(
            "delete".to_string(),
            command_operation(
                "deleteService",
                &["serviceId"],
                "CommandRequest",
                "ServiceCommandResponse",
            ),
        );
    }
    operation
}

fn put_service_operation() -> Value {
    json!({
        "operationId": "putService",
        "security": [{"bearerAuth": []}],
        "parameters": [
            {
                "name": "serviceId",
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            },
            {
                "name": "Idempotency-Key",
                "in": "header",
                "required": true,
                "schema": {"type": "string"}
            }
        ],
        "requestBody": {
            "required": true,
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ServiceWriteRequest"}
                }
            }
        },
        "responses": {
            "202": {
                "description": "Desired state accepted for reconciliation",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ServiceWriteResponse"}
                    }
                }
            },
            "400": {"description": "Invalid service request"},
            "409": {"description": "Revision or idempotency conflict"},
            "413": {"description": "Request body exceeds the service limit"}
        }
    })
}

fn list_operation(operation_id: &str, schema: &str) -> Value {
    json!({
        "get": {
            "operationId": operation_id,
            "security": [{"bearerAuth": []}],
            "responses": {
                "200": {
                    "description": "Ordered resource list",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "array",
                                "items": {"$ref": format!("#/components/schemas/{schema}")}
                            }
                        }
                    }
                }
            }
        }
    })
}

fn get_operation(operation_id: &str, parameter: &str, schema: &str) -> Value {
    json!({
        "get": {
            "operationId": operation_id,
            "security": [{"bearerAuth": []}],
            "parameters": [{
                "name": parameter,
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            }],
            "responses": {
                "200": {
                    "description": "Requested resource",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": format!("#/components/schemas/{schema}")}
                        }
                    }
                },
                "404": {"description": "Resource not found"}
            }
        }
    })
}

fn nested_list_operation(operation_id: &str, parameters: &[&str], schema: &str) -> Value {
    let mut operation = list_operation(operation_id, schema);
    if let Some(get) = operation.get_mut("get").and_then(Value::as_object_mut) {
        get.insert("parameters".to_string(), path_parameters(parameters));
    }
    operation
}

fn deployment_operation() -> Value {
    scoped_get_operation(
        "getDeployment",
        &["serviceId", "deploymentId"],
        "Deployment",
    )
}

fn scoped_get_operation(operation_id: &str, parameters: &[&str], schema: &str) -> Value {
    let parameter = parameters.last().copied().unwrap_or("resourceId");
    let mut operation = get_operation(operation_id, parameter, schema);
    if let Some(get) = operation.get_mut("get").and_then(Value::as_object_mut) {
        get.insert("parameters".to_string(), path_parameters(parameters));
    }
    operation
}

fn path_parameters(parameters: &[&str]) -> Value {
    Value::Array(
        parameters
            .iter()
            .map(|parameter| {
                json!({
                    "name": parameter,
                    "in": "path",
                    "required": true,
                    "schema": {"type": "string"}
                })
            })
            .collect(),
    )
}

fn health_operation() -> Value {
    json!({
        "get": {
            "operationId": "health",
            "responses": {"200": {"description": "Server is accepting requests"}}
        }
    })
}

fn openapi_operation() -> Value {
    json!({
        "get": {
            "operationId": "getOpenApi",
            "responses": {"200": {"description": "Canonical OpenAPI document"}}
        }
    })
}
