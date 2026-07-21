use serde_json::{Map, Value, json};

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
            "/api/services".to_string(),
            list_operation("listServices", "Service"),
        ),
        (
            "/api/services/{serviceId}".to_string(),
            get_operation("getService", "serviceId", "Service"),
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
        }
    }
    document
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
