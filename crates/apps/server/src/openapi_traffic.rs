use serde_json::{Map, Value, json};

pub(crate) fn paths() -> Map<String, Value> {
    Map::from_iter([
        (
            "/api/ingress/traffic".to_owned(),
            breakdown_operation("getIngressTraffic", false),
        ),
        (
            "/api/ingress/blocked-traffic".to_owned(),
            breakdown_operation("getBlockedIngressTraffic", false),
        ),
        (
            "/api/services/{serviceId}/traffic".to_owned(),
            service_traffic_operation(),
        ),
        (
            "/api/services/{serviceId}/traffic/breakdown".to_owned(),
            breakdown_operation("getServiceTrafficBreakdown", true),
        ),
    ])
}

pub(crate) fn insert_schemas(schemas: &mut Map<String, Value>) {
    schemas.insert(
        "TrafficBreakdownEntry".to_owned(),
        json!({
            "type": "object",
            "required": ["value", "statusCode", "requests", "lastSeenAtMs"],
            "properties": {
                "value": {"type": "string"},
                "statusCode": {"type": "integer", "minimum": 100, "maximum": 599},
                "requests": {"type": "integer", "format": "int64", "minimum": 0},
                "lastSeenAtMs": {"type": "integer", "format": "int64"}
            }
        }),
    );
    schemas.insert(
        "IngressTrafficBreakdown".to_owned(),
        json!({
            "type": "object",
            "required": ["byIp", "byPath"],
            "properties": {
                "byIp": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/TrafficBreakdownEntry"}
                },
                "byPath": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/TrafficBreakdownEntry"}
                }
            }
        }),
    );
    schemas.insert(
        "TrafficMetricPoint".to_owned(),
        json!({
            "type": "object",
            "required": [
                "ts", "serviceId", "deploymentId", "statusCode", "method", "requests",
                "bytesIn", "bytesOut", "latLe1s", "latLe5s", "latLe10s", "latTotal"
            ],
            "properties": {
                "ts": {"type": "integer", "format": "int64"},
                "serviceId": {"type": "string"},
                "deploymentId": {"type": "string", "nullable": true},
                "statusCode": {"type": "integer", "minimum": 100, "maximum": 599},
                "method": {"type": "string", "maxLength": 32},
                "requests": nonnegative_integer(),
                "bytesIn": nonnegative_integer(),
                "bytesOut": nonnegative_integer(),
                "latLe1s": nonnegative_integer(),
                "latLe5s": nonnegative_integer(),
                "latLe10s": nonnegative_integer(),
                "latTotal": nonnegative_integer()
            }
        }),
    );
}

fn breakdown_operation(operation_id: &str, service_scoped: bool) -> Value {
    let mut parameters = common_parameters(500);
    if service_scoped {
        parameters.insert(0, service_parameter());
    }
    json!({
        "get": {
            "operationId": operation_id,
            "security": [{"bearerAuth": []}],
            "parameters": parameters,
            "responses": {
                "200": {
                    "description": "Ranked access-log traffic grouped by client IP and path",
                    "content": {"application/json": {"schema": {
                        "$ref": "#/components/schemas/IngressTrafficBreakdown"
                    }}}
                },
                "400": {"description": "Invalid range, limit, service, or node selection"},
                "404": {"description": "Service does not exist"},
                "503": {"description": "One or more node traffic stores are unavailable"}
            }
        }
    })
}

fn service_traffic_operation() -> Value {
    let mut parameters = common_parameters(10_000);
    parameters.insert(0, service_parameter());
    json!({
        "get": {
            "operationId": "getServiceTraffic",
            "security": [{"bearerAuth": []}],
            "parameters": parameters,
            "responses": {
                "200": {
                    "description": "Five-second service traffic intervals derived from access logs",
                    "content": {"application/json": {"schema": {
                        "type": "array",
                        "items": {"$ref": "#/components/schemas/TrafficMetricPoint"}
                    }}}
                },
                "400": {"description": "Invalid range, limit, service, or node selection"},
                "404": {"description": "Service does not exist"},
                "503": {"description": "One or more node traffic stores are unavailable"}
            }
        }
    })
}

fn common_parameters(maximum_limit: usize) -> Vec<Value> {
    vec![
        query_parameter("from", json!({"type": "integer", "format": "int64"})),
        query_parameter("to", json!({"type": "integer", "format": "int64"})),
        query_parameter(
            "limit",
            json!({"type": "integer", "minimum": 1, "maximum": maximum_limit}),
        ),
        query_parameter("nodeId", json!({"$ref": "#/components/schemas/NodeId"})),
    ]
}

fn service_parameter() -> Value {
    json!({
        "name": "serviceId",
        "in": "path",
        "required": true,
        "schema": {"$ref": "#/components/schemas/ServiceId"}
    })
}

fn query_parameter(name: &str, schema: Value) -> Value {
    json!({"name": name, "in": "query", "required": false, "schema": schema})
}

fn nonnegative_integer() -> Value {
    json!({"type": "integer", "format": "int64", "minimum": 0})
}
