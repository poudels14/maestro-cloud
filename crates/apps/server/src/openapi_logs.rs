use serde_json::{Map, Value, json};

#[derive(Clone, Copy)]
enum LogScope {
    Standard,
    System,
}

#[derive(Clone, Copy)]
enum ResponseShape {
    Object,
    Array,
}

pub(crate) fn paths() -> Map<String, Value> {
    Map::from_iter([
        (
            "/api/logs".to_owned(),
            read_operation("listLogs", &[], LogScope::Standard),
        ),
        (
            "/api/logs/histogram".to_owned(),
            histogram_operation("getLogHistogram", &[], LogScope::Standard),
        ),
        (
            "/api/system/logs".to_owned(),
            read_operation("listSystemLogs", &[], LogScope::System),
        ),
        (
            "/api/system/logs/histogram".to_owned(),
            histogram_operation("getSystemLogHistogram", &[], LogScope::System),
        ),
        (
            "/api/services/{serviceId}/logs".to_owned(),
            read_operation("listServiceLogs", &["serviceId"], LogScope::Standard),
        ),
        (
            "/api/services/{serviceId}/logs/histogram".to_owned(),
            histogram_operation("getServiceLogHistogram", &["serviceId"], LogScope::Standard),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/logs".to_owned(),
            read_operation(
                "listDeploymentLogs",
                &["serviceId", "deploymentId"],
                LogScope::Standard,
            ),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/logs/histogram".to_owned(),
            histogram_operation(
                "getDeploymentLogHistogram",
                &["serviceId", "deploymentId"],
                LogScope::Standard,
            ),
        ),
        (
            "/api/services/{serviceId}/builds/{buildId}/logs".to_owned(),
            read_operation(
                "listBuildLogs",
                &["serviceId", "buildId"],
                LogScope::Standard,
            ),
        ),
        (
            "/api/services/{serviceId}/builds/{buildId}/logs/histogram".to_owned(),
            histogram_operation(
                "getBuildLogHistogram",
                &["serviceId", "buildId"],
                LogScope::Standard,
            ),
        ),
    ])
}

pub(crate) fn insert_schemas(schemas: &mut Map<String, Value>) {
    schemas.insert(
        "ClusterLogCursor".to_owned(),
        json!({
            "type": "object",
            "additionalProperties": {"type": "integer", "format": "int64", "minimum": 0}
        }),
    );
    schemas.insert(
        "ClusterLogEntry".to_owned(),
        json!({
            "type": "object",
            "required": ["nodeId", "sequence", "entry"],
            "properties": {
                "nodeId": {"type": "string"},
                "sequence": {"type": "integer", "format": "int64", "minimum": 0},
                "entry": {"$ref": "#/components/schemas/IngestLogEntry"}
            }
        }),
    );
    schemas.insert(
        "ClusterLogPage".to_owned(),
        json!({
            "type": "object",
            "required": ["entries", "cursor"],
            "properties": {
                "entries": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/ClusterLogEntry"}
                },
                "cursor": {"$ref": "#/components/schemas/ClusterLogCursor"}
            }
        }),
    );
    schemas.insert(
        "SequencedLogEntry".to_owned(),
        json!({
            "type": "object",
            "required": ["sequence", "entry"],
            "properties": {
                "sequence": {"type": "integer", "format": "int64", "minimum": 0},
                "entry": {"$ref": "#/components/schemas/IngestLogEntry"}
            }
        }),
    );
    schemas.insert(
        "IngestLogEntry".to_owned(),
        json!({
            "type": "object",
            "required": ["id", "observedAt", "eventAt", "severity", "stream", "origin", "body"],
            "properties": {
                "id": {"type": "object", "additionalProperties": true},
                "observedAt": {"$ref": "#/components/schemas/Timestamp"},
                "eventAt": {"$ref": "#/components/schemas/Timestamp"},
                "severity": {"type": "string"},
                "stream": {"type": "string", "enum": ["stdout", "stderr", "otlp", "system"]},
                "origin": {"type": "object", "additionalProperties": true},
                "body": {"type": "object", "additionalProperties": true},
                "attributes": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                }
            }
        }),
    );
    schemas.insert(
        "LogHistogramBucket".to_owned(),
        json!({
            "type": "object",
            "required": ["bucketAt", "count", "groups"],
            "properties": {
                "bucketAt": {"$ref": "#/components/schemas/Timestamp"},
                "count": {"type": "integer", "format": "int64", "minimum": 0},
                "groups": {
                    "type": "object",
                    "additionalProperties": {"type": "integer", "format": "int64", "minimum": 0}
                }
            }
        }),
    );
}

fn read_operation(operation_id: &str, path_names: &[&str], scope: LogScope) -> Value {
    let mut parameters = path_parameters(path_names);
    parameters.extend([
        query_parameter(
            "tail",
            json!({"type": "integer", "minimum": 1, "maximum": 10000}),
        ),
        query_parameter("cursor", json!({"type": "string", "maxLength": 65536})),
        query_parameter("from", json!({"type": "integer", "format": "int64"})),
        query_parameter("to", json!({"type": "integer", "format": "int64"})),
        query_parameter("query", json!({"type": "string", "maxLength": 4096})),
        query_parameter("nodeId", json!({"$ref": "#/components/schemas/NodeId"})),
    ]);
    if matches!(scope, LogScope::System) {
        parameters.push(query_parameter(
            "component",
            json!({"type": "string", "minLength": 1, "maxLength": 128}),
        ));
    }
    operation(
        operation_id,
        parameters,
        "Ordered normalized logs",
        "ClusterLogPage",
        ResponseShape::Object,
    )
}

fn histogram_operation(operation_id: &str, path_names: &[&str], scope: LogScope) -> Value {
    let mut parameters = path_parameters(path_names);
    parameters.extend([
        query_parameter("from", json!({"type": "integer", "format": "int64"})),
        query_parameter("to", json!({"type": "integer", "format": "int64"})),
        query_parameter("bucketMs", json!({"type": "integer", "minimum": 1})),
        query_parameter(
            "groupBy",
            json!({"type": "string", "enum": ["level", "status"]}),
        ),
        query_parameter("query", json!({"type": "string", "maxLength": 4096})),
        query_parameter("nodeId", json!({"$ref": "#/components/schemas/NodeId"})),
    ]);
    if matches!(scope, LogScope::System) {
        parameters.push(query_parameter(
            "component",
            json!({"type": "string", "minLength": 1, "maxLength": 128}),
        ));
    }
    operation(
        operation_id,
        parameters,
        "Event-time log histogram",
        "LogHistogramBucket",
        ResponseShape::Array,
    )
}

fn operation(
    operation_id: &str,
    parameters: Vec<Value>,
    description: &str,
    schema: &str,
    response_shape: ResponseShape,
) -> Value {
    let response_schema = match response_shape {
        ResponseShape::Array => json!({
            "type": "array",
            "items": {"$ref": format!("#/components/schemas/{schema}")}
        }),
        ResponseShape::Object => {
            json!({"$ref": format!("#/components/schemas/{schema}")})
        }
    };
    json!({
        "get": {
            "operationId": operation_id,
            "security": [{"bearerAuth": []}, {"browserSession": []}],
            "parameters": parameters,
            "responses": {
                "200": {
                    "description": description,
                    "content": {"application/json": {"schema": response_schema}}
                },
                "400": {"description": "Invalid scope, cursor, range, or LogQL expression"},
                "503": {"description": "One or more cluster log stores are unavailable"}
            }
        }
    })
}

fn path_parameters(names: &[&str]) -> Vec<Value> {
    names
        .iter()
        .map(|name| {
            json!({
                "name": name,
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            })
        })
        .collect()
}

fn query_parameter(name: &str, schema: Value) -> Value {
    json!({"name": name, "in": "query", "required": false, "schema": schema})
}
