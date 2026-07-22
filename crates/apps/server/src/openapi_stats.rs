use serde_json::{Map, Value, json};

pub(crate) fn paths() -> Map<String, Value> {
    Map::from_iter([
        (
            "/api/cluster/stats".to_owned(),
            operation(
                "getClusterStats",
                json!({"$ref": "#/components/schemas/ClusterStatsResponse"}),
            ),
        ),
        (
            "/api/cluster/stats/nodes".to_owned(),
            operation(
                "listClusterNodeStats",
                json!({"$ref": "#/components/schemas/NodeStatsMap"}),
            ),
        ),
        ("/api/metrics/stats".to_owned(), stats_metric_operation()),
    ])
}

pub(crate) fn insert_schemas(schemas: &mut Map<String, Value>) {
    schemas.insert(
        "SpoolStatsSnapshot".to_owned(),
        json!({
            "type": "object",
            "required": ["rowCount", "highWatermark", "oldestEntryAtMs", "databaseBytes"],
            "properties": {
                "rowCount": unsigned(),
                "highWatermark": {"type": "integer", "format": "int64"},
                "oldestEntryAtMs": nullable_integer(),
                "databaseBytes": unsigned()
            }
        }),
    );
    schemas.insert(
        "SinkStatsSnapshot".to_owned(),
        json!({
            "type": "object",
            "required": [
                "id", "cursor", "pendingEntries", "oldestPendingAtMs", "lastSuccessAtMs",
                "lastErrorAtMs", "lastError", "consecutiveFailures", "lastCursorAdvanceAtMs",
                "filteredEntries"
            ],
            "properties": {
                "id": {"type": "string"},
                "cursor": {"type": "integer", "format": "int64"},
                "pendingEntries": unsigned(),
                "oldestPendingAtMs": nullable_integer(),
                "lastSuccessAtMs": nullable_integer(),
                "lastErrorAtMs": nullable_integer(),
                "lastError": {"type": "string", "nullable": true},
                "consecutiveFailures": unsigned(),
                "lastCursorAdvanceAtMs": nullable_integer(),
                "filteredEntries": unsigned()
            }
        }),
    );
    schemas.insert(
        "DeadLetterStatsSnapshot".to_owned(),
        json!({
            "type": "object",
            "required": [
                "count", "capacity", "payloadBytes", "latestAtMs", "latestStatus", "latestError"
            ],
            "properties": {
                "count": unsigned(),
                "capacity": unsigned(),
                "payloadBytes": unsigned(),
                "latestAtMs": nullable_integer(),
                "latestStatus": {"type": "integer", "minimum": 100, "maximum": 599, "nullable": true},
                "latestError": {"type": "string", "nullable": true}
            }
        }),
    );
    schemas.insert(
        "ControllerStatsSnapshot".to_owned(),
        json!({
            "type": "object",
            "required": ["reportedAtMs", "version", "uptimeMs", "spool", "sinks", "deadLetters"],
            "properties": {
                "reportedAtMs": {"type": "integer", "format": "int64"},
                "version": {"type": "string"},
                "uptimeMs": unsigned(),
                "spool": {"$ref": "#/components/schemas/SpoolStatsSnapshot"},
                "sinks": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/SinkStatsSnapshot"}
                },
                "deadLetters": {"$ref": "#/components/schemas/DeadLetterStatsSnapshot"}
            }
        }),
    );
    schemas.insert(
        "BackupStatsSnapshot".to_owned(),
        json!({
            "type": "object",
            "required": [
                "configured", "lastAttemptAtMs", "lastSuccessAtMs", "lastErrorAtMs", "lastError",
                "pendingPartitions", "pendingBytes", "oldestPendingDate", "uploadedBytesLastRun",
                "completedPartitionsLastRun", "failedPartitionsLastRun"
            ],
            "properties": {
                "configured": {"type": "boolean"},
                "lastAttemptAtMs": nullable_integer(),
                "lastSuccessAtMs": nullable_integer(),
                "lastErrorAtMs": nullable_integer(),
                "lastError": {"type": "string", "nullable": true},
                "pendingPartitions": unsigned(),
                "pendingBytes": unsigned(),
                "oldestPendingDate": {"type": "string", "format": "date", "nullable": true},
                "uploadedBytesLastRun": unsigned(),
                "completedPartitionsLastRun": unsigned(),
                "failedPartitionsLastRun": unsigned()
            }
        }),
    );
    schemas.insert(
        "ProbeStatsSnapshot".to_owned(),
        json!({
            "type": "object",
            "required": ["version", "uptimeMs"],
            "properties": {
                "version": {"type": "string"},
                "uptimeMs": unsigned()
            }
        }),
    );
    schemas.insert(
        "StatsWarning".to_owned(),
        json!({
            "type": "object",
            "required": ["code", "severity", "message"],
            "properties": {
                "code": {"type": "string"},
                "severity": {"type": "string", "enum": ["warning", "error"]},
                "message": {"type": "string"}
            }
        }),
    );
    schemas.insert(
        "ClusterStatsResponse".to_owned(),
        json!({
            "type": "object",
            "required": [
                "generatedAtMs", "probe", "controller", "controllerHeartbeatAgeMs", "backup", "warnings"
            ],
            "properties": {
                "generatedAtMs": {"type": "integer", "format": "int64"},
                "probe": {"$ref": "#/components/schemas/ProbeStatsSnapshot"},
                "controller": {
                    "allOf": [{"$ref": "#/components/schemas/ControllerStatsSnapshot"}],
                    "nullable": true
                },
                "controllerHeartbeatAgeMs": {
                    "type": "integer", "format": "int64", "minimum": 0, "nullable": true
                },
                "backup": {"$ref": "#/components/schemas/BackupStatsSnapshot"},
                "warnings": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/StatsWarning"}
                }
            }
        }),
    );
    schemas.insert(
        "NodeStatsMap".to_owned(),
        json!({
            "type": "object",
            "additionalProperties": {"$ref": "#/components/schemas/ControllerStatsSnapshot"}
        }),
    );
    schemas.insert(
        "StatsMetricPoint".to_owned(),
        json!({
            "type": "object",
            "required": ["ts", "name", "value"],
            "properties": {
                "ts": {"type": "integer", "format": "int64"},
                "name": {"type": "string", "minLength": 1, "maxLength": 256},
                "value": {"type": "number", "format": "double"},
                "labels": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                    "maxProperties": 64
                }
            }
        }),
    );
}

fn stats_metric_operation() -> Value {
    json!({
        "get": {
            "operationId": "listOperationalStatsMetrics",
            "security": [{"bearerAuth": []}],
            "parameters": [
                query_parameter("name", json!({"type": "string", "minLength": 1, "maxLength": 256})),
                query_parameter("from", json!({"type": "integer", "format": "int64"})),
                query_parameter("to", json!({"type": "integer", "format": "int64"})),
                query_parameter("limit", json!({"type": "integer", "minimum": 1, "maximum": 10000}))
            ],
            "responses": {
                "200": {
                    "description": "Bounded controller and backup metric history",
                    "content": {"application/json": {"schema": {
                        "type": "array",
                        "items": {"$ref": "#/components/schemas/StatsMetricPoint"}
                    }}}
                },
                "400": {"description": "Invalid range, name, or limit"},
                "503": {"description": "One or more node stats stores are unavailable"}
            }
        }
    })
}

fn operation(operation_id: &str, schema: Value) -> Value {
    json!({
        "get": {
            "operationId": operation_id,
            "security": [{"bearerAuth": []}],
            "responses": {
                "200": {
                    "description": "Live controller and observability health",
                    "content": {"application/json": {"schema": schema}}
                },
                "503": {"description": "One or more node observability stores are unavailable"}
            }
        }
    })
}

fn unsigned() -> Value {
    json!({"type": "integer", "format": "int64", "minimum": 0})
}

fn nullable_integer() -> Value {
    json!({"type": "integer", "format": "int64", "nullable": true})
}

fn query_parameter(name: &str, schema: Value) -> Value {
    json!({"name": name, "in": "query", "required": false, "schema": schema})
}
