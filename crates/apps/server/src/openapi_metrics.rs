use serde_json::{Map, Value, json};

pub(crate) fn paths() -> Map<String, Value> {
    Map::from_iter([
        (
            "/api/metrics/node".to_owned(),
            metric_operation("listNodeMetrics", &[], false),
        ),
        (
            "/api/metrics/cluster".to_owned(),
            metric_operation("listClusterMetrics", &[], true),
        ),
        (
            "/api/services/{serviceId}/metrics".to_owned(),
            metric_operation("listServiceMetrics", &["serviceId"], true),
        ),
        (
            "/api/services/{serviceId}/metrics/containers".to_owned(),
            metric_operation("listContainerMetrics", &["serviceId"], false),
        ),
        (
            "/api/disks".to_owned(),
            disk_operation("listLocalDisks", false),
        ),
        (
            "/api/disks/nodes".to_owned(),
            disk_operation("listNodeDisks", true),
        ),
    ])
}

pub(crate) fn insert_schemas(schemas: &mut Map<String, Value>) {
    schemas.insert(
        "ResourceMetricPoint".to_owned(),
        json!({
            "type": "object",
            "required": [
                "ts", "source", "cpuPercent", "memoryBytes", "memoryLimitBytes",
                "netRxBytes", "netTxBytes"
            ],
            "properties": {
                "ts": {"type": "integer", "format": "int64"},
                "source": {"type": "string"},
                "cpuPercent": {"type": "number", "format": "double", "minimum": 0},
                "memoryBytes": {"type": "integer", "format": "int64", "minimum": 0},
                "memoryLimitBytes": {"type": "integer", "format": "int64", "minimum": 0},
                "netRxBytes": {"type": "integer", "format": "int64", "minimum": 0},
                "netTxBytes": {"type": "integer", "format": "int64", "minimum": 0}
            }
        }),
    );
    schemas.insert(
        "DiskInfo".to_owned(),
        json!({
            "type": "object",
            "required": [
                "name", "mountPoint", "totalBytes", "availableBytes", "fileSystem"
            ],
            "properties": {
                "name": {"type": "string"},
                "mountPoint": {"type": "string"},
                "totalBytes": {"type": "integer", "format": "int64", "minimum": 0},
                "availableBytes": {"type": "integer", "format": "int64", "minimum": 0},
                "fileSystem": {"type": "string"}
            }
        }),
    );
    schemas.insert(
        "NodeDiskMap".to_owned(),
        json!({
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"$ref": "#/components/schemas/DiskInfo"}
            }
        }),
    );
}

fn metric_operation(operation_id: &str, path_names: &[&str], bucketed: bool) -> Value {
    let mut parameters = path_parameters(path_names);
    parameters.extend([
        query_parameter("from", json!({"type": "integer", "format": "int64"})),
        query_parameter("to", json!({"type": "integer", "format": "int64"})),
        query_parameter(
            "limit",
            json!({"type": "integer", "minimum": 1, "maximum": 10000}),
        ),
    ]);
    if bucketed {
        parameters.push(query_parameter(
            "bucketMs",
            json!({"type": "integer", "minimum": 1000, "maximum": 3600000}),
        ));
    }
    json!({
        "get": {
            "operationId": operation_id,
            "security": [{"bearerAuth": []}],
            "parameters": parameters,
            "responses": {
                "200": {
                    "description": "Bounded resource metric history",
                    "content": {"application/json": {"schema": {
                        "type": "array",
                        "items": {"$ref": "#/components/schemas/ResourceMetricPoint"}
                    }}}
                },
                "400": {"description": "Invalid range, bucket, limit, or service"},
                "404": {"description": "Service does not exist"},
                "503": {"description": "One or more metric stores are unavailable"}
            }
        }
    })
}

fn disk_operation(operation_id: &str, cluster: bool) -> Value {
    let schema = if cluster {
        json!({"$ref": "#/components/schemas/NodeDiskMap"})
    } else {
        json!({
            "type": "array",
            "items": {"$ref": "#/components/schemas/DiskInfo"}
        })
    };
    json!({
        "get": {
            "operationId": operation_id,
            "security": [{"bearerAuth": []}],
            "responses": {
                "200": {
                    "description": "Latest complete disk inventory",
                    "content": {"application/json": {"schema": schema}}
                },
                "503": {"description": "One or more host metric stores are unavailable"}
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
