/// Workload-visible path of the private Maestro node API socket.
pub const WORKLOAD_NODE_SOCKET_PATH: &str = "/run/maestro/node.sock";

/// Workload-visible path of the base64url-encoded node API bearer token.
pub const WORKLOAD_NODE_TOKEN_PATH: &str = "/run/maestro/node.token";

/// gRPC metadata header carrying the base64url-encoded workload token.
pub const WORKLOAD_TOKEN_HEADER: &str = "x-maestro-workload-token";
