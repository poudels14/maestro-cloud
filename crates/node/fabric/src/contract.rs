/// Workload-visible directory containing the private node API credential pair.
pub const WORKLOAD_NODE_DIRECTORY: &str = "/run/maestro";

/// Filename of the Unix socket inside the workload node API directory.
pub const WORKLOAD_NODE_SOCKET_FILE: &str = "node.sock";

/// Filename of the credential inside the workload node API directory.
pub const WORKLOAD_NODE_TOKEN_FILE: &str = "node.token";

/// Workload-visible path of the private Maestro node API socket.
pub const WORKLOAD_NODE_SOCKET_PATH: &str = "/run/maestro/node.sock";

/// Workload-visible path of the base64url-encoded node API bearer token.
pub const WORKLOAD_NODE_TOKEN_PATH: &str = "/run/maestro/node.token";

/// gRPC metadata header carrying the base64url-encoded workload token.
pub const WORKLOAD_TOKEN_HEADER: &str = "x-maestro-workload-token";
