/// Managed Traefik workload service identity.
pub const TRAEFIK_SERVICE_ID: &str = "maestro-system-traefik";
/// Managed Cloudflare Tunnel workload service identity.
pub const CLOUDFLARE_SERVICE_ID: &str = "maestro-system-cloudflared";
/// Managed Tailscale gateway workload service identity.
pub const TAILSCALE_GATEWAY_SERVICE_ID: &str = "maestro-system-tailscale-gateway";
/// Managed cluster DNS resolver workload service identity.
pub const DNS_RESOLVER_SERVICE_ID: &str = "maestro-system-dns";

/// Reserved identity prefix for Maestro-managed system resources.
pub const SYSTEM_RESOURCE_PREFIX: &str = "maestro-system-";

/// Whether an arbitrary resource identity belongs to Maestro's managed system plane.
pub fn is_system_resource_id(resource_id: &str) -> bool {
    resource_id.starts_with(SYSTEM_RESOURCE_PREFIX)
}

/// Whether a service identity belongs to Maestro's managed system plane.
pub fn is_system_service(service_id: &crate::ServiceId) -> bool {
    is_system_resource_id(service_id.as_str())
}
