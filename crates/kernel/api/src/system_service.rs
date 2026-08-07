/// Managed Traefik workload service identity.
pub const TRAEFIK_SERVICE_ID: &str = "maestro-system-traefik";
/// Managed Cloudflare Tunnel workload service identity.
pub const CLOUDFLARE_SERVICE_ID: &str = "maestro-system-cloudflared";
/// Managed Tailscale gateway workload service identity.
pub const TAILSCALE_GATEWAY_SERVICE_ID: &str = "maestro-system-tailscale-gateway";
/// Managed cluster DNS resolver workload service identity.
pub const DNS_RESOLVER_SERVICE_ID: &str = "maestro-system-dns";

/// First host offset reserved for schedulable Maestro system services.
pub const SYSTEM_SERVICE_ADDRESS_START: u32 = 2;
/// Last host offset reserved for schedulable Maestro system services.
pub const SYSTEM_SERVICE_ADDRESS_END: u32 = 31;
/// Fixed host offset used by the node-local Admin API and panel.
pub const ADMIN_ADDRESS_OFFSET: u32 = 5;
/// First host offset available to user workloads.
pub const USER_WORKLOAD_ADDRESS_START: u32 = SYSTEM_SERVICE_ADDRESS_END + 1;

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

/// Desired replica count after applying the temporary override and the system-service floor.
pub fn desired_service_replicas(service: &crate::Service) -> u32 {
    let configured = service
        .status
        .replica_override
        .unwrap_or(service.spec.replicas);
    if is_system_service(&service.meta.id) {
        configured.max(1)
    } else {
        configured
    }
}
