#![allow(clippy::expect_used, clippy::unwrap_used)]

mod assignment;
mod assignment_plan;
mod assignment_restart;
#[cfg(target_os = "linux")]
mod cgroup_stats;
mod dns;
mod dns_resource;
mod dns_server;
mod exec;
mod fake_mesh;
mod firewall;
mod health;
#[cfg(target_os = "linux")]
mod host_disks;
#[cfg(target_os = "linux")]
mod host_stats;
#[cfg(target_os = "linux")]
mod linux_bridge;
#[cfg(target_os = "linux")]
mod linux_firewall;
#[cfg(target_os = "linux")]
mod linux_mesh;
#[cfg(unix)]
mod log_agent;
#[cfg(unix)]
mod log_checkpoint;
mod mesh;
mod mesh_identity;
mod mesh_resource;
#[cfg(unix)]
mod node_api;
#[cfg(unix)]
mod node_api_mount;
#[cfg(unix)]
mod node_api_support;
mod secret_mount;
#[cfg(target_os = "linux")]
mod stats;
mod workload_bridge;
