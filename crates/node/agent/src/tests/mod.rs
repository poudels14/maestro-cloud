#![allow(clippy::expect_used, clippy::unwrap_used)]

mod assignment;
mod assignment_plan;
mod assignment_restart;
#[cfg(target_os = "linux")]
mod cgroup_stats;
mod exec;
mod fake_mesh;
mod fake_network;
mod health;
#[cfg(target_os = "linux")]
mod linux_mesh;
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
