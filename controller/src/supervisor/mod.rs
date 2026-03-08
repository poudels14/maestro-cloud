use crate::error::Result;

#[allow(dead_code)]
pub mod config_source;
#[allow(dead_code)]
pub mod controller;
pub mod etcd;
pub mod model;
pub mod service;
pub mod worker;

pub async fn run(etcd_endpoint: &str) -> Result<()> {
    eprintln!("[maestro]: supervisor watching queued service deployments");
    etcd::run(etcd_endpoint).await
}
