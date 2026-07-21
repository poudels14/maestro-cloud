//! Pull-request preview derivation for Maestro services.
//!
//! This crate reads typed Preview, Service, and IngressRoute resources and
//! writes only fenced derived resources. GitHub transport and daemon
//! composition remain outside the derivation core.

mod reconciler;
mod resource;
mod snapshot;
mod writer;

pub use reconciler::{PreviewError, PreviewReconciler, PreviewSettings};

#[cfg(test)]
mod tests;
