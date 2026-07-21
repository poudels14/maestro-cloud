//! Leader-fenced delivery of typed Maestro resource transitions.
//!
//! This operator depends only on kernel contracts and its outbound transport
//! seam. It must not depend on nodes, runtimes, applications, or sibling
//! operators.

mod delivery;
mod http;
mod reconciler;
mod snapshot;
mod writer;

pub use delivery::{WebhookDelivery, WebhookDeliveryBackend, WebhookDeliveryError};
pub use http::{HttpWebhookBackend, HttpWebhookBackendError};
pub use reconciler::{WebhookError, WebhookReconciler, WebhookSettings};

#[cfg(test)]
mod tests;
