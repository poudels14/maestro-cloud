use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use kernel_api::{ClusterId, NodeId, NodeInstanceId, Timestamp};
use logs::{
    IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStore, LogStream, OriginCursor,
};
use tokio::sync::{mpsc, watch};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// Stable system-log component used by controller and deployment orchestration events.
pub const CONTROLLER_LOG_COMPONENT: &str = "controller";

const DEFAULT_QUEUE_CAPACITY: usize = 2_048;
const MAXIMUM_APPEND_BATCH: usize = 64;

/// A tracing layer that copies accepted daemon events into the controller system-log stream.
#[derive(Clone)]
pub struct ControllerLogLayer {
    shared: Arc<Mutex<CaptureState>>,
}

/// Attachable side of [`ControllerLogLayer`], retained until the node log store is available.
#[derive(Clone)]
pub struct ControllerLogCapture {
    shared: Arc<Mutex<CaptureState>>,
    queue_capacity: usize,
}

/// Drains captured controller events into the node-local normalized log store.
pub struct ControllerLogWorker {
    shared: Arc<Mutex<CaptureState>>,
    generation: u64,
    receiver: mpsc::Receiver<IngestLogEntry>,
    store: Arc<dyn LogStore>,
}

/// Creates the tracing layer and the handle used to attach its durable destination later.
pub fn controller_log_capture() -> (ControllerLogLayer, ControllerLogCapture) {
    controller_log_capture_with_capacity(DEFAULT_QUEUE_CAPACITY)
}

fn controller_log_capture_with_capacity(
    queue_capacity: usize,
) -> (ControllerLogLayer, ControllerLogCapture) {
    let shared = Arc::new(Mutex::new(CaptureState::default()));
    (
        ControllerLogLayer {
            shared: shared.clone(),
        },
        ControllerLogCapture {
            shared,
            queue_capacity,
        },
    )
}

impl ControllerLogCapture {
    /// Starts accepting tracing events for one daemon process identity.
    pub fn attach(
        &self,
        cluster_id: ClusterId,
        node_id: NodeId,
        instance_id: NodeInstanceId,
        store: Arc<dyn LogStore>,
    ) -> Result<ControllerLogWorker, ControllerLogCaptureError> {
        let (sender, receiver) = mpsc::channel(self.queue_capacity);
        let generation = {
            let mut state = lock_capture(&self.shared);
            if state.active.is_some() {
                return Err(ControllerLogCaptureError::AlreadyAttached);
            }
            state.next_generation = state.next_generation.saturating_add(1);
            let generation = state.next_generation;
            state.active = Some(ActiveCapture {
                generation,
                cluster_id,
                node_id,
                instance_id,
                next_sequence: 0,
                sender,
            });
            generation
        };
        Ok(ControllerLogWorker {
            shared: self.shared.clone(),
            generation,
            receiver,
            store,
        })
    }
}

impl ControllerLogWorker {
    /// Runs until agent shutdown, then detaches the layer and drains accepted events.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                entry = self.receiver.recv() => {
                    let Some(entry) = entry else {
                        return;
                    };
                    self.append_batch(entry).await;
                }
            }
        }
        self.detach();
        while let Some(entry) = self.receiver.recv().await {
            self.append_batch(entry).await;
        }
    }

    async fn append_batch(&mut self, first: IngestLogEntry) {
        let mut entries = Vec::with_capacity(MAXIMUM_APPEND_BATCH);
        entries.push(first);
        while entries.len() < MAXIMUM_APPEND_BATCH {
            match self.receiver.try_recv() {
                Ok(entry) => entries.push(entry),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }
        // Controller observability must never make the controller unavailable. The bounded
        // channel and node-local log store provide the normal delivery path; stderr remains the
        // fallback when the store itself is unavailable.
        let _ = self.store.append(&entries).await;
    }

    fn detach(&self) {
        let mut state = lock_capture(&self.shared);
        if state
            .active
            .as_ref()
            .is_some_and(|active| active.generation == self.generation)
        {
            state.active = None;
        }
    }
}

impl Drop for ControllerLogWorker {
    fn drop(&mut self) {
        self.detach();
    }
}

impl<S> Layer<S> for ControllerLogLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let Some(span) = context.span(id) else {
            return;
        };
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        span.extensions_mut().insert(SpanFields(visitor.fields));
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, context: Context<'_, S>) {
        let Some(span) = context.span(id) else {
            return;
        };
        let mut visitor = FieldVisitor::default();
        values.record(&mut visitor);
        let mut extensions = span.extensions_mut();
        match extensions.get_mut::<SpanFields>() {
            Some(fields) => fields.0.extend(visitor.fields),
            None => {
                extensions.insert(SpanFields(visitor.fields));
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let mut attributes = BTreeMap::new();
        if let Some(scope) = context.event_scope(event) {
            for span in scope.from_root() {
                if let Some(fields) = span.extensions().get::<SpanFields>() {
                    attributes.extend(fields.0.clone());
                }
            }
        }
        attributes.extend(visitor.fields);
        let metadata = event.metadata();
        attributes.insert(
            "maestro.log.target".to_owned(),
            metadata.target().to_owned(),
        );
        if let Some(module) = metadata.module_path() {
            attributes.insert("maestro.log.module".to_owned(), module.to_owned());
        }
        if let Some(file) = metadata.file() {
            attributes.insert("maestro.log.file".to_owned(), file.to_owned());
        }
        if let Some(line) = metadata.line() {
            attributes.insert("maestro.log.line".to_owned(), line.to_string());
        }
        let body = visitor
            .message
            .unwrap_or_else(|| metadata.name().to_owned());
        let timestamp = system_timestamp();
        let (entry, sender) = {
            let mut state = lock_capture(&self.shared);
            let Some(active) = state.active.as_mut() else {
                return;
            };
            active.next_sequence = active.next_sequence.saturating_add(1);
            let entry = IngestLogEntry {
                id: LogRecordId {
                    node_id: active.node_id.clone(),
                    producer: LogProducer::System(CONTROLLER_LOG_COMPONENT.to_owned()),
                    cursor: OriginCursor::new(format!(
                        "{}:{}",
                        active.instance_id, active.next_sequence
                    )),
                },
                observed_at: timestamp,
                event_at: timestamp,
                severity: metadata.level().as_str().to_ascii_lowercase(),
                stream: LogStream::System,
                origin: LogOrigin::System {
                    cluster_id: active.cluster_id.clone(),
                    node_id: Some(active.node_id.clone()),
                    component: CONTROLLER_LOG_COMPONENT.to_owned(),
                },
                body: LogBody::Text(body),
                attributes,
            };
            (entry, active.sender.clone())
        };
        let _ = sender.try_send(entry);
    }
}

#[derive(Default)]
struct CaptureState {
    next_generation: u64,
    active: Option<ActiveCapture>,
}

struct ActiveCapture {
    generation: u64,
    cluster_id: ClusterId,
    node_id: NodeId,
    instance_id: NodeInstanceId,
    next_sequence: u64,
    sender: mpsc::Sender<IngestLogEntry>,
}

struct SpanFields(BTreeMap<String, String>);

#[derive(Default)]
struct FieldVisitor {
    message: Option<String>,
    fields: BTreeMap<String, String>,
}

impl FieldVisitor {
    fn record_value(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = Some(value);
        } else {
            self.fields.insert(field.name().to_owned(), value);
        }
    }
}

impl Visit for FieldVisitor {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_value(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_value(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field, value.to_string());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_value(field, value.to_owned());
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.record_value(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record_value(field, format!("{value:?}"));
    }
}

fn lock_capture(shared: &Mutex<CaptureState>) -> std::sync::MutexGuard<'_, CaptureState> {
    shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn system_timestamp() -> Timestamp {
    let milliseconds = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(error) => i64::try_from(error.duration().as_millis())
            .unwrap_or(i64::MAX)
            .saturating_neg(),
    };
    Timestamp(milliseconds)
}

/// The same capture handle cannot be attached to two daemon stores concurrently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ControllerLogCaptureError {
    /// One process attempted to attach the same tracing layer to two stores at once.
    #[error("controller log capture is already attached")]
    AlreadyAttached,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kernel_api::{ClusterId, NodeId, NodeInstanceId};
    use logs::{InMemoryLogStore, LogBody, LogOrigin};
    use tracing_subscriber::layer::SubscriberExt;

    use super::{CONTROLLER_LOG_COMPONENT, controller_log_capture_with_capacity};

    #[tokio::test]
    async fn captures_event_and_reconcile_span_fields_as_system_logs()
    -> Result<(), Box<dyn std::error::Error>> {
        let (layer, capture) = controller_log_capture_with_capacity(8);
        let subscriber = tracing_subscriber::registry().with(layer);
        let store = Arc::new(InMemoryLogStore::new());
        let worker = capture.attach(
            ClusterId::new("test")?,
            NodeId::new("admin")?,
            NodeInstanceId::new("process-1")?,
            store.clone(),
        )?;
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(worker.run(receiver));
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                "reconcile",
                kind = "Build",
                resource_id = "build-1",
                attempt = 2_u64
            );
            let _entered = span.enter();
            tracing::error!(
                reason = "ExternalValueSourceRejected",
                "secret fetch failed"
            );
        });
        shutdown.send(true)?;
        worker.await?;

        let entries = store.entries()?;
        assert_eq!(entries.len(), 1);
        let entry = entries.first().ok_or("missing captured log entry")?;
        assert_eq!(entry.severity, "error");
        assert_eq!(entry.body, LogBody::Text("secret fetch failed".to_owned()));
        assert_eq!(
            entry.attributes.get("kind").map(String::as_str),
            Some("Build")
        );
        assert_eq!(
            entry.attributes.get("resource_id").map(String::as_str),
            Some("build-1")
        );
        assert!(matches!(
            &entry.origin,
            LogOrigin::System { component, .. } if component == CONTROLLER_LOG_COMPONENT
        ));
        Ok(())
    }
}
