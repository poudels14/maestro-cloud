use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use containerd::services::v1::StreamInit;
use containerd::types::transfer::{Data, WindowUpdate};

use crate::ArtifactStoreError;
use crate::containerd_artifact_stream::{TransferTask, decode};

struct DropMarker(Arc<AtomicBool>);

impl Drop for DropMarker {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn containerd_transfer_is_owned_polled_and_canceled_with_its_stream() {
    let polled = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let future_polled = polled.clone();
    let future_dropped = dropped.clone();
    let mut transfer = TransferTask::new(async move {
        future_polled.store(true, Ordering::SeqCst);
        let _drop_marker = DropMarker(future_dropped);
        pending::<()>().await;
        Ok(())
    });

    assert!(!polled.load(Ordering::SeqCst));
    tokio::select! {
        biased;
        result = &mut transfer => panic!("pending transfer completed unexpectedly: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    assert!(polled.load(Ordering::SeqCst));
    assert!(!dropped.load(Ordering::SeqCst));

    drop(transfer);
    assert!(dropped.load(Ordering::SeqCst));
}

#[test]
fn containerd_stream_decodes_the_servers_raw_full_name_type_urls() {
    let message = containerd::to_any(&Data {
        data: b"bounded".to_vec(),
    });
    assert_eq!(decode::<Data>(&message, "test").unwrap().data, b"bounded");

    let message = containerd::to_any(&WindowUpdate { update: 65_536 });
    assert_eq!(
        decode::<WindowUpdate>(&message, "test").unwrap().update,
        65_536
    );
}

#[test]
fn containerd_stream_rejects_unexpected_protocol_messages() {
    let message = containerd::to_any(&StreamInit {
        id: "wrong-direction".to_owned(),
    });
    assert!(matches!(
        decode::<Data>(&message, "test"),
        Err(ArtifactStoreError::Stream { .. })
    ));
}
