use containerd::services::v1::StreamInit;
use containerd::types::transfer::{Data, WindowUpdate};

use crate::ArtifactStoreError;
use crate::containerd_artifact_stream::decode;

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
