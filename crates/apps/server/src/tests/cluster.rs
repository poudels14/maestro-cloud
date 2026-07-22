use axum::http::StatusCode;
use kernel_api::ClusterInfo;

use crate::{ApiServer, ServerSettings};

use super::{decode, request, seeded_store};

#[tokio::test]
async fn cluster_info_summarizes_node_capabilities() -> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let response = request(&server, "/api/cluster", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        decode::<ClusterInfo>(response).await?,
        ClusterInfo {
            cluster_id,
            node_count: 1,
            control_plane_node_count: 1,
            workload_node_count: 1,
        }
    );
    Ok(())
}
