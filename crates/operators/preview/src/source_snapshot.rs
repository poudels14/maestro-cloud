use std::collections::BTreeMap;

use kernel_api::{
    Node, NodeId, NodeSpec, NodeStatus, Preview, PreviewId, PreviewSpec, PreviewStatus, Service,
    ServiceId, ServiceSpec, ServiceStatus,
};
use kernel_controller::FencedStore;
use kernel_store::Keyspace;

use crate::PreviewError;
use crate::snapshot::{StoredResource, decode_prefix};

pub(crate) struct PreviewSourceSnapshot {
    pub(crate) nodes: BTreeMap<NodeId, StoredResource<Node>>,
    pub(crate) services: BTreeMap<ServiceId, StoredResource<Service>>,
    pub(crate) previews: BTreeMap<PreviewId, StoredResource<Preview>>,
}

impl PreviewSourceSnapshot {
    pub(crate) async fn load(
        store: &FencedStore,
        keyspace: &Keyspace,
    ) -> Result<Self, PreviewError> {
        let nodes = decode_prefix::<NodeId, NodeSpec, NodeStatus>(
            store,
            keyspace.resource_kind(&kernel_api::ResourceKind::new("Node")?),
            "Node",
        )
        .await?;
        let services = decode_prefix::<ServiceId, ServiceSpec, ServiceStatus>(
            store,
            keyspace.resource_kind(&kernel_api::ResourceKind::new("Service")?),
            "Service",
        )
        .await?;
        let previews = decode_prefix::<PreviewId, PreviewSpec, PreviewStatus>(
            store,
            keyspace.resource_kind(&kernel_api::ResourceKind::new("Preview")?),
            "Preview",
        )
        .await?;
        Ok(Self {
            nodes,
            services,
            previews,
        })
    }

    pub(crate) fn coordinator(&self) -> Option<&NodeId> {
        self.nodes
            .values()
            .find(|node| node.resource.meta.deletion_timestamp.is_none())
            .map(|node| &node.resource.meta.id)
    }
}
