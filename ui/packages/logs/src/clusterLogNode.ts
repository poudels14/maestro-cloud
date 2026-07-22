type ClusterLogNodeRef = {
  nodeId?: string | null;
  nodeName?: string | null;
  hostname?: string | null;
  source?: string | null;
};

function clusterLogNodeLabel(node: ClusterLogNodeRef): string {
  return node.nodeId || node.nodeName || node.hostname || node.source || "";
}

export { clusterLogNodeLabel };
export type { ClusterLogNodeRef };
