import type { ClusterNode } from "./types";

function nodeAdminUrl(nodes: ClusterNode[] | undefined, nodeId: string | null | undefined) {
  if (!nodeId) return null;
  return nodes?.find((node) => node.nodeId === nodeId)?.adminUrl ?? null;
}

function nodeAdminLabel(adminUrl: string) {
  try {
    return new URL(adminUrl).host;
  } catch {
    return adminUrl;
  }
}

export { nodeAdminLabel, nodeAdminUrl };
