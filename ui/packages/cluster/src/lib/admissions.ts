import type { NodeJoinApprovalRequest } from "../types";

type AdmissionRequestResult = { request: NodeJoinApprovalRequest } | { error: string };

function parseAdmissionRequest(nodeId: string, fingerprint: string): AdmissionRequestResult {
  const normalizedNodeId = nodeId.trim();
  const normalizedFingerprint = fingerprint.trim().toLowerCase();

  if (!normalizedNodeId) {
    return { error: "Node ID is required" };
  }
  if (!/^[0-9a-f]{64}$/.test(normalizedFingerprint)) {
    return { error: "Join key fingerprint must be 64 hexadecimal characters" };
  }
  return {
    request: {
      nodeId: normalizedNodeId,
      publicKeySha256: normalizedFingerprint
    }
  };
}

export { parseAdmissionRequest };
export type { AdmissionRequestResult };
