function isCurrentMaster(nodeId: string, leaderNodeId: string | null | undefined): boolean {
  return leaderNodeId === nodeId;
}

export { isCurrentMaster };
