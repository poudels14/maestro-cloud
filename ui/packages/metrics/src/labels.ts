function nodeIdFromMetricSource(source: string | undefined): string | null {
  const prefix = "node:";
  if (!source?.startsWith(prefix)) return null;
  const nodeId = source.slice(prefix.length);
  return nodeId || null;
}

export { nodeIdFromMetricSource };
