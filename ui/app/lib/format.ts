function formatBytes(value: number): string {
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)} GB`;
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)} MB`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)} KB`;
  return `${Math.round(value)} B`;
}

function formatBytesRate(value: number): string {
  return `${formatBytes(value)}/s`;
}

function formatPercent(value: number): string {
  return `${value.toFixed(1)}%`;
}

function formatRate(value: number): string {
  if (value >= 1000) return `${(value / 1000).toFixed(1)}k/s`;
  if (value >= 10) return `${value.toFixed(0)}/s`;
  return `${value.toFixed(2)}/s`;
}

function formatMs(value: number): string {
  if (value >= 1000) return `${(value / 1000).toFixed(2)}s`;
  return `${Math.round(value)}ms`;
}

function formatDateTime(ms: number, withYear = false): string {
  const options: Intl.DateTimeFormatOptions = {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  };
  if (withYear) {
    options.year = "numeric";
  }
  return new Date(ms).toLocaleString(undefined, options);
}

export { formatBytes, formatBytesRate, formatPercent, formatRate, formatMs, formatDateTime };
