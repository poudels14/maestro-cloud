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

const QUANTITY_UNITS: Record<string, number> = {
  "": 1,
  K: 1e3,
  M: 1e6,
  G: 1e9,
  T: 1e12,
  Ki: 1024,
  Mi: 1024 ** 2,
  Gi: 1024 ** 3,
  Ti: 1024 ** 4
};

function quantityBytes(quantity: string): number {
  const match = /^([0-9.]+)\s*([KMGT]i?)?$/.exec(quantity.trim());
  return match ? Number(match[1]) * (QUANTITY_UNITS[match[2] ?? ""] ?? 1) : 0;
}

function formatUptime(ms: number): string {
  const minutes = Math.floor(ms / 60_000);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  if (days > 0) {
    return `${days}d ${hours % 24}h`;
  } else if (hours > 0) {
    return `${hours}h ${minutes % 60}m`;
  } else {
    return `${minutes}m`;
  }
}

export {
  quantityBytes,
  formatUptime,
  formatBytes,
  formatBytesRate,
  formatPercent,
  formatRate,
  formatMs,
  formatDateTime
};
