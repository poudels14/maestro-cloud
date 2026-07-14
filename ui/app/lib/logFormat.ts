const tsFormatter = new Intl.DateTimeFormat(undefined, {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false
});

const timeFormatter = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false
});

const dateFormatter = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "2-digit"
});

function logLevelPill(level: string) {
  const normalized = level.toLowerCase();
  if (normalized === "error" || normalized === "fatal") {
    return "bg-red-50 text-red-600 border border-red-100";
  }
  if (normalized === "warn" || normalized === "warning") {
    return "bg-amber-50 text-amber-600 border border-amber-100";
  }
  if (normalized === "debug" || normalized === "trace") {
    return "bg-gray-50 text-gray-400 border border-gray-100";
  }
  return "bg-blue-50 text-blue-600 border border-blue-100";
}

const HTTP_METHOD_KEYS = ["http.method", "method", "req.method", "request.method"];
const HTTP_STATUS_KEYS = [
  "http.status_code",
  "status_code",
  "statuscode",
  "status",
  "http.status",
  "response.status_code",
  "http.response.status_code",
  "downstreamstatus"
];
const HTTP_PATH_KEYS = [
  "http.url_details.path",
  "http.path",
  "path",
  "url",
  "route",
  "uri",
  "request.path",
  "target"
];
const HTTP_DURATION_KEYS = ["duration", "duration_ns"];

type HttpFields = {
  method?: string;
  status?: string;
  path?: string;
  durationLabel?: string;
};

function attrLookup(attrs: [string, string][] | undefined, keys: string[]): string | undefined {
  if (!attrs) return undefined;
  for (const [key, value] of attrs) {
    if (keys.includes(key.toLowerCase())) return value;
  }
  return undefined;
}

function httpFields(attrs: [string, string][] | undefined): HttpFields {
  const durationRaw = attrLookup(attrs, HTTP_DURATION_KEYS);
  const durationNs = durationRaw == null ? NaN : Number(durationRaw);
  return {
    method: attrLookup(attrs, HTTP_METHOD_KEYS),
    status: attrLookup(attrs, HTTP_STATUS_KEYS),
    path: attrLookup(attrs, HTTP_PATH_KEYS),
    durationLabel:
      Number.isFinite(durationNs) && durationNs > 0 ? formatDurationNs(durationNs) : undefined
  };
}

function formatDurationNs(ns: number): string {
  const ms = ns / 1_000_000;
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)}s`;
  if (ms >= 1) return `${Math.round(ms)}ms`;
  return `${Math.round(ns / 1000)}µs`;
}

function httpMethodColor(method: string): string {
  switch (method.toUpperCase()) {
    case "GET":
      return "text-sky-600";
    case "POST":
      return "text-emerald-600";
    case "PUT":
    case "PATCH":
      return "text-amber-600";
    case "DELETE":
      return "text-red-600";
    default:
      return "text-violet-600";
  }
}

function httpStatusPill(status: string): string {
  const code = Number.parseInt(status, 10);
  if (code >= 500) return "bg-red-50 text-red-700 border border-red-100";
  if (code >= 400) return "bg-amber-50 text-amber-700 border border-amber-100";
  if (code >= 300) return "bg-cyan-50 text-cyan-700 border border-cyan-100";
  if (code >= 200) return "bg-emerald-50 text-emerald-700 border border-emerald-100";
  return "bg-gray-50 text-gray-500 border border-gray-100";
}

function logLevelColors(level: string) {
  const normalized = level.toLowerCase();
  if (normalized === "error" || normalized === "fatal") {
    return {
      dot: "bg-red-500",
      text: "text-red-700",
      pillActive: "bg-red-50 text-red-700 border-red-200",
      pillHover: "hover:bg-red-50 hover:text-red-700 hover:border-red-200"
    };
  }
  if (normalized === "warn" || normalized === "warning") {
    return {
      dot: "bg-amber-500",
      text: "text-amber-700",
      pillActive: "bg-amber-50 text-amber-700 border-amber-200",
      pillHover: "hover:bg-amber-50 hover:text-amber-700 hover:border-amber-200"
    };
  }
  if (normalized === "debug" || normalized === "trace") {
    return {
      dot: "bg-gray-400",
      text: "text-gray-500",
      pillActive: "bg-gray-100 text-gray-700 border-gray-200",
      pillHover: "hover:bg-gray-100 hover:text-gray-700 hover:border-gray-200"
    };
  }
  return {
    dot: "bg-blue-500",
    text: "text-blue-700",
    pillActive: "bg-blue-50 text-blue-700 border-blue-200",
    pillHover: "hover:bg-blue-50 hover:text-blue-700 hover:border-blue-200"
  };
}

export {
  tsFormatter,
  timeFormatter,
  dateFormatter,
  logLevelPill,
  logLevelColors,
  httpFields,
  httpMethodColor,
  httpStatusPill,
  type HttpFields
};
