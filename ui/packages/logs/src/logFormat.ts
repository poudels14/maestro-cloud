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

const HTTP_METHOD_KEYS = [
  "http.method",
  "http.request.method",
  "method",
  "req.method",
  "request.method",
  "requestmethod"
];
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
  "http.route",
  "url.path",
  "path",
  "url",
  "route",
  "uri",
  "request.path",
  "requestpath",
  "target"
];
const HTTP_DURATION_KEYS = ["duration", "duration_ns"];
const HTTP_HOST_KEYS = [
  "http.host",
  "http.request.host",
  "server.address",
  "request.host",
  "requesthost"
];
const HTTP_CLIENT_IP_KEYS = [
  "maestro.client_ip",
  "client.address",
  "http.client_ip",
  "client_ip",
  "clienthost"
];
const TRAEFIK_ROUTER_KEYS = ["routername"];
const TRAEFIK_SERVICE_KEYS = ["servicename"];
const TRAEFIK_ENTRYPOINT_KEYS = ["entrypointname"];
const TRAEFIK_SCHEME_KEYS = ["requestscheme"];

type HttpFields = {
  method?: string | undefined;
  status?: string | undefined;
  path?: string | undefined;
  durationLabel?: string | undefined;
  requestHost?: string | undefined;
  clientIp?: string | undefined;
  router?: string | undefined;
  service?: string | undefined;
  entryPoint?: string | undefined;
  scheme?: string | undefined;
  isTraefikAccessLog: boolean;
};

function attrLookup(attrs: [string, string][] | undefined, keys: string[]): string | undefined {
  if (!attrs) return undefined;
  for (const key of keys) {
    const match = attrs.find(([candidate]) => candidate.toLowerCase() === key);
    if (match) return match[1];
  }
  return undefined;
}

function httpFields(attrs: [string, string][] | undefined): HttpFields {
  const durationRaw = attrLookup(attrs, HTTP_DURATION_KEYS);
  const durationNs = durationRaw == null ? NaN : Number(durationRaw);
  const method = attrLookup(attrs, HTTP_METHOD_KEYS);
  const status = attrLookup(attrs, HTTP_STATUS_KEYS);
  const path = attrLookup(attrs, HTTP_PATH_KEYS);
  const traefikMethod = attrLookup(attrs, ["requestmethod"]);
  const traefikStatus = attrLookup(attrs, ["downstreamstatus"]);
  const traefikPath = attrLookup(attrs, ["requestpath"]);
  return {
    method,
    status,
    path,
    durationLabel:
      Number.isFinite(durationNs) && durationNs >= 0 ? formatDurationNs(durationNs) : undefined,
    requestHost: attrLookup(attrs, HTTP_HOST_KEYS),
    clientIp: attrLookup(attrs, HTTP_CLIENT_IP_KEYS),
    router: attrLookup(attrs, TRAEFIK_ROUTER_KEYS),
    service: attrLookup(attrs, TRAEFIK_SERVICE_KEYS),
    entryPoint: attrLookup(attrs, TRAEFIK_ENTRYPOINT_KEYS),
    scheme: attrLookup(attrs, TRAEFIK_SCHEME_KEYS),
    isTraefikAccessLog: Boolean(traefikMethod && traefikStatus && traefikPath)
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
      return "text-brand";
    case "POST":
      return "text-emerald-600";
    case "PUT":
    case "PATCH":
      return "text-amber-600";
    case "DELETE":
      return "text-red-600";
    default:
      return "text-brand";
  }
}

function httpStatusPill(status: string): string {
  const code = Number.parseInt(status, 10);
  if (code >= 500) return "bg-red-100 text-red-600 border border-red-200";
  if (code >= 400) return "bg-amber-100 text-amber-600 border border-amber-200";
  if (code >= 300) return "bg-brand-ring text-brand border border-brand-border";
  if (code >= 200) return "bg-emerald-100 text-emerald-600 border border-emerald-200";
  return "bg-gray-100 text-gray-500 border border-gray-200";
}

function logLevelColors(level: string) {
  const normalized = level.toLowerCase();
  if (normalized === "error" || normalized === "fatal") {
    return {
      dot: "bg-red-500",
      text: "text-red-600",
      pillActive: "bg-red-50 text-red-700 border-red-200",
      pillHover: "hover:bg-red-50 hover:text-red-700 hover:border-red-200"
    };
  }
  if (normalized === "warn" || normalized === "warning") {
    return {
      dot: "bg-amber-500",
      text: "text-amber-600",
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
    dot: "bg-brand",
    text: "text-brand",
    pillActive: "bg-brand-light text-brand-hover border-brand-border",
    pillHover: "hover:bg-brand-light hover:text-brand-hover hover:border-brand-border"
  };
}

export {
  dateFormatter,
  httpFields,
  httpMethodColor,
  httpStatusPill,
  logLevelColors,
  timeFormatter,
  tsFormatter,
  type HttpFields
};
