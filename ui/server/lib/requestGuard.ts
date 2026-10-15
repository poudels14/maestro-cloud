const ALLOWED_HOST_SUFFIXES = [".ts.net"];

function isAllowedHost(host: string | null): boolean {
  if (!host) return false;
  const hostname = hostnameOf(host);
  const configured = (process.env.MAESTRO_ADMIN_HOSTS ?? "")
    .split(",")
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
  return (
    hostname === "localhost" ||
    isIpLiteral(hostname) ||
    ALLOWED_HOST_SUFFIXES.some((suffix) => hostname.endsWith(suffix)) ||
    configured.includes(hostname)
  );
}

function isAllowedOrigin(origin: string | null, host: string | null): boolean {
  if (!origin) return true;
  if (!host) return false;
  try {
    return new URL(origin).host.toLowerCase() === host.toLowerCase();
  } catch {
    return false;
  }
}

function guardRequest(request: { method: string; headers: Headers }): string | null {
  const host = request.headers.get("host");
  const safeMethod = request.method === "GET" || request.method === "HEAD";
  const upgrade = request.headers.get("upgrade")?.toLowerCase() === "websocket";
  if (!isAllowedHost(host)) {
    return "unknown host; add it to MAESTRO_ADMIN_HOSTS";
  } else if ((!safeMethod || upgrade) && !isAllowedOrigin(request.headers.get("origin"), host)) {
    return "cross-origin requests are not allowed";
  } else {
    return null;
  }
}

function hostnameOf(host: string): string {
  const lowered = host.toLowerCase();
  if (lowered.startsWith("[")) {
    return lowered.slice(1, lowered.indexOf("]"));
  } else {
    return lowered.split(":")[0] ?? lowered;
  }
}

function isIpLiteral(hostname: string): boolean {
  return /^\d{1,3}(\.\d{1,3}){3}$/.test(hostname) || hostname.includes(":");
}

export { guardRequest };
