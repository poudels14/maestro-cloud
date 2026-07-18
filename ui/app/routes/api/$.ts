import { createFileRoute } from "@tanstack/solid-router";
import { createHmac, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";

function base64Url(value: string): string {
  return Buffer.from(value).toString("base64url");
}

function serviceJwt(): string | undefined {
  const path = process.env.MAESTRO_SERVICE_JWT_KEY_FILE;
  if (!path) return undefined;
  const secret = readFileSync(path, "utf8").trim();
  if (!secret) return undefined;
  const now = Math.floor(Date.now() / 1000);
  const header = base64Url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payload = base64Url(
    JSON.stringify({ sub: "maestro-admin", scope: "operator", iat: now, exp: now + 300 })
  );
  const unsigned = `${header}.${payload}`;
  const signature = createHmac("sha256", secret).update(unsigned).digest("base64url");
  return `${unsigned}.${signature}`;
}

async function proxy(request: Request): Promise<Response> {
  const apiHost = process.env.MAESTRO_API_HOST || "http://127.0.0.1:3001";
  const url = new URL(request.url);
  if (/^\/api\/services\/[^/]+\/exec$/.test(url.pathname)) {
    return new Response("Not found", { status: 404 });
  }
  const target = new URL(url.pathname + url.search, apiHost);
  const hasBody = request.method !== "GET" && request.method !== "HEAD";
  const headers = new Headers(request.headers);
  headers.delete("host");
  const jwt = serviceJwt();
  if (jwt) headers.set("authorization", `Bearer ${jwt}`);
  if (hasBody && !headers.has("idempotency-key")) {
    headers.set("idempotency-key", randomUUID());
  }
  const response = await fetch(target, {
    method: request.method,
    headers,
    body: hasBody ? request.body : undefined,
    ...(hasBody ? { duplex: "half" } : {})
  } as RequestInit);
  return response;
}

export const Route = createFileRoute("/api/$")({
  server: {
    handlers: {
      GET: ({ request }) => proxy(request),
      POST: ({ request }) => proxy(request),
      PUT: ({ request }) => proxy(request),
      PATCH: ({ request }) => proxy(request),
      DELETE: ({ request }) => proxy(request)
    }
  }
});
