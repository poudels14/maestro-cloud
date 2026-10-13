import { createFileRoute } from "@tanstack/solid-router";
import { createHmac, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { Readable } from "node:stream";

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
  const apiSocket = process.env.MAESTRO_API_SOCKET;
  if (apiSocket) {
    return socketFetch(
      apiSocket,
      url.pathname + url.search,
      request.method,
      headers,
      hasBody ? request.body : null
    );
  } else {
    return fetch(target, {
      method: request.method,
      headers,
      body: hasBody ? request.body : undefined,
      ...(hasBody ? { duplex: "half" } : {})
    } as RequestInit);
  }
}

function socketFetch(
  socketPath: string,
  path: string,
  method: string,
  headers: Headers,
  body: ReadableStream<Uint8Array> | null
): Promise<Response> {
  return new Promise((resolve, reject) => {
    const upstream = httpRequest(
      { socketPath, path, method, headers: Object.fromEntries(headers.entries()) },
      (incoming) => {
        const responseHeaders = new Headers();
        Object.entries(incoming.headers).forEach(([name, value]) => {
          if (Array.isArray(value)) {
            value.forEach((item) => responseHeaders.append(name, item));
          } else if (value !== undefined) {
            responseHeaders.set(name, value);
          }
        });
        resolve(
          new Response(Readable.toWeb(incoming) as ReadableStream<Uint8Array>, {
            status: incoming.statusCode ?? 502,
            headers: responseHeaders
          })
        );
      }
    );
    upstream.on("error", reject);
    if (body) {
      Readable.fromWeb(body as never).pipe(upstream);
    } else {
      upstream.end();
    }
  });
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
