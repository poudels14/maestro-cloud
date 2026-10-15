import { createFileRoute } from "@tanstack/solid-router";
import { request as httpRequest } from "node:http";
import { Readable } from "node:stream";
import { guardRequest } from "../../../server/lib/requestGuard";

const NULL_BODY_STATUSES = new Set([101, 204, 205, 304]);

async function proxy(request: Request): Promise<Response> {
  const rejection = guardRequest(request);
  if (rejection) return new Response(rejection, { status: 403 });
  const apiHost = process.env.MAESTRO_API_HOST || "http://127.0.0.1:3000";
  const url = new URL(request.url);
  if (/^\/api\/services\/[^/]+\/exec$/.test(url.pathname)) {
    return new Response("Not found", { status: 404 });
  }
  const target = new URL(url.pathname + url.search, apiHost);
  const hasBody = request.method !== "GET" && request.method !== "HEAD";
  const headers = new Headers(request.headers);
  headers.delete("host");
  const apiToken = process.env.MAESTRO_API_TOKEN;
  if (apiToken) headers.set("authorization", `Bearer ${apiToken}`);
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
        const status = incoming.statusCode ?? 502;
        const body = NULL_BODY_STATUSES.has(status)
          ? null
          : (Readable.toWeb(incoming) as ReadableStream<Uint8Array>);
        if (body === null) incoming.resume();
        resolve(new Response(body, { status, headers: responseHeaders }));
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
