import { createFileRoute } from "@tanstack/solid-router";

async function proxy(request: Request): Promise<Response> {
  const apiHost = process.env.MAESTRO_API_HOST || "http://127.0.0.1:3001";
  const url = new URL(request.url);
  const target = new URL(url.pathname + url.search, apiHost);
  console.log(`[proxy] ${request.method} ${url.pathname} -> ${target.toString()}`);
  const hasBody = request.method !== "GET" && request.method !== "HEAD";
  const headers = new Headers(request.headers);
  headers.delete("host");
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
