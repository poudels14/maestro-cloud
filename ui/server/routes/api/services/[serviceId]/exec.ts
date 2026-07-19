import { createHmac } from "node:crypto";
import { readFileSync } from "node:fs";
import NodeWebSocket from "crossws/websocket";
import { defineWebSocketHandler } from "nitro";
import {
  closeExecProxy,
  forwardExecProxyMessage,
  openExecProxy,
  type ExecProxyContext,
  type ExecUpstreamConstructor
} from "../../../../lib/execProxy";

function base64Url(value: string): string {
  return Buffer.from(value).toString("base64url");
}

function serviceJwt(): string {
  const path = process.env.MAESTRO_SERVICE_JWT_KEY_FILE;
  if (!path) throw new Response("Admin service authentication is unavailable", { status: 503 });
  const secret = readFileSync(path, "utf8").trim();
  if (!secret) throw new Response("Admin service authentication is unavailable", { status: 503 });
  const now = Math.floor(Date.now() / 1000);
  const header = base64Url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payload = base64Url(
    JSON.stringify({ sub: "maestro-admin", scope: "operator", iat: now, exp: now + 300 })
  );
  const unsigned = `${header}.${payload}`;
  const signature = createHmac("sha256", secret).update(unsigned).digest("base64url");
  return `${unsigned}.${signature}`;
}

function upstreamUrl(request: Request): string {
  const apiHost = new URL(process.env.MAESTRO_API_HOST || "http://127.0.0.1:3001");
  const requestUrl = new URL(request.url);
  apiHost.protocol = apiHost.protocol === "https:" ? "wss:" : "ws:";
  apiHost.pathname = requestUrl.pathname;
  apiHost.search = requestUrl.search;
  return apiHost.toString();
}

export default defineWebSocketHandler({
  upgrade(request) {
    return {
      context: {
        url: upstreamUrl(request),
        authorization: `Bearer ${serviceJwt()}`,
        pending: [],
        clientClosed: false
      } satisfies ExecProxyContext
    };
  },
  open(peer) {
    openExecProxy(peer, NodeWebSocket as unknown as ExecUpstreamConstructor);
  },
  message(peer, message) {
    forwardExecProxyMessage(peer, message);
  },
  close(peer) {
    closeExecProxy(peer);
  }
});
