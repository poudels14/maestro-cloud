import NodeWebSocket from "crossws/websocket";
import { defineWebSocketHandler } from "nitro";
import {
  closeExecProxy,
  forwardExecProxyMessage,
  openExecProxy,
  type ExecProxyContext,
  type ExecUpstreamConstructor
} from "../../../../lib/execProxy";
import { guardRequest } from "../../../../lib/requestGuard";

function upstreamUrl(request: Request): string {
  const requestUrl = new URL(request.url);
  const apiSocket = process.env.MAESTRO_API_SOCKET;
  if (apiSocket) {
    return `ws+unix://${apiSocket}:${requestUrl.pathname}${requestUrl.search}`;
  } else {
    const apiHost = new URL(process.env.MAESTRO_API_HOST || "http://127.0.0.1:3000");
    apiHost.protocol = apiHost.protocol === "https:" ? "wss:" : "ws:";
    apiHost.pathname = requestUrl.pathname;
    apiHost.search = requestUrl.search;
    return apiHost.toString();
  }
}

export default defineWebSocketHandler({
  upgrade(request) {
    const rejection = guardRequest(request);
    if (rejection) throw new Response(rejection, { status: 403 });
    return {
      context: {
        url: upstreamUrl(request),
        authorization: process.env.MAESTRO_API_TOKEN
          ? `Bearer ${process.env.MAESTRO_API_TOKEN}`
          : undefined,
        pending: []
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
