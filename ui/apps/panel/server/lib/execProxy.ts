export type ExecUpstreamEvent = {
  data?: unknown;
  code?: number;
  message?: string;
  reason?: string;
};

export type ExecUpstream = {
  binaryType: string;
  readyState: number;
  addEventListener(type: string, listener: (event: ExecUpstreamEvent) => void): void;
  send(data: string | ArrayBuffer | ArrayBufferView): void;
  close(): void;
};

export type ExecUpstreamConstructor = new (
  url: string,
  protocols: string[],
  options: { headers: Record<string, string> }
) => ExecUpstream;

export interface ExecProxyContext {
  url: string;
  authorization: string;
  upstream?: ExecUpstream;
  pending: Uint8Array[];
  clientClosed: boolean;
}

export interface ExecProxyPeer {
  context: unknown;
  send(data: Uint8Array | string): void;
  close(code?: number, reason?: string): void;
}

function proxyContext(peer: { context: unknown }): ExecProxyContext {
  return peer.context as ExecProxyContext;
}

function forwardUpstreamMessage(peer: ExecProxyPeer, data: unknown) {
  if (typeof data === "string") {
    peer.send(data);
  } else if (data instanceof ArrayBuffer) {
    peer.send(new Uint8Array(data));
  } else if (ArrayBuffer.isView(data)) {
    peer.send(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
  }
}

export function openExecProxy(peer: ExecProxyPeer, WebSocketClient: ExecUpstreamConstructor) {
  const context = proxyContext(peer);
  const upstream = new WebSocketClient(context.url, [], {
    headers: { authorization: context.authorization }
  });
  context.upstream = upstream;
  upstream.binaryType = "arraybuffer";
  upstream.addEventListener("open", () => {
    for (const message of context.pending.splice(0)) upstream.send(message);
  });
  upstream.addEventListener("message", (event) => {
    forwardUpstreamMessage(peer, event.data);
  });
  upstream.addEventListener("close", (event) => {
    if (context.clientClosed) return;
    context.clientClosed = true;
    peer.close(event.code || 1000, event.reason);
  });
  upstream.addEventListener("error", () => {
    if (context.clientClosed) return;
    context.clientClosed = true;
    peer.close(1011, "exec upstream connection failed");
  });
}

export function forwardExecProxyMessage(
  peer: ExecProxyPeer,
  message: { uint8Array(): Uint8Array }
) {
  const context = proxyContext(peer);
  const bytes = message.uint8Array();
  if (context.upstream?.readyState === 1) {
    context.upstream.send(bytes);
  } else {
    context.pending.push(bytes);
  }
}

export function closeExecProxy(peer: ExecProxyPeer) {
  const { upstream } = proxyContext(peer);
  if (upstream && (upstream.readyState === 0 || upstream.readyState === 1)) {
    upstream.close();
  }
}
