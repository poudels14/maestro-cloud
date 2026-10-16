import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import type { IncomingMessage } from "node:http";
import { test } from "vitest";
import {
  closeExecProxy,
  forwardExecProxyMessage,
  openExecProxy,
  type ExecProxyContext,
  type ExecProxyPeer,
  type ExecUpstream
} from "./execProxy.ts";

type Listener = (event: any, ...rest: any[]) => void;

class FakeUpstream implements ExecUpstream {
  static instance: FakeUpstream;

  url: string;
  protocols: string[];
  options: { headers: Record<string, string> };
  binaryType = "blob";
  readyState = 0;
  sent: Array<string | ArrayBuffer | ArrayBufferView> = [];
  closed = false;
  listeners = new Map<string, Listener[]>();

  constructor(url: string, protocols: string[], options: { headers: Record<string, string> }) {
    this.url = url;
    this.protocols = protocols;
    this.options = options;
    FakeUpstream.instance = this;
  }

  addEventListener(type: string, listener: Listener) {
    const listeners = this.listeners.get(type) ?? [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  on(event: string, listener: (...args: any[]) => void) {
    this.addEventListener(event, listener);
  }

  reject(statusCode: number, body: string) {
    const response = Object.assign(new EventEmitter(), { statusCode });
    this.emit("unexpected-response", {}, response as unknown as IncomingMessage);
    response.emit("data", Buffer.from(body));
    response.emit("end");
  }

  emit(type: string, event: any = {}, ...rest: any[]) {
    for (const listener of this.listeners.get(type) ?? []) listener(event, ...rest);
  }

  send(data: string | ArrayBuffer | ArrayBufferView) {
    this.sent.push(data);
  }

  close() {
    this.closed = true;
  }
}

test("exec relay authenticates upstream and preserves messages in both directions", () => {
  const context: ExecProxyContext = {
    url: "ws://controller/api/services/test-service/exec?command=id",
    authorization: "Bearer service-token",
    pending: []
  };
  const received: Array<Uint8Array | string> = [];
  const closes: Array<[number | undefined, string | undefined]> = [];
  const peer: ExecProxyPeer = {
    context,
    send: (data) => received.push(data),
    close: (code, reason) => closes.push([code, reason])
  };

  openExecProxy(peer, FakeUpstream);
  const upstream = FakeUpstream.instance;
  assert.equal(upstream.url, context.url);
  assert.deepEqual(upstream.protocols, []);
  assert.equal(upstream.options.headers.authorization, "Bearer service-token");
  assert.equal(upstream.binaryType, "arraybuffer");

  forwardExecProxyMessage(peer, { uint8Array: () => new Uint8Array([1, 2, 3]) });
  assert.equal(upstream.sent.length, 0);
  assert.equal(context.pending.length, 1);

  upstream.readyState = 1;
  upstream.emit("open");
  assert.deepEqual(Array.from(upstream.sent[0] as Uint8Array), [1, 2, 3]);
  assert.equal(context.pending.length, 0);

  upstream.emit("message", { data: new Uint8Array([4, 5]) });
  assert.deepEqual(Array.from(received[0] as Uint8Array), [4, 5]);

  upstream.emit("error");
  assert.deepEqual(closes, [[1011, "Maestro exec relay failed"]]);

  closeExecProxy(peer);
  assert.equal(upstream.closed, true);
});

test("exec relay turns reserved upstream close codes into a normal close", () => {
  const context: ExecProxyContext = { url: "ws://controller/exec", pending: [] };
  const closes: Array<[number | undefined, string | undefined]> = [];
  const peer: ExecProxyPeer = {
    context,
    send: () => {},
    close: (code, reason) => closes.push([code, reason])
  };

  openExecProxy(peer, FakeUpstream);
  FakeUpstream.instance.emit("close", { code: 1005, reason: "" });
  FakeUpstream.instance.emit("close", { code: 4001, reason: "exec disabled" });
  assert.deepEqual(closes, [
    [1000, ""],
    [4001, "exec disabled"]
  ]);
  assert.deepEqual(FakeUpstream.instance.options.headers, {});
});

test("exec relay passes the control plane's rejection to the client", () => {
  const context: ExecProxyContext = { url: "ws://controller/exec", pending: [] };
  const closes: Array<[number | undefined, string | undefined]> = [];
  const peer: ExecProxyPeer = {
    context,
    send: () => {},
    close: (code, reason) => closes.push([code, reason])
  };

  openExecProxy(peer, FakeUpstream);
  const upstream = FakeUpstream.instance;
  upstream.reject(404, JSON.stringify({ error: "service nosuchservice not found" }));
  upstream.emit("error");
  upstream.emit("close", { code: 1006, reason: "" });
  assert.deepEqual(closes, [[4404, "service nosuchservice not found"]]);
  assert.equal(upstream.closed, true);
});
