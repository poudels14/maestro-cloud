import assert from "node:assert/strict";
import { test } from "vitest";
import { guardRequest } from "./requestGuard";

const request = (method: string, headers: Record<string, string>) => ({
  method,
  headers: new Headers(headers)
});

test("allows reads from tailnet, IP, and localhost hosts", () => {
  assert.equal(guardRequest(request("GET", { host: "node-1.tail1234.ts.net:7490" })), null);
  assert.equal(guardRequest(request("GET", { host: "100.64.0.3:7490" })), null);
  assert.equal(guardRequest(request("GET", { host: "localhost:7490" })), null);
});

test("rejects rebound hostnames", () => {
  assert.notEqual(guardRequest(request("GET", { host: "attacker.example" })), null);
});

test("requires a same-origin Origin for writes and websockets", () => {
  const host = "node-1.tail1234.ts.net:7490";
  assert.equal(
    guardRequest(request("POST", { host, origin: `http://${host}` })),
    null
  );
  assert.notEqual(guardRequest(request("POST", { host })), null);
  assert.notEqual(
    guardRequest(request("POST", { host, origin: "https://evil.example" })),
    null
  );
  assert.notEqual(
    guardRequest(request("GET", { host, upgrade: "websocket", origin: "https://evil.example" })),
    null
  );
});
