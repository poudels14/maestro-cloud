import assert from "node:assert/strict";
import { test } from "vitest";
import type { LogEntry } from "./api";
import { buildLogQueryCatalog, mergeLogEntries } from "./logViewerModel";

test("merges polling pages without duplicating stable log identities", () => {
  const first = entry({ seq: 1, nodeId: "node-a", text: "first" });
  const duplicate = entry({ seq: 1, nodeId: "node-a", text: "replayed" });
  const next = entry({ seq: 1, nodeId: "node-b", text: "next" });

  assert.deepEqual(
    mergeLogEntries([first], [duplicate, next]).map((value) => value.text),
    ["first", "next"]
  );
});

test("prepends preceding pages without duplicating their boundary", () => {
  const earlier = entry({ seq: 1, ts: 100, text: "earlier" });
  const boundary = entry({ seq: 2, ts: 300, text: "boundary" });
  const duplicate = entry({ seq: 2, ts: 300, text: "replayed boundary" });
  const interleaved = entry({ seq: 3, ts: 200, text: "interleaved" });

  assert.deepEqual(
    mergeLogEntries([earlier, boundary], [interleaved, duplicate]).map((value) => value.text),
    ["earlier", "interleaved", "boundary"]
  );
});

test("builds bounded query suggestions from normalized log fields", () => {
  const catalog = buildLogQueryCatalog("api", [
    entry({
      serviceId: "worker",
      level: "WARN",
      source: "worker-0/deploy",
      attrs: [
        ["http.status_code", "503"],
        ["custom.region", "us-west-2"],
        ["invalid field", "ignored"]
      ]
    })
  ]);

  assert.deepEqual(catalog.fields, ["@custom.region", "@http.status_code"]);
  assert.deepEqual(catalog.values.get("service"), ["api", "worker"]);
  assert.deepEqual(catalog.values.get("level"), ["warn"]);
  assert.deepEqual(catalog.values.get("@http.status_code"), ["503"]);
  assert.equal(catalog.values.has("@invalid field"), false);
});

function entry(overrides: Partial<LogEntry>): LogEntry {
  return {
    seq: overrides.seq ?? 1,
    ts: overrides.ts ?? 100,
    level: overrides.level ?? "info",
    stream: overrides.stream ?? "stdout",
    text: overrides.text ?? "message",
    origin: overrides.origin ?? "workload",
    nodeId: overrides.nodeId ?? "node-a",
    tier: overrides.tier ?? "service",
    ...(overrides.serviceId ? { serviceId: overrides.serviceId } : {}),
    ...(overrides.source ? { source: overrides.source } : {}),
    ...(overrides.attrs ? { attrs: overrides.attrs } : {})
  };
}
