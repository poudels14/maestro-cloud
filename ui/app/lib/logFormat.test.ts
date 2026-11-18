import { describe, expect, it } from "vitest";
import { isStructuredLog } from "./logFormat";
import type { LogEntry } from "./types";

const entry = (text: string, message?: string): LogEntry => ({
  seq: 1,
  ts: 0,
  level: "error",
  stream: "stdout",
  text,
  message
});

describe("isStructuredLog", () => {
  it("treats lines the backend parsed into a separate message as structured", () => {
    const json =
      '{"level":"error","msg":"[evals] failed to recover stale runs","error":{"type":"DrizzleQueryError"}}';
    expect(isStructuredLog(entry(json, "[evals] failed to recover stale runs"))).toBe(true);
  });

  it("leaves plain lines alone", () => {
    expect(isStructuredLog(entry("listening on :3000", "listening on :3000"))).toBe(false);
    expect(isStructuredLog(entry("listening on :3000"))).toBe(false);
  });
});
