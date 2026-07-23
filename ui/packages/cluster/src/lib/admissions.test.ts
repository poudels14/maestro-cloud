import { expect, test } from "vitest";
import { parseAdmissionRequest } from "./admissions";

test("normalizes a prepared node admission request", () => {
  const fingerprint = "AB".repeat(32);

  expect(parseAdmissionRequest("  worker-a  ", ` ${fingerprint} `)).toEqual({
    request: {
      nodeId: "worker-a",
      publicKeySha256: "ab".repeat(32)
    }
  });
});

test("requires a declared node ID and exact SHA-256 fingerprint", () => {
  expect(parseAdmissionRequest(" ", "ab".repeat(32))).toEqual({ error: "Node ID is required" });
  expect(parseAdmissionRequest("worker-a", "ab".repeat(31))).toEqual({
    error: "Join key fingerprint must be 64 hexadecimal characters"
  });
  expect(parseAdmissionRequest("worker-a", `${"ab".repeat(31)}zz`)).toEqual({
    error: "Join key fingerprint must be 64 hexadecimal characters"
  });
});
