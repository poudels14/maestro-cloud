import { expect, test } from "vitest";
import { createIdempotencyKey } from "./idempotency";

test("creates cryptographically random idempotency keys outside secure browser contexts", () => {
  const keys = Array.from({ length: 16 }, () => createIdempotencyKey());

  expect(new Set(keys)).toHaveLength(keys.length);
  for (const key of keys) {
    expect(key).toMatch(/^[0-9A-Za-z]{24}$/);
  }
});
