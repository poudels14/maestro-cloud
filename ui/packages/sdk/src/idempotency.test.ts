import { expect, test } from "vitest";
import { createIdempotencyKey } from "./idempotency";

test("creates RFC 4122 idempotency keys outside secure browser contexts", () => {
  const keys = Array.from({ length: 16 }, () => createIdempotencyKey());

  expect(new Set(keys)).toHaveLength(keys.length);
  for (const key of keys) {
    expect(key).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  }
});
