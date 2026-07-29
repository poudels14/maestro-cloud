import { customAlphabet } from "nanoid";

const generateIdempotencyKey = customAlphabet(
  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
  24
);

function createIdempotencyKey(): string {
  return generateIdempotencyKey();
}

export { createIdempotencyKey };
