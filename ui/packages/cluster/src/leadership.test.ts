import { describe, expect, test } from "vitest";
import { isCurrentMaster } from "./leadership";

describe("isCurrentMaster", () => {
  test("identifies only the current elected controller leader", () => {
    expect(isCurrentMaster("node-b", "node-b")).toBe(true);
    expect(isCurrentMaster("node-a", "node-b")).toBe(false);
    expect(isCurrentMaster("node-a", null)).toBe(false);
  });
});
