import { expect, test } from "vitest";
import { mergeDefinedProperties } from "./search";

test("merges search state while removing cleared values", () => {
  expect(
    mergeDefinedProperties(
      { query: "level:error", range: "6h" },
      { query: undefined, node: "node-a" }
    )
  ).toEqual({ range: "6h", node: "node-a" });
});
