import assert from "node:assert/strict";
import { test } from "vitest";
import { buildSuggestions, tokenAtCursor, type LogQueryCatalog } from "./logQuerySuggestions";

const catalog: LogQueryCatalog = {
  fields: ["@custom.region"],
  values: new Map([
    ["service", ["api gateway"]],
    ["@custom.region", ["us-west-2"]]
  ])
};

test("suggests built-in and observed fields at the active token", () => {
  assert.ok(buildSuggestions("", 0, catalog).some((suggestion) => suggestion.label === "level:"));
  assert.deepEqual(
    buildSuggestions("@custom", 7, catalog).map((suggestion) => suggestion.label),
    ["@custom.region:"]
  );
  assert.equal(tokenAtCursor("(level:error) @custom", 21).text, "@custom");
});

test("suggests quoted observed values and preserves exclusion intent", () => {
  assert.ok(
    buildSuggestions("service:a", 9, catalog).some(
      (suggestion) => suggestion.label === 'service:"api gateway"' && suggestion.complete
    )
  );
  assert.equal(buildSuggestions("-@custom", 8, catalog)[0]?.label, "-@custom.region:");
});
