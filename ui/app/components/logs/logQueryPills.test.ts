import assert from "node:assert/strict";
import { test } from "vitest";
import {
  combineLogQueries,
  logQueryPills,
  removeLogQueryPill,
  withLogHistogramGroupFilter
} from "./logQueryPills.ts";

test("extracts top-level field filters and preserves their values", () => {
  const pills = logQueryPills(
    'level:error AND @http.status_code:[500 TO 599] message:"connection refused"'
  );
  assert.deepEqual(
    pills.map(({ prefix, field, value }) => ({ prefix, field, value })),
    [
      { prefix: "", field: "level", value: "error" },
      { prefix: "", field: "@http.status_code", value: "[500 TO 599]" },
      { prefix: "", field: "message", value: '"connection refused"' }
    ]
  );
});

test("does not represent top-level OR or grouped expressions as independent pills", () => {
  assert.deepEqual(logQueryPills("level:error OR level:warn"), []);
  assert.deepEqual(logQueryPills("(level:error OR level:warn)"), []);

  const pills = logQueryPills("(level:error OR level:warn) @http.status_code:>=400");
  assert.deepEqual(
    pills.map(({ field, value }) => ({ field, value })),
    [{ field: "@http.status_code", value: ">=400" }]
  );
});

test("keeps exclusion operators on their pills", () => {
  const pills = logQueryPills(
    '-message:"health check" AND - @http.status_code:200 AND NOT level:info'
  );
  assert.deepEqual(
    pills.map(({ prefix, field }) => ({ prefix, field })),
    [
      { prefix: "-", field: "message" },
      { prefix: "-", field: "@http.status_code" },
      { prefix: "NOT ", field: "level" }
    ]
  );

  const nested = "NOT NOT level:info AND service:app";
  const level = logQueryPills(nested)[0];
  assert.equal(level.prefix, "NOT NOT ");
  assert.equal(removeLogQueryPill(nested, level), "service:app");
});

test("removes explicit and implicit conjunction terms without breaking the query", () => {
  const explicit = "level:error AND @http.status_code:404 AND service:app";
  const explicitPills = logQueryPills(explicit);
  assert.equal(
    removeLogQueryPill(explicit, explicitPills[0]),
    "@http.status_code:404 AND service:app"
  );
  assert.equal(removeLogQueryPill(explicit, explicitPills[1]), "level:error AND service:app");
  assert.equal(
    removeLogQueryPill(explicit, explicitPills[2]),
    "level:error AND @http.status_code:404"
  );

  const implicit = "timeout level:error @http.status_code:404 service:app";
  const status = logQueryPills(implicit).find((pill) => pill.field === "@http.status_code");
  assert.ok(status);
  assert.equal(removeLogQueryPill(implicit, status), "timeout level:error service:app");
});

test("only recognizes supported reserved fields or custom attributes", () => {
  assert.deepEqual(logQueryPills("unknown:value"), []);
  assert.equal(logQueryPills("@custom.value:present").length, 1);
  assert.deepEqual(logQueryPills('message:"unterminated'), []);
});

test("keeps a required disjunction mandatory when the user query also contains OR", () => {
  assert.equal(
    combineLogQueries(
      "@required.primary:true OR @required.legacy:true",
      "@user.first:true OR @user.second:true"
    ),
    "(@required.primary:true OR @required.legacy:true) AND (@user.first:true OR @user.second:true)"
  );
  assert.equal(combineLogQueries("@required.primary:true", ""), "@required.primary:true");
});

test("maps HTTP histogram groups to status-code filters", () => {
  assert.equal(withLogHistogramGroupFilter("", "status", "5xx"), "@http.status_code:[500 TO 599]");
  assert.equal(
    withLogHistogramGroupFilter("service:api AND @http.status_code:[400 TO 499]", "status", "2xx"),
    "service:api AND @http.status_code:[200 TO 299]"
  );
});
