import { expect, test } from "vitest";
import { groupEntries, ipLogQuery, statusClassSummary } from "./traffic";

test("groups traffic deterministically and aggregates status classes", () => {
  const groups = groupEntries([
    { value: "198.51.100.2", requests: 2, statusCode: 503, lastSeenAtMs: 10 },
    { value: "198.51.100.1", requests: 4, statusCode: 200, lastSeenAtMs: 20 },
    { value: "198.51.100.2", requests: 3, statusCode: 502, lastSeenAtMs: 30 }
  ]);

  expect(
    groups.map(({ value, requests, lastSeenAtMs }) => ({ value, requests, lastSeenAtMs }))
  ).toEqual([
    { value: "198.51.100.2", requests: 5, lastSeenAtMs: 30 },
    { value: "198.51.100.1", requests: 4, lastSeenAtMs: 20 }
  ]);
  expect(statusClassSummary(groups[0]!.statuses)).toEqual([
    { label: "5xx", statusCode: 500, requests: 5 }
  ]);
});

test("quotes client IP log filters when the value contains query syntax", () => {
  expect(ipLogQuery("198.51.100.2")).toBe("@maestro.client_ip:198.51.100.2");
  expect(ipLogQuery('client "edge"')).toBe('@maestro.client_ip:"client \\"edge\\""');
});
