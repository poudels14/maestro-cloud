import { expect, test } from "vitest";
import type { TrafficPoint } from "./api";
import { buildServiceTrafficSeries } from "./traffic";

function point(overrides: Partial<TrafficPoint>): TrafficPoint {
  return {
    ts: 100,
    serviceId: "api",
    deploymentId: null,
    method: "GET",
    statusCode: 200,
    requests: 10,
    bytesIn: 500,
    bytesOut: 1_000,
    latLe1s: 5,
    latLe5s: 9,
    latLe10s: 10,
    latTotal: 10,
    ...overrides
  };
}

test("builds sorted request, bandwidth, and latency series", () => {
  const series = buildServiceTrafficSeries([
    point({ ts: 200, requests: 5 }),
    point({}),
    point({ statusCode: 503, requests: 2, bytesIn: 100, bytesOut: 200 })
  ]);

  expect(series.totalRequestRate).toEqual([
    { ts: 100, value: 2.4 },
    { ts: 200, value: 1 }
  ]);
  expect(series.errorRequestRate).toEqual([
    { ts: 100, value: 0.4 },
    { ts: 200, value: 0 }
  ]);
  expect(series.bytesInRate).toEqual([
    { ts: 100, value: 120 },
    { ts: 200, value: 100 }
  ]);
  expect(series.bytesOutRate).toEqual([
    { ts: 100, value: 240 },
    { ts: 200, value: 200 }
  ]);
  expect(series.p50LatencyMs).toEqual([
    { ts: 100, value: 1000 },
    { ts: 200, value: 1000 }
  ]);
  expect(series.p95LatencyMs).toEqual([
    { ts: 100, value: 7500 },
    { ts: 200, value: 7500 }
  ]);
});

test("returns zero latency for empty histograms", () => {
  const series = buildServiceTrafficSeries([
    point({ latLe1s: 0, latLe5s: 0, latLe10s: 0, latTotal: 0 })
  ]);

  expect(series.p50LatencyMs).toEqual([{ ts: 100, value: 0 }]);
  expect(series.p95LatencyMs).toEqual([{ ts: 100, value: 0 }]);
});
