import type { TrafficPoint } from "./api";

interface SeriesPoint {
  ts: number;
  value: number;
}

interface ServiceTrafficSeries {
  totalRequestRate: SeriesPoint[];
  errorRequestRate: SeriesPoint[];
  p50LatencyMs: SeriesPoint[];
  p95LatencyMs: SeriesPoint[];
  bytesInRate: SeriesPoint[];
  bytesOutRate: SeriesPoint[];
}

function buildServiceTrafficSeries(
  points: TrafficPoint[],
  scrapeIntervalSeconds = 5
): ServiceTrafficSeries {
  const buckets = groupByTimestamp(points);
  const series = (value: (bucket: TrafficPoint[]) => number) =>
    buckets.map(([ts, bucket]) => ({ ts, value: value(bucket) }));

  return {
    totalRequestRate: series((bucket) => sumRequests(bucket) / scrapeIntervalSeconds),
    errorRequestRate: series(
      (bucket) =>
        sumRequestsWhere(bucket, (point) => point.statusCode >= 400) / scrapeIntervalSeconds
    ),
    p50LatencyMs: series((bucket) => bucketPercentileSec(bucket, 0.5) * 1000),
    p95LatencyMs: series((bucket) => bucketPercentileSec(bucket, 0.95) * 1000),
    bytesInRate: series((bucket) => sumBytes(bucket, "in") / scrapeIntervalSeconds),
    bytesOutRate: series((bucket) => sumBytes(bucket, "out") / scrapeIntervalSeconds)
  };
}

function groupByTimestamp(points: TrafficPoint[]): [number, TrafficPoint[]][] {
  const buckets = new Map<number, TrafficPoint[]>();
  for (const point of points) {
    const bucket = buckets.get(point.ts);
    if (bucket) bucket.push(point);
    else buckets.set(point.ts, [point]);
  }
  return Array.from(buckets.entries()).sort(([left], [right]) => left - right);
}

function sumRequests(points: TrafficPoint[]): number {
  return points.reduce((total, point) => total + point.requests, 0);
}

function sumRequestsWhere(
  points: TrafficPoint[],
  predicate: (point: TrafficPoint) => boolean
): number {
  return points.reduce((total, point) => total + (predicate(point) ? point.requests : 0), 0);
}

function sumBytes(points: TrafficPoint[], direction: "in" | "out"): number {
  return points.reduce(
    (total, point) => total + (direction === "in" ? point.bytesIn : point.bytesOut),
    0
  );
}

function bucketPercentileSec(points: TrafficPoint[], percentile: number): number {
  let le1 = 0;
  let le5 = 0;
  let le10 = 0;
  let total = 0;
  for (const point of points) {
    le1 += point.latLe1s;
    le5 += point.latLe5s;
    le10 += point.latLe10s;
    total += point.latTotal;
  }
  if (total === 0) return 0;
  const rank = percentile * total;
  if (le1 >= rank) return le1 === 0 ? 0 : rank / le1;
  if (le5 >= rank) {
    const span = le5 - le1;
    return span === 0 ? 1 : 1 + 4 * ((rank - le1) / span);
  }
  if (le10 >= rank) {
    const span = le10 - le5;
    return span === 0 ? 5 : 5 + 5 * ((rank - le5) / span);
  }
  return 10;
}

export { buildServiceTrafficSeries };
export type { SeriesPoint, ServiceTrafficSeries };
