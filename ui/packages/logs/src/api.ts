import type {
  ApiSchemas,
  LogHistogramQuery,
  LogReadQuery,
  MaestroApiClient
} from "@maestro/api-client";
import { mapClusterLogEntry, sortLogEntries, type LogEntry } from "./logView";

export type LogScope =
  | { type: "all" }
  | { type: "system"; component?: string }
  | { type: "service"; serviceId: string }
  | { type: "deployment"; serviceId: string; deploymentId: string }
  | { type: "build"; serviceId: string; buildId: string };

export interface LogPage {
  entries: LogEntry[];
  cursor: ApiSchemas["ClusterLogCursor"];
  previousCursor: ApiSchemas["ClusterLogCursor"] | null;
  hasPrevious: boolean;
}

export interface LogHistogramBucket {
  ts: number;
  count: number;
  levels?: Record<string, number>;
}

export interface LogHistogram {
  from: number;
  to: number;
  bucketMs: number;
  buckets: LogHistogramBucket[];
}

export interface LogPageRequest {
  scope: LogScope;
  tail?: number;
  cursor?: ApiSchemas["ClusterLogCursor"];
  beforeCursor?: ApiSchemas["ClusterLogCursor"];
  nodeId?: string;
  query?: string;
  from?: number;
  to?: number;
}

export interface LogHistogramRequest {
  scope: LogScope;
  from: number;
  to: number;
  bucketMs: number;
  groupBy?: "level" | "status";
  nodeId?: string;
  query?: string;
}

interface LogsApi {
  getLogPage: (request: LogPageRequest) => Promise<LogPage>;
  getLogHistogram: (request: LogHistogramRequest) => Promise<LogHistogram>;
}

type LogsErrorMapper = (error: unknown, fallback: string) => Error;

async function getLogPage(
  client: MaestroApiClient,
  mapError: LogsErrorMapper,
  request: LogPageRequest
): Promise<LogPage> {
  const query = readQuery(request);
  try {
    const page = await readScope(client, request.scope, query);
    return {
      cursor: page.cursor,
      previousCursor: page.previousCursor,
      hasPrevious: page.hasPrevious,
      entries: sortLogEntries(page.entries.map(mapClusterLogEntry))
    };
  } catch (error) {
    throw mapError(error, "Failed to load logs");
  }
}

async function getLogHistogram(
  client: MaestroApiClient,
  mapError: LogsErrorMapper,
  request: LogHistogramRequest
): Promise<LogHistogram> {
  const query: LogHistogramQuery = {
    from: request.from,
    to: request.to,
    bucketMs: request.bucketMs,
    ...(request.groupBy ? { groupBy: request.groupBy } : {}),
    ...(request.nodeId ? { nodeId: request.nodeId } : {}),
    ...(request.query ? { query: request.query } : {})
  };
  try {
    const buckets = await histogramScope(client, request.scope, query);
    return {
      from: request.from,
      to: request.to,
      bucketMs: request.bucketMs,
      buckets: buckets.map((bucket) => ({
        ts: bucket.bucketAt,
        count: bucket.count,
        ...(Object.keys(bucket.groups).length > 0 ? { levels: bucket.groups } : {})
      }))
    };
  } catch (error) {
    throw mapError(error, "Failed to load log counts");
  }
}

function readQuery(request: LogPageRequest): LogReadQuery {
  return {
    ...(request.tail != null ? { tail: request.tail } : {}),
    ...(request.cursor && Object.keys(request.cursor).length > 0
      ? { cursor: JSON.stringify(request.cursor) }
      : {}),
    ...(request.beforeCursor && Object.keys(request.beforeCursor).length > 0
      ? { beforeCursor: JSON.stringify(request.beforeCursor) }
      : {}),
    ...(request.from != null ? { from: request.from } : {}),
    ...(request.to != null ? { to: request.to } : {}),
    ...(request.query ? { query: request.query } : {}),
    ...(request.nodeId ? { nodeId: request.nodeId } : {})
  };
}

function readScope(
  client: MaestroApiClient,
  scope: LogScope,
  query: LogReadQuery
): Promise<ApiSchemas["ClusterLogPage"]> {
  switch (scope.type) {
    case "all":
      return client.listLogs(query);
    case "system":
      return client.listSystemLogs({
        ...query,
        ...(scope.component ? { component: scope.component } : {})
      });
    case "service":
      return client.listServiceLogs(scope.serviceId, query);
    case "deployment":
      return client.listDeploymentLogs(scope.serviceId, scope.deploymentId, query);
    case "build":
      return client.listBuildLogs(scope.serviceId, scope.buildId, query);
  }
}

function histogramScope(
  client: MaestroApiClient,
  scope: LogScope,
  query: LogHistogramQuery
): Promise<ApiSchemas["LogHistogramBucket"][]> {
  switch (scope.type) {
    case "all":
      return client.getLogHistogram(query);
    case "system":
      return client.getSystemLogHistogram({
        ...query,
        ...(scope.component ? { component: scope.component } : {})
      });
    case "service":
      return client.getServiceLogHistogram(scope.serviceId, query);
    case "deployment":
      return client.getDeploymentLogHistogram(scope.serviceId, scope.deploymentId, query);
    case "build":
      return client.getBuildLogHistogram(scope.serviceId, scope.buildId, query);
  }
}

function createLogsApi(client: () => MaestroApiClient, mapError: LogsErrorMapper): LogsApi {
  return {
    getLogPage: (request) => getLogPage(client(), mapError, request),
    getLogHistogram: (request) => getLogHistogram(client(), mapError, request)
  };
}

export { createLogsApi };
export type { LogEntry } from "./logView";
export type { LogsApi, LogsErrorMapper };
