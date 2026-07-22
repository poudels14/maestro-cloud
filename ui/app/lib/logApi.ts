import type { ApiSchemas, LogHistogramQuery, LogReadQuery } from "@maestro/api-client";
import { apiClient, apiRequestError } from "./client";
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

async function getLogPage(request: LogPageRequest): Promise<LogPage> {
  const query = readQuery(request);
  try {
    const page = await readScope(request.scope, query);
    return {
      cursor: page.cursor,
      entries: sortLogEntries(page.entries.map(mapClusterLogEntry))
    };
  } catch (error) {
    throw apiRequestError(error, "Failed to load logs");
  }
}

async function getLogHistogram(request: LogHistogramRequest): Promise<LogHistogram> {
  const query: LogHistogramQuery = {
    from: request.from,
    to: request.to,
    bucketMs: request.bucketMs,
    ...(request.groupBy ? { groupBy: request.groupBy } : {}),
    ...(request.nodeId ? { nodeId: request.nodeId } : {}),
    ...(request.query ? { query: request.query } : {})
  };
  try {
    const buckets = await histogramScope(request.scope, query);
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
    throw apiRequestError(error, "Failed to load log counts");
  }
}

function readQuery(request: LogPageRequest): LogReadQuery {
  return {
    ...(request.tail != null ? { tail: request.tail } : {}),
    ...(request.cursor && Object.keys(request.cursor).length > 0
      ? { cursor: JSON.stringify(request.cursor) }
      : {}),
    ...(request.from != null ? { from: request.from } : {}),
    ...(request.to != null ? { to: request.to } : {}),
    ...(request.query ? { query: request.query } : {}),
    ...(request.nodeId ? { nodeId: request.nodeId } : {})
  };
}

function readScope(
  scope: LogScope,
  query: LogReadQuery
): Promise<ApiSchemas["ClusterLogPage"]> {
  switch (scope.type) {
    case "all":
      return apiClient().listLogs(query);
    case "system":
      return apiClient().listSystemLogs({ ...query, ...(scope.component ? { component: scope.component } : {}) });
    case "service":
      return apiClient().listServiceLogs(scope.serviceId, query);
    case "deployment":
      return apiClient().listDeploymentLogs(scope.serviceId, scope.deploymentId, query);
    case "build":
      return apiClient().listBuildLogs(scope.serviceId, scope.buildId, query);
  }
}

function histogramScope(
  scope: LogScope,
  query: LogHistogramQuery
): Promise<ApiSchemas["LogHistogramBucket"][]> {
  switch (scope.type) {
    case "all":
      return apiClient().getLogHistogram(query);
    case "system":
      return apiClient().getSystemLogHistogram({
        ...query,
        ...(scope.component ? { component: scope.component } : {})
      });
    case "service":
      return apiClient().getServiceLogHistogram(scope.serviceId, query);
    case "deployment":
      return apiClient().getDeploymentLogHistogram(scope.serviceId, scope.deploymentId, query);
    case "build":
      return apiClient().getBuildLogHistogram(scope.serviceId, scope.buildId, query);
  }
}

export { getLogHistogram, getLogPage };
export type { LogEntry } from "./logView";
