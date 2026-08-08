import type { ApiSchemas } from "@maestro/api-client";

type Service = ApiSchemas["Service"] & {
  previewResource?: ApiSchemas["Preview"];
};
type Deployment = ApiSchemas["Deployment"];
type Assignment = ApiSchemas["Assignment"];
type ReplicaState = ApiSchemas["ReplicaState"];
type DnsRecord = ApiSchemas["DnsRecord"];
type ServiceDetailSearchUpdate = {
  query?: string | undefined;
  range?: string | undefined;
  deployment?: string | undefined;
  tab?: "logs" | "build" | "details" | undefined;
};

export type { Assignment, Deployment, DnsRecord, ReplicaState, Service, ServiceDetailSearchUpdate };
