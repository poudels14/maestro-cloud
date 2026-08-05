import type { ApiSchemas } from "@maestro/api-client";

type Service = ApiSchemas["Service"] & {
  previewResource?: ApiSchemas["Preview"];
};
type Deployment = ApiSchemas["Deployment"];
type ReplicaState = ApiSchemas["ReplicaState"];
type DnsRecord = ApiSchemas["DnsRecord"];
type ServiceDetailSearchUpdate = {
  query?: string | undefined;
  range?: string | undefined;
  deployment?: string | undefined;
  tab?: "logs" | "build" | "details" | undefined;
};

export type { Deployment, DnsRecord, ReplicaState, Service, ServiceDetailSearchUpdate };
