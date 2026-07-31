import type { ApiSchemas } from "@maestro/api-client";

type Service = ApiSchemas["Service"] & {
  previewResource?: ApiSchemas["Preview"];
};
type Deployment = ApiSchemas["Deployment"];
type ReplicaState = ApiSchemas["ReplicaState"];
type DnsRecord = ApiSchemas["DnsRecord"];

export type { Deployment, DnsRecord, ReplicaState, Service };
