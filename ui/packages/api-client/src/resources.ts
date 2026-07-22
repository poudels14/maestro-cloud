import type { components } from "./schema";

export type ApiSchemas = components["schemas"];

export interface BuiltinResources {
  Node: ApiSchemas["Node"];
  NodeNetwork: ApiSchemas["NodeNetwork"];
  NodeFirewall: ApiSchemas["NodeFirewall"];
  Service: ApiSchemas["Service"];
  Deployment: ApiSchemas["Deployment"];
  Assignment: ApiSchemas["Assignment"];
  ReplicaState: ApiSchemas["ReplicaState"];
  IngressRoute: ApiSchemas["IngressRoute"];
  IngressBlocklist: ApiSchemas["IngressBlocklist"];
  TrafficGeneration: ApiSchemas["TrafficGeneration"];
  FirewallPolicy: ApiSchemas["FirewallPolicy"];
  DnsRecord: ApiSchemas["DnsRecord"];
  Build: ApiSchemas["Build"];
  Preview: ApiSchemas["Preview"];
  UpgradeRun: ApiSchemas["UpgradeRun"];
  Webhook: ApiSchemas["Webhook"];
}

export type BuiltinResourceKind = keyof BuiltinResources;
export type BuiltinResource<Kind extends BuiltinResourceKind> = BuiltinResources[Kind];
