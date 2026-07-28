import type { ApiSchemas } from "./resources";
import type { operations } from "./schema";
import type { ApiRequestOptions } from "./transport";

export type IngressTrafficQuery = NonNullable<
  operations["getIngressTraffic"]["parameters"]["query"]
>;
export type PlacementHistoryQuery = NonNullable<
  operations["listPlacementHistory"]["parameters"]["query"]
>;
export type ClusterMetricQuery = NonNullable<
  operations["listClusterMetrics"]["parameters"]["query"]
>;
export type NodeMetricQuery = NonNullable<operations["listNodeMetrics"]["parameters"]["query"]>;
export type StatsMetricQuery = NonNullable<
  operations["listOperationalStatsMetrics"]["parameters"]["query"]
>;
export type ServiceMetricQuery = NonNullable<
  operations["listServiceMetrics"]["parameters"]["query"]
>;
export type ServiceTrafficQuery = NonNullable<
  operations["getServiceTraffic"]["parameters"]["query"]
>;
export type ServiceTrafficBreakdownQuery = NonNullable<
  operations["getServiceTrafficBreakdown"]["parameters"]["query"]
>;
export type ContainerMetricQuery = NonNullable<
  operations["listContainerMetrics"]["parameters"]["query"]
>;
export type LogReadQuery = NonNullable<operations["listLogs"]["parameters"]["query"]>;
export type SystemLogReadQuery = NonNullable<operations["listSystemLogs"]["parameters"]["query"]>;
export type LogHistogramQuery = NonNullable<operations["getLogHistogram"]["parameters"]["query"]>;
export type SystemLogHistogramQuery = NonNullable<
  operations["getSystemLogHistogram"]["parameters"]["query"]
>;

export interface MaestroApiClient {
  createBrowserSession(operatorToken: string, options?: ApiRequestOptions): Promise<void>;
  deleteBrowserSession(options?: ApiRequestOptions): Promise<void>;
  getClusterConfig(options?: ApiRequestOptions): Promise<ApiSchemas["MaskedClusterConfig"]>;
  getClusterInfo(options?: ApiRequestOptions): Promise<ApiSchemas["ClusterInfo"]>;
  getClusterStats(options?: ApiRequestOptions): Promise<ApiSchemas["ClusterStatsResponse"]>;
  listClusterNodeStats(options?: ApiRequestOptions): Promise<ApiSchemas["NodeStatsMap"]>;
  listPlacementHistory(
    query?: PlacementHistoryQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["PlacementHistory"][]>;
  getTailscaleAuthKeyStatus(
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["TailscaleAuthKeyStatus"]>;
  rotateTailscaleAuthKey(
    request: ApiSchemas["TailscaleAuthKeyRotationRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["TailscaleAuthKeyRotationResponse"]>;
  listLocalDisks(options?: ApiRequestOptions): Promise<ApiSchemas["DiskInfo"][]>;
  listNodeDisks(options?: ApiRequestOptions): Promise<ApiSchemas["NodeDiskMap"]>;
  listNodeMetrics(
    query?: NodeMetricQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ResourceMetricPoint"][]>;
  listClusterMetrics(
    query?: ClusterMetricQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ResourceMetricPoint"][]>;
  listOperationalStatsMetrics(
    query?: StatsMetricQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["StatsMetricPoint"][]>;
  listNodes(options?: ApiRequestOptions): Promise<ApiSchemas["Node"][]>;
  listUnschedulableReplicas(
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["UnschedulableReplica"][]>;
  getNode(nodeId: string, options?: ApiRequestOptions): Promise<ApiSchemas["Node"]>;
  drainNode(
    nodeId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["NodeCommandResponse"]>;
  restoreNode(
    nodeId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["NodeCommandResponse"]>;
  removeNode(
    nodeId: string,
    request: ApiSchemas["NodeRemovalRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["NodeRemovalResponse"]>;
  listUpgrades(options?: ApiRequestOptions): Promise<ApiSchemas["UpgradeRun"][]>;
  getUpgrade(upgradeRunId: string, options?: ApiRequestOptions): Promise<ApiSchemas["UpgradeRun"]>;
  startUpgrade(
    request: ApiSchemas["UpgradeCreateRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["UpgradeCommandResponse"]>;
  cancelUpgrade(
    upgradeRunId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["UpgradeCommandResponse"]>;
  listNodeNetworks(options?: ApiRequestOptions): Promise<ApiSchemas["NodeNetwork"][]>;
  getNodeNetwork(
    networkId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["NodeNetwork"]>;
  listNodeFirewalls(options?: ApiRequestOptions): Promise<ApiSchemas["NodeFirewall"][]>;
  getNodeFirewall(
    firewallId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["NodeFirewall"]>;
  listDnsRecords(options?: ApiRequestOptions): Promise<ApiSchemas["DnsRecord"][]>;
  getDnsRecord(recordId: string, options?: ApiRequestOptions): Promise<ApiSchemas["DnsRecord"]>;
  listFirewallPolicies(options?: ApiRequestOptions): Promise<ApiSchemas["FirewallPolicy"][]>;
  getFirewallPolicy(
    policyId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["FirewallPolicy"]>;
  putFirewallPolicy(
    policyId: string,
    request: ApiSchemas["FirewallPolicyWriteRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["FirewallPolicyCommandResponse"]>;
  deleteFirewallPolicy(
    policyId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["FirewallPolicyCommandResponse"]>;
  dryRunFirewallPolicy(
    policyId: string,
    request: ApiSchemas["FirewallDryRunRequest"],
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["FirewallDryRunResponse"]>;
  listPreviews(options?: ApiRequestOptions): Promise<ApiSchemas["Preview"][]>;
  getPreview(previewId: string, options?: ApiRequestOptions): Promise<ApiSchemas["Preview"]>;
  listWebhooks(options?: ApiRequestOptions): Promise<ApiSchemas["Webhook"][]>;
  getWebhook(webhookId: string, options?: ApiRequestOptions): Promise<ApiSchemas["Webhook"]>;
  putWebhook(
    webhookId: string,
    request: ApiSchemas["WebhookWriteRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["WebhookCommandResponse"]>;
  deleteWebhook(
    webhookId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["WebhookCommandResponse"]>;
  testWebhook(
    webhookId: string,
    request: ApiSchemas["WebhookTestRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["WebhookTestResponse"]>;
  listServices(options?: ApiRequestOptions): Promise<ApiSchemas["Service"][]>;
  getService(serviceId: string, options?: ApiRequestOptions): Promise<ApiSchemas["Service"]>;
  diffService(
    serviceId: string,
    request: ApiSchemas["ServiceDiffRequest"],
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceDiffResponse"]>;
  listDeployments(
    serviceId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["Deployment"][]>;
  getDeployment(
    serviceId: string,
    deploymentId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["Deployment"]>;
  listAssignments(
    serviceId: string,
    deploymentId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["Assignment"][]>;
  getAssignment(
    serviceId: string,
    deploymentId: string,
    assignmentId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["Assignment"]>;
  listReplicas(
    serviceId: string,
    deploymentId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ReplicaState"][]>;
  getReplica(
    serviceId: string,
    deploymentId: string,
    replicaId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ReplicaState"]>;
  listBuilds(serviceId: string, options?: ApiRequestOptions): Promise<ApiSchemas["Build"][]>;
  getBuild(
    serviceId: string,
    buildId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["Build"]>;
  listLogs(
    query?: LogReadQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ClusterLogPage"]>;
  listSystemLogs(
    query?: SystemLogReadQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ClusterLogPage"]>;
  listServiceLogs(
    serviceId: string,
    query?: LogReadQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ClusterLogPage"]>;
  listDeploymentLogs(
    serviceId: string,
    deploymentId: string,
    query?: LogReadQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ClusterLogPage"]>;
  listBuildLogs(
    serviceId: string,
    buildId: string,
    query?: LogReadQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ClusterLogPage"]>;
  getLogHistogram(
    query?: LogHistogramQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["LogHistogramBucket"][]>;
  getSystemLogHistogram(
    query?: SystemLogHistogramQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["LogHistogramBucket"][]>;
  getServiceLogHistogram(
    serviceId: string,
    query?: LogHistogramQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["LogHistogramBucket"][]>;
  getDeploymentLogHistogram(
    serviceId: string,
    deploymentId: string,
    query?: LogHistogramQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["LogHistogramBucket"][]>;
  getBuildLogHistogram(
    serviceId: string,
    buildId: string,
    query?: LogHistogramQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["LogHistogramBucket"][]>;
  listIngressRoutes(
    serviceId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["IngressRoute"][]>;
  getIngressRoute(
    serviceId: string,
    routeId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["IngressRoute"]>;
  listServiceMetrics(
    serviceId: string,
    query?: ServiceMetricQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ResourceMetricPoint"][]>;
  getServiceTraffic(
    serviceId: string,
    query?: ServiceTrafficQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["TrafficMetricPoint"][]>;
  getServiceTrafficBreakdown(
    serviceId: string,
    query?: ServiceTrafficBreakdownQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["IngressTrafficBreakdown"]>;
  listContainerMetrics(
    serviceId: string,
    query?: ContainerMetricQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ResourceMetricPoint"][]>;
  listActiveIngressRoutes(options?: ApiRequestOptions): Promise<ApiSchemas["IngressRouting"][]>;
  getIngressBlocklist(options?: ApiRequestOptions): Promise<ApiSchemas["BlockedIpsResponse"]>;
  setBlockedIngressIp(
    request: ApiSchemas["BlockedIpRequest"],
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["BlockedIpsResponse"]>;
  getIngressTraffic(
    query?: IngressTrafficQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["IngressTrafficBreakdown"]>;
  getBlockedIngressTraffic(
    query?: IngressTrafficQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["IngressTrafficBreakdown"]>;
  listTrafficGenerations(
    serviceId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["TrafficGeneration"][]>;
  getTrafficGeneration(
    serviceId: string,
    generationId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["TrafficGeneration"]>;
  redeployService(
    serviceId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceCommandResponse"]>;
  applyServiceRollout(
    serviceId: string,
    request: ApiSchemas["ServiceRolloutRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceRolloutResponse"]>;
  diffServiceRollout(
    serviceId: string,
    request: ApiSchemas["ServiceRolloutDiffRequest"],
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceRolloutDiffResponse"]>;
  freezeService(
    serviceId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceCommandResponse"]>;
  unfreezeService(
    serviceId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceCommandResponse"]>;
  setServiceReplicas(
    serviceId: string,
    request: ApiSchemas["ServiceReplicaOverrideRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceCommandResponse"]>;
  deleteService(
    serviceId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceCommandResponse"]>;
  restartDeployment(
    serviceId: string,
    deploymentId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["DeploymentCommandResponse"]>;
  cancelDeployment(
    serviceId: string,
    deploymentId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["DeploymentCommandResponse"]>;
  removeDeployment(
    serviceId: string,
    deploymentId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["DeploymentCommandResponse"]>;
  putService(
    serviceId: string,
    request: ApiSchemas["ServiceWriteRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceWriteResponse"]>;
}
