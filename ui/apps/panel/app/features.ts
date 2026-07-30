import { createClusterApi, createClusterFeature } from "@maestro/cluster";
import { createFirewallApi, createFirewallFeature } from "@maestro/firewall";
import { createIngressApi, createIngressFeature } from "@maestro/ingress";
import { createLogsApi, createLogsFeature } from "@maestro/logs";
import { createMetricsApi, createMetricsFeature } from "@maestro/metrics";
import { createServicesApi, createServicesFeature, nonPreviewServices } from "@maestro/services";
import { composeFeatureManifests } from "@maestro/sdk";
import { apiClient, apiRequestError } from "./lib/client";

const clusterApi = createClusterApi(apiClient, apiRequestError);
const firewallApi = createFirewallApi(apiClient, apiRequestError);
const ingressApi = createIngressApi(apiClient, apiRequestError);
const logsApi = createLogsApi(apiClient, apiRequestError);
const metricsApi = createMetricsApi(apiClient, apiRequestError);
const servicesApi = createServicesApi(apiClient, apiRequestError);

const clusterFeature = createClusterFeature(clusterApi);
const firewallFeature = createFirewallFeature(firewallApi);
const metricsFeature = createMetricsFeature(metricsApi);
const servicesFeature = createServicesFeature(servicesApi);
const ingressFeature = createIngressFeature(ingressApi, logsApi);
const logsFeature = createLogsFeature(logsApi, {
  listNodes: clusterApi.listNodes,
  listServices: async () =>
    nonPreviewServices(await servicesApi.listServices()).map((service) => ({
      id: service.meta.id,
      name: service.spec.name
    }))
});
const panelFeatureRegistry = composeFeatureManifests([
  clusterFeature,
  metricsFeature,
  servicesFeature,
  ingressFeature,
  logsFeature,
  firewallFeature
]);

type PanelFeaturePath = (typeof panelFeatureRegistry.routes)[number]["path"];

export { clusterApi, ingressApi, logsApi, metricsApi, panelFeatureRegistry, servicesApi };
export type { PanelFeaturePath };
