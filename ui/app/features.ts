import { createFirewallApi, createFirewallFeature } from "@maestro/firewall";
import { createLogsApi, createLogsFeature } from "@maestro/logs";
import { createMetricsApi, createMetricsFeature } from "@maestro/metrics";
import { composeFeatureManifests } from "@maestro/sdk";
import { apiClient, apiRequestError } from "./lib/client";

const firewallApi = createFirewallApi(apiClient, apiRequestError);
const logsApi = createLogsApi(apiClient, apiRequestError);
const metricsApi = createMetricsApi(apiClient, apiRequestError);

const firewallFeature = createFirewallFeature(firewallApi);
const logsFeature = createLogsFeature(logsApi);
const metricsFeature = createMetricsFeature(metricsApi);
const panelFeatureRegistry = composeFeatureManifests([
  metricsFeature,
  logsFeature,
  firewallFeature
]);

type PanelFeaturePath = (typeof panelFeatureRegistry.routes)[number]["path"];

export { logsApi, metricsApi, panelFeatureRegistry };
export type { PanelFeaturePath };
