import { For, Show, type JSX } from "solid-js";
import { useQuery } from "@maestro/sdk";
import type { ServicesApi } from "./api";
import type { Service } from "./types";
import { isSystemService } from "./serviceView";
import { deploymentsQuery } from "./queries";
import { SecretConfig } from "./SecretConfig";
import { ConfigSection } from "./overview/ConfigSection";
import { ReplicasEditor } from "./overview/ReplicasEditor";
import { VolumesList } from "./overview/VolumesList";
import { FreezeToggle } from "./overview/FreezeToggle";

function OverviewTab(props: { api: ServicesApi; service: Service; ingress: JSX.Element }) {
  const deployments = useQuery(() => deploymentsQuery(props.api, props.service.meta.id));
  const artifact = () => props.service.spec.artifact;
  const sourceItems = () => {
    const value = artifact();
    if (value.type === "image") return [{ label: "Image", value: value.reference }];
    const source = value.source;
    return [
      ...(source.type === "git"
        ? [
            { label: "Git repository", value: source.repository },
            { label: "Git revision", value: source.revision }
          ]
        : [{ label: "Upload archive", value: source.archiveId }]),
      { label: "Dockerfile", value: value.dockerfile },
      ...(value.watch ? [{ label: "Watch", value: "enabled" }] : [])
    ];
  };

  const buildEnvItems = () => {
    const value = artifact();
    return value.type === "build"
      ? Object.entries(value.environment ?? {}).map(([label, entryValue]) => ({
          label,
          value: entryValue
        }))
      : [];
  };
  const buildSecretKeys = () => {
    const value = artifact();
    return value.type === "build" ? Object.keys(value.secrets ?? {}).sort() : [];
  };

  const deployItems = () => {
    const spec = props.service.spec;
    const items: { label: string; value: string }[] = [];
    if (spec.command && !isSystemService(props.service)) {
      items.push({
        label: "Command",
        value: `${spec.command.executable} ${(spec.command.arguments ?? []).join(" ")}`.trim()
      });
    }
    if ((spec.exposedPorts?.length ?? 0) > 0) {
      items.push({ label: "Exposed ports", value: spec.exposedPorts!.join(", ") });
    }
    if (spec.healthCheck) {
      const probe = spec.healthCheck.probe;
      items.push({
        label: "Health check",
        value: probe.protocol === "http" ? `HTTP ${probe.port}${probe.path}` : `TCP ${probe.port}`
      });
      items.push({
        label: "Health interval",
        value: `${spec.healthCheck.intervalSecs}s · ${spec.healthCheck.unhealthyThreshold} failures`
      });
    }
    items.push({
      label: "Maximum restart attempts",
      value: isSystemService(props.service) ? "Unlimited" : String(spec.maxRestartAttempts)
    });
    items.push({ label: "Interactive exec", value: spec.exec });
    items.push({ label: "Node API", value: spec.nodeApi });
    if (spec.placement.nodeId) {
      items.push({ label: "Pinned node", value: spec.placement.nodeId });
    }
    for (const [label, value] of Object.entries(spec.placement.labels ?? {})) {
      items.push({ label: `Placement: ${label}`, value });
    }
    return items;
  };

  const envItems = () =>
    Object.entries(props.service.spec.environment ?? {}).map(([label, value]) => ({
      label,
      value
    }));
  const currentDeployment = () =>
    deployments.data?.find(
      (deployment) => deployment.spec.serviceGeneration === props.service.meta.generation
    ) ??
    deployments.data?.find(
      (deployment) => deployment.meta.id === props.service.status.activeDeploymentId
    );
  const isPreview = () => props.service.previewResource != null;

  return (
    <div class="space-y-6">
      <ConfigSection title="Source" items={sourceItems()} />

      <Show when={deployItems().length > 0}>
        <ConfigSection title="Runtime" items={deployItems()} />
      </Show>

      <Show when={buildEnvItems().length > 0}>
        <ConfigSection title="Build environment variables" items={buildEnvItems()} />
      </Show>

      <Show when={buildSecretKeys().length > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">Build secrets</h4>
          <SecretsList keys={buildSecretKeys()} />
        </div>
      </Show>

      {props.ingress}

      <Show when={!isPreview() && !isSystemService(props.service)}>
        <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
          <ReplicasEditor api={props.api} service={props.service} />
          <FreezeToggle api={props.api} service={props.service} />
        </div>
      </Show>

      <Show when={envItems().length > 0}>
        <ConfigSection title="Environment variables" items={envItems()} />
      </Show>

      <SecretConfig
        secrets={props.service.spec.secrets}
        resolvedSecrets={currentDeployment()?.status.resolvedSecrets}
      />

      <VolumesList service={props.service} />
    </div>
  );
}

function SecretsList(props: { keys: string[] }) {
  return (
    <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
      <For each={props.keys}>
        {(key) => (
          <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
            <span class="text-xs font-medium text-gray-700 shrink-0">{key}</span>
            <span class="text-xs text-gray-400">••••••••</span>
          </div>
        )}
      </For>
    </div>
  );
}

export { OverviewTab };
