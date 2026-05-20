import { createResource, createSignal, For, Show } from "solid-js";
import { Eye, EyeOff } from "lucide-solid";
import clsx from "clsx";
import type { Service } from "../../lib/types";
import { freezeService, getIngressRoutes } from "../../lib/api";

function OverviewTab(props: { service: Service; onServiceUpdate: () => void }) {
  const s = props.service;

  const isIngress = s.id === "maestro-ingress";
  const [ingressRoutes] = createResource(
    () => (isIngress ? true : null),
    () => getIngressRoutes()
  );

  const sourceItems =
    s.build != null
      ? [
          { label: "Git repository", value: s.build.repo },
          ...(s.build.branch ? [{ label: "Branch", value: s.build.branch }] : []),
          { label: "Dockerfile", value: s.build.dockerfile },
          ...(s.build.registry ? [{ label: "Registry", value: s.build.registry }] : []),
          ...(s.build.watch ? [{ label: "Watch", value: "enabled" }] : [])
        ]
      : [{ label: "Image", value: s.image ?? "(not set)" }];

  const buildEnvItems = Object.entries(s.build?.env?.items ?? {}).map(([key, value]) => ({
    label: key,
    value
  }));
  const buildEnvSource = s.build?.env?.source ?? null;
  const buildSecretKeys = Object.keys(s.build?.secrets?.items ?? {}).sort();
  const buildSecretSource = s.build?.secrets?.source ?? null;

  const deployItems = [
    { label: "Replicas", value: String(s.deploy.replicas ?? 1) },
    ...(s.deploy.command
      ? [
          {
            label: "Deploy command",
            value: `${s.deploy.command.command} ${s.deploy.command.args.join(" ")}`.trim()
          }
        ]
      : []),
    { label: "Healthcheck path", value: s.deploy.healthcheckPath },
    { label: "Healthcheck interval", value: `${s.deploy.healthcheckInterval}s` }
  ];

  const envItems = Object.entries(s.deploy.env?.items ?? {}).map(([key, value]) => ({
    label: key,
    value
  }));
  const envSource = s.deploy.env?.source ?? null;

  const secretKeys = Object.keys(s.deploy.secrets?.keys ?? {}).sort();
  const secretSource = s.deploy.secrets?.source ?? null;

  const volumes = s.deploy.volumes ?? [];

  const ingressHosts = s.ingress
    ? [s.ingress.host, ...(s.ingress.hosts ?? [])].filter((host): host is string => !!host)
    : [];

  return (
    <div class="space-y-6">
      <ConfigSection title="Source" items={sourceItems} />
      <Show when={buildEnvItems.length > 0 || buildEnvSource}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">
            Build Environment
            <Show when={buildEnvSource}>
              <span class="ml-1.5 text-gray-300 normal-case font-mono">{buildEnvSource}</span>
            </Show>
          </h4>
          <Show when={buildEnvItems.length > 0}>
            <ConfigSection items={buildEnvItems} maskValues />
          </Show>
        </div>
      </Show>
      <Show when={buildSecretKeys.length > 0 || buildSecretSource}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">
            Build Secrets
            <Show when={buildSecretSource}>
              <span class="ml-1.5 text-gray-300 normal-case font-mono">{buildSecretSource}</span>
            </Show>
          </h4>
          <Show when={buildSecretKeys.length > 0}>
            <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
              <For each={buildSecretKeys}>
                {(key) => (
                  <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                    <span class="text-xs text-gray-500 shrink-0">{key}</span>
                    <span class="text-sm font-mono text-gray-400">••••••••</span>
                  </div>
                )}
              </For>
            </div>
          </Show>
        </div>
      </Show>
      <Show when={s.ingress}>
        {(ingress) => (
          <div>
            <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">Ingress</h4>
            <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
              <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                <span class="text-xs text-gray-500 shrink-0">
                  {ingressHosts.length > 1 ? "Hosts" : "Host"}
                </span>
                <Show
                  when={ingressHosts.length > 1}
                  fallback={
                    <span class="text-sm font-mono text-gray-800 text-right truncate">
                      {ingressHosts[0] ?? "(not set)"}
                    </span>
                  }
                >
                  <div class="flex flex-col items-end gap-1 min-w-0">
                    <For each={ingressHosts}>
                      {(host) => (
                        <span class="text-sm font-mono text-gray-800 text-right truncate max-w-full">
                          {host}
                        </span>
                      )}
                    </For>
                  </div>
                </Show>
              </div>
              <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                <span class="text-xs text-gray-500 shrink-0">Port</span>
                <span class="text-sm font-mono text-gray-800 text-right truncate">
                  {String(ingress().port ?? 80)}
                </span>
              </div>
            </div>
          </div>
        )}
      </Show>
      <Show when={isIngress && ingressRoutes()?.length}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">Routes</h4>
          <div class="space-y-3">
            <For each={ingressRoutes()}>
              {(route) => (
                <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
                  <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                    <span class="text-xs text-gray-500 shrink-0">Service</span>
                    <span class="text-sm font-mono text-gray-800 text-right truncate">
                      {route.serviceId}
                    </span>
                  </div>
                  <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                    <span class="text-xs text-gray-500 shrink-0">Rule</span>
                    <span class="text-sm font-mono text-gray-800 text-right truncate">
                      {route.rule}
                    </span>
                  </div>
                  <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                    <span class="text-xs text-gray-500 shrink-0">Entry points</span>
                    <span class="text-sm font-mono text-gray-800 text-right truncate">
                      {route.entryPoints.join(", ")}
                    </span>
                  </div>
                  <For each={route.servers}>
                    {(server, idx) => (
                      <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                        <span class="text-xs text-gray-500 shrink-0">
                          {route.servers.length > 1 ? `Server ${idx() + 1}` : "Server"}
                        </span>
                        <span class="text-sm font-mono text-gray-800 text-right truncate">
                          {server}
                        </span>
                      </div>
                    )}
                  </For>
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>
      <ConfigSection title="Deploy" items={deployItems} />
      <Show when={envItems.length > 0 || envSource}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">
            Deploy Environment
            <Show when={envSource}>
              <span class="ml-1.5 text-gray-300 normal-case font-mono">{envSource}</span>
            </Show>
          </h4>
          <Show when={envItems.length > 0}>
            <ConfigSection items={envItems} maskValues />
          </Show>
        </div>
      </Show>
      <Show when={secretKeys.length > 0 || secretSource}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">
            Deploy Secrets
            <span class="ml-1.5 text-gray-300 normal-case">
              (mounted at {s.deploy.secrets?.mountPath})
            </span>
          </h4>
          <Show when={secretSource}>
            <div class="text-xs font-mono text-gray-400 mb-2">{secretSource}</div>
          </Show>
          <Show when={secretKeys.length > 0}>
            <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
              <For each={secretKeys}>
                {(key) => (
                  <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                    <span class="text-xs text-gray-500 shrink-0">{key}</span>
                    <span class="text-sm font-mono text-gray-400">••••••••</span>
                  </div>
                )}
              </For>
            </div>
          </Show>
        </div>
      </Show>
      <Show when={volumes.length > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">Volumes</h4>
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <For each={volumes}>
              {(volume) => (
                <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                  <span class="text-xs font-mono text-gray-500 shrink-0 truncate">
                    {volume.hostPath}
                  </span>
                  <span class="text-sm font-mono text-gray-800 text-right truncate">
                    {volume.mountPath}
                    <Show when={volume.readOnly}>
                      <span class="ml-1.5 text-xs text-gray-400">(ro)</span>
                    </Show>
                    <Show when={volume.owner}>
                      {(owner) => (
                        <span class="ml-1.5 text-xs text-gray-400">
                          {owner().uid}:{owner().gid ?? owner().uid}
                        </span>
                      )}
                    </Show>
                  </span>
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>
      <Show when={!s.system}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">
            Deploy freeze
          </h4>
          <div class="bg-white rounded-lg border border-gray-200 px-4 py-3 flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-700">
                {s.deployFrozen ? "Deploys are frozen" : "Deploys are active"}
              </p>
              <p class="text-xs text-gray-400 mt-0.5">
                {s.deployFrozen
                  ? "Auto-deploys from git watch are paused. Manual deploys require force."
                  : "Services will auto-deploy when new commits are detected."}
              </p>
            </div>
            <button
              type="button"
              onClick={async () => {
                await freezeService(s.id, !s.deployFrozen);
                props.onServiceUpdate();
              }}
              class={clsx(
                "px-3 py-1.5 text-xs font-medium rounded-lg transition-colors outline-none",
                {
                  "bg-amber-100 text-amber-700 hover:bg-amber-200": s.deployFrozen,
                  "bg-gray-100 text-gray-600 hover:bg-gray-200": !s.deployFrozen
                }
              )}
            >
              {s.deployFrozen ? "Unfreeze" : "Freeze"}
            </button>
          </div>
        </div>
      </Show>
    </div>
  );
}

function ConfigSection(props: {
  title?: string;
  items: { label: string; value: string }[];
  maskValues?: boolean;
}) {
  const [revealed, setRevealed] = createSignal(false);
  const masked = () => props.maskValues && !revealed();

  return (
    <div>
      <Show when={props.title}>
        <div class="flex items-center justify-between mb-2">
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider">{props.title}</h4>
          <Show when={props.maskValues}>
            <button
              type="button"
              onClick={() => setRevealed(!revealed())}
              class="text-gray-400 hover:text-gray-600 transition-colors"
            >
              <Show when={revealed()} fallback={<Eye class="size-3.5" />}>
                <EyeOff class="size-3.5" />
              </Show>
            </button>
          </Show>
        </div>
      </Show>
      <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
        <For each={props.items}>
          {(item) => (
            <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
              <span class="text-xs text-gray-500 shrink-0">{item.label}</span>
              <span class="text-sm font-mono text-gray-800 text-right truncate">
                {masked() ? "••••••••" : item.value}
              </span>
            </div>
          )}
        </For>
      </div>
    </div>
  );
}

export { OverviewTab };
