import { createSignal, For, Match, Show, Switch } from "solid-js";
import clsx from "clsx";
import { ChevronRight } from "lucide-solid";
import type { Discovery, Service } from "../../../lib/types";
import { useQuery } from "../../../lib/useQuery";
import { serviceDiscoveryQuery } from "../../../lib/queries";

function DiscoveryList(props: { service: Service; services: Service[] }) {
  const callerIds = () =>
    [props.service.id, props.service.previewSource?.baseServiceId].filter(
      (id): id is string => !!id
    );
  const serviceIds = () =>
    props.services
      .filter((service) => service.id !== props.service.id)
      .filter((service) => service.network?.from.some((id) => callerIds().includes(id)))
      .map((service) => service.id);

  return (
    <Show when={serviceIds().length > 0}>
      <div>
        <h4 class="text-xs font-medium text-gray-400 mb-2">Service discovery</h4>
        <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
          <For each={serviceIds()}>
            {(serviceId) => <DiscoverableService serviceId={serviceId} />}
          </For>
        </div>
      </div>
    </Show>
  );
}

function DiscoverableService(props: { serviceId: string }) {
  const [open, setOpen] = createSignal(false);

  return (
    <div>
      <button
        type="button"
        onClick={() => setOpen(!open())}
        class="w-full px-4 py-2.5 flex items-center gap-2 text-left outline-none hover:bg-gray-50"
      >
        <ChevronRight
          class={clsx("size-3.5 shrink-0 text-gray-400 transition-transform", {
            "rotate-90": open()
          })}
        />
        <span class="text-xs font-mono text-gray-700">{props.serviceId}</span>
      </button>
      <Show when={open()}>
        <DiscoveredReplicas serviceId={props.serviceId} />
      </Show>
    </div>
  );
}

function DiscoveredReplicas(props: { serviceId: string }) {
  const discovery = useQuery(() => serviceDiscoveryQuery(props.serviceId));

  return (
    <div class="border-t border-gray-100 bg-gray-50/60 divide-y divide-gray-100">
      <Switch>
        <Match when={discovery.isError}>
          <div class="pl-9 pr-4 py-2.5 text-xs text-red-600">Failed to load replicas</div>
        </Match>
        <Match when={discovery.data}>
          {(data) => (
            <For
              each={data().replicas}
              fallback={
                <div class="pl-9 pr-4 py-2.5 text-xs text-gray-400">
                  No replicas with an address
                </div>
              }
            >
              {(replica) => <ReplicaRow replica={replica} />}
            </For>
          )}
        </Match>
      </Switch>
    </div>
  );
}

function ReplicaRow(props: { replica: Discovery.Replica }) {
  const address = () =>
    props.replica.port != null ? `${props.replica.ip}:${props.replica.port}` : props.replica.ip;
  const url = () =>
    props.replica.port != null && props.replica.port !== 80
      ? `http://${props.replica.host}:${props.replica.port}`
      : `http://${props.replica.host}`;

  return (
    <div class="pl-9 pr-4 py-2 flex items-start justify-between gap-6">
      <div class="min-w-0">
        <a
          href={url()}
          target="_blank"
          rel="noreferrer"
          class="block text-xs font-mono text-blue-600 hover:underline truncate"
        >
          {props.replica.host}
        </a>
        <div class="mt-0.5 flex flex-wrap gap-x-3 text-[11px] text-gray-400 font-mono">
          <span>{address()}</span>
          <Show when={props.replica.version}>{(version) => <span>v{version()}</span>}</Show>
          <span>{props.replica.deploymentId}</span>
        </div>
      </div>
      <span
        class={clsx("shrink-0 text-[11px] font-medium", {
          "text-emerald-600": props.replica.healthy,
          "text-amber-600": !props.replica.healthy
        })}
      >
        {props.replica.healthy ? "healthy" : "not healthy"}
      </span>
    </div>
  );
}

export { DiscoveryList };
