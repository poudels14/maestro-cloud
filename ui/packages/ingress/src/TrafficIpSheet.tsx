import { For, Show, type JSX } from "solid-js";
import { Dialog } from "@kobalte/core/dialog";
import { LogViewer, type LogsApi } from "@maestro/logs";
import { timeAgo } from "@maestro/kit";
import { X } from "lucide-solid";
import clsx from "clsx";
import { ipLogQuery, statusCodeSummary, statusColor, type TrafficGroup } from "./traffic";

function TrafficIpSheet(props: {
  logsApi: LogsApi;
  group: TrafficGroup | null;
  action: (group: TrafficGroup) => JSX.Element;
  range: string;
  onRangeChange: (range: string) => void;
  onClose: () => void;
}) {
  return (
    <Dialog
      open={props.group !== null}
      onOpenChange={(open) => {
        if (!open) props.onClose();
      }}
    >
      <Dialog.Portal>
        <Dialog.Overlay class="fixed inset-0 bg-black/20 z-40 backdrop-blur-[1px]" />
        <Dialog.Content
          class="fixed top-0 right-0 bottom-0 w-full max-w-4xl bg-white border-l border-gray-200 shadow-2xl z-50 flex flex-col outline-none"
          onOpenAutoFocus={(event) => event.preventDefault()}
        >
          <Show when={props.group} keyed>
            {(group) => (
              <>
                <div class="px-4 sm:px-5 py-3 sm:py-4 border-b border-gray-200 shrink-0">
                  <div class="flex items-start justify-between gap-3">
                    <div class="min-w-0 flex-1">
                      <div
                        class="font-mono text-xl font-semibold text-gray-900 leading-snug tracking-tight truncate"
                        title={group.value}
                      >
                        {group.value}
                      </div>
                      <div class="flex items-center gap-2 flex-wrap text-xs mt-1.5 tabular-nums">
                        <span class="font-medium text-gray-600">
                          {group.requests.toLocaleString()}{" "}
                          {group.requests === 1 ? "request" : "requests"}
                        </span>
                        <span class="text-gray-300">·</span>
                        <span class="text-gray-400">last seen {timeAgo(group.lastSeenAtMs)}</span>
                      </div>
                    </div>
                    <div class="flex items-center gap-2">
                      {props.action(group)}
                      <Dialog.CloseButton class="p-1 text-gray-400 hover:text-gray-700 hover:bg-gray-100 rounded-md transition-colors outline-none">
                        <X class="size-4" />
                      </Dialog.CloseButton>
                    </div>
                  </div>
                </div>
                <div class="px-4 sm:px-5 py-3 border-b border-gray-100 shrink-0">
                  <div class="flex flex-wrap items-center gap-1">
                    <For each={statusCodeSummary(group.statuses)}>
                      {(status) => (
                        <span
                          class={clsx(
                            "inline-flex gap-1 rounded px-1.5 py-0.5 font-mono text-xs tabular-nums",
                            statusColor(status.statusCode)
                          )}
                        >
                          {status.statusCode}
                          <span class="opacity-60">×{status.requests.toLocaleString()}</span>
                        </span>
                      )}
                    </For>
                  </div>
                </div>
                <div class="flex-1 min-h-0 overflow-hidden">
                  <LogViewer
                    api={props.logsApi}
                    serviceId="maestro-ingress"
                    deploymentId={null}
                    isSystem
                    phase="deploy"
                    embedded={true}
                    fillHeight
                    query={ipLogQuery(group.value)}
                    range={props.range}
                    onRangeChange={props.onRangeChange}
                  />
                </div>
              </>
            )}
          </Show>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog>
  );
}

export { TrafficIpSheet };
