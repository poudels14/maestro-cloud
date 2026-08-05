import { Show } from "solid-js";
import clsx from "clsx";
import type { LogEntry } from "./logView";
import { clusterLogNodeLabel } from "./clusterLogNode";
import { httpFields } from "./logFormat";
import {
  ExpanderCell,
  HostCell,
  LevelCell,
  MessageCell,
  MethodCell,
  PathCell,
  StatusCell,
  TimeCell
} from "./cells";
import { LogDetailPanel } from "./LogDetailPanel";

const LOG_COLUMNS = {
  time: "sm:w-[118px]",
  host: "sm:w-[112px]",
  service: "sm:w-[124px]",
  level: "sm:w-[52px]",
  method: "sm:w-[60px]",
  status: "sm:w-[56px]"
};

function LogRow(props: {
  line: LogEntry;
  showHost: boolean;
  showService: boolean;
  showHttp: boolean;
  stream?: boolean;
  expanded: boolean;
  onToggle: () => void;
}) {
  const host = () => clusterLogNodeLabel(props.line);
  const service = () => props.line.serviceId || props.line.source?.split("/")[0] || "";
  const http = () => httpFields(props.line.attrs);
  return (
    <li
      data-log-entry
      class={clsx("border-b border-gray-50 transition-colors", {
        "cursor-pointer": !props.stream,
        "bg-brand-light": props.expanded,
        "even:bg-white even:hover:bg-gray-50 odd:bg-gray-50/60 odd:hover:bg-gray-100/60":
          !props.expanded
      })}
      onClick={() => {
        if (!props.stream) props.onToggle();
      }}
    >
      <div
        class={clsx("flex flex-col gap-1 pr-2 py-1.5 sm:flex-row sm:items-start sm:gap-0 sm:py-1", {
          "pl-2": !props.stream,
          "pl-4": props.stream
        })}
      >
        <div class="flex shrink-0 flex-wrap items-center gap-x-2 gap-y-1 sm:contents">
          <Show when={!props.stream}>
            <ExpanderCell
              expanded={props.expanded}
              onToggle={(event) => {
                event.stopPropagation();
                props.onToggle();
              }}
            />
          </Show>
          <div class={clsx("shrink-0 sm:pr-2 sm:pt-px", LOG_COLUMNS.time)}>
            <TimeCell ts={props.line.ts} />
          </div>
          <Show when={props.showHost}>
            <div
              class={clsx("min-w-0 max-w-[120px] sm:max-w-none sm:px-2 sm:pt-px", LOG_COLUMNS.host)}
            >
              <HostCell value={host()} />
            </div>
          </Show>
          <Show when={props.showService}>
            <div
              class={clsx(
                "min-w-0 max-w-[140px] sm:max-w-none sm:px-2 sm:pt-px",
                LOG_COLUMNS.service
              )}
            >
              <HostCell value={service()} />
            </div>
          </Show>
          <div class={clsx("shrink-0 sm:px-2 sm:pt-px", LOG_COLUMNS.level)}>
            <LevelCell level={props.line.level} />
          </div>
          <Show when={props.showHttp}>
            <div class={clsx("shrink-0 sm:px-2 sm:pt-px", LOG_COLUMNS.method)}>
              <MethodCell method={http().method} />
            </div>
            <div class={clsx("shrink-0 sm:px-2 sm:pt-px", LOG_COLUMNS.status)}>
              <StatusCell status={http().status} />
            </div>
          </Show>
        </div>
        <div class="min-w-0 w-full flex-1 pl-6 sm:w-auto sm:pl-2 sm:pr-4">
          <Show
            when={http().path}
            fallback={
              <MessageCell
                text={props.line.text}
                class={props.stream ? streamMessageClass(props.line.level) : undefined}
              />
            }
          >
            <PathCell
              path={http().path!}
              durationLabel={http().durationLabel}
              requestHost={http().requestHost}
              clientIp={http().clientIp}
              router={http().router}
            />
          </Show>
        </div>
      </div>
      <Show when={props.expanded}>
        <div class="border-t border-gray-100 bg-gray-50/80 px-4 py-3">
          <LogDetailPanel entry={props.line} />
        </div>
      </Show>
    </li>
  );
}

function streamMessageClass(level: string) {
  switch (level.toLowerCase()) {
    case "error":
    case "err":
    case "fatal":
    case "panic":
      return "text-red-600";
    case "warn":
    case "warning":
      return "text-amber-700";
    default:
      return "text-gray-700";
  }
}

export { LOG_COLUMNS, LogRow };
