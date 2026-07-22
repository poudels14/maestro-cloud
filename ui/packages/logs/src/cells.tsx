import { Show } from "solid-js";
import { ChevronRight } from "lucide-solid";
import clsx from "clsx";
import {
  tsFormatter,
  timeFormatter,
  dateFormatter,
  logLevelColors,
  httpMethodColor,
  httpStatusPill
} from "./logFormat";

function TimeCell(props: { ts: number }) {
  const d = () => new Date(props.ts);
  return (
    <span class="whitespace-nowrap tabular-nums text-gray-400" title={tsFormatter.format(d())}>
      <span>{dateFormatter.format(d())}</span>
      <span class="ml-1.5">{timeFormatter.format(d())}</span>
    </span>
  );
}

function ExpanderCell(props: { expanded: boolean; onToggle: (ev: MouseEvent) => void }) {
  return (
    <button
      type="button"
      class="p-0.5 text-gray-300 hover:text-gray-500 transition-colors outline-none"
      onClick={props.onToggle}
    >
      <ChevronRight
        size={12}
        class={clsx("transition-transform", { "rotate-90": props.expanded })}
      />
    </button>
  );
}

function HostCell(props: { value: string }) {
  return (
    <span class="block truncate text-gray-500" title={props.value}>
      {props.value}
    </span>
  );
}

function LevelCell(props: { level: string }) {
  return (
    <span
      class={clsx(
        "inline-block text-xs uppercase whitespace-nowrap tracking-wide",
        logLevelColors(props.level).text
      )}
      title={props.level}
    >
      {props.level}
    </span>
  );
}

function MessageCell(props: { text: string; class?: string | undefined }) {
  return (
    <span class={clsx("block whitespace-pre-wrap break-words", props.class ?? "text-gray-700")}>
      {props.text}
    </span>
  );
}

function MethodCell(props: { method?: string | undefined }) {
  return (
    <Show when={props.method} fallback={<span class="text-gray-300">·</span>}>
      <span class={clsx("uppercase font-medium", httpMethodColor(props.method!))}>
        {props.method}
      </span>
    </Show>
  );
}

function StatusCell(props: { status?: string | undefined }) {
  return (
    <Show when={props.status} fallback={<span class="text-gray-300">·</span>}>
      <span
        class={clsx(
          "inline-block rounded px-1 tabular-nums font-medium",
          httpStatusPill(props.status!)
        )}
      >
        {props.status}
      </span>
    </Show>
  );
}

function PathCell(props: {
  path: string;
  durationLabel?: string | undefined;
  requestHost?: string | undefined;
  clientIp?: string | undefined;
  router?: string | undefined;
}) {
  return (
    <span class="flex flex-col gap-0.5 min-w-0">
      <span class="flex items-baseline gap-2 min-w-0">
        <span class="text-gray-700 truncate" title={props.path}>
          {props.path}
        </span>
        <Show when={props.durationLabel}>
          <span class="text-gray-400 tabular-nums shrink-0">{props.durationLabel}</span>
        </Show>
      </span>
      <Show when={props.requestHost || props.clientIp || props.router}>
        <span class="flex flex-wrap items-center gap-x-3 gap-y-0.5 text-[10px] text-gray-400 min-w-0">
          <Show when={props.requestHost}>
            <span class="truncate max-w-72" title={`Request host: ${props.requestHost}`}>
              <span class="text-gray-300">host</span> {props.requestHost}
            </span>
          </Show>
          <Show when={props.clientIp}>
            <span class="truncate max-w-60" title={`Client IP: ${props.clientIp}`}>
              <span class="text-gray-300">client</span> {props.clientIp}
            </span>
          </Show>
          <Show when={props.router}>
            <span class="truncate max-w-52" title={`Traefik router: ${props.router}`}>
              <span class="text-gray-300">router</span> {props.router}
            </span>
          </Show>
        </span>
      </Show>
    </span>
  );
}

export {
  TimeCell,
  ExpanderCell,
  HostCell,
  LevelCell,
  MessageCell,
  MethodCell,
  StatusCell,
  PathCell
};
