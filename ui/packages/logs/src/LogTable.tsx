import { For, Match, Show, Switch } from "solid-js";
import clsx from "clsx";
import type { LogEntry } from "./api";
import { LOG_COLUMNS, LogRow } from "./LogRow";
import { logEntryKey } from "./logViewerModel";

function LogTable(props: {
  lines: LogEntry[];
  loading: boolean;
  cluster: boolean;
  showHost: boolean;
  showHttp: boolean;
  stream: boolean;
  expanded: ReadonlySet<string>;
  onToggle: (key: string) => void;
}) {
  return (
    <Switch>
      <Match when={props.loading}>
        <div class="text-gray-400 text-center py-8 font-mono text-xs">Loading logs…</div>
      </Match>
      <Match when={!props.loading && props.lines.length === 0}>
        <div class="text-gray-400 text-center py-8 font-mono text-xs">No logs available.</div>
      </Match>
      <Match when={props.lines.length > 0}>
        <ul class="font-mono text-xs">
          <li class="hidden sm:flex items-stretch py-1.5 px-2 border-b border-gray-200 bg-gray-100 text-[11px] font-sans font-medium text-gray-500 sticky top-0 z-10">
            <span class={clsx("shrink-0", props.stream ? "w-2" : "w-[18px]")} />
            <span class={clsx(LOG_COLUMNS.time, "shrink-0 pr-2 truncate")}>Time</span>
            <Show when={props.showHost}>
              <span
                class={clsx(LOG_COLUMNS.host, "shrink-0 px-2 truncate border-l border-gray-300")}
              >
                {props.cluster ? "Node" : "Host"}
              </span>
            </Show>
            <Show when={props.cluster}>
              <span
                class={clsx(LOG_COLUMNS.service, "shrink-0 px-2 truncate border-l border-gray-300")}
              >
                Service
              </span>
            </Show>
            <span
              class={clsx(LOG_COLUMNS.level, "shrink-0 px-2 truncate border-l border-gray-300")}
            >
              Level
            </span>
            <Show when={props.showHttp}>
              <span
                class={clsx(LOG_COLUMNS.method, "shrink-0 px-2 truncate border-l border-gray-300")}
              >
                Method
              </span>
              <span
                class={clsx(LOG_COLUMNS.status, "shrink-0 px-2 truncate border-l border-gray-300")}
              >
                Status
              </span>
            </Show>
            <span class="flex-1 pl-2 truncate border-l border-gray-300">
              {props.showHttp ? "Request" : "Message"}
            </span>
          </li>
          <For each={props.lines}>
            {(line, index) => {
              const key = logEntryKey(line);
              return (
                <LogRow
                  line={line}
                  index={index()}
                  showHost={props.showHost}
                  showService={props.cluster}
                  showHttp={props.showHttp}
                  stream={props.stream}
                  expanded={props.expanded.has(key)}
                  onToggle={() => props.onToggle(key)}
                />
              );
            }}
          </For>
        </ul>
      </Match>
    </Switch>
  );
}

export { LogTable };
