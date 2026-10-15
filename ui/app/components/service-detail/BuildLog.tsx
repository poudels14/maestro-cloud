import { createEffect, For, Match, on, Switch } from "solid-js";
import { useQuery } from "../../lib/useQuery";
import { getBuildLog } from "../../lib/api";
import { ErrorBanner } from "../../lib/ui";

const BUILD_POLL_INTERVAL_MS = 3_000;
const ANSI_PATTERN = /\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[@-_]/g;
const TIMESTAMP_PATTERN = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z) (.*)$/;

function BuildLog(props: { serviceId: string; deploymentId: string; building: boolean }) {
  const log = useQuery(() => ({
    queryKey: ["build-log", props.serviceId, props.deploymentId],
    queryFn: () => getBuildLog(props.serviceId, props.deploymentId),
    refetchInterval: props.building ? BUILD_POLL_INTERVAL_MS : (false as const)
  }));
  let scrollRef: HTMLPreElement | undefined;

  createEffect(
    on(
      () => log.data,
      () => {
        if (scrollRef && props.building) {
          scrollRef.scrollTop = scrollRef.scrollHeight;
        }
      }
    )
  );

  return (
    <Switch>
      <Match when={log.isError}>
        <div class="p-4">
          <ErrorBanner message={log.error?.message ?? "Failed to load build log"} />
        </div>
      </Match>
      <Match when={log.isLoading}>
        <div class="py-20 text-center text-sm text-gray-400">Loading build log…</div>
      </Match>
      <Match when={!log.data}>
        <div class="py-20 text-center text-sm text-gray-400">
          {props.building
            ? "Waiting for the build to start."
            : "No build log is stored for this deployment."}
        </div>
      </Match>
      <Match when={log.data}>
        {(text) => (
          <pre
            ref={scrollRef}
            class="h-full overflow-auto bg-gray-950 px-4 py-3 font-mono text-[11px] leading-relaxed whitespace-pre-wrap break-all text-gray-200"
          >
            <For each={text().replace(ANSI_PATTERN, "").split("\n")}>{(line) => <BuildLogLine line={line} />}</For>
          </pre>
        )}
      </Match>
    </Switch>
  );
}

function BuildLogLine(props: { line: string }) {
  const match = () => TIMESTAMP_PATTERN.exec(props.line);
  return (
    <Switch fallback={<div>{props.line}</div>}>
      <Match when={match()}>
        {(parts) => (
          <div class="flex gap-3">
            <span class="shrink-0 text-gray-500 select-none" title={parts()[1]}>
              {formatTime(parts()[1])}
            </span>
            <span class="min-w-0">{parts()[2]}</span>
          </div>
        )}
      </Match>
    </Switch>
  );
}

function formatTime(timestamp: string): string {
  return new Date(timestamp).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
}

export { BuildLog };
