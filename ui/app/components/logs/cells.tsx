import { ChevronRight } from "lucide-solid";
import clsx from "clsx";
import { tsFormatter, timeFormatter, dateFormatter, logLevelPill } from "../../lib/logFormat";

function TimeCell(props: { ts: number }) {
  const d = () => new Date(props.ts);
  return (
    <span class="select-none whitespace-nowrap tabular-nums" title={tsFormatter.format(d())}>
      <span class="text-gray-400">{dateFormatter.format(d())}</span>
      <span class="text-gray-500 ml-1.5">{timeFormatter.format(d())}</span>
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
    <span class="text-violet-400 truncate" title={props.value}>
      {props.value}
    </span>
  );
}

function LevelCell(props: { level: string }) {
  return (
    <span
      class={clsx(
        "inline-block text-[10px] font-medium uppercase whitespace-nowrap rounded px-1.5 py-px tracking-wide",
        logLevelPill(props.level)
      )}
      title={props.level}
    >
      {props.level}
    </span>
  );
}

function MessageCell(props: { text: string }) {
  return <span class="block text-gray-700 whitespace-pre-wrap break-words">{props.text}</span>;
}

export { TimeCell, ExpanderCell, HostCell, LevelCell, MessageCell };
