import { ChevronRight } from "lucide-solid";
import clsx from "clsx";
import { tsFormatter, timeFormatter, dateFormatter, logLevelPill } from "../../lib/logFormat";

function TimeCell(info: { getValue: <T>() => T }) {
  const ts = info.getValue<number>();
  const d = new Date(ts);
  return (
    <span class="select-none whitespace-nowrap tabular-nums" title={tsFormatter.format(d)}>
      <span class="text-gray-400">{dateFormatter.format(d)}</span>
      <span class="text-gray-500 ml-1.5">{timeFormatter.format(d)}</span>
    </span>
  );
}

function ExpanderCell({
  row
}: {
  row: { toggleExpanded: () => void; getIsExpanded: () => boolean };
}) {
  return (
    <button
      type="button"
      class="p-0.5 text-gray-300 hover:text-gray-500 transition-colors outline-none"
      onClick={(ev) => {
        ev.stopPropagation();
        row.toggleExpanded();
      }}
    >
      <ChevronRight
        size={12}
        class={clsx("transition-transform", { "rotate-90": row.getIsExpanded() })}
      />
    </button>
  );
}

function HostCell(info: { getValue: <T>() => T }) {
  const value = info.getValue<string>();
  return (
    <span class="text-violet-400 truncate block" title={value}>
      {value}
    </span>
  );
}

function LevelCell(info: { getValue: <T>() => T }) {
  const level = info.getValue<string>();
  return (
    <span
      class={clsx(
        "inline-block text-[10px] font-medium uppercase whitespace-nowrap rounded px-1.5 py-px tracking-wide",
        logLevelPill(level)
      )}
      title={level}
    >
      {level}
    </span>
  );
}

function MessageCell(info: { getValue: <T>() => T }) {
  return (
    <span class="block text-gray-700 whitespace-pre-wrap break-words">
      {info.getValue<string>()}
    </span>
  );
}

export { TimeCell, ExpanderCell, HostCell, LevelCell, MessageCell };
