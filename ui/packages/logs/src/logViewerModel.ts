import { compareLogEntries, type LogEntry } from "./logView";
import { httpFields } from "./logFormat";
import type { LogQueryCatalog } from "./logQuerySuggestions";

function mergeLogEntries(current: LogEntry[], incoming: LogEntry[]) {
  const existing = new Set(current.map(logEntryKey));
  const unique = incoming.filter((entry) => {
    const key = logEntryKey(entry);
    if (existing.has(key)) return false;
    existing.add(key);
    return true;
  });
  const merged: LogEntry[] = [];
  let currentIndex = 0;
  let incomingIndex = 0;
  while (currentIndex < current.length && incomingIndex < unique.length) {
    const currentEntry = current[currentIndex]!;
    const incomingEntry = unique[incomingIndex]!;
    if (compareLogEntries(currentEntry, incomingEntry) <= 0) {
      merged.push(currentEntry);
      currentIndex += 1;
    } else {
      merged.push(incomingEntry);
      incomingIndex += 1;
    }
  }
  merged.push(...current.slice(currentIndex), ...unique.slice(incomingIndex));
  return merged;
}

function logEntryKey(entry: LogEntry) {
  return `${entry.nodeId ?? "local"}:${entry.tier ?? "logs"}:${entry.seq}`;
}

function buildLogQueryCatalog(serviceId: string, lines: LogEntry[]): LogQueryCatalog {
  const fields = new Set<string>();
  const values = new Map<string, Set<string>>();
  const addValue = (field: string, value: string | undefined) => {
    const normalized = value?.trim();
    if (!normalized || normalized.length > 160) return;
    let candidates = values.get(field);
    if (!candidates) {
      candidates = new Set();
      values.set(field, candidates);
    }
    if (candidates.size < 25) candidates.add(normalized);
  };

  addValue("service", serviceId);
  for (const line of lines) {
    addValue("service", line.serviceId);
    addValue("level", line.level.toLowerCase());
    addValue("status", line.level.toLowerCase());
    addValue("source", line.source);
    addValue("@http.status_code", httpFields(line.attrs).status);
    for (const [name, value] of line.attrs ?? []) {
      if (!/^[A-Za-z0-9._-]+$/.test(name)) continue;
      const field = `@${name}`;
      fields.add(field);
      addValue(field, value);
    }
  }

  return {
    fields: Array.from(fields)
      .sort((left, right) => left.localeCompare(right))
      .slice(0, 100),
    values: new Map(
      Array.from(values, ([field, candidates]) => [
        field,
        Array.from(candidates).sort((left, right) =>
          left.localeCompare(right, undefined, { numeric: true })
        )
      ])
    )
  };
}

export { buildLogQueryCatalog, logEntryKey, mergeLogEntries };
