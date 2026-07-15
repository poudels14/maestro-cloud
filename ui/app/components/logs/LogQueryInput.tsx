import {
  For,
  Show,
  createEffect,
  createMemo,
  createSignal,
  createUniqueId,
  onCleanup
} from "solid-js";
import { Search, X } from "lucide-solid";
import clsx from "clsx";
import { logQueryPills, removeLogQueryPill } from "./logQueryPills";

type LogQueryCatalog = {
  fields: string[];
  values: ReadonlyMap<string, readonly string[]>;
};

type QuerySuggestion = {
  id: string;
  label: string;
  insert: string;
  description: string;
  example?: string;
};

type FieldDefinition = {
  field: string;
  description: string;
  example: string;
};

const FIELD_DEFINITIONS: FieldDefinition[] = [
  { field: "level", description: "Log severity", example: "level:error" },
  { field: "message", description: "Log message text", example: 'message:"connection refused"' },
  { field: "service", description: "Service or service source prefix", example: "service:app" },
  {
    field: "source",
    description: "Exact log source; wildcards are supported",
    example: "source:app/*"
  },
  { field: "status", description: "Alias for log severity", example: "status:error" },
  {
    field: "@http.status_code",
    description: "Canonical HTTP response status",
    example: "@http.status_code:404"
  },
  {
    field: "@http.method",
    description: "Structured HTTP method attribute",
    example: "@http.method:GET"
  },
  {
    field: "@http.url_details.path",
    description: "Structured HTTP request path",
    example: "@http.url_details.path:/api/*"
  },
  {
    field: "@maestro.client_ip",
    description: "Normalized ingress client address",
    example: "@maestro.client_ip:203.0.113.10"
  }
];

const EXAMPLE_SUGGESTIONS: QuerySuggestion[] = [
  {
    id: "example-status",
    label: "@http.status_code:404",
    insert: "@http.status_code:404",
    description: "Exact HTTP status"
  },
  {
    id: "example-status-range",
    label: "@http.status_code:[500 TO 599]",
    insert: "@http.status_code:[500 TO 599]",
    description: "Numeric range"
  },
  {
    id: "example-level",
    label: "level:",
    insert: "level:",
    description: "Filter by log severity",
    example: "error · warn · info · debug · trace"
  },
  {
    id: "example-message",
    label: 'message:"connection refused"',
    insert: 'message:"connection refused"',
    description: "Exact phrase"
  },
  {
    id: "example-exclude",
    label: '-message:"health check"',
    insert: '-message:"health check"',
    description: "Exclude matching logs"
  }
];

const STATIC_VALUES: Record<string, { value: string; description: string }[]> = {
  level: ["error", "warn", "info", "debug", "trace"].map((value) => ({
    value,
    description: "Log severity"
  })),
  status: ["error", "warn", "info", "debug", "trace"].map((value) => ({
    value,
    description: "Log severity"
  })),
  "@http.status_code": [
    { value: "200", description: "Successful responses" },
    { value: "400", description: "Bad request" },
    { value: "401", description: "Unauthorized" },
    { value: "403", description: "Forbidden" },
    { value: "404", description: "Not found" },
    { value: "429", description: "Rate limited" },
    { value: "500", description: "Internal server error" },
    { value: "[400 TO 499]", description: "Any 4xx response" },
    { value: "[500 TO 599]", description: "Any 5xx response" },
    { value: ">=400", description: "Any error response" }
  ],
  "@http.method": ["GET", "POST", "PUT", "PATCH", "DELETE"].map((value) => ({
    value,
    description: "HTTP method"
  }))
};

const OPERATOR_SUGGESTIONS: QuerySuggestion[] = [
  { id: "operator-and", label: "AND", insert: "AND ", description: "Require both expressions" },
  { id: "operator-or", label: "OR", insert: "OR ", description: "Match either expression" },
  { id: "operator-not", label: "NOT", insert: "NOT ", description: "Exclude the next expression" },
  { id: "operator-minus", label: "-", insert: "-", description: "Short form exclusion" }
];

const MAX_SUGGESTIONS = 10;

function tokenAtCursor(query: string, cursor: number) {
  let start = Math.min(cursor, query.length);
  while (start > 0 && !/[\s()]/.test(query[start - 1])) start -= 1;
  return { start, end: cursor, text: query.slice(start, cursor) };
}

function quoteQueryValue(value: string) {
  if (/^[^\s:()[\]"]+$/.test(value)) return value;
  return `"${value.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

function fieldSuggestions(prefix: string, negative: string, catalog: LogQueryCatalog) {
  const fieldPriorities = new Map(
    FIELD_DEFINITIONS.map((definition, index) => [definition.field, index])
  );
  const definitions = new Map(
    FIELD_DEFINITIONS.map((definition) => [definition.field, definition])
  );
  for (const field of catalog.fields) {
    if (!definitions.has(field)) {
      const firstValue = catalog.values.get(field)?.[0];
      definitions.set(field, {
        field,
        description: "Observed log attribute",
        example: firstValue ? `${field}:${quoteQueryValue(firstValue)}` : `${field}:*`
      });
    }
  }

  const normalized = prefix.toLowerCase();
  return Array.from(definitions.values())
    .filter((definition) => {
      const field = definition.field.toLowerCase();
      return field.startsWith(normalized) || field.includes(normalized);
    })
    .sort((left, right) => {
      const leftStarts = left.field.toLowerCase().startsWith(normalized);
      const rightStarts = right.field.toLowerCase().startsWith(normalized);
      if (leftStarts !== rightStarts) return leftStarts ? -1 : 1;
      const priority =
        (fieldPriorities.get(left.field) ?? Number.MAX_SAFE_INTEGER) -
        (fieldPriorities.get(right.field) ?? Number.MAX_SAFE_INTEGER);
      if (priority !== 0) return priority;
      return left.field.localeCompare(right.field);
    })
    .slice(0, MAX_SUGGESTIONS)
    .map<QuerySuggestion>((definition) => ({
      id: `field-${negative}${definition.field}`,
      label: `${negative}${definition.field}:`,
      insert: `${negative}${definition.field}:`,
      description: definition.description,
      example: definition.example
    }));
}

function valueSuggestions(
  field: string,
  partial: string,
  negative: string,
  catalog: LogQueryCatalog
) {
  const candidates = new Map<string, string>();
  for (const candidate of STATIC_VALUES[field] ?? []) {
    candidates.set(candidate.value, candidate.description);
  }
  for (const value of catalog.values.get(field) ?? []) {
    const formatted = quoteQueryValue(value);
    if (!candidates.has(formatted)) candidates.set(formatted, "Observed value");
  }
  if (field.startsWith("@") && !candidates.has("*")) {
    candidates.set("*", "Attribute exists");
  }

  const normalized = partial.toLowerCase().replace(/^"/, "");
  return Array.from(candidates.entries())
    .filter(([value]) => !normalized || value.toLowerCase().includes(normalized))
    .sort(([left], [right]) => {
      const leftStarts = left.toLowerCase().startsWith(normalized);
      const rightStarts = right.toLowerCase().startsWith(normalized);
      if (leftStarts !== rightStarts) return leftStarts ? -1 : 1;
      return left.localeCompare(right, undefined, { numeric: true });
    })
    .slice(0, MAX_SUGGESTIONS)
    .map<QuerySuggestion>(([value, description]) => ({
      id: `value-${negative}${field}-${value}`,
      label: `${negative}${field}:${value}`,
      insert: `${negative}${field}:${value}`,
      description
    }));
}

function buildSuggestions(query: string, cursor: number, catalog: LogQueryCatalog) {
  const token = tokenAtCursor(query, cursor);
  const negative = token.text.startsWith("-") ? "-" : "";
  const raw = negative ? token.text.slice(1) : token.text;

  if (!raw && !query.trim()) return EXAMPLE_SUGGESTIONS;
  if (!raw) {
    return [
      ...OPERATOR_SUGGESTIONS,
      ...fieldSuggestions("", negative, catalog).slice(
        0,
        MAX_SUGGESTIONS - OPERATOR_SUGGESTIONS.length
      )
    ];
  }

  const colon = raw.indexOf(":");
  if (colon >= 0) {
    return valueSuggestions(raw.slice(0, colon), raw.slice(colon + 1), negative, catalog);
  }
  return fieldSuggestions(raw, negative, catalog);
}

function LogQueryInput(props: {
  value: string;
  appliedQuery: string;
  catalog: LogQueryCatalog;
  onInput: (value: string) => void;
  onApply: () => void;
  onClear: () => void;
  onAppliedQueryChange: (value: string) => void;
}) {
  const [open, setOpen] = createSignal(false);
  const [cursor, setCursor] = createSignal(0);
  const [activeIndex, setActiveIndex] = createSignal(-1);
  const listboxId = `log-query-suggestions-${createUniqueId()}`;
  let inputRef: HTMLInputElement | undefined;
  let closeTimer: ReturnType<typeof setTimeout> | undefined;

  const suggestions = createMemo(() => buildSuggestions(props.value, cursor(), props.catalog));
  const appliedPills = createMemo(() => logQueryPills(props.appliedQuery));

  createEffect(() => {
    if (activeIndex() >= suggestions().length) setActiveIndex(-1);
  });

  createEffect(() => {
    const index = activeIndex();
    if (!open() || index < 0) return;
    requestAnimationFrame(() => {
      document.getElementById(`${listboxId}-${index}`)?.scrollIntoView({ block: "nearest" });
    });
  });

  onCleanup(() => {
    if (closeTimer) clearTimeout(closeTimer);
  });

  const updateCursor = () => setCursor(inputRef?.selectionStart ?? props.value.length);

  const acceptSuggestion = (suggestion: QuerySuggestion) => {
    const token = tokenAtCursor(props.value, cursor());
    const next = `${props.value.slice(0, token.start)}${suggestion.insert}${props.value.slice(token.end)}`;
    const nextCursor = token.start + suggestion.insert.length;
    props.onInput(next);
    setCursor(nextCursor);
    setActiveIndex(-1);
    setOpen(true);
    requestAnimationFrame(() => {
      inputRef?.focus();
      inputRef?.setSelectionRange(nextCursor, nextCursor);
    });
  };

  return (
    <div class="flex-1 min-w-0">
      <div class="relative">
        <button
          type="button"
          onClick={() => {
            props.onApply();
            setOpen(false);
          }}
          title="Apply log query"
          class="absolute left-1.5 top-1/2 -translate-y-1/2 z-10 p-1 text-gray-400 hover:text-indigo-600 outline-none rounded hover:bg-indigo-50"
        >
          <Search class="size-3.5" />
        </button>
        <input
          ref={inputRef}
          type="text"
          value={props.value}
          role="combobox"
          aria-haspopup="listbox"
          aria-autocomplete="list"
          aria-expanded={open() && suggestions().length > 0}
          aria-controls={listboxId}
          aria-activedescendant={activeIndex() >= 0 ? `${listboxId}-${activeIndex()}` : undefined}
          onFocus={() => {
            if (closeTimer) clearTimeout(closeTimer);
            updateCursor();
            setOpen(true);
            setActiveIndex(-1);
          }}
          onBlur={() => {
            closeTimer = setTimeout(() => setOpen(false), 100);
          }}
          onClick={updateCursor}
          onSelect={updateCursor}
          onInput={(event) => {
            props.onInput(event.currentTarget.value);
            setCursor(event.currentTarget.selectionStart ?? event.currentTarget.value.length);
            setOpen(true);
            setActiveIndex(-1);
          }}
          onKeyDown={(event) => {
            if (event.key === "ArrowDown") {
              event.preventDefault();
              setOpen(true);
              setActiveIndex((index) => Math.min(index + 1, suggestions().length - 1));
              return;
            }
            if (event.key === "ArrowUp") {
              event.preventDefault();
              setOpen(true);
              setActiveIndex((index) => (index <= 0 ? suggestions().length - 1 : index - 1));
              return;
            }
            if (event.key === "Escape") {
              event.preventDefault();
              setOpen(false);
              setActiveIndex(-1);
              return;
            }
            if ((event.key === "Enter" || event.key === "Tab") && activeIndex() >= 0) {
              event.preventDefault();
              const suggestion = suggestions()[activeIndex()];
              if (suggestion) acceptSuggestion(suggestion);
              return;
            }
            if (event.key === "Enter") {
              event.preventDefault();
              props.onApply();
              setOpen(false);
            }
          }}
          placeholder="Filter logs… type @ to see available fields"
          title="Datadog-style query; press Enter to apply"
          class="w-full text-sm pl-8 pr-8 py-1.5 bg-gray-50 border border-gray-200 rounded-md outline-none focus:border-indigo-300 focus:bg-white focus:ring-2 focus:ring-indigo-100 transition-colors placeholder:text-gray-400"
        />
        <Show when={props.value.length > 0}>
          <button
            type="button"
            onClick={() => {
              props.onClear();
              setOpen(false);
              setActiveIndex(-1);
            }}
            title="Clear"
            class="absolute right-1.5 top-1/2 -translate-y-1/2 z-10 p-1 text-gray-400 hover:text-gray-600 outline-none rounded hover:bg-gray-100"
          >
            <X class="size-3" />
          </button>
        </Show>
        <Show when={open() && suggestions().length > 0}>
          <div class="absolute z-40 mt-1 w-full overflow-hidden rounded-md border border-gray-200 bg-white shadow-lg">
            <ul id={listboxId} role="listbox" class="max-h-72 overflow-y-auto py-1">
              <For each={suggestions()}>
                {(suggestion, index) => (
                  <li
                    id={`${listboxId}-${index()}`}
                    role="option"
                    aria-selected={activeIndex() === index()}
                  >
                    <button
                      type="button"
                      onMouseDown={(event) => event.preventDefault()}
                      onMouseEnter={() => setActiveIndex(index())}
                      onClick={() => acceptSuggestion(suggestion)}
                      class={clsx(
                        "flex w-full items-center justify-between gap-4 px-3 py-2 text-left outline-none",
                        activeIndex() === index() ? "bg-indigo-50" : "hover:bg-gray-50"
                      )}
                    >
                      <span class="min-w-0">
                        <span class="block truncate font-mono text-xs text-gray-800">
                          {suggestion.label}
                        </span>
                        <span class="block truncate text-[11px] text-gray-400">
                          {suggestion.description}
                        </span>
                      </span>
                      <Show when={suggestion.example}>
                        <span class="hidden shrink-0 font-mono text-[10px] text-gray-400 lg:block">
                          {suggestion.example}
                        </span>
                      </Show>
                    </button>
                  </li>
                )}
              </For>
            </ul>
            <div class="flex items-center justify-between border-t border-gray-100 bg-gray-50 px-3 py-1.5 text-[10px] text-gray-400">
              <span>Custom attributes use @ · AND, OR, NOT, and - are supported</span>
              <span class="hidden shrink-0 sm:inline">↑↓ select · Enter insert · Esc close</span>
            </div>
          </div>
        </Show>
      </div>
      <Show when={appliedPills().length > 0}>
        <div class="mt-2 flex flex-wrap items-center gap-1.5" aria-label="Applied log filters">
          <span class="mr-0.5 text-[10px] font-medium uppercase tracking-wide text-gray-400">
            Applied
          </span>
          <For each={appliedPills()}>
            {(pill) => (
              <span class="inline-flex max-w-full items-center overflow-hidden rounded-md border border-indigo-200 bg-indigo-50 text-[11px] shadow-sm">
                <span class="shrink-0 border-r border-indigo-200 bg-indigo-100/70 px-2 py-1 font-mono font-medium text-indigo-700">
                  {pill.prefix}
                  {pill.field}
                </span>
                <span class="min-w-0 truncate px-2 py-1 font-mono text-gray-700" title={pill.value}>
                  {pill.value}
                </span>
                <button
                  type="button"
                  onClick={() =>
                    props.onAppliedQueryChange(removeLogQueryPill(props.appliedQuery, pill))
                  }
                  aria-label={`Remove filter ${pill.prefix}${pill.field}:${pill.value}`}
                  title="Remove filter"
                  class="self-stretch border-l border-indigo-200 px-1.5 text-indigo-400 outline-none hover:bg-indigo-100 hover:text-indigo-700"
                >
                  <X class="size-3" />
                </button>
              </span>
            )}
          </For>
        </div>
      </Show>
    </div>
  );
}

export { LogQueryInput };
export type { LogQueryCatalog };
