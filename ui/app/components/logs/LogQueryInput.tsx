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
import { logQueryPills, removeLogQueryPill, type LogQueryPill } from "./logQueryPills";

type LogQueryCatalog = {
  fields: string[];
  values: ReadonlyMap<string, readonly string[]>;
};

type SuggestionKind = "operator" | "field" | "value";

type QuerySuggestion = {
  id: string;
  kind: SuggestionKind;
  label: string;
  insert: string;
  description?: string;
  example?: string;
  complete?: boolean;
};

const SUGGESTION_GROUP_LABELS: Record<SuggestionKind, string> = {
  operator: "Operators",
  field: "Fields",
  value: "Values"
};

type FieldDefinition = {
  field: string;
  example: string;
};

const FIELD_DEFINITIONS: FieldDefinition[] = [
  { field: "level", example: "level:error" },
  { field: "message", example: 'message:"connection refused"' },
  { field: "service", example: "service:app" },
  {
    field: "source",
    example: "source:app/*"
  },
  { field: "status", example: "status:error" },
  {
    field: "@http.status_code",
    example: "@http.status_code:404"
  },
  {
    field: "@http.method",
    example: "@http.method:GET"
  },
  {
    field: "@http.url_details.path",
    example: "@http.url_details.path:/api/*"
  },
  {
    field: "@maestro.client_ip",
    example: "@maestro.client_ip:203.0.113.10"
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
  {
    id: "operator-and",
    kind: "operator",
    label: "AND",
    insert: "AND ",
    description: "Require both expressions"
  },
  {
    id: "operator-or",
    kind: "operator",
    label: "OR",
    insert: "OR ",
    description: "Match either expression"
  },
  {
    id: "operator-not",
    kind: "operator",
    label: "NOT",
    insert: "NOT ",
    description: "Exclude the next expression"
  },
  {
    id: "operator-minus",
    kind: "operator",
    label: "-",
    insert: "-",
    description: "Short form exclusion"
  }
];

const MAX_SUGGESTIONS = 10;

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

  const focusShortcut = (event: KeyboardEvent) => {
    if (event.key !== "/" || event.metaKey || event.ctrlKey || event.altKey) return;
    const target = event.target as HTMLElement | null;
    if (
      target &&
      (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable)
    ) {
      return;
    }
    event.preventDefault();
    inputRef?.focus();
  };
  document.addEventListener("keydown", focusShortcut);
  onCleanup(() => document.removeEventListener("keydown", focusShortcut));

  const appliedPills = createMemo(() => logQueryPills(props.appliedQuery));
  const appliedRemainder = createMemo(() => logQueryRemainder(props.appliedQuery, appliedPills()));
  const hasApplied = () => props.appliedQuery.trim().length > 0;
  const suggestions = createMemo(() => buildSuggestions(props.value, cursor(), props.catalog));
  const currentToken = createMemo(() => {
    const token = tokenAtCursor(props.value, cursor()).text;
    return token.startsWith("-") ? token.slice(1) : token;
  });
  const suggestionGroups = createMemo(() => {
    const groups: { label: string; items: { suggestion: QuerySuggestion; flatIndex: number }[] }[] =
      [];
    suggestions().forEach((suggestion, flatIndex) => {
      const label = SUGGESTION_GROUP_LABELS[suggestion.kind];
      const lastGroup = groups.at(-1);
      if (lastGroup && lastGroup.label === label) {
        lastGroup.items.push({ suggestion, flatIndex });
      } else {
        groups.push({ label, items: [{ suggestion, flatIndex }] });
      }
    });
    return groups;
  });

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

  const focusInput = (nextCursor: number) => {
    setCursor(nextCursor);
    setActiveIndex(-1);
    setOpen(true);
    requestAnimationFrame(() => {
      inputRef?.focus();
      inputRef?.setSelectionRange(nextCursor, nextCursor);
    });
  };

  const acceptSuggestion = (suggestion: QuerySuggestion) => {
    const token = tokenAtCursor(props.value, cursor());
    const next = `${props.value.slice(0, token.start)}${suggestion.insert}${props.value.slice(token.end)}`;
    props.onInput(next);
    if (suggestion.complete) {
      props.onApply();
      focusInput(0);
    } else {
      focusInput(token.start + suggestion.insert.length);
    }
  };

  const editFragment = (fragment: string, nextAppliedQuery: string) => {
    props.onAppliedQueryChange(nextAppliedQuery);
    const draft = props.value.trim() ? `${fragment} ${props.value}` : fragment;
    props.onInput(draft);
    focusInput(fragment.length);
  };

  const editPill = (pill: LogQueryPill) =>
    editFragment(pillFilterText(pill), removeLogQueryPill(props.appliedQuery, pill));

  const editRemainder = () =>
    editFragment(appliedRemainder(), appliedPills().map(pillFilterText).join(" "));

  const popLastToken = () => {
    if (appliedRemainder()) {
      editRemainder();
    } else {
      const lastPill = appliedPills().at(-1);
      if (lastPill) editPill(lastPill);
    }
  };

  return (
    <div class="flex-1 min-w-0">
      <div class="relative">
        <div
          onClick={() => inputRef?.focus()}
          class="flex w-full cursor-text flex-wrap items-center gap-1 rounded-md border border-gray-200 bg-gray-50 py-1 pl-8 pr-8 transition-colors focus-within:border-indigo-300 focus-within:bg-white"
        >
          <button
            type="button"
            onClick={(event) => {
              event.stopPropagation();
              props.onApply();
              setOpen(false);
            }}
            title="Apply log query"
            class="absolute left-1.5 top-1/2 -translate-y-1/2 z-10 p-1 text-gray-400 hover:text-indigo-600 outline-none rounded hover:bg-indigo-50"
          >
            <Search class="size-3.5" />
          </button>
          <For each={appliedPills()}>
            {(pill) => (
              <span class="inline-flex max-w-full items-center overflow-hidden rounded-md border border-gray-200 bg-gray-100 font-mono text-[11px]">
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    editPill(pill);
                  }}
                  title="Edit filter"
                  class="flex min-w-0 items-baseline py-0.5 pl-2 pr-1 outline-none transition-colors hover:bg-gray-200/70"
                >
                  <span class="shrink-0 font-medium text-gray-700">
                    {pill.prefix}
                    {pill.field}:
                  </span>
                  <span class="min-w-0 truncate text-gray-600" title={pill.value}>
                    {pill.value}
                  </span>
                </button>
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    props.onAppliedQueryChange(removeLogQueryPill(props.appliedQuery, pill));
                    inputRef?.focus();
                  }}
                  aria-label={`Remove filter ${pillFilterText(pill)}`}
                  title="Remove filter"
                  class="self-stretch pl-0.5 pr-1.5 text-gray-400 outline-none transition-colors hover:bg-gray-200 hover:text-gray-700"
                >
                  <X class="size-3" />
                </button>
              </span>
            )}
          </For>
          <Show when={appliedRemainder()}>
            <span class="inline-flex max-w-full items-center overflow-hidden rounded-md border border-gray-200 bg-gray-100 font-mono text-[11px]">
              <button
                type="button"
                onClick={(event) => {
                  event.stopPropagation();
                  editRemainder();
                }}
                title="Edit query"
                class="min-w-0 truncate py-0.5 pl-2 pr-1 text-gray-700 outline-none transition-colors hover:bg-gray-200/70"
              >
                {appliedRemainder()}
              </button>
              <button
                type="button"
                onClick={(event) => {
                  event.stopPropagation();
                  props.onAppliedQueryChange(appliedPills().map(pillFilterText).join(" "));
                  inputRef?.focus();
                }}
                aria-label={`Remove query ${appliedRemainder()}`}
                title="Remove query"
                class="self-stretch pl-0.5 pr-1.5 text-gray-400 outline-none transition-colors hover:bg-gray-200 hover:text-gray-700"
              >
                <X class="size-3" />
              </button>
            </span>
          </Show>
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
            onClick={(event) => {
              event.stopPropagation();
              updateCursor();
            }}
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
              if (event.key === "Backspace" && props.value.length === 0 && hasApplied()) {
                event.preventDefault();
                popLastToken();
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
            placeholder={
              hasApplied() ? "Add filter…" : "Filter logs… type @ to see available fields"
            }
            title="Datadog-style query; press Enter to apply"
            class="min-w-[140px] flex-1 bg-transparent py-0.5 text-sm outline-none placeholder:text-gray-400"
          />
          <Show when={props.value.length > 0 || hasApplied()}>
            <button
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                props.onClear();
                setOpen(false);
                setActiveIndex(-1);
              }}
              title="Clear all filters"
              class="absolute right-1.5 top-1/2 -translate-y-1/2 z-10 p-1 text-gray-400 hover:text-gray-600 outline-none rounded hover:bg-gray-100"
            >
              <X class="size-3" />
            </button>
          </Show>
        </div>
        <Show when={open() && suggestions().length > 0}>
          <div class="absolute z-40 mt-1 w-full overflow-hidden rounded-md border border-gray-200 bg-white shadow-lg">
            <ul id={listboxId} role="listbox" class="max-h-72 overflow-y-auto pb-1">
              <For each={suggestionGroups()}>
                {(group) => (
                  <>
                    <li
                      role="presentation"
                      class="select-none border-b border-gray-100 bg-gray-50/70 px-3 py-1 text-[10px] font-medium text-gray-400"
                    >
                      {group.label}
                    </li>
                    <For each={group.items}>
                      {(item) => (
                        <li
                          id={`${listboxId}-${item.flatIndex}`}
                          role="option"
                          aria-selected={activeIndex() === item.flatIndex}
                        >
                          <button
                            type="button"
                            onMouseDown={(event) => {
                              event.preventDefault();
                              acceptSuggestion(item.suggestion);
                            }}
                            onMouseEnter={() => setActiveIndex(item.flatIndex)}
                            class={clsx(
                              "flex w-full items-center justify-between gap-4 px-3 py-1.5 text-left outline-none",
                              activeIndex() === item.flatIndex ? "bg-indigo-50" : "hover:bg-gray-50"
                            )}
                          >
                            <span class="min-w-0">
                              <span class="block truncate font-mono text-xs text-gray-800">
                                <HighlightedLabel
                                  label={item.suggestion.label}
                                  match={currentToken()}
                                />
                              </span>
                              <Show when={item.suggestion.description}>
                                <span class="block truncate text-[11px] text-gray-400">
                                  {item.suggestion.description}
                                </span>
                              </Show>
                            </span>
                            <Show when={item.suggestion.example}>
                              <span class="hidden shrink-0 font-mono text-[10px] text-gray-400 lg:block">
                                {item.suggestion.example}
                              </span>
                            </Show>
                          </button>
                        </li>
                      )}
                    </For>
                  </>
                )}
              </For>
            </ul>
            <div class="flex items-center justify-between gap-4 border-t border-gray-100 bg-gray-50 px-3 py-1.5 text-[10px] text-gray-400">
              <span class="min-w-0 truncate">
                AND, OR, NOT and - are supported · e.g.{" "}
                <span class="font-mono text-gray-500">
                  level:error -message:"health check" @http.status_code:[500 TO 599]
                </span>
              </span>
              <span class="hidden shrink-0 sm:inline">
                ↑↓ select · Enter apply · Backspace edit last filter
              </span>
            </div>
          </div>
        </Show>
      </div>
    </div>
  );
}

function HighlightedLabel(props: { label: string; match: string }) {
  const matchRange = () => {
    const needle = props.match.trim().toLowerCase();
    if (!needle) return null;
    const index = props.label.toLowerCase().indexOf(needle);
    return index >= 0 ? { start: index, end: index + needle.length } : null;
  };

  return (
    <Show when={matchRange()} fallback={<>{props.label}</>}>
      {(range) => (
        <>
          {props.label.slice(0, range().start)}
          <span class="rounded-sm bg-indigo-100/70 text-indigo-700">
            {props.label.slice(range().start, range().end)}
          </span>
          {props.label.slice(range().end)}
        </>
      )}
    </Show>
  );
}

function tokenAtCursor(query: string, cursor: number) {
  let start = Math.min(cursor, query.length);
  while (start > 0 && !/[\s()]/.test(query[start - 1])) start -= 1;
  return { start, end: cursor, text: query.slice(start, cursor) };
}

function quoteQueryValue(value: string) {
  if (/^[^\s:()[\]"]+$/.test(value)) return value;
  return `"${value.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

function pillFilterText(pill: LogQueryPill) {
  return `${pill.prefix}${pill.field}:${pill.value}`;
}

function logQueryRemainder(query: string, pills: LogQueryPill[]) {
  let remainder = "";
  let sliceStart = 0;
  for (const pill of pills) {
    remainder += `${query.slice(sliceStart, Math.max(sliceStart, pill.removeStart))} `;
    sliceStart = Math.max(sliceStart, pill.removeEnd);
  }
  remainder += query.slice(sliceStart);
  return remainder.replace(/\s+/g, " ").trim();
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
      kind: "field",
      label: `${negative}${definition.field}:`,
      insert: `${negative}${definition.field}:`,
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
      kind: "value",
      label: `${negative}${field}:${value}`,
      insert: `${negative}${field}:${value}`,
      description,
      complete: true
    }));
}

function buildSuggestions(query: string, cursor: number, catalog: LogQueryCatalog) {
  const token = tokenAtCursor(query, cursor);
  const negative = token.text.startsWith("-") ? "-" : "";
  const raw = negative ? token.text.slice(1) : token.text;

  if (!raw && !query.trim()) {
    return fieldSuggestions("", negative, catalog);
  }
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

export { LogQueryInput };
export type { LogQueryCatalog };
