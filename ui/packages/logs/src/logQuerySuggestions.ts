interface LogQueryCatalog {
  fields: string[];
  values: ReadonlyMap<string, readonly string[]>;
}

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

type QueryToken = {
  start: number;
  end: number;
  text: string;
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

const SUGGESTION_GROUP_LABELS: Record<SuggestionKind, string> = {
  operator: "Operators",
  field: "Fields",
  value: "Values"
};

const MAX_SUGGESTIONS = 10;

function tokenAtCursor(query: string, cursor: number): QueryToken {
  let start = Math.min(cursor, query.length);
  while (start > 0 && !/[\s()]/.test(query[start - 1]!)) start -= 1;
  return { start, end: cursor, text: query.slice(start, cursor) };
}

function buildSuggestions(
  query: string,
  cursor: number,
  catalog: LogQueryCatalog
): QuerySuggestion[] {
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

function fieldSuggestions(
  prefix: string,
  negative: string,
  catalog: LogQueryCatalog
): QuerySuggestion[] {
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
    .map((definition) => ({
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
): QuerySuggestion[] {
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
    .map(([value, description]) => ({
      id: `value-${negative}${field}-${value}`,
      kind: "value",
      label: `${negative}${field}:${value}`,
      insert: `${negative}${field}:${value}`,
      description,
      complete: true
    }));
}

function quoteQueryValue(value: string) {
  if (/^[^\s:()[\]"]+$/.test(value)) return value;
  return `"${value.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

export { buildSuggestions, SUGGESTION_GROUP_LABELS, tokenAtCursor };
export type { LogQueryCatalog, QuerySuggestion, SuggestionKind };
