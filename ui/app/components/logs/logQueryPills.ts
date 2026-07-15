type QueryToken = {
  text: string;
  start: number;
  end: number;
};

type ParsedFilter = {
  prefix: string;
  field: string;
  value: string;
};

type LogQueryPill = ParsedFilter & {
  removeStart: number;
  removeEnd: number;
};

const RESERVED_FIELDS = new Set(["level", "status", "message", "source", "service"]);

function topLevelTokens(query: string): QueryToken[] | null {
  const tokens: QueryToken[] = [];
  let start = -1;
  let parenthesisDepth = 0;
  let bracketDepth = 0;
  let quoted = false;
  let escaped = false;

  const finish = (end: number) => {
    if (start < 0) return;
    tokens.push({ text: query.slice(start, end), start, end });
    start = -1;
  };

  for (let index = 0; index < query.length; index += 1) {
    const character = query[index];
    if (!quoted && parenthesisDepth === 0 && bracketDepth === 0 && /\s/.test(character)) {
      finish(index);
      continue;
    }
    if (start < 0) start = index;

    if (quoted) {
      if (escaped) {
        escaped = false;
      } else if (character === "\\") {
        escaped = true;
      } else if (character === '"') {
        quoted = false;
      }
      continue;
    }

    if (character === '"') quoted = true;
    else if (character === "(") parenthesisDepth += 1;
    else if (character === ")") parenthesisDepth -= 1;
    else if (character === "[") bracketDepth += 1;
    else if (character === "]") bracketDepth -= 1;

    if (parenthesisDepth < 0 || bracketDepth < 0) return null;
  }
  finish(query.length);

  if (quoted || escaped || parenthesisDepth !== 0 || bracketDepth !== 0) return null;
  return tokens;
}

function parseSimpleFilter(text: string, unaryPrefix = ""): ParsedFilter | null {
  let candidate = text;
  let prefix = unaryPrefix;
  if (candidate.startsWith("-")) {
    prefix += "-";
    candidate = candidate.slice(1);
  }

  const colon = candidate.indexOf(":");
  if (colon <= 0 || colon === candidate.length - 1) return null;
  const field = candidate.slice(0, colon);
  const value = candidate.slice(colon + 1);
  if (!/^@?[A-Za-z0-9._-]+$/.test(field)) return null;
  if (!field.startsWith("@") && !RESERVED_FIELDS.has(field)) return null;
  return { prefix, field, value };
}

function logQueryPills(query: string): LogQueryPill[] {
  const tokens = topLevelTokens(query);
  if (!tokens || tokens.some((token) => token.text === "OR")) return [];

  const pills: LogQueryPill[] = [];
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token.text === "AND") continue;

    const firstToken = index;
    let lastToken = index;
    let unaryPrefix = "";
    let filterToken = token;
    while (filterToken.text === "NOT" || filterToken.text === "-") {
      unaryPrefix += filterToken.text === "NOT" ? "NOT " : "-";
      const next = tokens[lastToken + 1];
      if (!next || next.text === "AND" || next.text === "OR") break;
      filterToken = next;
      lastToken += 1;
      index = lastToken;
    }

    const parsed = parseSimpleFilter(filterToken.text, unaryPrefix);
    if (!parsed) continue;

    let removeStart = tokens[firstToken].start;
    let removeEnd = tokens[lastToken].end;
    if (tokens[firstToken - 1]?.text === "AND") {
      removeStart = tokens[firstToken - 1].start;
    } else if (tokens[lastToken + 1]?.text === "AND") {
      removeEnd = tokens[lastToken + 1].end;
    }
    pills.push({
      ...parsed,
      removeStart,
      removeEnd
    });
  }
  return pills;
}

function removeLogQueryPill(query: string, pill: LogQueryPill) {
  const before = query.slice(0, pill.removeStart).trimEnd();
  const after = query.slice(pill.removeEnd).trimStart();
  if (!before) return after;
  if (!after) return before;
  return `${before} ${after}`;
}

export { logQueryPills, removeLogQueryPill };
export type { LogQueryPill };
