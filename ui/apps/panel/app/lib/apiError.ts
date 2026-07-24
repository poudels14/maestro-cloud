interface ApiErrorPayload {
  code: string;
  message: string;
}

class ApiRequestError extends Error {
  readonly status: number;
  readonly code: string | null;

  constructor(
    message: string,
    options: {
      status: number;
      code?: string | null;
    }
  ) {
    super(message);
    this.name = "ApiRequestError";
    this.status = options.status;
    this.code = options.code ?? null;
  }
}

function apiErrorFromBody(status: number, body: string, fallback: string) {
  const error = parseApiError(body);
  return new ApiRequestError(error?.message ?? fallback, {
    status,
    ...(error ? { code: error.code } : {})
  });
}

function parseApiError(body: string): ApiErrorPayload | null {
  try {
    const parsed = JSON.parse(body) as unknown;
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      "code" in parsed &&
      typeof parsed.code === "string" &&
      parsed.code.length > 0 &&
      "message" in parsed &&
      typeof parsed.message === "string" &&
      parsed.message.length > 0
    ) {
      return { code: parsed.code, message: parsed.message };
    }
  } catch {
    return null;
  }
  return null;
}

export { ApiRequestError, apiErrorFromBody };
