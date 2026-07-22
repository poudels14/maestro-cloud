interface ApiErrorPayload {
  code?: string;
  message?: string;
  details?: Record<string, unknown>;
}

class ApiRequestError extends Error {
  readonly status: number;
  readonly code: string | null;
  readonly details: Record<string, unknown> | null;

  constructor(
    message: string,
    options: {
      status: number;
      code?: string | null;
      details?: Record<string, unknown> | null;
    }
  ) {
    super(message);
    this.name = "ApiRequestError";
    this.status = options.status;
    this.code = options.code ?? null;
    this.details = options.details ?? null;
  }
}

function apiErrorFromBody(status: number, body: string, fallback: string) {
  let error: ApiErrorPayload | null = null;
  if (body) {
    try {
      const parsed = JSON.parse(body) as { error?: ApiErrorPayload | string };
      error =
        typeof parsed.error === "string"
          ? { message: parsed.error }
          : parsed.error && typeof parsed.error === "object"
            ? parsed.error
            : null;
    } catch {
      error = { message: body };
    }
  }
  return new ApiRequestError(error?.message || fallback, {
    status,
    ...(error?.code !== undefined ? { code: error.code } : {}),
    ...(error?.details !== undefined ? { details: error.details } : {})
  });
}

export { ApiRequestError, apiErrorFromBody };
