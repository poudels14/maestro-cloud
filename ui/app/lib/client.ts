import {
  ApiHttpError,
  createApiClient,
  createFetchTransport,
  type MaestroApiClient
} from "@maestro/api-client";

function apiClient(): MaestroApiClient {
  return createApiClient(createFetchTransport(location.origin));
}

function apiRequestError(error: unknown, fallback: string): Error {
  if (!(error instanceof ApiHttpError)) {
    return error instanceof Error ? error : new Error(fallback);
  }
  let message = error.body || fallback;
  try {
    const payload = JSON.parse(error.body) as {
      error?: { message?: string } | string;
    };
    message = typeof payload.error === "string" ? payload.error : payload.error?.message || message;
  } catch {
    // Preserve a non-JSON response body from the API proxy.
  }
  return new Error(message);
}

export { apiClient, apiRequestError };
