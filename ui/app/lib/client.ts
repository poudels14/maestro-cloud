import {
  ApiHttpError,
  createApiClient,
  createFetchTransport,
  type MaestroApiClient
} from "@maestro/api-client";
import { apiErrorFromBody } from "./apiError";

function apiClient(): MaestroApiClient {
  return createApiClient(createFetchTransport(location.origin));
}

function apiRequestError(error: unknown, fallback: string): Error {
  if (!(error instanceof ApiHttpError)) {
    return error instanceof Error ? error : new Error(fallback);
  }
  return apiErrorFromBody(error.status, error.body, fallback);
}

export { apiClient, apiRequestError };
