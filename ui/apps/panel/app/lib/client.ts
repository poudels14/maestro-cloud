import {
  ApiHttpError,
  createApiClient,
  createFetchTransport,
  type ApiTransport,
  type MaestroApiClient
} from "@maestro/api-client";
import { apiErrorFromBody } from "./apiError";

const SESSION_UNAUTHENTICATED_EVENT = "maestro:session-unauthenticated";

function apiClient(): MaestroApiClient {
  const transport = createFetchTransport(location.origin);
  return createApiClient(notifyWhenUnauthenticated(transport));
}

function notifyWhenUnauthenticated(transport: ApiTransport): ApiTransport {
  return {
    async request(request) {
      try {
        return await transport.request(request);
      } catch (error) {
        if (isUnauthenticated(error)) {
          window.dispatchEvent(new Event(SESSION_UNAUTHENTICATED_EVENT));
        }
        throw error;
      }
    }
  };
}

function isUnauthenticated(error: unknown): boolean {
  return error instanceof ApiHttpError && error.status === 401;
}

function apiRequestError(error: unknown, fallback: string): Error {
  if (!(error instanceof ApiHttpError)) {
    return error instanceof Error ? error : new Error(fallback);
  }
  return apiErrorFromBody(error.status, error.body, fallback);
}

export { SESSION_UNAUTHENTICATED_EVENT, apiClient, apiRequestError, isUnauthenticated };
