export interface TransportRequest<Response, Body = never> {
  method: "DELETE" | "GET" | "PATCH" | "POST" | "PUT";
  path: string;
  body?: Body;
  headers?: Readonly<Record<string, string>>;
  signal?: AbortSignal;
  decode: (response: globalThis.Response) => Promise<Response>;
}

export interface ApiTransport {
  request<Response, Body = never>(request: TransportRequest<Response, Body>): Promise<Response>;
}

export class ApiHttpError extends Error {
  readonly status: number;
  readonly body: string;

  constructor(status: number, body: string) {
    super(`Maestro API request failed with HTTP ${status}`);
    this.name = "ApiHttpError";
    this.status = status;
    this.body = body;
  }
}

export function createFetchTransport(
  baseUrl: string | URL,
  fetcher: typeof globalThis.fetch = globalThis.fetch
): ApiTransport {
  const base = new URL(baseUrl);
  return {
    async request<Response, Body = never>(
      request: TransportRequest<Response, Body>
    ): Promise<Response> {
      const headers = new Headers(request.headers);
      const hasBody = request.body !== undefined;
      if (hasBody && !headers.has("content-type")) {
        headers.set("content-type", "application/json");
      }
      const init: RequestInit = {
        method: request.method,
        headers
      };
      if (hasBody) {
        init.body = JSON.stringify(request.body);
      }
      if (request.signal !== undefined) {
        init.signal = request.signal;
      }
      const response = await fetcher(new URL(request.path, base), init);
      if (!response.ok) {
        throw new ApiHttpError(response.status, await response.text());
      }
      return request.decode(response);
    }
  };
}

export async function decodeJson<Response>(response: globalThis.Response): Promise<Response> {
  return (await response.json()) as Response;
}

export interface ApiRequestOptions {
  headers?: Readonly<Record<string, string>>;
  signal?: AbortSignal;
}
