# @maestro/api-client

`kernel-api` is the source of truth. `pnpm generate` writes the deterministic
OpenAPI document and regenerates `src/schema.d.ts` with `openapi-typescript`.
Do not edit either generated file directly.

The handwritten `src/index.ts` layer contains only stable resource aliases and
transport primitives. Route-specific methods will be generated once the server
composes path operations into the shared OpenAPI document.
