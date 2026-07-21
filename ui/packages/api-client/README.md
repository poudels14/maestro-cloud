# @maestro/api-client

`kernel-api` is the component-schema source of truth. `apps/server` composes
route operations over those schemas. `pnpm generate` writes the deterministic
OpenAPI document and regenerates `src/schema.d.ts` with `openapi-typescript`;
do not edit either generated file directly.

The handwritten `src/index.ts` layer contains only stable resource aliases and
transport primitives.
