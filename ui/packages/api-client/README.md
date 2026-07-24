# @maestro/api-client

`kernel-api` is the component-schema source of truth. `apps/server` composes
route operations over those schemas. `pnpm generate` writes the deterministic
OpenAPI document and regenerates `src/schema.d.ts` with `openapi-typescript`;
do not edit either generated file directly.

The handwritten layer contains stable resource aliases, transport primitives,
and the JSON operator API facade used by the panel. Its operation-coverage test
compares that facade with the generated document. Health, OpenAPI discovery,
binary artifact upload, and node bootstrap/join stay explicit exclusions
because they use non-panel response or transport contracts.
