# Rewrite cutover migration

`maestro-migrate` captures, reviews, and applies the one-shot etcd schema
conversion. It writes only the rewrite namespace and commits its migration
marker last. A native etcd snapshot remains the rollback artifact; the logical
snapshot described here is the reviewed migration input.

The snapshot, client private key, and secret files must be absolute, regular,
owner-only files. The master-secret file contains the exact legacy/new store
secret bytes: do not append a newline. Outputs are installed with mode `0600`
and are never overwritten.

Legacy telemetry does not share the rewrite's on-disk schema. Each node's probe
owns three DuckDB files and a manifest-committed Parquet tree under
`<cluster>/system/probe/data`; the rewrite owns versioned log and metric stores
under `<cluster>/agent`. Treating those files as interchangeable would silently
start empty observability stores.

## Rehearsal

1. Restore a verified production etcd snapshot into an isolated staging
   cluster. Keep all legacy and rewrite daemons stopped and allow legacy leases
   to expire.
2. On every stopped node, capture and review the exact node-local telemetry
   inventory. The command opens all three DuckDB files read-only, verifies every
   Parquet object against its commit manifest, rejects temporary/uncommitted
   files, and binds the ordered file inventory into `sourceSha256`:

   ```sh
   maestro-migrate telemetry-plan \
     --legacy-data-directory /var/lib/maestro/production/system/probe/data \
     --cluster-id production \
     --node-id node-a \
     --output /var/lib/maestro/cutover/node-a-telemetry-plan.json
   ```

   Repeat this for every node. Retain these artifacts with the native etcd
   snapshot; they are the input fences for node-local telemetry conversion.

3. Capture one revision-consistent logical snapshot:

   ```sh
   maestro-migrate capture \
     --endpoint https://127.0.0.1:2379 \
     --certificate-authority /run/maestro/etcd-ca.pem \
     --client-certificate /run/maestro/etcd-client.pem \
     --client-private-key /run/maestro/etcd-client-key.pem \
     --output /var/lib/maestro/cutover/legacy-snapshot.json
   ```

4. Generate and review the secret-free plan report. The command validates every
   known legacy key family and fails if leadership, requests, maintenance, or
   node lifecycle work is still in progress. Successful legacy `up` deployments
   are frozen to the image they already built because the old controller
   intentionally deleted their one-use source archives. An incomplete upload
   without a resolved image must finish or be removed before capture.

   ```sh
   maestro-migrate plan \
     --snapshot /var/lib/maestro/cutover/legacy-snapshot.json \
     --master-secret-file /run/maestro/store-master-secret \
     --output /var/lib/maestro/cutover/plan-report.json
   ```

5. Apply the exact reviewed artifact:

   ```sh
   maestro-migrate apply \
     --endpoint https://127.0.0.1:2379 \
     --certificate-authority /run/maestro/etcd-ca.pem \
     --client-certificate /run/maestro/etcd-client.pem \
     --client-private-key /run/maestro/etcd-client-key.pem \
     --snapshot /var/lib/maestro/cutover/legacy-snapshot.json \
     --master-secret-file /run/maestro/store-master-secret \
     --migration-id legacy-v1
   ```

   Apply compares the live legacy digest before any destination writes and
   recaptures it immediately before the completion marker. Success prints an
   `applied` or `alreadyComplete` outcome, the same plan report, and an exact
   destination verification.

6. While every daemon is still stopped, independently verify the completion
   marker and every migrated resource and request barrier. Archive the
   secret-free evidence file with the reviewed plan:

   ```sh
   maestro-migrate verify \
     --endpoint https://127.0.0.1:2379 \
     --certificate-authority /run/maestro/etcd-ca.pem \
     --client-certificate /run/maestro/etcd-client.pem \
     --client-private-key /run/maestro/etcd-client-key.pem \
     --snapshot /var/lib/maestro/cutover/legacy-snapshot.json \
     --master-secret-file /run/maestro/store-master-secret \
     --migration-id legacy-v1 \
     --output /var/lib/maestro/cutover/verification.json
   ```

   Verification fails if the marker is absent or bound to another snapshot, or
   if any planned logical value is absent or changed, or if unreviewed state
   exists in the destination namespace. Run it before starting rewrite daemons
   because reconcilers legitimately update migrated resources.

7. On every stopped node, apply and independently verify the reviewed
   telemetry plan:

   ```sh
   maestro-migrate telemetry-apply \
     --plan /var/lib/maestro/cutover/node-a-telemetry-plan.json \
     --legacy-data-directory /var/lib/maestro/production/system/probe/data \
     --data-directory /var/lib/maestro/production

   maestro-migrate telemetry-verify \
     --plan /var/lib/maestro/cutover/node-a-telemetry-plan.json \
     --legacy-data-directory /var/lib/maestro/production/system/probe/data \
     --data-directory /var/lib/maestro/production \
     --output /var/lib/maestro/cutover/node-a-telemetry-verification.json
   ```

   Apply creates an owner-only intent before opening the rewrite stores and
   installs its completion marker only after recapturing the source and matching
   an ordered logical digest of every destination record. Exact reruns are
   idempotent. Workload and host resource samples are normalized into the
   rewrite's cumulative metric model; cluster/service aggregates are regenerated
   from workload samples, and retired Prometheus traffic aggregates remain in
   the source archive because rewrite traffic history is derived from access
   logs.

8. Boot the rewrite daemons and complete the parity/adoption checklist. Do not
   treat a successful schema migration as approval for a runtime transition;
   the production runtime adoption mode is a separate cutover gate.

## Production and recovery

Immediately before production capture, take and verify a native etcd snapshot,
then stop every legacy daemon while leaving etcd available. Retain that native
snapshot, the logical snapshot, plan report, old binaries, and migration output
through the burn-in window. Retain the verification evidence alongside them.

Etcd and telemetry destination writes are collision-safe and resumable. If
either apply stops before its marker and the corresponding source digest is
unchanged, rerun the same artifact. If an etcd source digest changed, do not
plan a different artifact over partial destinations: restore the native
rollback snapshot first, re-establish quiescence, and restart capture. If a
node-local telemetry source changed, retain its untouched legacy telemetry
directory, move the incomplete rewrite `agent` directory aside, then recapture
and review that node before retrying.

Rollback remains: stop rewrite daemons, restore the verified native etcd
snapshot, leave each legacy telemetry directory in place, and start the old
daemons. The rewrite `agent` stores are isolated and can remain offline as
forensic evidence. Whether workloads can remain running during that operation
depends on the separately approved runtime-adoption mode.
