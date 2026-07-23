# Rewrite cutover migration

`maestro-migrate` captures, reviews, and applies the one-shot etcd schema
conversion. It writes only the rewrite namespace and commits its migration
marker last. A native etcd snapshot remains the rollback artifact; the logical
snapshot described here is the reviewed migration input.

The snapshot, client private key, and secret files must be absolute, regular,
owner-only files. The master-secret file contains the exact legacy/new store
secret bytes: do not append a newline. Outputs are installed with mode `0600`
and are never overwritten.

## Rehearsal

1. Restore a verified production etcd snapshot into an isolated staging
   cluster. Keep all legacy and rewrite daemons stopped and allow legacy leases
   to expire.
2. Capture one revision-consistent logical snapshot:

   ```sh
   maestro-migrate capture \
     --endpoint https://127.0.0.1:2379 \
     --certificate-authority /run/maestro/etcd-ca.pem \
     --client-certificate /run/maestro/etcd-client.pem \
     --client-private-key /run/maestro/etcd-client-key.pem \
     --output /var/lib/maestro/cutover/legacy-snapshot.json
   ```

3. Generate and review the secret-free plan report. The command validates every
   known legacy key family and fails if leadership, requests, maintenance, or
   node lifecycle work is still in progress.

   ```sh
   maestro-migrate plan \
     --snapshot /var/lib/maestro/cutover/legacy-snapshot.json \
     --master-secret-file /run/maestro/store-master-secret \
     --output /var/lib/maestro/cutover/plan-report.json
   ```

4. Apply the exact reviewed artifact:

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

5. While every daemon is still stopped, independently verify the completion
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

6. Boot the rewrite daemons and complete the parity/adoption checklist. Do not
   treat a successful schema migration as approval for a runtime transition;
   the production runtime adoption mode is a separate cutover gate.

## Production and recovery

Immediately before production capture, take and verify a native etcd snapshot,
then stop every legacy daemon while leaving etcd available. Retain that native
snapshot, the logical snapshot, plan report, old binaries, and migration output
through the burn-in window. Retain the verification evidence alongside them.

Destination writes are collision-safe and resumable. If apply stops before its
marker and the live source digest is unchanged, rerun the same artifact with the
same migration ID. If the source digest changed, do not plan a different
artifact over partial destinations: restore the native rollback snapshot first,
re-establish quiescence, and restart capture.

Rollback remains: stop rewrite daemons, restore the verified native etcd
snapshot, and start the old daemons. Whether workloads can remain running during
that operation depends on the separately approved runtime-adoption mode.
