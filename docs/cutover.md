# Rewrite cutover migration

`maestro-migrate` converts legacy etcd resources and node-local telemetry, then
restores the converted etcd database into the rewrite's embedded-store layout.
The transition is one-way: rewrite daemons restart desired workloads under the
new runtime, and legacy API/runtime compatibility is not retained.

The logical snapshot, native snapshots, client private key, launch documents,
and secret files must be absolute, regular, owner-only files. The store-secret
file contains the exact legacy master-secret bytes; do not trim or append a
newline. Outputs use mode `0600` and are never overwritten.

Legacy telemetry does not share the rewrite's on-disk schema. Each node's probe
owns three DuckDB files and a manifest-committed Parquet tree under
`<cluster>/system/probe/data`; the rewrite owns versioned log and metric stores
under `<cluster>/agent`. The telemetry conversion is therefore a required part
of cutover. A cleanly stopped DuckDB may retain an adjacent `.wal` for any of
the three databases; planning fences that WAL and replays it read-only so its
uncheckpointed rows are migrated without changing the legacy source.

## Rehearsal

Use the same paths, configuration, secrets, binary versions, and node count
planned for production. Copy
[the production rehearsal evidence checklist](rehearsal-evidence.md), complete
it during the run, and archive it with the snapshot and verification artifacts.
Unchecked required items are a no-go; CI or local acceptance results do not
substitute for staging evidence.

1. Restore a verified production etcd snapshot into an isolated staging
   cluster. Keep all legacy and rewrite daemons stopped, start only legacy etcd,
   and allow legacy leases to expire.

2. On every stopped node, capture and review the exact node-local telemetry
   inventory:

   ```sh
   maestro-migrate telemetry-plan \
     --legacy-data-directory /var/lib/maestro/production/system/probe/data \
     --cluster-id production \
     --node-id node-a \
     --output /var/lib/maestro/cutover/node-a-telemetry-plan.json
   ```

   Repeat for every node. Retain the plans with both native etcd snapshots.

3. Capture one revision-consistent logical snapshot:

   ```sh
   maestro-migrate capture \
     --endpoint https://127.0.0.1:2379 \
     --certificate-authority /run/maestro/etcd-ca.pem \
     --client-certificate /run/maestro/etcd-client.pem \
     --client-private-key /run/maestro/etcd-client-key.pem \
     --output /var/lib/maestro/cutover/legacy-snapshot.json
   ```

4. Generate and review both secret-free plans:

   ```sh
   maestro-migrate plan \
     --snapshot /var/lib/maestro/cutover/legacy-snapshot.json \
     --master-secret-file /run/maestro/store-master-secret \
     --output /var/lib/maestro/cutover/plan-report.json

   maestro-migrate store-plan \
     --snapshot /var/lib/maestro/cutover/legacy-snapshot.json \
     --store-peer-port 2380 \
     --output /var/lib/maestro/cutover/store-plan.json
   ```

   Planning validates every known legacy key family and fails if leadership,
   requests, maintenance, or node-lifecycle work is still in progress. Compare
   the store plan's cluster ID, control-plane node IDs, addresses, and peer
   ports with the rewrite `maestro.jsonc` before proceeding. At first rewrite
   startup, the protected launch topology adopts the target hostname and role
   for migration-annotated nodes while preserving their labels and status; the
   control-plane address must still match exactly. Legacy nodes may use
   different dynamically allocated etcd ports; `--store-peer-port`
   deliberately normalizes every restored member onto the rewrite's shared
   peer port. A replica state without a current assignment remains invalid
   unless it is a superseded `CRASHED` placement. Those historical failures are
   archived on their owning Deployment, or on the Service when legacy history
   already pruned that Deployment, under the
   `migration.maestro.dev/legacy-orphan-replica-states` annotation rather than
   being reintroduced as runnable assignments. Legacy replica DNS and placement
   hostnames are validated case-insensitively because deployment-derived labels
   may contain uppercase ASCII; the rewrite regenerates the DNS projection.

5. Apply and independently verify the exact reviewed logical artifact:

   ```sh
   maestro-migrate apply \
     --endpoint https://127.0.0.1:2379 \
     --certificate-authority /run/maestro/etcd-ca.pem \
     --client-certificate /run/maestro/etcd-client.pem \
     --client-private-key /run/maestro/etcd-client-key.pem \
     --snapshot /var/lib/maestro/cutover/legacy-snapshot.json \
     --master-secret-file /run/maestro/store-master-secret \
     --migration-id legacy-v1

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

   Apply fences the live legacy digest before writes and immediately before its
   completion marker. After successful verification it disables the legacy
   etcd user database so the post-migration snapshot accepts rewrite-issued
   node certificates. Mutual TLS remains mandatory. Verification must run
   before rewrite daemons can mutate migrated resources.

6. While legacy etcd is still the only running process, take a second native
   snapshot containing the verified rewrite namespace:

   ```sh
   ETCDCTL_API=3 etcdctl \
     --endpoints=https://127.0.0.1:2379 \
     --cacert=/run/maestro/etcd-ca.pem \
     --cert=/run/maestro/etcd-client.pem \
     --key=/run/maestro/etcd-client-key.pem \
     snapshot save /var/lib/maestro/cutover/post-migration.db

   chmod 0600 /var/lib/maestro/cutover/post-migration.db
   etcdutl snapshot status \
     --write-out=table \
     /var/lib/maestro/cutover/post-migration.db
   ```

   The pre-migration native snapshot is the last abort point. Confirm the apply
   report uses schema version 3 and contains `legacyAuthWasEnabled` before
   taking the post-migration snapshot. That snapshot is the source for every
   rewrite store member.

7. Create one shared CA and one launch document per declared node. The operator
   secret is new; the store secret must be the exact secret used by migration:

   ```sh
   install -d -m 0700 \
     /var/lib/maestro-cutover-authority \
     /var/lib/maestro/cutover/launches

   umask 077
   openssl rand -hex 32 | tr -d '\n' \
     >/run/maestro/operator-jwt-secret

   maestro cluster init-ca \
     --config maestro.jsonc \
     --data-dir /var/lib/maestro-cutover-authority

   maestro cluster prepare-cutover \
     --config maestro.jsonc \
     --authority-data-dir /var/lib/maestro-cutover-authority \
     --data-dir /var/lib/maestro/production \
     --etcd-binary /run/current-system/sw/bin/etcd \
     --store-secret-file /run/maestro/store-master-secret \
     --operator-secret-file /run/maestro/operator-jwt-secret \
     --output-dir /var/lib/maestro/cutover/launches
   ```

   Securely copy each `<node-id>.launch.json` only to its named node. The
   documents are create-only and exact reruns verify rather than rotate their
   certificates or secrets.

8. Stop legacy etcd. On each control-plane node, restore the same
   post-migration snapshot into an otherwise empty rewrite data root:

   ```sh
   maestro-migrate store-restore \
     --snapshot /var/lib/maestro/cutover/legacy-snapshot.json \
     --native-snapshot /var/lib/maestro/cutover/post-migration.db \
     --node-id node-a \
     --data-directory /var/lib/maestro/production \
     --store-peer-port 2380 \
     --etcdutl-binary /run/current-system/sw/bin/etcdutl

   maestro-migrate store-verify \
     --snapshot /var/lib/maestro/cutover/legacy-snapshot.json \
     --native-snapshot /var/lib/maestro/cutover/post-migration.db \
     --node-id node-a \
     --data-directory /var/lib/maestro/production \
     --store-peer-port 2380 \
     --output /var/lib/maestro/cutover/node-a-store-verification.json
   ```

   Repeat for every control-plane node. Restore writes into a tool-owned partial
   directory, binds the result to both snapshot digests, seeds provider restart
   state, and atomically installs `<data-directory>/store`. Exact reruns are
   idempotent.

9. On every stopped node, apply and verify its reviewed telemetry plan:

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

   Interrupted log imports resume after the destination's validated contiguous
   sequence high-water mark. The migration skips that deterministic source
   prefix, appends only new rows, and still reprojects and verifies the complete
   source and destination before writing its completion marker.

10. Stop and remove the legacy Maestro workload and system containers without
    deleting their volumes or data directories. This is the one-way commit
    point: there is no supported downgrade to the legacy controller after
    workloads begin restarting under the rewrite runtime.

11. Start all restored control-plane daemons close enough together to establish
    quorum, then start worker daemons:

    ```sh
    maestro-daemon start /run/maestro/node-a.launch.json
    ```

    Verify cluster quorum, node identity, and the WireGuard mesh before
    releasing workload scheduling. Migrated nodes deliberately retain their
    `Maintenance/CutoverPending` condition until the operator restores each
    verified node:

    ```sh
    maestro cluster restore node-a
    maestro cluster restore node-b
    ```

    Restore nodes gradually and confirm assignment convergence after each
    command. The restore operation removes only the migration-owned cutover
    freeze; unrelated maintenance conditions remain intact. Then verify
    workload recreation, ingress, DNS, firewall, logs, metrics, and every
    service lifecycle operation in the
    [production rehearsal evidence checklist](rehearsal-evidence.md). Flip
    external DNS or ingress only after those checks pass.

## Production and recovery

Production repeats the rehearsed artifact flow exactly. Archive the logical
snapshot, both native snapshots, plan reports, per-node store and telemetry
verification, launch-document fingerprints, binary versions, and final parity
evidence.

Before the one-way commit point, an apply interrupted before its marker may be
rerun only if the corresponding source digest is unchanged. If the legacy etcd
source changed, restore the pre-migration native snapshot and restart capture.
If a telemetry source changed, archive the incomplete rewrite `agent`
directory, recapture that node, and review a new plan.

After the commit point, recovery always moves forward on the rewrite. An exact
store restore can be rerun; if an installed store is damaged, archive it,
restore every member again from the retained post-migration snapshot, and use
the same launch documents. There is no compatibility shim, workload adoption
mode, old-schema daemon mode, or control-plane-only rollback procedure to
maintain.
