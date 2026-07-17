# Multi-node Maestro

Maestro uses the EC2 private network for its control API, consensus traffic, and
the internal workload gateway. Tailscale remains optional and is not part of
cluster correctness.

## Configuration contract

Declare every planned node in one shared `cluster.nodes` map and select the local
entry with the top-level `node` string:

```jsonc
{
  "node": "node1",
  "cluster": {
    "name": "prod",
    "nodes": {
      "node1": {
        "endpoint": "10.20.0.11",
        "subnet": "10.1.0.0/24",
        "role": "master"
      },
      "node2": {
        "endpoint": "10.20.0.12:3100",
        "subnet": "10.2.0.0/24"
      },
      "node3": {
        "endpoint": "10.20.0.13",
        "subnet": "10.3.0.0/24",
        "role": "voter"
      }
    },
    "image-registry": "ghcr.io/acme",
    "join-secret": "<high-entropy-secret-of-at-least-32-characters>"
  },
  "ingress": { "port": 8080 },
  "jwt-secret-key": "<at-least-32-character-jwt-secret>",
  "encryption-key": "<encryption-key>",
  "runtime": "docker"
}
```

On the second machine, use the same cluster map and set `"node": "node2"`.
The node entry supplies its workload subnet, private control address, optional
API port, and optional role. The role defaults to `hybrid`; exactly one node must
still explicitly use `master`. When the endpoint has no port, the API defaults
to `3000`.

`cluster.control-allow-cidrs` is optional defense in depth. When configured, it
restricts signed join requests to those private source ranges and must include
every configured control endpoint. When omitted, the high-entropy join secret,
the private source-address check, and live endpoint/subnet reservations remain
the admission boundary.

This is a breaking config format. The old node array, top-level cluster subnet,
`node.role`, and explicit cluster gateway/etcd port fields are not accepted.
Standalone installations continue to use a top-level `subnet` and omit both
`cluster.nodes` and the top-level `node` selector.

The shared map lets Maestro reject duplicate API endpoints, overlapping workload
subnets, workload/control-network overlap, missing control CIDR coverage, and an
invalid voter count before startup. Node names are lowercase DNS labels. Every
cluster must have exactly one `master`, and the total number of master, hybrid,
and voter nodes must be one or three.

## Roles and bootstrap

- `master` is the one-time bootstrap authority. It votes and runs workloads.
- `hybrid` votes and runs workloads.
- `voter` runs the control plane without workloads.
- `worker` runs workloads without a local etcd member.

The master starts a one-member etcd cluster immediately. It does not wait for an
inbound connectivity check and it does not retain permanent leadership. Once the
cluster is formed, ordinary lease-based election selects the leader.

Additional voters join as learners and are promoted after catching up. A signed
join using `cluster.join-secret` is the admission authority, so adding a node does
not require restarting existing voters. The live etcd member list and signed port
reservations are authoritative after bootstrap; `cluster.nodes` remains the
shared desired-membership and subnet preflight map.

## Ports and AWS security groups

The API port is the endpoint port or `3000`. On first startup, each node asks the
operating system for three distinct unused TCP ports for the gateway, etcd client,
and etcd peer listeners. Maestro writes them to:

```text
<cluster-data-dir>/system/cluster-ports.json
```

Those ports are reused on restart and advertised in the signed join request.
They are not derived from the API port and must not be edited after a node has
joined.

AWS security groups apply to private traffic too. Allow these inbound flows from
the cluster's security group (or a narrowly scoped private CIDR):

- the configured API port on every node;
- the persisted gateway port on master, hybrid, and worker nodes;
- the persisted etcd client port on voters;
- the persisted etcd peer port between voters.

Do not expose etcd publicly. Maestro binds these listeners to the selected private
endpoint IP and uses cluster-issued mTLS, but it does not modify the host firewall
or the EC2 security group.

At startup Maestro logs the selected ports. The master's admin UI also displays
the effective ports and a formation banner while fewer live nodes are registered
than the shared map declares. This allows the master to start and report the exact
security-group changes even when inbound traffic is initially blocked.

## Workload subnet rules

Every node gets one canonical private IPv4 `/24`. These subnets must be unique and
non-overlapping across the shared map. They also must not overlap any control CIDR
or contain a configured EC2 private endpoint.

The workload subnet is an internal container address pool behind that node's
gateway; it is not an AWS subnet and AWS does not assign those addresses to EC2
interfaces. Ranges such as `10.1.0.0/24`, `10.2.0.0/24`, and `10.3.0.0/24` are
valid only when the VPC/control ranges and other routed networks do not overlap
them.

Maestro reserves the gateway and fixed-address system containers near the high
end of each `/24`; workload replicas use the remaining addresses. Cross-node
requests enter through the destination node's mTLS gateway rather than routing
container subnets through Tailscale.

## Form a cluster

1. Put the shared map and secrets in the common config source.
2. Give each instance a small config that extends the common source and sets only
   its node name:

   ```jsonc
   {
     "$extends": "aws-secret://maestro/production/common",
     "node": "node2"
   }
   ```

3. Start the master. It creates the cluster identity, CA, certificates, persisted
   ports, and bootstrap permit under the daemon lock.
4. Open the ports shown in its log/admin UI from the private cluster security
   group.
5. Start the other nodes. Each authenticates CA discovery with the join secret,
   advertises its actual ports and subnet, and installs the returned identity.
6. Verify formation with `maestro cluster info`, `maestro cluster nodes`, and the
   admin UI.

For a three-voter cluster, avoid interrupting the temporary two-voter stage; both
members are required until the third voter is promoted.

## Add or replace a node

Add the named entry to the shared map, deploy that effective config to the new
machine, and set its top-level `node` selector. Existing voters do not need a
restart. The leader validates live endpoint/subnet reservations and updates the
authoritative voter cache after a voter joins.

Remove or drain an old node through the normal cluster lifecycle before reusing
its name, endpoint, or subnet. Removed node identities cannot silently rejoin.

## Registry and scheduling

Multi-node mode requires `cluster.image-registry`. It is a base image
namespace: Maestro appends `/<service-id>:<deployment-id>`. Workload nodes must
be able to pull from it, and any node that can lead builds must be able to push.
Configure runtime registry credentials independently on every host.

The scheduler places workloads on ready `master`, `hybrid`, and `worker` nodes.
Dedicated voters are never placement targets. Hard `deploy.nodeAffinity` rules
continue to apply; writable host volumes require an exact node ID because their
data cannot move between machines.
