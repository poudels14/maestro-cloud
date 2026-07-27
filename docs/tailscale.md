# Tailscale operator access

Maestro can manage a highly available Tailscale subnet-router fleet as ordinary
cluster resources. It gives approved tailnet clients routed access to Maestro
workload addresses and split DNS without making Tailscale part of cluster
correctness.

The gateway service:

- runs in Tailscale userspace-networking mode without host networking or extra
  Linux capabilities;
- uses a digest-pinned Tailscale image;
- best-effort spreads replica-managed state across workload-capable nodes,
  including rebalancing co-located replicas after a node joins or returns;
- advertises only routes inside the cluster CIDR; and
- is created, updated, or removed with its scoped firewall policy under the
  active controller leadership fence.

Loss of every gateway replica interrupts tailnet access only. Cluster
membership, consensus, scheduling, DNS inside workloads, and WireGuard
east-west traffic continue independently.

## Prepare the tailnet

Define the gateway tag before generating a tagged key. The following HuJSON
fragment lets tagged gateways auto-approve one cluster pool and grants a
specific operator group access to that pool:

```json
{
  "groups": {
    "group:maestro-operators": [
      "alice@example.com",
      "bob@example.com",
    ],
  },
  "tagOwners": {
    "tag:maestro-gateway": ["autogroup:admin"],
  },
  "autoApprovers": {
    "routes": {
      "172.22.0.0/16": ["tag:maestro-gateway"],
    },
  },
  "grants": [
    {
      "src": ["group:maestro-operators"],
      "dst": ["172.22.0.0/16"],
      "ip": ["*"],
    },
  ],
}
```

Use the cluster's exact CIDR instead of the example. Tailnet access policy is
evaluated against the advertised subnet destination, not the gateway device's
tag, so grant only the source identities and destination ports that operators
need. Tailscale recommends grants for new network access rules.

If you omit `autoApprovers`, approve the advertised routes for every active
gateway in the Tailscale admin console. Adding an auto-approver later does not
retroactively approve an existing advertisement; remove and re-advertise it or
approve it manually.

Generate an auth key with these properties:

- reusable, because each gateway replica has its own Tailscale identity;
- pre-approved when the tailnet requires device approval;
- tagged with `tag:maestro-gateway`; and
- non-ephemeral, because each replica persists its identity.

Store a reusable key in a secrets manager. Tailscale auth keys expire after at
most 90 days, but gateways already authenticated with persisted state continue
using their node identity. Tagged-device key expiry is disabled by default.
Rotate the auth key before a new or stateless replica needs it.

See Tailscale's current
[auth key](https://tailscale.com/docs/features/access-control/auth-keys),
[policy syntax](https://tailscale.com/kb/1337/policy-syntax), and
[subnet-router](https://tailscale.com/kb/1104/enable-ip-forwarding)
documentation when adapting the policy to a production tailnet.

## Enable the gateway

Add `tailscale` beside `cluster` and `node` in the shared cluster document:

```jsonc
{
  "cluster": {
    "name": "prod",
    "cluster-cidr": "172.22.0.0/16",
    // nodes, ports, allowlists, and join-secret omitted
  },
  "tailscale": {
    "auth-key": "aws-secret://maestro/production/tailscale-auth-key",
    "advertise-routes": null,
    "replicas": 2,
    "tags": ["tag:maestro-gateway"],
    "cross-cluster-dns": [
      {
        "cluster-id": "staging",
        "nameservers": ["172.23.1.1", "172.23.2.1"]
      }
    ]
  },
  "node": "node-1"
}
```

`auth-key` may contain a literal value or reference a local `file://` or
`aws-secret://` string source. It is resolved while creating protected launch
documents and is never returned by the cluster config API.

The defaults are:

- `advertise-routes: null`, which advertises the complete cluster CIDR;
- `replicas: 2`; and
- `tags: ["tag:maestro-gateway"]`; and
- `cross-cluster-dns: []`, which disables remote suffix forwarding.

Every explicit advertised route must be unique, canonical, contained by the
cluster CIDR, and include at least one workload bridge resolver. Replica count
cannot exceed the number of workload-capable nodes. Set `replicas` to `1` for a
single-node cluster. Set `tags` to `[]` when the auth key is intentionally
untagged; tagged gateways remain the recommended production configuration.

Validate the merged config before bootstrap:

```sh
maestro config validate /etc/maestro/maestro.jsonc
```

The active leader reconciles these reserved resources:

- Service `maestro-system-tailscale-gateway`; and
- FirewallPolicy `maestro-system-tailscale-egress`.

Those `maestro-system-*` IDs cannot be created, changed, or deleted through
normal user resource mutations.

## Configure split DNS

After the gateway replicas are healthy and their routes are approved, inspect
the secret-free effective configuration:

```sh
maestro cluster config
```

Use every address in `tailscale.dnsNameservers` as a restricted nameserver for
`maestro.internal` in the tailnet DNS settings. For example:

```json
{
  "tailscale": {
    "advertiseRoutes": ["172.22.0.0/16"],
    "dnsNameservers": ["172.22.1.1", "172.22.2.1"],
    "replicas": 2,
    "tags": ["tag:maestro-gateway"],
    "crossClusterDns": [
      {
        "clusterId": "staging",
        "nameservers": ["172.23.1.1", "172.23.2.1"]
      }
    ]
  }
}
```

These are node workload-bridge addresses routed through the subnet routers.
They are not Tailscale `100.x` addresses and are not fixed `.254` proxy
addresses. Configure more than one returned resolver so DNS remains available
during a node failure.

Each managed gateway also exposes the authenticated HTTPS API and panel
directly on its Tailscale identity with Tailscale Serve. This is the preferred
operator path because it does not depend on client subnet-route settings and
uses a certificate valid for the gateway's MagicDNS name. Find either online
`maestro-<cluster-name>-gateway-<replica>` device in Tailscale and open:

```text
https://<gateway MagicDNS name>/
```

For example, the first replica of cluster `production` requests the hostname
`maestro-production-gateway-0`. Tailscale may append a collision suffix when a
retired device still owns that name, so use the actual MagicDNS name shown for
the online device. Sign in with a Maestro operator token.

The daemon also serves the same panel on each returned bridge address. Node
certificates include both the control-plane endpoint and the bridge address,
and Maestro admits bridge-to-API traffic only from running managed Tailscale
gateway replicas. This subnet-routed fallback is:

```text
https://<dnsNameserver>:<cluster API port>/
```

For example, a node whose bridge resolver is `172.22.1.1` and whose API port is
`3000` serves the panel at `https://172.22.1.1:3000/`. Trust the private Maestro
cluster CA in the operator browser when using this fallback.

In the Tailscale admin console, add each address as a custom nameserver and
restrict it to `maestro.internal`. Do not make it a global nameserver unless
that is an intentional tailnet-wide DNS policy. Tailscale documents this model
as [restricted nameservers (split DNS)](https://tailscale.com/docs/reference/dns-in-tailscale).

## Configure cross-cluster DNS

Each `cross-cluster-dns` entry delegates exactly
`<cluster-id>.maestro.internal` to the listed bridge resolvers. The local
resolver forwards those queries over DNS/TCP through a ready managed Tailscale
gateway's SOCKS5 listener. It still refuses every undeclared suffix and never
performs general recursion.

Use resolver addresses returned by `maestro cluster config` on the remote
cluster. They must be private addresses outside the local cluster CIDR. Add at
least two addresses when the remote cluster has multiple workload nodes.
Tailscale gateways accept remote subnet routes automatically; the routes must
also be approved in the tailnet policy.

Cross-cluster discovery fails closed with `SERVFAIL` when no managed gateway or
remote resolver is reachable. Local cluster DNS continues to answer from its
store-fed authoritative snapshot.

## Verify access

From an allowed tailnet client:

1. Confirm the client accepted the advertised cluster route.
2. Query one returned bridge resolver for a known `*.maestro.internal` record.
3. Resolve the same name through the client's normal resolver.
4. Connect to the workload address and port allowed by the tailnet and Maestro
   firewall policies.
5. Stop one gateway replica and repeat the route, DNS, and application checks.

Tailscale route availability and Maestro application authorization are separate
layers. A working subnet route does not override a Maestro FirewallPolicy, and
a Maestro allow rule does not grant a tailnet identity access to the subnet.

## Rotation and recovery

Each active rollout replica owns an isolated `tailscale-state` volume. The auth
key is loaded on every launch so an incomplete state file cannot suppress
authentication. `TS_AUTH_ONCE` makes ordinary restarts reuse the existing node
identity instead of authenticating it again.

For auth-key rotation:

1. create a replacement key with the same approved tags;
2. replace the value in its protected local file or AWS Secrets Manager secret;
3. submit that source with a stable idempotency key:

   ```sh
   maestro cluster rotate-tailscale-key \
     --auth-key-source aws-secret://maestro/production/tailscale-auth-key \
     --idempotency-key tailscale-auth-2026-07
   ```

4. wait for every gateway replica to become healthy and approve its advertised
   route if the tailnet does not use a matching `autoApprovers` rule; and
5. revoke the old auth key and retired gateway devices.

The CLI reads the source locally, sends the key only over the authenticated
HTTPS API, and never prints it. The API stores one optimistic override in the
encrypted cluster store. The active fenced leader watches that record and
updates the reserved gateway Service. A credential change creates a new
rollout, so every replacement replica receives fresh managed state and
authenticates as a new Tailscale device. Healthy old replicas remain available
during the rollout.

The live override supersedes the original launch-document key across leadership
changes without rewriting launch documents. Reusing the same idempotency key
after an ambiguous transport failure replays the original receipt; a concurrent
rotation fails on its observed override revision instead of silently replacing
another operator's key.

If all gateways are unavailable, use SSH or a bastion on the private host
network, point a CLI context at a private HTTPS API endpoint, and diagnose:

```sh
maestro cluster nodes
maestro cluster config
maestro services ls
```

Check in this order:

- gateway replica placement and health;
- route approval or matching `autoApprovers`;
- tailnet grants for the advertised CIDR;
- reachability of a returned bridge resolver; and
- Maestro firewall policy for the target workload.

Do not change cluster networking to recover remote access. Restore the optional
gateway path while keeping the private API and WireGuard cluster plane intact.
