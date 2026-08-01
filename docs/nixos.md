# NixOS deployment

The flake exposes Maestro through these production entry points:

- `packages.<system>.default` installs `maestro`, `maestro-daemon`,
  `maestro-migrate`, and the static panel served by the daemon;
- `packages.<system>.panel` exposes the static panel separately for packaging
  inspection;
- `packages.<system>.static`, `release-bundle`, `daemon-image`, and
  `daemon-image-bundle` expose Linux release artifacts;
- `apps.<system>.default`, `daemon`, and `migrate` run the shipped
  binaries; and
- `nixosModules.default` defines `services.maestro`.

## Host configuration

Import the module, pass the live cluster config source, and keep the node
bootstrap document outside the Nix store:

```nix
{
  inputs.maestro.url = "path:/path/to/maestro-cloud";

  outputs = {nixpkgs, maestro, ...}: {
    nixosConfigurations.rehearsal = nixpkgs.lib.nixosSystem {
      modules = [
        maestro.nixosModules.default
        ({...}: {
          services.maestro = {
            enable = true;
            config = "aws-secret://maestro/production/cluster";
            launch = "/run/maestro/launch.json";
          };

          networking.firewall.enable = true;
          networking.firewall.allowedTCPPorts = [ 53 2379 2380 3000 ];
          networking.firewall.allowedUDPPorts = [ 53 51820 ];
        })
      ];
    };
  };
}
```

`services.maestro.config` is a source identifier accepted by `maestro config
validate`, such as an `aws-secret://` reference. Only the identifier enters the
Nix expression; the daemon fetches and resolves its current value on every
start. `services.maestro.launch` is the absolute path to the owner-only node
bootstrap document, conventionally `/run/maestro/launch.json`. It contains an
encrypted node identity and local paths, not a cached resolved cluster config.
`services.maestro.extraArgs` remains available for non-secret daemon arguments;
never place credentials there because Nix-built service definitions are not a
secret store. For compatibility, a one-node deployment may provide
`extraArgs = ["--subnet" "10.202.0.0/16"];`. This value only fills an omitted
subnet in a one-node config. An explicit config subnet takes precedence, and a
multi-node config ignores the one-node fallback.

The config must define a stable `encryption-key` of at least 32 characters.
Maestro deterministically derives distinct keys for local node-bootstrap and
etcd encryption domains. Do not change it until a supported re-encryption
rotation workflow is available.

The module does not enable or modify the NixOS host firewall. Maestro owns its
runtime nftables table, and deployments that enable another host firewall must
configure it separately so it does not block the workload bridge or cluster
control traffic. The example allows the default node API, etcd, WireGuard, and
workload DNS ports. DNS needs both UDP and TCP port `53`; allowing only UDP can
break fallback and larger responses. Use the cluster's configured ports when
they differ from the defaults, restrict API and etcd traffic with cloud
security groups, and never expose etcd publicly. The DNS listener is bound to
the local workload bridge, so no public port `53` security-group rule is
required.

The daemon and migration tool write newline-delimited JSON diagnostics to
stderr. Under systemd, these records flow directly into the journal with
structured `level`, `message`, `error`, and resource identity fields. The
`RUST_LOG` environment variable selects tracing directives and defaults to
`info`; launch configuration, secret values, and full environment maps are
never emitted.

Control-plane bootstrap documents should use
`/run/current-system/sw/bin/etcd` for `etcdBinary`. The module installs the
selected etcd package into the system profile, enables native containerd, and
starts the containerd-backed BuildKit worker. It also installs Depot and exposes
the nftables, network, Git, and NixOS tools used by Maestro adapters. The module
enables the `nix-command` and `flakes` features required by those adapters.

Create a new master document directly from the validated cluster config:

```sh
sudo maestro cluster bootstrap \
  --config /etc/maestro/maestro.jsonc \
  --data-dir /var/lib/maestro \
  --etcd-binary /run/current-system/sw/bin/etcd \
  --output /run/maestro/launch.json
```

For another declared node, join over the cluster's HTTPS endpoint. The command
creates and persists the node's private join key automatically:

```sh
sudo maestro cluster join https://10.20.0.11:3000 \
  --config /etc/maestro/maestro.jsonc \
  --data-dir /var/lib/maestro \
  --output /run/maestro/launch.json
```

Add `--etcd-binary /run/current-system/sw/bin/etcd` to the join command for a
control-plane node. Bootstrap and join documents are create-only and
owner-only; retrying the same completed operation verifies and reuses the
existing document.

Build or inspect the canonical package with:

```sh
nix build
nix flake check
nix run .# -- --help
nix run .#migrate -- --help
```

Linux release tags publish deterministic static-musl bundles for x86_64 and
ARM64. Build the bundle for the current Linux architecture and verify it with:

Set the `kernel-api` package version in `crates/kernel/api/Cargo.toml` for a
release. `kernel_api::MAESTRO_VERSION`, every shipped binary, Nix package, and
generated API document read that same Cargo version. Other Cargo packages keep
independent internal versions.

```sh
nix build .#release-bundle
(cd result && sha256sum --check *.sha256)
tar -xzf result/*.tar.gz
```

The archive contains standalone `maestro`, `maestro-daemon`, and
`maestro-migrate` executables, the static panel, the production rehearsal
evidence checklist, and the cutover, multi-node, NixOS, and Tailscale runbooks.
The daemon serves the panel from its HTTPS API origin and exchanges operator
tokens for HttpOnly session cookies. The daemon still requires the host
runtime, network privileges, and external tools described by the NixOS module;
the static bundle does not turn the daemon into an isolated container
deployment.

Release builds also publish a minimal, deterministic daemon image archive for
each Linux architecture. Build and verify it locally with:

```sh
nix build .#daemon-image-bundle --out-link result-daemon-image
(cd result-daemon-image && sha256sum --check *.sha256)
gzip -dc result-daemon-image/*.docker.tar.gz | docker load
version=$(nix eval --raw .#static.version)
docker run --rm "maestro-daemon:$version" --help
```

The daemon image contains the static daemon and panel, with no Node runtime or
shell. It is a packaging artifact, not a replacement for the host integration
in `services.maestro`: a real daemon still needs host networking,
containerd, privileged network access, the launch document, and the external
adapter binaries selected by the live config. Prefer the NixOS module for
production and use the image only where those dependencies are explicitly
supplied by the container orchestrator.

Selecting this module does not approve production cutover. Complete the
one-way store restore, planned workload-restart rehearsal, and evidence checks
in `cutover.md` and `rehearsal-evidence.md` before enabling the service on a
production node.
