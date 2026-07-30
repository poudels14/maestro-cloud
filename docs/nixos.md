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

Import the module and point it at a launch document created outside the Nix
store:

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
            config = "/run/maestro/launch.json";
          };
        })
      ];
    };
  };
}
```

`services.maestro.config` names the protected daemon launch document, not
the shared JSON/JSONC config source. The launch document contains cluster
private keys, application secrets, and the shared JWT signing key. It must be
an absolute owner-only regular file such as
`/run/maestro/launch.json`; placing its contents in a Nix expression would copy
those secrets into the world-readable Nix store. The daemon validates the file
before starting.

The module does not enable or modify the NixOS host firewall. Maestro owns its
runtime nftables table, and deployments that enable another host firewall must
configure it separately so it does not block the workload bridge or cluster
control traffic.

The daemon and migration tool write newline-delimited JSON diagnostics to
stderr. Under systemd, these records flow directly into the journal with
structured `level`, `message`, `error`, and resource identity fields. The
`RUST_LOG` environment variable selects tracing directives and defaults to
`info`; launch configuration, secret values, and full environment maps are
never emitted.

Control-plane launch documents should use
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
adapter binaries selected by that document. Prefer the NixOS module for
production and use the image only where those dependencies are explicitly
supplied by the container orchestrator.

Selecting this module does not approve production cutover. Complete the
one-way store restore, planned workload-restart rehearsal, and evidence checks
in `cutover.md` and `rehearsal-evidence.md` before enabling the service on a
production node.
