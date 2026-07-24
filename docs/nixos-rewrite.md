# Rewrite NixOS deployment

The flake exposes the rewrite without replacing the production legacy package
or module before cutover:

- `packages.<system>.rewrite` installs `maestro`, `maestro-daemon`,
  `maestro-migrate`, and the static panel served by the daemon.
- `packages.<system>.rewrite-panel` exposes the same static panel separately
  for packaging inspection.
- `apps.<system>.rewrite`, `daemon`, and `migrate` run those binaries.
- `nixosModules.rewrite` defines `services.maestro-rewrite`.

The rewrite module is for a rehearsal host or the cutover generation. Do not
enable it together with the legacy `services.maestro` module: both own the same
cluster endpoints and workload runtime.

## Host configuration

Import the module and point it at a launch document created outside the Nix
store:

```nix
{
  inputs.maestro.url = "path:/path/to/maestro-cloud";

  outputs = {nixpkgs, maestro, ...}: {
    nixosConfigurations.rehearsal = nixpkgs.lib.nixosSystem {
      modules = [
        maestro.nixosModules.rewrite
        ({...}: {
          services.maestro-rewrite = {
            enable = true;
            launchConfig = "/run/maestro/launch.json";
          };
        })
      ];
    };
  };
}
```

The launch document contains cluster private keys and application secrets. It
must be an absolute owner-only regular file such as
`/run/maestro/launch.json`; placing its contents in a Nix expression would copy
those secrets into the world-readable Nix store. The daemon validates the file
before starting.

Control-plane launch documents should use
`/run/current-system/sw/bin/etcd` for `etcdBinary`. The module installs the
selected etcd package into the system profile, enables native containerd, and
starts the containerd-backed BuildKit worker. It also installs Depot and exposes
the nftables, network, Git, and NixOS tools used by rewrite adapters.

Create a new master document directly from the validated cluster config:

```sh
sudo maestro cluster bootstrap \
  --config /etc/maestro/maestro.jsonc \
  --data-dir /var/lib/maestro \
  --etcd-binary /run/current-system/sw/bin/etcd \
  --output /run/maestro/launch.json
```

For another declared node, prepare its stable private join key, approve the
printed fingerprint through an authenticated operator context, then join over
the cluster's HTTPS endpoint:

```sh
sudo maestro cluster prepare-join \
  --config /etc/maestro/maestro.jsonc \
  --data-dir /var/lib/maestro
maestro cluster approve-node node-2 <printed-sha256>
sudo maestro cluster join https://10.20.0.11:3000 \
  --config /etc/maestro/maestro.jsonc \
  --data-dir /var/lib/maestro \
  --output /run/maestro/launch.json
```

Existing automation may continue to use `maestro cluster join --prepare` in
place of `cluster prepare-join`; both prepare the same durable local join key.

Add `--etcd-binary /run/current-system/sw/bin/etcd` to the join command for a
control-plane node. Bootstrap and join documents are create-only and
owner-only; retrying the same completed operation verifies and reuses the
existing document.

Build or inspect the release bundle with:

```sh
nix build .#rewrite
nix flake check
nix run .#rewrite -- --help
nix run .#migrate -- --help
```

Linux release tags publish deterministic static-musl bundles for x86_64 and
ARM64. Build the bundle for the current Linux architecture and verify it with:

```sh
nix build .#rewrite-static-bundle
(cd result && sha256sum --check *.sha256)
tar -xzf result/*.tar.gz
```

The archive contains standalone `maestro`, `maestro-daemon`, and
`maestro-migrate` executables, the static panel, and the cutover, multi-node,
NixOS, and Tailscale runbooks. The daemon serves the panel from its HTTPS API
origin and exchanges operator tokens for HttpOnly session cookies. The daemon
still requires the host runtime, network privileges, and external tools
described by the NixOS module; the static bundle does not turn the daemon into
an isolated container deployment.

Release builds also publish a minimal, deterministic daemon image archive for
each Linux architecture. Build and verify it locally with:

```sh
nix build .#rewrite-daemon-image-bundle --out-link result-daemon-image
(cd result-daemon-image && sha256sum --check *.sha256)
gzip -dc result-daemon-image/*.docker.tar.gz | docker load
version=$(nix eval --raw .#rewrite-static.version)
docker run --rm "maestro-daemon:$version" --help
```

The daemon image contains the static daemon and panel, with no Node runtime or
shell. It is a packaging artifact, not a replacement for the host integration
in `services.maestro-rewrite`: a real daemon still needs host networking,
containerd, privileged network access, the launch document, and the external
adapter binaries selected by that document. Prefer the NixOS module for
production and use the image only where those dependencies are explicitly
supplied by the container orchestrator.

Selecting this module does not approve production cutover. Complete the
one-way store restore, planned workload-restart rehearsal, and evidence checks
in `cutover.md` before enabling the service on a production node.
