# Rewrite NixOS deployment

The flake exposes the rewrite without replacing the production legacy package
or module before cutover:

- `packages.<system>.rewrite` installs `maestro`, `maestro-daemon`, and
  `maestro-migrate`.
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
starts the containerd-backed BuildKit worker. It also exposes the nftables,
network, Git, and NixOS tools used by rewrite adapters.

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
`maestro-migrate` executables plus the cutover documentation. The daemon still
requires the host runtime, network privileges, and external tools described by
the NixOS module; the static bundle does not turn the daemon into an isolated
container deployment.

Release builds also publish a minimal, deterministic daemon image archive for
each Linux architecture. Build and verify it locally with:

```sh
nix build .#rewrite-daemon-image-bundle
(cd result && sha256sum --check *.sha256)
gzip -dc result/*.docker.tar.gz | docker load
version=$(nix eval --raw .#rewrite-static.version)
docker run --rm "maestro-daemon:$version" --help
```

The image contains the static daemon and no shell. It is a packaging artifact,
not a replacement for the host integration in `services.maestro-rewrite`: a
real daemon still needs host networking, containerd, privileged network access,
the launch document, and the external adapter binaries selected by that
document. Prefer the NixOS module for production and use the image only where
those dependencies are explicitly supplied by the container orchestrator.

Selecting this module does not approve production runtime adoption. Complete
the runtime-adoption gate and the migration rehearsal in `cutover.md` before
enabling the service on a production node.
