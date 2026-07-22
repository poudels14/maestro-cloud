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

Selecting this module does not approve production runtime adoption. Complete
the runtime-adoption gate and the migration rehearsal in `cutover.md` before
enabling the service on a production node.
