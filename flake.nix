{
  description = "Maestro - multi-node workload platform";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = {
    self,
    nixpkgs,
  }: let
    supportedSystems = ["aarch64-darwin" "x86_64-linux" "aarch64-linux"];
    forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
    rustOverlay = builtins.getFlake "github:oxalica/rust-overlay/a286e5b998e852297a403786f063fb2c9fe7f57a";
    pkgsFor = system:
      import nixpkgs {
        inherit system;
        overlays = [rustOverlay.overlays.default];
      };
    rustToolchainFor = pkgs: pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
    rewriteVersion =
      (builtins.fromTOML (builtins.readFile ./crates/apps/cli/Cargo.toml)).package.version;
  in {
    packages = forAllSystems (
      system: let
        pkgs = pkgsFor system;
        rustToolchain = rustToolchainFor pkgs;
        rewritePanel = import ./nix/rewrite-panel.nix {
          inherit pkgs;
          inherit rewriteVersion;
        };
        rewritePackage = import ./nix/rewrite-package.nix {
          inherit pkgs rustToolchain;
          panel = rewritePanel;
        };
      in {
        default = rewritePackage;
        rewrite = rewritePackage;
        rewrite-panel = rewritePanel;
      }
      // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux (let
        muslPkgs =
          {
            aarch64-linux = pkgs.pkgsCross.aarch64-multiplatform-musl;
            x86_64-linux = pkgs.pkgsCross.musl64;
          }
          .${system};
        staticTarget = muslPkgs.stdenv.hostPlatform.rust.rustcTarget;
        staticRewrite = import ./nix/rewrite-package.nix {
          inherit muslPkgs pkgs;
          panel = rewritePanel;
          rustToolchain = (rustToolchainFor pkgs).override {
            targets = [staticTarget];
          };
        };
        rewriteDaemonImage = import ./nix/rewrite-daemon-image.nix {
          inherit pkgs;
          rewritePackage = staticRewrite;
        };
        imageArchitecture =
          {
            aarch64-linux = "arm64";
            x86_64-linux = "amd64";
          }
          .${system};
      in {
        rewrite-static = staticRewrite;
        rewrite-static-bundle = import ./nix/rewrite-release-bundle.nix {
          inherit pkgs;
          rewritePackage = staticRewrite;
        };
        rewrite-daemon-image = rewriteDaemonImage;
        rewrite-daemon-image-bundle = import ./nix/rewrite-daemon-image-bundle.nix {
          daemonImage = rewriteDaemonImage;
          inherit imageArchitecture pkgs;
          rewritePackage = staticRewrite;
        };
      })
    );

    apps = forAllSystems (system: let
      app = package: program: {
        type = "app";
        program = "${package}/bin/${program}";
      };
    in {
      default = app self.packages.${system}.default "maestro";
      rewrite = app self.packages.${system}.rewrite "maestro";
      daemon = app self.packages.${system}.rewrite "maestro-daemon";
      migrate = app self.packages.${system}.rewrite "maestro-migrate";
    });

    checks = forAllSystems (
      system: let
        pkgs = pkgsFor system;
        rewritePackage = self.packages.${system}.rewrite;
      in
        {
          rewrite = rewritePackage;
        }
        // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          rewrite-daemon-image-bundle =
            self.packages.${system}.rewrite-daemon-image-bundle;
          rewrite-module = import ./nix/rewrite-module-check.nix {
            inherit pkgs rewritePackage;
            config = (nixpkgs.lib.nixosSystem {
              inherit system;
              modules = [
                self.nixosModules.default
                {
                  system.stateVersion = "24.11";
                  services.maestro = {
                    enable = true;
                    config = "/run/maestro/launch.json";
                  };
                }
              ];
            }).config;
          };
          rewrite-static-bundle =
            self.packages.${system}.rewrite-static-bundle;
        }
    );

    devShells = forAllSystems (
      system: let
        pkgs = pkgsFor system;
        rustToolchain = rustToolchainFor pkgs;
      in {
        default = pkgs.mkShell {
          inputsFrom = [self.packages.${system}.default];
          packages =
            (with pkgs; [
              actionlint
              cargo-deny
              cargo-nextest
              nodejs_22
              (pnpm_10.override {nodejs = nodejs_22;})
              pkg-config
              protobuf
              python3
              rustToolchain
              rust-analyzer
              shellcheck
            ])
            ++ pkgs.lib.optionals pkgs.stdenv.isLinux (with pkgs; [
              buildkit
              containerd
              etcd
              iproute2
              iputils
              jq
              kmod
              nftables
              procps
              runc
            ]);
        };
      }
    );

    nixosModules.default = import ./nix/rewrite-module.nix {inherit self;};
    nixosModules.rewrite = self.nixosModules.default;
  };
}
