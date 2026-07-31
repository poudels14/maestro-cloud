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
    maestroVersion =
      (builtins.fromTOML (builtins.readFile ./crates/kernel/api/Cargo.toml)).package.version;
  in {
    packages = forAllSystems (
      system: let
        pkgs = pkgsFor system;
        rustToolchain = rustToolchainFor pkgs;
        maestroPanel = import ./nix/panel.nix {
          inherit maestroVersion;
          inherit pkgs;
        };
        maestroPackage = import ./nix/package.nix {
          inherit pkgs rustToolchain;
          panel = maestroPanel;
          version = maestroVersion;
        };
      in {
        default = maestroPackage;
        panel = maestroPanel;
      }
      // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux (let
        muslPkgs =
          {
            aarch64-linux = pkgs.pkgsCross.aarch64-multiplatform-musl;
            x86_64-linux = pkgs.pkgsCross.musl64;
          }
          .${system};
        staticTarget = muslPkgs.stdenv.hostPlatform.rust.rustcTarget;
        staticPackage = import ./nix/package.nix {
          inherit muslPkgs pkgs;
          panel = maestroPanel;
          rustToolchain = (rustToolchainFor pkgs).override {
            targets = [staticTarget];
          };
          version = maestroVersion;
        };
        daemonImage = import ./nix/daemon-image.nix {
          maestroPackage = staticPackage;
          inherit pkgs;
        };
        imageArchitecture =
          {
            aarch64-linux = "arm64";
            x86_64-linux = "amd64";
          }
          .${system};
      in {
        static = staticPackage;
        release-bundle = import ./nix/release-bundle.nix {
          maestroPackage = staticPackage;
          inherit pkgs;
        };
        daemon-image = daemonImage;
        daemon-image-bundle = import ./nix/daemon-image-bundle.nix {
          inherit daemonImage;
          inherit imageArchitecture pkgs;
          maestroPackage = staticPackage;
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
      daemon = app self.packages.${system}.default "maestro-daemon";
      migrate = app self.packages.${system}.default "maestro-migrate";
    });

    checks = forAllSystems (
      system: let
        pkgs = pkgsFor system;
        maestroPackage = self.packages.${system}.default;
      in
        {
          package = maestroPackage;
        }
        // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          daemon-image-bundle =
            self.packages.${system}.daemon-image-bundle;
          module = import ./nix/module-check.nix {
            inherit maestroPackage pkgs;
            config = (nixpkgs.lib.nixosSystem {
              inherit system;
              modules = [
                self.nixosModules.default
                {
                  system.stateVersion = "24.11";
                  services.maestro = {
                    enable = true;
                    config = "aws-secret://maestro/test/config.json";
                    extraArgs = ["--test-option" "test-value"];
                  };
                }
              ];
            }).config;
          };
          release-bundle =
            self.packages.${system}.release-bundle;
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
              curl
              dnsutils
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

    nixosModules.default = import ./nix/module.nix {inherit self;};
  };
}
