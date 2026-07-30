{
  muslPkgs ? null,
  panel ? null,
  pkgs,
  rustToolchain,
  version,
}: let
  static = muslPkgs != null;
  packagePkgs =
    if static
    then muslPkgs
    else pkgs;
  rustPlatform = packagePkgs.makeRustPlatform {
    cargo = rustToolchain;
    rustc = rustToolchain;
  };
  packageFlags = [
    "--package"
    "maestro-cli"
    "--package"
    "daemon"
    "--package"
    "migrate"
  ];
  source = pkgs.lib.fileset.toSource {
    root = ../.;
    fileset = pkgs.lib.fileset.unions [
      ../Cargo.lock
      ../Cargo.toml
      ../crates
      ../rust-toolchain.toml
    ];
  };
  binaries = rustPlatform.buildRustPackage {
    pname = "maestro-rewrite-binaries";
    inherit version;
    src = source;

    cargoLock.lockFile = ../Cargo.lock;
    cargoBuildFlags = packageFlags;
    # CI and focused development commands own test execution. Keeping package
    # construction build-only avoids rerunning the full workspace suite during
    # every node rollout.
    doCheck = false;
    env = pkgs.lib.optionalAttrs static {
      RUSTFLAGS = "-C target-feature=+crt-static";
    };
    nativeBuildInputs = with pkgs; [
      binutils
      pkg-config
      protobuf
    ];
    buildInputs = pkgs.lib.optionals pkgs.stdenv.isDarwin [
      pkgs.apple-sdk_15
    ];

    installPhase = ''
      runHook preInstall

      release_directory="target/${packagePkgs.stdenv.hostPlatform.rust.rustcTarget}/release"
      if [ ! -x "$release_directory/daemon" ]; then
        release_directory=target/release
      fi

      install -Dm755 "$release_directory/maestro" "$out/bin/maestro"
      install -Dm755 "$release_directory/daemon" "$out/bin/maestro-daemon"
      install -Dm755 "$release_directory/maestro-migrate" "$out/bin/maestro-migrate"

      runHook postInstall
    '';

    postInstall = pkgs.lib.optionalString static ''
      for executable in "$out"/bin/*; do
        if ${pkgs.binutils}/bin/readelf -l "$executable" | grep --quiet 'Requesting program interpreter'; then
          echo "$executable has a dynamic ELF interpreter" >&2
          exit 1
        fi
        if ${pkgs.binutils}/bin/readelf -d "$executable" 2>/dev/null | grep --quiet '(NEEDED)'; then
          echo "$executable has a dynamic library dependency" >&2
          exit 1
        fi
      done
    '';
  };
  packageName =
    if static
    then "maestro-rewrite-${packagePkgs.stdenv.hostPlatform.rust.rustcTarget}"
    else "maestro-rewrite";
in
  pkgs.runCommand "${packageName}-${version}" {
    inherit version;
    passthru = {
      inherit binaries;
    };
    meta = {
      description = "Rewritten Maestro operator, daemon, and cutover tools";
      mainProgram = "maestro";
    };
  } ''
    mkdir -p "$out/bin" "$out/share/doc/maestro"
    cp -a ${binaries}/bin/. "$out/bin/"
    install -m644 ${../docs/cutover.md} "$out/share/doc/maestro/cutover.md"
    install -m644 ${../docs/rehearsal-evidence.md} \
      "$out/share/doc/maestro/rehearsal-evidence.md"
    install -m644 ${../docs/multi-node.md} "$out/share/doc/maestro/multi-node.md"
    install -m644 ${../docs/nixos-rewrite.md} "$out/share/doc/maestro/nixos-rewrite.md"
    install -m644 ${../docs/tailscale.md} "$out/share/doc/maestro/tailscale.md"
    ${pkgs.lib.optionalString (panel != null) ''
      mkdir -p "$out/share/maestro-panel"
      cp -a ${panel}/share/maestro-panel/. "$out/share/maestro-panel/"
      test -f "$out/share/maestro-panel/index.html"
    ''}
  ''
