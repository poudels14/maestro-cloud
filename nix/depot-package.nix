{pkgs}: let
  version = "2.101.43";
  architecture =
    {
      x86_64-linux = "amd64";
      aarch64-linux = "arm64";
    }
    .${pkgs.stdenv.hostPlatform.system};
  sha256 =
    {
      x86_64-linux = "fa80b793eb74e04d5620fa19f32010d93a30250d75ccd074b6a61d65124138d1";
      aarch64-linux = "985ff1808ccdc0b9fa20b6d3be737b5cc669dfe6ba8634e9ebdb334eb115cad6";
    }
    .${pkgs.stdenv.hostPlatform.system};
in
  pkgs.stdenvNoCC.mkDerivation {
    pname = "depot";
    inherit version;

    src = pkgs.fetchurl {
      url = "https://dl.depot.dev/cli/download/Linux/${architecture}/${version}";
      inherit sha256;
    };

    dontUnpack = true;
    nativeBuildInputs = [pkgs.gnutar];

    installPhase = ''
      runHook preInstall
      mkdir -p "$out/bin"
      tmpdir=$(mktemp -d)
      ${pkgs.gnutar}/bin/tar -xzf "$src" -C "$tmpdir"
      install -m755 "$tmpdir/bin/depot" "$out/bin/depot"
      rm -rf "$tmpdir"
      runHook postInstall
    '';
  }
