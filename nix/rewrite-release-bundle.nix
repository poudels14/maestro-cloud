{
  pkgs,
  rewritePackage,
}: let
  version = rewritePackage.version;
  system = pkgs.stdenv.hostPlatform.system;
  bundleName = "maestro-rewrite-${version}-${system}";
in
  pkgs.runCommand "${bundleName}-bundle" {
    nativeBuildInputs = with pkgs; [
      binutils
      coreutils
      gnutar
      gzip
    ];
  } ''
    bundle_root="$TMPDIR/${bundleName}"
    mkdir -p "$bundle_root/bin" "$bundle_root/share/doc/maestro" "$out"

    install -m755 ${rewritePackage}/bin/maestro "$bundle_root/bin/maestro"
    install -m755 ${rewritePackage}/bin/maestro-daemon "$bundle_root/bin/maestro-daemon"
    install -m755 ${rewritePackage}/bin/maestro-migrate "$bundle_root/bin/maestro-migrate"
    install -m644 ${../LICENSE} "$bundle_root/LICENSE"
    install -m644 ${rewritePackage}/share/doc/maestro/cutover.md \
      "$bundle_root/share/doc/maestro/cutover.md"
    install -m644 ${rewritePackage}/share/doc/maestro/multi-node.md \
      "$bundle_root/share/doc/maestro/multi-node.md"
    install -m644 ${rewritePackage}/share/doc/maestro/nixos-rewrite.md \
      "$bundle_root/share/doc/maestro/nixos-rewrite.md"
    install -m644 ${rewritePackage}/share/doc/maestro/tailscale.md \
      "$bundle_root/share/doc/maestro/tailscale.md"
    strip --strip-all "$bundle_root"/bin/*
    test "$("$bundle_root/bin/maestro" --version)" = "maestro-next ${version}"
    test "$("$bundle_root/bin/maestro-daemon" --version)" = "daemon ${version}"
    test "$("$bundle_root/bin/maestro-migrate" --version)" = "maestro-migrate ${version}"
    "$bundle_root/bin/maestro-migrate" verify --help >/dev/null

    tar \
      --create \
      --directory="$TMPDIR" \
      --file=- \
      --format=gnu \
      --group=0 \
      --mtime='@1' \
      --numeric-owner \
      --owner=0 \
      --sort=name \
      "${bundleName}" \
      | gzip --no-name >"$out/${bundleName}.tar.gz"
    (
      cd "$out"
      sha256sum "${bundleName}.tar.gz" >"${bundleName}.tar.gz.sha256"
    )
  ''
