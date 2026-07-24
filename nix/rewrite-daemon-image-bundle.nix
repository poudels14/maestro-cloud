{
  daemonImage,
  imageArchitecture,
  pkgs,
  rewritePackage,
}: let
  version = rewritePackage.version;
  system = pkgs.stdenv.hostPlatform.system;
  archiveName = "maestro-daemon-${version}-${system}.docker.tar.gz";
in
  pkgs.runCommand "maestro-daemon-${version}-${system}-image-bundle" {
    nativeBuildInputs = with pkgs; [
      coreutils
      gnugrep
      gnutar
      gzip
      jq
    ];
  } ''
    image_root="$TMPDIR/image"
    filesystem_root="$TMPDIR/root"
    mkdir -p "$image_root" "$filesystem_root" "$out"
    tar -xzf ${daemonImage} -C "$image_root"

    test "$(jq 'length' "$image_root/manifest.json")" = 1
    config_path=$(jq -er '.[0].Config' "$image_root/manifest.json")
    test -f "$image_root/$config_path"
    jq -e \
      --arg architecture ${pkgs.lib.escapeShellArg imageArchitecture} \
      --arg version ${pkgs.lib.escapeShellArg version} \
      '.architecture == $architecture
        and .os == "linux"
        and .config.Entrypoint == ["/bin/maestro-daemon"]
        and .config.WorkingDir == "/var/lib/maestro"
        and .config.Labels["org.opencontainers.image.version"] == $version' \
      "$image_root/$config_path" >/dev/null
    test "$(jq -r '.[0].RepoTags[0]' "$image_root/manifest.json")" = \
      "maestro-daemon:${version}"

    jq -er '.[0].Layers[]' "$image_root/manifest.json" \
      | while IFS= read -r layer; do
          test -f "$image_root/$layer"
          tar -xf "$image_root/$layer" -C "$filesystem_root"
        done
    test -x "$filesystem_root/bin/maestro-daemon"
    test -f "$filesystem_root/share/maestro-panel/index.html"
    test ! -e "$filesystem_root/bin/sh"
    "$filesystem_root/bin/maestro-daemon" --help >/dev/null
    test "$("$filesystem_root/bin/maestro-daemon" --version)" = "daemon ${version}"

    install -m0444 ${daemonImage} "$out/${archiveName}"
    (
      cd "$out"
      sha256sum "${archiveName}" >"${archiveName}.sha256"
    )
  ''
