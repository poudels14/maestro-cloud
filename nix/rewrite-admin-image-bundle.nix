{
  adminImage,
  imageArchitecture,
  nodeRuntime,
  pkgs,
  version,
}: let
  archiveName = "maestro-admin-${version}-${pkgs.stdenv.hostPlatform.system}.docker.tar.gz";
in
  pkgs.runCommand "maestro-admin-${version}-${pkgs.stdenv.hostPlatform.system}-image-bundle" {
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
    tar -xzf ${adminImage} -C "$image_root"

    test "$(jq 'length' "$image_root/manifest.json")" = 1
    test "$(jq '.[0].Layers | length' "$image_root/manifest.json")" = 1
    config_path=$(jq -er '.[0].Config' "$image_root/manifest.json")
    test -f "$image_root/$config_path"
    jq -e \
      --arg architecture ${pkgs.lib.escapeShellArg imageArchitecture} \
      --arg entrypoint ${pkgs.lib.escapeShellArg nodeRuntime.nodeBinary} \
      --arg version ${pkgs.lib.escapeShellArg version} \
      '.architecture == $architecture
        and .os == "linux"
        and .config.Entrypoint == [$entrypoint]
        and .config.Cmd == ["/app/server/index.mjs"]
        and .config.WorkingDir == "/app"
        and .config.User == "65532:65532"
        and (.config.Env | contains(["HOST=0.0.0.0", "NODE_ENV=production", "PORT=80"]))
        and .config.ExposedPorts == {"80/tcp": {}}
        and .config.Labels["org.opencontainers.image.version"] == $version' \
      "$image_root/$config_path" >/dev/null
    test "$(jq -r '.[0].RepoTags[0]' "$image_root/manifest.json")" = \
      "maestro-admin:${version}"

    jq -er '.[0].Layers[]' "$image_root/manifest.json" \
      | while IFS= read -r layer; do
          test -f "$image_root/$layer"
          tar -xf "$image_root/$layer" -C "$filesystem_root"
      done
    test -x "$filesystem_root${nodeRuntime.nodeBinary}"
    test -x "$filesystem_root${nodeRuntime.runtimeInterpreter}"
    test -f "$filesystem_root/app/server/index.mjs"
    test -f "$filesystem_root/app/nitro.json"
    test ! -e "$filesystem_root/nix/store"
    node_binary="$filesystem_root${nodeRuntime.nodeBinary}"
    if find "$filesystem_root/opt" -path '*/bin/*' \
      ! -path "$node_binary" \
      -print -quit | grep -q .; then
      echo "admin image contains an unexpected package binary" >&2
      exit 1
    fi
    if find "$filesystem_root" \
      \( -path '*/bin/sh' \
      -o -path '*/bin/ash' \
      -o -path '*/bin/bash' \
      -o -path '*/bin/dash' \
      -o -path '*/bin/ksh' \
      -o -path '*/bin/zsh' \) \
      -print -quit | grep -q .; then
      echo "admin image unexpectedly contains a shell" >&2
      exit 1
    fi
    "$filesystem_root${nodeRuntime.runtimeInterpreter}" \
      --library-path "$filesystem_root${nodeRuntime.runtimeLib}" \
      "$node_binary" --version >/dev/null
    "$filesystem_root${nodeRuntime.runtimeInterpreter}" \
      --library-path "$filesystem_root${nodeRuntime.runtimeLib}" \
      "$node_binary" --check \
      "$filesystem_root/app/server/index.mjs" >/dev/null

    install -m0444 ${adminImage} "$out/${archiveName}"
    (
      cd "$out"
      sha256sum "${archiveName}" >"${archiveName}.sha256"
    )
  ''
