#!/usr/bin/env python3
"""Reject workspace dependency edges that violate the rewrite architecture."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parent.parent

ALLOWED_TARGETS = {
    "app": {
        "app",
        "operator",
        "cluster",
        "observability",
        "runtime",
        "node",
        "kernel",
        "test",
    },
    "operator": {"runtime", "node", "kernel"},
    "cluster": {"runtime", "node", "kernel"},
    "observability": {"observability", "runtime", "node", "kernel"},
    "runtime": {"kernel"},
    "node": {"node", "runtime", "kernel"},
    "kernel": {"kernel"},
    "test": {
        "app",
        "operator",
        "cluster",
        "observability",
        "runtime",
        "node",
        "kernel",
        "test",
    },
    # Remove this family with the legacy controller workspace member.
    "legacy": {"test"},
}


def family(manifest_path: Path) -> str:
    relative = manifest_path.relative_to(ROOT)
    parts = relative.parts

    if parts[0] == "controller":
        return "legacy"
    if parts[:2] == ("crates", "apps"):
        return "app"
    if parts[:2] == ("crates", "operators"):
        return "operator"
    if parts[:2] == ("crates", "observability"):
        return "observability"
    if parts[:2] == ("crates", "node"):
        return "node"
    if parts[:2] == ("crates", "kernel"):
        return "kernel"
    if parts[:2] == ("crates", "cluster"):
        return "cluster"
    if parts[:2] == ("crates", "runtime"):
        return "runtime"
    if parts[:2] == ("crates", "clustertest"):
        return "test"

    raise ValueError(f"unclassified workspace manifest: {relative}")


def load_metadata() -> dict[str, object]:
    command = [
        "cargo",
        "metadata",
        "--locked",
        "--no-deps",
        "--format-version",
        "1",
    ]
    result = subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def main() -> int:
    metadata = load_metadata()
    packages = metadata["packages"]
    workspace_ids = set(metadata["workspace_members"])
    by_name = {package["name"]: package for package in packages if package["id"] in workspace_ids}
    failures: list[str] = []
    checked = 0

    for package in by_name.values():
        source_family = family(Path(package["manifest_path"]))
        for dependency in package["dependencies"]:
            target = by_name.get(dependency["name"])
            if target is None:
                continue

            checked += 1
            target_family = family(Path(target["manifest_path"]))
            dependency_kind = dependency["kind"] or "normal"
            if source_family == "operator" and target_family == "operator":
                failures.append(
                    f"{package['name']} -> {target['name']}: operators cannot depend on siblings"
                )
            elif target_family not in ALLOWED_TARGETS[source_family]:
                failures.append(
                    f"{package['name']} ({source_family}) -> "
                    f"{target['name']} ({target_family}) [{dependency_kind}]"
                )
            if target_family == "test" and source_family != "test" and dependency_kind != "dev":
                failures.append(
                    f"{package['name']} -> {target['name']}: test harness dependencies must be dev-only"
                )

    if failures:
        print("crate boundary violations:", file=sys.stderr)
        for failure in sorted(set(failures)):
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print(f"checked {checked} workspace dependency edges")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
