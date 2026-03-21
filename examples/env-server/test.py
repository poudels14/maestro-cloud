"""
Tests env vars and secrets across redeploys and config updates.

Usage (from project root):
    python examples/env-server/test.py \
        --url http://127.0.0.1:8888/ \
        --host env-server.local \
        --service env-server \
        --config examples/env-server/maestro.cluster.jsonc
"""

import argparse
import json
import os
import subprocess
import tempfile
import time
import urllib.request
import urllib.error

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"


def fetch_json(url, host=None, timeout=10):
    headers = {"Host": host} if host else {}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def wait_for_ready(url, host, max_wait=60):
    deadline = time.monotonic() + max_wait
    last_error = None
    while time.monotonic() < deadline:
        try:
            data = fetch_json(url, host)
            if data.get("hostname"):
                return data
        except Exception as err:
            last_error = err
        time.sleep(1)
    raise TimeoutError(f"service did not become ready within {max_wait}s (last error: {last_error})")


def wait_for_new_deployment(url, host, old_hostname, max_wait=90):
    deadline = time.monotonic() + max_wait
    while time.monotonic() < deadline:
        try:
            data = fetch_json(url, host)
            if data.get("hostname") and data["hostname"] != old_hostname:
                return data
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(f"new deployment did not become ready within {max_wait}s (still on {old_hostname})")


def rollout(admin_host, config_path):
    result = subprocess.run(
        [
            "cargo", "run", "--release", "--",
            "rollout", "--host", admin_host, "--config", config_path, "--apply",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    output = (result.stdout + result.stderr).strip()
    print(f"  [rollout] {output}")
    skipped = "config unchanged" in output or "skipped" in output.lower()
    return {"ok": result.returncode == 0, "skipped": skipped}


def redeploy(admin_host, service_id):
    result = subprocess.run(
        [
            "cargo", "run", "--release", "--",
            "redeploy", service_id, "--host", admin_host,
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    output = (result.stdout + result.stderr).strip()
    print(f"  [redeploy] {output}")
    return result.returncode == 0


def write_config(base_config, env_overrides=None, secret_overrides=None):
    config = json.loads(json.dumps(base_config))
    service = next(iter(config["services"].values()))
    if env_overrides:
        service["deploy"]["env"].update(env_overrides)
    if secret_overrides:
        service["deploy"]["secrets"]["items"].update(secret_overrides)
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonc", delete=False, dir=PROJECT_ROOT
    )
    json.dump(config, tmp, indent=2)
    tmp.close()
    return tmp.name


def assert_eq(name, actual, expected):
    if actual == expected:
        print(f"  {PASS} {name}")
        return True
    print(f"  {FAIL} {name}: expected {expected!r}, got {actual!r}")
    return False


def run(url, host, service_id, config_path, admin_host):
    with open(config_path) as f:
        # strip comments for json parsing
        lines = [l for l in f if not l.strip().startswith("//")]
        base_config = json.loads("".join(lines))

    results = {"pass": 0, "fail": 0}

    def check(name, actual, expected):
        if assert_eq(name, actual, expected):
            results["pass"] += 1
        else:
            results["fail"] += 1

    # --- Test 1: Initial rollout ---
    print("\n=== Test 1: Initial rollout ===")
    old_hostname = None
    try:
        old_data = fetch_json(url, host)
        old_hostname = old_data.get("hostname")
    except Exception:
        pass
    result = rollout(admin_host, config_path)
    if result["skipped"] and old_hostname:
        data = fetch_json(url, host)
    elif old_hostname:
        data = wait_for_new_deployment(url, host, old_hostname)
    else:
        data = wait_for_ready(url, host)
    hostname1 = data["hostname"]
    print(f"  hostname: {hostname1}")

    check("env APP_NAME", data["env"].get("APP_NAME"), "maestro-test")
    check("env APP_ENV", data["env"].get("APP_ENV"), "development")
    check("secret DB_PASSWORD", data["secrets"].get("DB_PASSWORD"), "secret123")
    check("secret API_KEY", data["secrets"].get("API_KEY"), "test-api-key")

    # --- Test 2: Redeploy (same config) ---
    print("\n=== Test 2: Redeploy (same config, new deployment) ===")
    redeploy(admin_host, service_id)
    data = wait_for_new_deployment(url, host, hostname1)
    hostname2 = data["hostname"]
    print(f"  hostname: {hostname2} (was {hostname1})")

    check("hostname changed", hostname2 != hostname1, True)
    check("env APP_NAME preserved", data["env"].get("APP_NAME"), "maestro-test")
    check("secret DB_PASSWORD preserved", data["secrets"].get("DB_PASSWORD"), "secret123")

    # --- Test 3: Update env via rollout ---
    print("\n=== Test 3: Update env var ===")
    tmp_config = write_config(base_config, env_overrides={"APP_ENV": "staging"})
    try:
        rollout(admin_host, tmp_config)
        data = wait_for_new_deployment(url, host, hostname2)
        hostname3 = data["hostname"]
        print(f"  hostname: {hostname3} (was {hostname2})")

        check("hostname changed", hostname3 != hostname2, True)
        check("env APP_ENV updated", data["env"].get("APP_ENV"), "staging")
        check("env APP_NAME unchanged", data["env"].get("APP_NAME"), "maestro-test")
        check("secret DB_PASSWORD unchanged", data["secrets"].get("DB_PASSWORD"), "secret123")
    finally:
        os.unlink(tmp_config)

    # --- Test 4: Update secret via rollout ---
    print("\n=== Test 4: Update secret ===")
    tmp_config = write_config(
        base_config,
        env_overrides={"APP_ENV": "staging"},
        secret_overrides={"DB_PASSWORD": "new-password-456"},
    )
    try:
        rollout(admin_host, tmp_config)
        data = wait_for_new_deployment(url, host, hostname3)
        hostname4 = data["hostname"]
        print(f"  hostname: {hostname4} (was {hostname3})")

        check("hostname changed", hostname4 != hostname3, True)
        check("secret DB_PASSWORD updated", data["secrets"].get("DB_PASSWORD"), "new-password-456")
        check("secret API_KEY unchanged", data["secrets"].get("API_KEY"), "test-api-key")
        check("env APP_ENV preserved", data["env"].get("APP_ENV"), "staging")
    finally:
        os.unlink(tmp_config)

    # --- Test 5: Redeploy after secret change ---
    print("\n=== Test 5: Redeploy preserves updated values ===")
    redeploy(admin_host, service_id)
    data = wait_for_new_deployment(url, host, hostname4)
    hostname5 = data["hostname"]
    print(f"  hostname: {hostname5} (was {hostname4})")

    check("hostname changed", hostname5 != hostname4, True)
    check("secret DB_PASSWORD preserved after redeploy", data["secrets"].get("DB_PASSWORD"), "new-password-456")
    check("env APP_ENV preserved after redeploy", data["env"].get("APP_ENV"), "staging")

    # --- Test 6: Log API ---
    print("\n=== Test 6: Log API ===")

    sys_logs_url = f"{admin_host}/api/system/maestro-probe/logs?tail=10"
    try:
        sys_logs = fetch_json(sys_logs_url)
        check("system logs returns array", isinstance(sys_logs, list), True)
        check("system logs not empty", len(sys_logs) > 0, True)
        if sys_logs:
            entry = sys_logs[0]
            check("log entry has 'text'", "text" in entry, True)
            check("log entry has 'ts'", "ts" in entry, True)
            check("log entry has 'level'", "level" in entry, True)
    except Exception as err:
        print(f"  {FAIL} system logs: {err}")
        results["fail"] += 1

    # --- Summary ---
    total = results["pass"] + results["fail"]
    print(f"\n{'=' * 40}")
    print(f"Results: {results['pass']}/{total} passed, {results['fail']}/{total} failed")
    if results["fail"] > 0:
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="ingress URL")
    parser.add_argument("--host", required=True, help="Host header")
    parser.add_argument("--service", required=True, help="service id")
    parser.add_argument("--config", required=True, help="path to maestro.cluster.jsonc")
    parser.add_argument("--admin-host", required=True, help="maestro API host")
    args = parser.parse_args()
    run(args.url, args.host, args.service, args.config, args.admin_host)
