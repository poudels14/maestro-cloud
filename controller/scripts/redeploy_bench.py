import argparse
import os
import subprocess
import threading
import time
import urllib.request
import urllib.error
from collections import Counter

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")


def request_loop(host, url, interval_ms, results, lock, stop_event):
    interval = interval_ms / 1000
    while not stop_event.is_set():
        start = time.monotonic()
        try:
            req = urllib.request.Request(url, headers={"Host": host})
            with urllib.request.urlopen(req, timeout=5) as resp:
                status = resp.status
        except urllib.error.HTTPError as err:
            status = err.code
        except Exception as err:
            status = str(err)

        with lock:
            results[status] += 1

        elapsed = time.monotonic() - start
        remaining = interval - elapsed
        if remaining > 0:
            time.sleep(remaining)


def redeploy_loop(service_id, admin_host, redeploy_interval, deploys, lock, stop_event):
    while not stop_event.is_set():
        time.sleep(redeploy_interval)
        if stop_event.is_set():
            break
        start = time.monotonic()
        result = subprocess.run(
            ["cargo", "run", "--release", "--", "redeploy", service_id, "--host", admin_host],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
        elapsed = time.monotonic() - start
        output = (result.stdout + result.stderr).strip()
        with lock:
            deploys.append(
                {
                    "elapsed": round(elapsed, 2),
                    "exit_code": result.returncode,
                    "output": output,
                }
            )
        print(f"  [redeploy] {output} ({elapsed:.2f}s)")


def run(host, url, service_id, admin_host, duration, interval_ms, redeploy_interval):
    results = Counter()
    deploys = []
    lock = threading.Lock()
    stop_event = threading.Event()

    print(f"Benchmarking {url} (Host: {host}) for {duration}s")
    print(f"  service: {service_id}")
    print(f"  request interval: {interval_ms}ms")
    print(f"  redeploy interval: {redeploy_interval}s")
    print()

    request_thread = threading.Thread(
        target=request_loop,
        args=(host, url, interval_ms, results, lock, stop_event),
    )
    redeploy_thread = threading.Thread(
        target=redeploy_loop,
        args=(service_id, admin_host, redeploy_interval, deploys, lock, stop_event),
    )

    request_thread.start()
    redeploy_thread.start()

    time.sleep(duration)
    stop_event.set()

    request_thread.join()
    redeploy_thread.join()

    total = sum(results.values())
    print(f"\nTotal requests: {total}")
    print(f"Duration: {duration}s")
    print(f"Redeploys: {len(deploys)}\n")
    print("Status     Count    %")
    print("-" * 30)
    for status, count in sorted(results.items(), key=lambda x: -x[1]):
        pct = count / total * 100
        print(f"{str(status):<10} {count:<8} {pct:.1f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=60, help="total duration in seconds")
    parser.add_argument("--interval", type=float, default=5, help="request interval in ms")
    parser.add_argument("--redeploy-interval", type=float, default=10, help="seconds between redeploys")
    parser.add_argument("--host", required=True, help="Host header for requests")
    parser.add_argument("--url", required=True, help="ingress URL to hit")
    parser.add_argument("--service", required=True, help="service id to redeploy")
    parser.add_argument("--admin-host", required=True, help="maestro API host")
    args = parser.parse_args()
    run(
        args.host,
        args.url,
        args.service,
        args.admin_host,
        args.duration,
        args.interval,
        args.redeploy_interval,
    )
