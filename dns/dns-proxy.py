"""
DNS proxy for maestro clusters.

Resolves {hostname}.{cluster}.maestro.internal queries by stripping the domain
suffix and forwarding the bare hostname to Docker's embedded DNS (127.0.0.11).

For cross-cluster queries, peers are auto-discovered via `tailscale status --json`
and verified with a magic TXT query. Queries for remote clusters are forwarded
to the peer's DNS proxy over the Tailscale network.

Usage: dns-proxy.py <canonical-cluster-name> [cluster-alias]
"""

import json
import os
import socket
import struct
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from dns_http import http_server_loop

DNS_PORT = 53
DNS_UPSTREAM_IS_COREDNS = "MAESTRO_DNS_UPSTREAM" in os.environ
DNS_UPSTREAM = "127.0.0.1" if DNS_UPSTREAM_IS_COREDNS else "127.0.0.11"
DNS_UPSTREAM_PORT = 5353 if DNS_UPSTREAM_IS_COREDNS else DNS_PORT
TAILSCALE_SOCKS5_ADDRESS = ("127.0.0.1", 1055)
NETWORK_TIMEOUT = 5
DNS_WORKERS = 16
DNS_QUEUE_CAPACITY = 128
ROOT_DOMAIN = ["maestro", "internal"]
PEER_REFRESH_INTERVAL = 15
# Used to verify that a peer is a maestro DNS proxy
MAGIC_QUERY = "_maestro-dns"
MAGIC_RESPONSE = "maestro-dns-ok"


class BoundedWorkerPool:
    def __init__(self, max_workers, max_queued, thread_name_prefix):
        self._slots = threading.BoundedSemaphore(max_workers + max_queued)
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
        )

    def submit(self, function, *args):
        if not self._slots.acquire(blocking=False):
            return False
        try:
            future = self._executor.submit(function, *args)
        except Exception:
            self._slots.release()
            raise
        future.add_done_callback(lambda _: self._slots.release())
        return True

    def shutdown(self):
        self._executor.shutdown(wait=True)


def read_name(data, offset):
    labels = []
    start = offset
    jumped = False
    while offset < len(data):
        length = data[offset]
        if length == 0:
            if not jumped:
                offset += 1
            break
        if (length & 0xC0) == 0xC0:
            if not jumped:
                start = offset + 2
            pointer = ((length & 0x3F) << 8) | data[offset + 1]
            offset = pointer
            jumped = True
            continue
        offset += 1
        label = data[offset : offset + length].decode("ascii", errors="replace")
        labels.append(label)
        offset += length
    end = start if jumped else offset
    return labels, end


def encode_name(labels):
    result = b""
    for label in labels:
        encoded = label.encode("ascii")
        result += bytes([len(encoded)]) + encoded
    result += b"\x00"
    return result


def build_txt_response(query_data, qname_end, txt_value):
    txn_id = query_data[:2]
    flags = struct.pack("!H", 0x8400)
    counts = struct.pack("!HHHH", 1, 1, 0, 0)
    question = query_data[12:qname_end + 4]
    qname = query_data[12:qname_end]
    txt_bytes = txt_value.encode("ascii")
    rdata = bytes([len(txt_bytes)]) + txt_bytes
    answer = qname + struct.pack("!HHIH", 16, 1, 0, len(rdata)) + rdata
    return txn_id + flags + counts + question + answer


def build_servfail_response(query_data):
    if len(query_data) < 12:
        return None
    try:
        question_count = struct.unpack("!H", query_data[4:6])[0]
        if question_count != 1:
            return None
        _, qname_end = read_name(query_data, 12)
        question_end = qname_end + 4
        if question_end > len(query_data):
            return None
    except (IndexError, struct.error):
        return None

    query_flags = struct.unpack("!H", query_data[2:4])[0]
    response_flags = 0x8000 | (query_flags & 0x7910) | 0x0080 | 0x0002
    return (
        query_data[:2]
        + struct.pack("!H", response_flags)
        + struct.pack("!HHHH", 1, 0, 0, 0)
        + query_data[12:question_end]
    )


def resolve_upstream(sock, query_data, upstream_host, upstream_port=DNS_PORT):
    sock.sendto(query_data, (upstream_host, upstream_port))
    response, _ = sock.recvfrom(4096)
    return response


def read_exact(sock, length):
    chunks = []
    remaining = length
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("peer DNS connection closed before the response completed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def connect_tailnet_tcp(peer_ip, peer_port):
    sock = socket.create_connection(TAILSCALE_SOCKS5_ADDRESS, timeout=NETWORK_TIMEOUT)
    try:
        sock.sendall(b"\x05\x01\x00")
        if read_exact(sock, 2) != b"\x05\x00":
            raise ConnectionError("Tailscale SOCKS5 proxy rejected unauthenticated access")

        try:
            packed_ip = socket.inet_aton(peer_ip)
        except OSError as error:
            raise ValueError(f"invalid Tailscale IPv4 address: {peer_ip}") from error

        sock.sendall(
            b"\x05\x01\x00\x01" + packed_ip + struct.pack("!H", peer_port)
        )
        version, status, _, address_type = read_exact(sock, 4)
        if version != 5:
            raise ConnectionError(f"invalid SOCKS5 response version: {version}")
        if status != 0:
            raise ConnectionError(
                f"Tailscale SOCKS5 connection to {peer_ip}:{peer_port} failed with status {status}"
            )

        if address_type == 1:
            address_length = 4
        elif address_type == 4:
            address_length = 16
        elif address_type == 3:
            address_length = read_exact(sock, 1)[0]
        else:
            raise ConnectionError(f"invalid SOCKS5 address type: {address_type}")
        read_exact(sock, address_length + 2)
        return sock
    except Exception:
        sock.close()
        raise


def resolve_peer(query_data, peer_ip):
    sock = connect_tailnet_tcp(peer_ip, DNS_PORT)
    try:
        sock.sendall(struct.pack("!H", len(query_data)) + query_data)
        response_length = struct.unpack("!H", read_exact(sock, 2))[0]
        return read_exact(sock, response_length)
    finally:
        sock.close()


def resolve_local(sock, data, original_qname, bare_labels, canonical_cluster, qname_end, upstream=None):
    if DNS_UPSTREAM_IS_COREDNS and not upstream:
        canonical_labels = bare_labels + [canonical_cluster] + ROOT_DOMAIN
        canonical_qname = encode_name(canonical_labels)
        rewritten = data[:12] + canonical_qname + data[qname_end:]
        response = resolve_upstream(sock, rewritten, DNS_UPSTREAM, DNS_UPSTREAM_PORT)
        return response.replace(canonical_qname, original_qname)
    else:
        bare_qname = encode_name(bare_labels)
        rewritten = data[:12] + bare_qname + data[qname_end:]
        response = (
            resolve_peer(rewritten, upstream)
            if upstream
            else resolve_upstream(sock, rewritten, DNS_UPSTREAM)
        )
        return response.replace(bare_qname, original_qname)


def verify_peer(ip):
    try:
        query_name = encode_name([MAGIC_QUERY, "maestro", "internal"])
        txn_id = b"\xfe\xfe"
        header = txn_id + struct.pack("!HHHHH", 0x0100, 1, 0, 0, 0)
        qtype_class = struct.pack("!HH", 16, 1)
        query = header + query_name + qtype_class
        response = resolve_peer(query, ip)
        return MAGIC_RESPONSE.encode("ascii") in response
    except Exception:
        return False


def derive_alias(cluster_name):
    if "-" not in cluster_name:
        return cluster_name
    base, suffix = cluster_name.rsplit("-", 1)
    if base and len(suffix) == 4 and suffix.isalnum():
        return base
    return cluster_name


def fetch_tailscale_status():
    try:
        result = subprocess.run(
            ["tailscale", "status", "--json"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)
    except Exception as err:
        print(f"tailscale status error: {err}", file=sys.stderr, flush=True)
        return None


def extract_cluster_peers(status, my_cluster):
    candidates = {}
    for node in (status.get("Peer") or {}).values():
        hostname = node.get("HostName", "")
        cluster = cluster_from_tailnet_hostname(hostname)
        if not cluster or cluster == my_cluster:
            continue
        tailscale_ips = node.get("TailscaleIPs", [])
        ipv4 = next((ip for ip in tailscale_ips if "." in ip), None)
        if ipv4 and verify_peer(ipv4):
            candidates.setdefault(cluster, []).append((hostname, ipv4))
    return {
        cluster: sorted(routers)[0][1]
        for cluster, routers in candidates.items()
    }


def cluster_from_tailnet_hostname(hostname):
    prefix = "maestro-tailscale-"
    if not hostname.startswith(prefix):
        return None
    value = hostname[len(prefix):]
    if "-" in value:
        cluster, node_suffix = value.rsplit("-", 1)
        if len(node_suffix) == 12 and all(ch in "0123456789abcdefghijklmnopqrstuvwxyz" for ch in node_suffix):
            return cluster
    return value


def audit_peer_changes(status, last_peers):
    users = status.get("User") or {}
    current = {}
    for node_id, node in (status.get("Peer") or {}).items():
        hostname = node.get("HostName", "")
        ips = node.get("TailscaleIPs", [])
        ipv4 = next((ip for ip in ips if "." in ip), "")
        user_login = users.get(str(node.get("UserID", "")), {}).get("LoginName", "unknown")
        current[node_id] = {
            "hostname": hostname,
            "user": user_login,
            "ip": ipv4,
            "os": node.get("OS", "unknown"),
        }

    for node_id in set(current.keys()) - set(last_peers.keys()):
        p = current[node_id]
        print(
            f"tailscale peer added: id={node_id} hostname={p['hostname']} "
            f"user={p['user']} ip={p['ip']} os={p['os']}",
            file=sys.stderr, flush=True,
        )

    for node_id in set(last_peers.keys()) - set(current.keys()):
        p = last_peers[node_id]
        print(
            f"tailscale peer removed: id={node_id} hostname={p['hostname']} "
            f"user={p['user']} ip={p['ip']} os={p['os']}",
            file=sys.stderr, flush=True,
        )

    return current


def compute_alias_owners(my_cluster, my_alias, peers):
    claims = {}

    def add_claim(alias, canonical):
        claims.setdefault(alias, set()).add(canonical)

    add_claim(my_alias, my_cluster)
    for canonical in peers.keys():
        add_claim(derive_alias(canonical), canonical)

    alias_owners = {}
    for alias, claimants in claims.items():
        if len(claimants) == 1:
            alias_owners[alias] = next(iter(claimants))
    return alias_owners


def peer_refresh_loop(my_cluster, my_alias, state_ref, lock):
    audit_peers = {}
    while True:
        time.sleep(PEER_REFRESH_INTERVAL)
        try:
            status = fetch_tailscale_status()
            if status is None:
                continue
            new_peers = extract_cluster_peers(status, my_cluster)
            alias_owners = compute_alias_owners(my_cluster, my_alias, new_peers)
            with lock:
                state_ref["peers"] = new_peers
                state_ref["alias_owners"] = alias_owners
            if new_peers:
                print(
                    f"peers refreshed: {new_peers}, alias_owners={alias_owners}",
                    file=sys.stderr,
                    flush=True,
                )
            audit_peers = audit_peer_changes(status, audit_peers)
        except Exception as err:
            print(f"peer refresh error: {err}", file=sys.stderr, flush=True)


def handle_dns_query(data, my_cluster, my_alias, state, lock):
    """Process a DNS query and return the response bytes, or None."""
    if len(data) < 12:
        return None

    labels, qname_end = read_name(data, 12)
    lower_labels = [l.lower() for l in labels]

    if lower_labels == [MAGIC_QUERY] + ROOT_DOMAIN:
        qtype = struct.unpack("!H", data[qname_end : qname_end + 2])[0]
        if qtype == 16:
            return build_txt_response(data, qname_end, MAGIC_RESPONSE)

    upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    upstream_sock.settimeout(5)

    try:
        if len(lower_labels) >= 3 and lower_labels[-2:] == ROOT_DOMAIN:
            query_cluster = lower_labels[-3]
            bare_labels = labels[:-3]
            lower_bare_labels = lower_labels[:-3]

            if not bare_labels:
                return resolve_upstream(upstream_sock, data, DNS_UPSTREAM, DNS_UPSTREAM_PORT)

            original_qname = encode_name(labels)

            with lock:
                peers = state.get("peers", {})
                alias_owners = state.get("alias_owners", {})
                peer_ip = peers.get(query_cluster)
                alias_owner = alias_owners.get(query_cluster)

            if lower_bare_labels == ["admin"]:
                if query_cluster == my_alias:
                    return resolve_upstream(upstream_sock, data, DNS_UPSTREAM, DNS_UPSTREAM_PORT)
                elif query_cluster == my_cluster:
                    return resolve_local(
                        upstream_sock, data, original_qname, bare_labels, my_cluster, qname_end
                    )
                elif peer_ip:
                    return resolve_peer(data, peer_ip)
                elif alias_owner and alias_owner in peers:
                    return resolve_peer(data, peers[alias_owner])
                else:
                    return resolve_upstream(upstream_sock, data, DNS_UPSTREAM, DNS_UPSTREAM_PORT)

            if query_cluster == my_cluster or alias_owner == my_cluster:
                return resolve_local(upstream_sock, data, original_qname, bare_labels, my_cluster, qname_end)
            elif peer_ip:
                return resolve_local(upstream_sock, data, original_qname, bare_labels, query_cluster, qname_end, peer_ip)
            elif alias_owner and alias_owner in peers:
                return resolve_local(upstream_sock, data, original_qname, bare_labels, alias_owner, qname_end, peers[alias_owner])
            else:
                return resolve_upstream(upstream_sock, data, DNS_UPSTREAM, DNS_UPSTREAM_PORT)
        else:
            return resolve_upstream(upstream_sock, data, DNS_UPSTREAM, DNS_UPSTREAM_PORT)
    finally:
        upstream_sock.close()


def handle_tcp_client(conn, my_cluster, my_alias, state, lock):
    try:
        conn.settimeout(NETWORK_TIMEOUT)
        length_bytes = read_exact(conn, 2)
        msg_len = struct.unpack("!H", length_bytes)[0]
        data = read_exact(conn, msg_len)
        response = handle_dns_query(data, my_cluster, my_alias, state, lock)
        if response:
            conn.sendall(struct.pack("!H", len(response)) + response)
    except Exception as err:
        print(f"tcp dns error: {err}", file=sys.stderr, flush=True)
    finally:
        conn.close()


def tcp_listener_loop(tcp_server, my_cluster, my_alias, state, lock, worker_pool):
    while True:
        try:
            conn, _ = tcp_server.accept()
            if not worker_pool.submit(
                handle_tcp_client,
                conn,
                my_cluster,
                my_alias,
                state,
                lock,
            ):
                conn.close()
        except Exception as err:
            print(f"tcp accept error: {err}", file=sys.stderr, flush=True)


def handle_udp_client(server, data, addr, my_cluster, my_alias, state, lock):
    try:
        response = handle_dns_query(data, my_cluster, my_alias, state, lock)
        if response:
            server.sendto(response, addr)
    except Exception as err:
        print(f"dns error for {addr}: {err}", file=sys.stderr, flush=True)


def dispatch_udp_client(server, data, addr, my_cluster, my_alias, state, lock, worker_pool):
    if worker_pool.submit(
        handle_udp_client,
        server,
        data,
        addr,
        my_cluster,
        my_alias,
        state,
        lock,
    ):
        return True

    response = build_servfail_response(data)
    if response:
        server.sendto(response, addr)
    return False


def main():
    if len(sys.argv) < 2:
        print("usage: dns-proxy.py <canonical-cluster-name> [cluster-alias]", file=sys.stderr)
        sys.exit(1)

    my_cluster = sys.argv[1].lower()
    my_alias = (
        sys.argv[2].lower() if len(sys.argv) >= 3 and sys.argv[2] else derive_alias(my_cluster)
    )
    status = fetch_tailscale_status()
    peers = extract_cluster_peers(status, my_cluster) if status else {}
    alias_owners = compute_alias_owners(my_cluster, my_alias, peers)
    state = {"peers": peers, "alias_owners": alias_owners}
    lock = threading.Lock()
    alias_status = "active" if alias_owners.get(my_alias) == my_cluster else "conflicted"

    print(
        f"dns proxy started, cluster={my_cluster}, alias={my_alias} ({alias_status}), peers={peers}, upstream={DNS_UPSTREAM} (coredns={DNS_UPSTREAM_IS_COREDNS})",
        file=sys.stderr,
        flush=True,
    )

    refresh_thread = threading.Thread(
        target=peer_refresh_loop, args=(my_cluster, my_alias, state, lock), daemon=True
    )
    refresh_thread.start()

    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("0.0.0.0", DNS_PORT))

    tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_server.bind(("0.0.0.0", DNS_PORT))
    tcp_server.listen(16)

    udp_worker_pool = BoundedWorkerPool(
        DNS_WORKERS,
        DNS_QUEUE_CAPACITY,
        "dns-udp",
    )
    tcp_worker_pool = BoundedWorkerPool(
        DNS_WORKERS,
        DNS_QUEUE_CAPACITY,
        "dns-tcp",
    )

    tcp_thread = threading.Thread(
        target=tcp_listener_loop,
        args=(tcp_server, my_cluster, my_alias, state, lock, tcp_worker_pool),
        daemon=True,
    )
    tcp_thread.start()

    http_thread = threading.Thread(
        target=http_server_loop,
        args=(my_cluster, my_alias, state, lock, derive_alias),
        daemon=True,
    )
    http_thread.start()

    while True:
        data, addr = server.recvfrom(4096)
        dispatch_udp_client(
            server,
            data,
            addr,
            my_cluster,
            my_alias,
            state,
            lock,
            udp_worker_pool,
        )


if __name__ == "__main__":
    main()
