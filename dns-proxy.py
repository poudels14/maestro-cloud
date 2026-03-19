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
import socket
import struct
import subprocess
import sys
import threading
import time

DOCKER_DNS = "127.0.0.11"
DNS_PORT = 53
ROOT_DOMAIN = ["maestro", "internal"]
PEER_REFRESH_INTERVAL = 15
# Used to verify that a peer is a maestro DNS proxy
MAGIC_QUERY = "_maestro-dns"
MAGIC_RESPONSE = "maestro-dns-ok"


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


def resolve_upstream(sock, query_data, upstream_host, upstream_port=DNS_PORT):
    sock.sendto(query_data, (upstream_host, upstream_port))
    response, _ = sock.recvfrom(4096)
    return response


def verify_peer(ip):
    try:
        query_name = encode_name([MAGIC_QUERY, "maestro", "internal"])
        txn_id = b"\xfe\xfe"
        header = txn_id + struct.pack("!HHHHH", 0x0100, 1, 0, 0, 0)
        qtype_class = struct.pack("!HH", 16, 1)
        query = header + query_name + qtype_class
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(3)
        try:
            sock.sendto(query, (ip, DNS_PORT))
            response, _ = sock.recvfrom(4096)
            return MAGIC_RESPONSE.encode("ascii") in response
        finally:
            sock.close()
    except Exception:
        return False


def derive_alias(cluster_name):
    if "-" not in cluster_name:
        return cluster_name
    base, suffix = cluster_name.rsplit("-", 1)
    if base and len(suffix) == 4 and suffix.isalnum():
        return base
    return cluster_name


def discover_peers(my_cluster):
    peers = {}
    try:
        result = subprocess.run(
            ["tailscale", "status", "--json"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return peers
        status = json.loads(result.stdout)

        for node in status.get("Peer", {}).values():
            hostname = node.get("HostName", "")
            if not hostname.startswith("maestro-tailscale-"):
                continue
            cluster = hostname[len("maestro-tailscale-"):]
            if cluster == my_cluster:
                continue
            tailscale_ips = node.get("TailscaleIPs", [])
            ipv4 = next((ip for ip in tailscale_ips if "." in ip), None)
            if ipv4 and verify_peer(ipv4):
                peers[cluster] = ipv4
    except Exception as err:
        print(f"peer discovery error: {err}", file=sys.stderr, flush=True)
    return peers


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
    while True:
        time.sleep(PEER_REFRESH_INTERVAL)
        try:
            new_peers = discover_peers(my_cluster)
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
        except Exception as err:
            print(f"peer refresh error: {err}", file=sys.stderr, flush=True)


def handle_dns_query(data, my_cluster, state, lock):
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

            if not bare_labels:
                return resolve_upstream(upstream_sock, data, DOCKER_DNS)

            original_qname = encode_name(labels)
            bare_qname = encode_name(bare_labels)

            with lock:
                peers = state.get("peers", {})
                alias_owners = state.get("alias_owners", {})
                peer_ip = peers.get(query_cluster)
                alias_owner = alias_owners.get(query_cluster)

            if query_cluster == my_cluster or alias_owner == my_cluster:
                rewritten = data[:12] + bare_qname + data[qname_end:]
                response = resolve_upstream(upstream_sock, rewritten, DOCKER_DNS)
                return response.replace(bare_qname, original_qname)
            elif peer_ip:
                rewritten = data[:12] + bare_qname + data[qname_end:]
                response = resolve_upstream(upstream_sock, rewritten, peer_ip)
                return response.replace(bare_qname, original_qname)
            elif alias_owner and alias_owner in peers:
                rewritten = data[:12] + bare_qname + data[qname_end:]
                response = resolve_upstream(upstream_sock, rewritten, peers[alias_owner])
                return response.replace(bare_qname, original_qname)
            else:
                return resolve_upstream(upstream_sock, data, DOCKER_DNS)
        else:
            return resolve_upstream(upstream_sock, data, DOCKER_DNS)
    finally:
        upstream_sock.close()


def handle_tcp_client(conn, my_cluster, state, lock):
    try:
        conn.settimeout(5)
        length_bytes = conn.recv(2)
        if len(length_bytes) < 2:
            return
        msg_len = struct.unpack("!H", length_bytes)[0]
        data = conn.recv(msg_len)
        if len(data) < msg_len:
            return
        response = handle_dns_query(data, my_cluster, state, lock)
        if response:
            conn.sendall(struct.pack("!H", len(response)) + response)
    except Exception as err:
        print(f"tcp dns error: {err}", file=sys.stderr, flush=True)
    finally:
        conn.close()


def tcp_listener_loop(tcp_server, my_cluster, state, lock):
    while True:
        try:
            conn, _ = tcp_server.accept()
            threading.Thread(
                target=handle_tcp_client,
                args=(conn, my_cluster, state, lock),
                daemon=True,
            ).start()
        except Exception as err:
            print(f"tcp accept error: {err}", file=sys.stderr, flush=True)


def main():
    if len(sys.argv) < 2:
        print("usage: dns-proxy.py <canonical-cluster-name> [cluster-alias]", file=sys.stderr)
        sys.exit(1)

    my_cluster = sys.argv[1].lower()
    my_alias = (
        sys.argv[2].lower() if len(sys.argv) >= 3 and sys.argv[2] else derive_alias(my_cluster)
    )
    peers = discover_peers(my_cluster)
    alias_owners = compute_alias_owners(my_cluster, my_alias, peers)
    state = {"peers": peers, "alias_owners": alias_owners}
    lock = threading.Lock()
    alias_status = "active" if alias_owners.get(my_alias) == my_cluster else "conflicted"

    print(
        f"dns proxy started, cluster={my_cluster}, alias={my_alias} ({alias_status}), peers={peers}, upstream={DOCKER_DNS}",
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

    tcp_thread = threading.Thread(
        target=tcp_listener_loop,
        args=(tcp_server, my_cluster, state, lock),
        daemon=True,
    )
    tcp_thread.start()

    while True:
        data, addr = server.recvfrom(4096)
        try:
            response = handle_dns_query(data, my_cluster, state, lock)
            if response:
                server.sendto(response, addr)
        except Exception as err:
            print(f"dns error for {addr}: {err}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
