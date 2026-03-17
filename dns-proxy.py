"""
DNS proxy for maestro clusters.

Resolves {hostname}.{cluster}.maestro.internal queries by stripping the domain
suffix and forwarding the bare hostname to Docker's embedded DNS (127.0.0.11).

For cross-cluster queries, peers are auto-discovered via `tailscale status --json`
and verified with a magic TXT query. Queries for remote clusters are forwarded
to the peer's DNS proxy over the Tailscale network.

Usage: dns-proxy.py <cluster-name>
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


def peer_refresh_loop(my_cluster, peers_ref, lock):
    while True:
        time.sleep(PEER_REFRESH_INTERVAL)
        try:
            new_peers = discover_peers(my_cluster)
            with lock:
                peers_ref.clear()
                peers_ref.update(new_peers)
            if new_peers:
                print(f"peers refreshed: {new_peers}", file=sys.stderr, flush=True)
        except Exception as err:
            print(f"peer refresh error: {err}", file=sys.stderr, flush=True)


def main():
    if len(sys.argv) < 2:
        print("usage: dns-proxy.py <cluster-name>", file=sys.stderr)
        sys.exit(1)

    my_cluster = sys.argv[1].lower()
    peers = discover_peers(my_cluster)
    lock = threading.Lock()

    print(
        f"dns proxy started, cluster={my_cluster}, peers={peers}, upstream={DOCKER_DNS}",
        file=sys.stderr,
        flush=True,
    )

    refresh_thread = threading.Thread(
        target=peer_refresh_loop, args=(my_cluster, peers, lock), daemon=True
    )
    refresh_thread.start()

    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("0.0.0.0", DNS_PORT))

    upstream = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    upstream.settimeout(5)

    while True:
        data, addr = server.recvfrom(4096)
        try:
            if len(data) < 12:
                continue

            labels, qname_end = read_name(data, 12)
            lower_labels = [l.lower() for l in labels]

            # Respond to magic health check query
            if lower_labels == [MAGIC_QUERY] + ROOT_DOMAIN:
                qtype = struct.unpack("!H", data[qname_end : qname_end + 2])[0]
                if qtype == 16:
                    response = build_txt_response(data, qname_end, MAGIC_RESPONSE)
                    server.sendto(response, addr)
                    continue

            if len(lower_labels) >= 3 and lower_labels[-2:] == ROOT_DOMAIN:
                query_cluster = lower_labels[-3]
                bare_labels = labels[:-3]

                if not bare_labels:
                    upstream.sendto(data, (DOCKER_DNS, DNS_PORT))
                    response, _ = upstream.recvfrom(4096)
                    server.sendto(response, addr)
                    continue

                original_qname = encode_name(labels)
                bare_qname = encode_name(bare_labels)

                with lock:
                    peer_ip = peers.get(query_cluster)

                if query_cluster == my_cluster:
                    rewritten = data[:12] + bare_qname + data[qname_end:]
                    response = resolve_upstream(upstream, rewritten, DOCKER_DNS)
                    response = response.replace(bare_qname, original_qname)
                elif peer_ip:
                    rewritten = data[:12] + bare_qname + data[qname_end:]
                    response = resolve_upstream(upstream, rewritten, peer_ip)
                    response = response.replace(bare_qname, original_qname)
                else:
                    upstream.sendto(data, (DOCKER_DNS, DNS_PORT))
                    response, _ = upstream.recvfrom(4096)
            else:
                upstream.sendto(data, (DOCKER_DNS, DNS_PORT))
                response, _ = upstream.recvfrom(4096)

            server.sendto(response, addr)
        except Exception as err:
            print(f"dns error for {addr}: {err}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
