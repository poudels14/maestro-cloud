import socket
import sys


DOCKER_DNS = "127.0.0.11"
DNS_PORT = 53


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


def main():
    if len(sys.argv) < 2:
        print("usage: dns-proxy.py <domain>", file=sys.stderr)
        sys.exit(1)

    domain = sys.argv[1]
    domain_labels = domain.lower().split(".")
    suffix_len = len(domain_labels)

    print(
        f"dns proxy started, domain={domain}, upstream={DOCKER_DNS}",
        file=sys.stderr,
        flush=True,
    )

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

            if (
                len(lower_labels) > suffix_len
                and lower_labels[-suffix_len:] == domain_labels
            ):
                bare_labels = labels[:-suffix_len]
                original_qname = encode_name(labels)
                bare_qname = encode_name(bare_labels)
                rewritten = data[:12] + bare_qname + data[qname_end:]
                upstream.sendto(rewritten, (DOCKER_DNS, DNS_PORT))
                response, _ = upstream.recvfrom(4096)
                response = response.replace(bare_qname, original_qname)
            else:
                upstream.sendto(data, (DOCKER_DNS, DNS_PORT))
                response, _ = upstream.recvfrom(4096)

            server.sendto(response, addr)
        except Exception as err:
            print(f"dns error for {addr}: {err}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
