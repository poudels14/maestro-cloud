import runpy
import struct
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import patch


DNS_DIR = Path(__file__).parent
sys.path.insert(0, str(DNS_DIR))
PROXY = runpy.run_path(str(DNS_DIR / "dns-proxy.py"))


class FakeSocket:
    def __init__(self, response):
        self.response = bytearray(
            b"\x05\x00"
            + b"\x05\x00\x00\x01\x7f\x00\x00\x01\x00\x35"
            + struct.pack("!H", len(response))
            + response
        )
        self.sent = []
        self.closed = False

    def sendall(self, data):
        self.sent.append(data)

    def recv(self, length):
        if not self.response:
            return b""
        chunk_length = min(length, 1)
        chunk = bytes(self.response[:chunk_length])
        del self.response[:chunk_length]
        return chunk

    def close(self):
        self.closed = True


class RejectingWorkerPool:
    def submit(self, function, *args):
        return False


class FakeUdpServer:
    def __init__(self):
        self.sent = []

    def sendto(self, data, addr):
        self.sent.append((data, addr))


class DnsProxyTests(unittest.TestCase):
    def test_alias_derivation_supports_standalone_and_multinode_suffixes(self):
        derive_alias = PROXY["derive_alias"]

        self.assertEqual(derive_alias("sandbox-ab12"), "sandbox")
        self.assertEqual(derive_alias("sandbox-5e02de75"), "sandbox")
        self.assertEqual(derive_alias("sandbox"), "sandbox")
        self.assertEqual(derive_alias("sandbox-not-an-id"), "sandbox-not-an-id")

    def test_duplicate_alias_claims_are_reported_as_conflicted(self):
        owners = PROXY["compute_alias_owners"](
            "sandbox-5e02de75",
            "sandbox",
            {"sandbox-a12bc345": "100.64.0.10"},
        )

        self.assertNotIn("sandbox", owners)

    def test_peer_query_uses_tailscale_socks5_proxy(self):
        sock = FakeSocket(b"peer-response")
        with patch.object(PROXY["socket"], "create_connection", return_value=sock) as connect:
            response = PROXY["resolve_peer"](b"dns-query", "100.64.0.10")

        self.assertEqual(response, b"peer-response")
        connect.assert_called_once_with(("127.0.0.1", 1055), timeout=5)
        self.assertEqual(
            sock.sent,
            [
                b"\x05\x01\x00",
                b"\x05\x01\x00\x01\x64\x40\x00\x0a\x00\x35",
                struct.pack("!H", 9) + b"dns-query",
            ],
        )
        self.assertTrue(sock.closed)

    def test_peer_verification_uses_tailnet_query(self):
        verify_peer = PROXY["verify_peer"]
        original = verify_peer.__globals__["resolve_peer"]
        try:
            verify_peer.__globals__["resolve_peer"] = lambda query, ip: b"maestro-dns-ok"
            self.assertTrue(verify_peer("100.64.0.10"))
        finally:
            verify_peer.__globals__["resolve_peer"] = original

    def test_worker_pool_bounds_running_and_queued_queries(self):
        pool = PROXY["BoundedWorkerPool"](1, 1, "dns-test")
        started = threading.Event()
        release = threading.Event()

        def blocked_query():
            started.set()
            release.wait(timeout=5)

        try:
            self.assertTrue(pool.submit(blocked_query))
            self.assertTrue(started.wait(timeout=1))
            self.assertTrue(pool.submit(blocked_query))
            self.assertFalse(pool.submit(blocked_query))
        finally:
            release.set()
            pool.shutdown()

    def test_blocked_lookup_does_not_delay_an_unrelated_query(self):
        pool = PROXY["BoundedWorkerPool"](2, 0, "dns-test")
        blocked = threading.Event()
        release = threading.Event()
        completed = threading.Event()

        def blocked_query():
            blocked.set()
            release.wait(timeout=5)

        try:
            self.assertTrue(pool.submit(blocked_query))
            self.assertTrue(blocked.wait(timeout=1))
            self.assertTrue(pool.submit(completed.set))
            self.assertTrue(completed.wait(timeout=1))
            self.assertFalse(release.is_set())
        finally:
            release.set()
            pool.shutdown()

    def test_saturated_udp_queue_returns_servfail(self):
        query_name = PROXY["encode_name"](["service", "cluster", "maestro", "internal"])
        query = (
            b"\x12\x34"
            + struct.pack("!HHHHH", 0x0100, 1, 0, 0, 0)
            + query_name
            + struct.pack("!HH", 1, 1)
        )
        server = FakeUdpServer()
        addr = ("10.50.0.1", 53000)

        accepted = PROXY["dispatch_udp_client"](
            server,
            query,
            addr,
            "cluster-abcd",
            "cluster",
            {},
            threading.Lock(),
            RejectingWorkerPool(),
        )

        self.assertFalse(accepted)
        self.assertEqual(len(server.sent), 1)
        response, response_addr = server.sent[0]
        self.assertEqual(response_addr, addr)
        self.assertEqual(response[:2], b"\x12\x34")
        self.assertEqual(struct.unpack("!H", response[2:4])[0] & 0x000F, 2)
        self.assertEqual(struct.unpack("!HHHH", response[4:12]), (1, 0, 0, 0))
        self.assertEqual(response[12:], query[12:])


if __name__ == "__main__":
    unittest.main()
