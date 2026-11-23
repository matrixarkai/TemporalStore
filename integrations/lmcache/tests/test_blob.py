# SPDX-License-Identifier: Apache-2.0
import http.client
import http.server
import json
import threading
import unittest
from unittest.mock import patch

from lmcache_temporalstore.blob import BlobClient


class BlobHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    blobs = {}
    read_addresses = []
    mutation_addresses = []
    drop_next_get = False
    batch_deletes = []
    supports_batch_deletes = False
    supports_batch_reads = False
    truncate_batch_read = False
    batch_reads = []
    supports_batch_writes = False
    batch_writes = []
    write_probes = 0
    lose_write_receipt = False

    def do_PUT(self):
        BlobHandler.mutation_addresses.append(self.client_address)
        BlobHandler.last_content_length = int(self.headers["Content-Length"])
        self.blobs[self.path] = self.rfile.read(BlobHandler.last_content_length)
        if self.path == "/blob/drop-receipt":
            self.close_connection = True
            return
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path == "/batch_write_blobs":
            BlobHandler.write_probes += 1
            self.send_response(200 if BlobHandler.supports_batch_writes else 404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        BlobHandler.read_addresses.append(self.client_address)
        if BlobHandler.drop_next_get:
            BlobHandler.drop_next_get = False
            self.close_connection = True
            return
        if self.path == "/blob/truncated":
            self.send_response(200)
            self.send_header("Content-Length", "4")
            self.end_headers()
            self.wfile.write(b"kv")
            self.close_connection = True
            return
        payload = self.blobs.get(self.path)
        if payload is None:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        if self.path == "/batch_write_blobs":
            self.receive_batch_write()
            return
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/batch_read_blobs":
            self.send_batch_read(request)
            return
        names = request["keys"]
        BlobHandler.batch_deletes.append(names)
        if not BlobHandler.supports_batch_deletes:
            self.send_error(404)
            return
        deleted = []
        for key in names:
            if key == "blocked":
                deleted.append(False)
                continue
            self.blobs.pop(f"/blob/{key}", None)
            deleted.append(True)
        data = json.dumps({"deleted": deleted}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def receive_batch_write(self):
        control_length = int.from_bytes(self.rfile.read(4), "big")
        items = json.loads(self.rfile.read(control_length))
        BlobHandler.batch_writes.append(items)
        statuses = bytearray()
        for item in items:
            payload = self.rfile.read(item["length"])
            if item["key"] == "blocked":
                statuses.append(0)
                continue
            self.blobs[f'/blob/{item["key"]}'] = payload
            statuses.append(1)
        if BlobHandler.lose_write_receipt:
            self.close_connection = True
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(statuses)))
        self.end_headers()
        self.wfile.write(statuses)

    def send_batch_read(self, request):
        BlobHandler.batch_reads.append(request)
        if not BlobHandler.supports_batch_reads:
            self.send_error(404)
            return
        statuses = bytearray()
        payloads = []
        for item in request:
            payload = self.blobs.get("/blob/" + item["key"])
            if payload is None:
                statuses.append(0)
            elif len(payload) != item["length"]:
                statuses.append(2)
            else:
                statuses.append(1)
                payloads.append(payload)
        data = bytes(statuses) + b"".join(payloads)
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if BlobHandler.truncate_batch_read:
            self.wfile.write(data[:-1])
            self.close_connection = True
        else:
            self.wfile.write(data)

    def do_DELETE(self):
        BlobHandler.mutation_addresses.append(self.client_address)
        self.blobs.pop(self.path, None)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *_args):
        pass


class BlobClientTest(unittest.TestCase):
    def setUp(self):
        BlobHandler.blobs = {}
        BlobHandler.read_addresses = []
        BlobHandler.mutation_addresses = []
        BlobHandler.drop_next_get = False
        BlobHandler.batch_deletes = []
        BlobHandler.supports_batch_deletes = False
        BlobHandler.supports_batch_reads = False
        BlobHandler.truncate_batch_read = False
        BlobHandler.batch_reads = []
        BlobHandler.supports_batch_writes = False
        BlobHandler.batch_writes = []
        BlobHandler.write_probes = 0
        BlobHandler.lose_write_receipt = False
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), BlobHandler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.client = BlobClient(
            f"http://127.0.0.1:{self.server.server_port}",
            reuse_read_connection=True, reuse_mutation_connection=True,
        )

    def tearDown(self):
        self.client.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_binary_round_trip_and_delete(self):
        payload = bytes(range(256)) * 4096
        self.client.put("lmcache/v1/test", memoryview(payload))
        self.assertEqual(BlobHandler.last_content_length, len(payload))
        destination = bytearray(len(payload))
        self.assertTrue(self.client.get_into("lmcache/v1/test", memoryview(destination)))
        self.assertEqual(payload, destination)
        self.client.delete("lmcache/v1/test")
        self.assertFalse(self.client.get_into("lmcache/v1/test", memoryview(destination)))

    def test_length_mismatch_does_not_modify_destination(self):
        self.client.put("lmcache/v1/short", memoryview(b"short"))
        destination = bytearray(b"untouched")
        self.assertFalse(self.client.get_into("lmcache/v1/short", memoryview(destination)))
        self.assertEqual(destination, b"untouched")

    def test_reuses_connection_and_reconnects_after_idle_disconnect(self):
        self.client.put("key", memoryview(b"data"))
        output = memoryview(bytearray(4))
        self.assertTrue(self.client.get_into("key", output))
        self.assertTrue(self.client.get_into("key", output))
        BlobHandler.drop_next_get = True
        self.assertTrue(self.client.get_into("key", output))
        self.assertEqual(output, b"data")
        first, second, dropped, reconnected = BlobHandler.read_addresses
        self.assertEqual(first, second)
        self.assertEqual(first, dropped)
        self.assertNotEqual(first, reconnected)

    def test_retires_connection_after_incomplete_response(self):
        self.client.put("key", memoryview(b"data"))
        output = memoryview(bytearray(4))
        self.assertTrue(self.client.get_into("key", output))
        self.assertFalse(self.client.get_into("truncated", output))
        self.assertTrue(self.client.get_into("key", output))
        self.assertEqual(output, b"data")
        first, truncated, reconnected = BlobHandler.read_addresses
        self.assertEqual(first, truncated)
        self.assertNotEqual(first, reconnected)

    def test_reuses_a_separate_connection_for_puts_and_deletes(self):
        payload = memoryview(b"data")
        self.client.put("key", payload)
        self.client.put("other", payload)
        output = memoryview(bytearray(4))
        self.assertTrue(self.client.get_into("key", output))
        self.client.delete("key")

        mutations = BlobHandler.mutation_addresses
        self.assertEqual(len(mutations), 4)
        self.assertEqual(len(set(mutations)), 1)
        self.assertNotEqual(mutations[0], BlobHandler.read_addresses[0])
        self.assertFalse(self.client.get_into("key", output))

    def test_put_is_not_replayed_after_the_server_drops_its_receipt(self):
        payload = memoryview(b"data")
        self.client.put("key", payload)
        with self.assertRaises(http.client.RemoteDisconnected):
            self.client.put("drop-receipt", payload)
        self.assertEqual(BlobHandler.blobs["/blob/drop-receipt"], b"data")
        self.client.put("other", payload)

        first, dropped, reconnected = BlobHandler.mutation_addresses
        self.assertEqual(first, dropped)
        self.assertNotEqual(first, reconnected)

    def test_batch_deletes_both_key_names_and_preserves_partial_results(self):
        BlobHandler.supports_batch_deletes = True
        self.client.put("key", memoryview(b"data"))
        self.client.put("anonymous/key", memoryview(b"data"))
        self.client.put("blocked", memoryview(b"data"))
        self.assertEqual(self.client.delete_many(["key", "blocked"]), [True, False])
        self.assertEqual(BlobHandler.batch_deletes, [
            ["key", "anonymous/key", "blocked", "anonymous/blocked"],
        ])
        self.assertEqual(BlobHandler.blobs, {"/blob/blocked": b"data"})
        self.assertEqual(len(BlobHandler.mutation_addresses), 3)

    def test_missing_batch_endpoint_uses_individual_deletes(self):
        self.client.put("key", memoryview(b"data"))
        self.assertEqual(self.client.delete_many(["key"]), [True])
        self.assertEqual(BlobHandler.batch_deletes, [["key", "anonymous/key"]])
        self.assertEqual(BlobHandler.blobs, {})
        self.assertEqual(len(BlobHandler.mutation_addresses), 3)

    def test_canonical_namespace_uses_only_one_path_for_reads_and_deletes(self):
        self.client.close()
        self.client = BlobClient(f"http://127.0.0.1:{self.server.server_port}", legacy_names=False)
        BlobHandler.supports_batch_deletes = True
        self.client.put("anonymous/owned", memoryview(b"legacy"))
        out = memoryview(bytearray(6))
        self.assertFalse(self.client.get_into("owned", out))
        self.assertEqual(len(BlobHandler.read_addresses), 1)

        self.client.put("owned", memoryview(b"new"))
        self.assertEqual(self.client.delete_many(["owned", "missing"]), [True, True])
        self.assertEqual(BlobHandler.batch_deletes, [["owned", "missing"]])
        self.assertEqual(BlobHandler.blobs, {"/blob/anonymous/owned": b"legacy"})

    def test_batch_write_matches_ordered_receipts_and_binary_payloads(self):
        BlobHandler.supports_batch_writes = True
        items = [("first", memoryview(b"abc")), ("blocked", memoryview(b"longer")),
                 ("empty", memoryview(b"")), ("last", memoryview(bytes((0, 255))))]
        self.assertEqual(self.client.put_many(items), [True, False, True, True])
        self.assertEqual(BlobHandler.blobs, {
            "/blob/first": b"abc", "/blob/empty": b"", "/blob/last": bytes((0, 255)),
        })
        self.assertEqual(len(BlobHandler.batch_writes), 1)
        self.assertEqual(BlobHandler.mutation_addresses, [])

    def test_batch_write_falls_back_without_sending_payload_to_older_nodes(self):
        items = [("first", memoryview(b"abc")), ("last", memoryview(b"def"))]
        self.assertEqual(self.client.put_many(items), [True, True])
        self.assertEqual(self.client.put_many(items), [True, True])
        self.assertEqual(BlobHandler.write_probes, 1)
        self.assertEqual(BlobHandler.batch_writes, [])
        self.assertEqual(len(BlobHandler.mutation_addresses), 4)

    def test_batch_write_does_not_replay_unconfirmed_mutations(self):
        BlobHandler.supports_batch_writes = True
        BlobHandler.lose_write_receipt = True
        items = [("first", memoryview(b"abc")), ("last", memoryview(b"def"))]
        self.assertEqual(self.client.put_many(items), [False, False])
        self.assertEqual(BlobHandler.blobs, {"/blob/first": b"abc", "/blob/last": b"def"})
        self.assertEqual(len(BlobHandler.batch_writes), 1)
        self.assertEqual(BlobHandler.mutation_addresses, [])

    def test_transport_failure_cools_down_without_replaying_pending_writes(self):
        self.client.close()
        self.client = BlobClient(
            f"http://127.0.0.1:{self.server.server_port}",
            reuse_mutation_connection=True, mutation_cooldown_seconds=1,
        )
        BlobHandler.supports_batch_writes = True
        BlobHandler.lose_write_receipt = True
        items = [("first", memoryview(b"abc")), ("last", memoryview(b"def"))]
        with patch("lmcache_temporalstore.blob.monotonic", return_value=100):
            self.assertEqual(self.client.put_many(items), [False, False])
            self.assertFalse(self.client.mutations_available())
            self.assertEqual(self.client.put_many(items), [None, None])
            self.assertEqual(len(BlobHandler.batch_writes), 1)
        BlobHandler.lose_write_receipt = False
        with patch("lmcache_temporalstore.blob.monotonic", return_value=102):
            self.assertEqual(self.client.put_many(items), [True, True])
            self.assertTrue(self.client.mutations_available())
            self.assertEqual(len(BlobHandler.batch_writes), 2)

    def test_connection_refusal_never_marks_upload_as_attempted(self):
        self.client.close()
        self.client = BlobClient(
            f"http://127.0.0.1:{self.server.server_port}", mutation_cooldown_seconds=1,
        )
        with patch.object(http.client.HTTPConnection, "connect", side_effect=ConnectionRefusedError):
            items = [("first", memoryview(b"abc")), ("last", memoryview(b"def"))]
            self.assertEqual(self.client.put_many(items), [None, None])
            self.assertFalse(self.client.mutations_available())
        self.assertEqual(BlobHandler.batch_writes, [])
        self.assertEqual(BlobHandler.blobs, {})

    def test_batch_load_preserves_positions_and_uses_legacy_paths_for_local_misses(self):
        BlobHandler.supports_batch_reads = True
        self.client.put("key", memoryview(b"data"))
        self.client.put("anonymous/legacy", memoryview(b"copy"))
        self.client.put("short", memoryview(b"x"))
        destinations = [memoryview(bytearray(b"!!!!")) for _ in range(4)]
        keys = ["key", "missing", "legacy", "short"]
        self.assertEqual(
            self.client.get_many_into(list(zip(keys, destinations))),
            [True, False, True, False],
        )
        self.assertEqual([bytes(dst) for dst in destinations], [b"data", b"!!!!", b"copy", b"!!!!"])
        self.assertEqual(len(BlobHandler.batch_reads), 1)
        self.assertEqual(len(BlobHandler.read_addresses), 4)

    def test_batch_load_uses_single_reads_when_route_is_missing(self):
        self.client.put("key", memoryview(b"data"))
        self.client.put("other", memoryview(b"copy"))
        values = [memoryview(bytearray(4)) for _ in range(2)]
        entries = list(zip(("key", "other"), values))
        for _ in range(2):
            self.assertEqual(self.client.get_many_into(entries), [True, True])
            self.assertEqual([bytes(dst) for dst in values], [b"data", b"copy"])
        self.assertEqual(len(BlobHandler.batch_reads), 1)
        self.assertEqual(len(BlobHandler.read_addresses), 4)

        BlobHandler.supports_batch_reads = True
        with patch("lmcache_temporalstore.blob.monotonic", return_value=float("inf")):
            self.assertEqual(self.client.get_many_into(entries), [True, True])
        self.assertEqual(len(BlobHandler.batch_reads), 2)
        self.assertEqual([bytes(dst) for dst in values], [b"data", b"copy"])

    def test_truncated_batch_fails_and_the_next_read_reconnects(self):
        BlobHandler.supports_batch_reads = True
        BlobHandler.truncate_batch_read = True
        self.client.put("key", memoryview(b"data"))
        self.client.put("other", memoryview(b"copy"))
        values = [memoryview(bytearray(4)) for _ in range(2)]
        self.assertEqual(self.client.get_many_into(list(zip(("key", "other"), values))), [False, False])
        self.assertTrue(self.client.get_into("other", values[1]))
        self.assertEqual(values[1], b"copy")


if __name__ == "__main__":
    unittest.main()
