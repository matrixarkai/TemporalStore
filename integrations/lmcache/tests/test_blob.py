# SPDX-License-Identifier: Apache-2.0
import http.client
import http.server
import json
import threading
import unittest

from lmcache_temporalstore.blob import BlobClient


class BlobHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    blobs = {}
    read_addresses = []
    mutation_addresses = []
    drop_next_get = False
    batch_deletes = []
    supports_batch_deletes = False

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
        names = json.loads(self.rfile.read(int(self.headers["Content-Length"])))["keys"]
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


if __name__ == "__main__":
    unittest.main()
