# SPDX-License-Identifier: Apache-2.0
import http.server
import threading
import unittest

from lmcache_temporalstore.blob import BlobClient


class BlobHandler(http.server.BaseHTTPRequestHandler):
    blobs = {}

    def do_PUT(self):
        BlobHandler.last_content_length = int(self.headers["Content-Length"])
        self.blobs[self.path] = self.rfile.read(BlobHandler.last_content_length)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        payload = self.blobs.get(self.path)
        if payload is None:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_DELETE(self):
        self.blobs.pop(self.path, None)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *_args):
        pass


class BlobClientTest(unittest.TestCase):
    def setUp(self):
        BlobHandler.blobs = {}
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), BlobHandler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.client = BlobClient(f"http://127.0.0.1:{self.server.server_port}")

    def tearDown(self):
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


if __name__ == "__main__":
    unittest.main()
