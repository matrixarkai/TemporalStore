"""The HTTP transport must return the SAME answers the pipe returns.

Not "HTTP works": the point of the change is that a client swaps where it sends without changing
what it sends or what it gets back, so every case here runs the identical request down BOTH
transports and compares. A test that only exercised HTTP would pass just as happily if the two
had quietly diverged.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import http.client

PROXY = os.environ.get("PROXY_BIN", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "target", "release", "matrixark_rust_proxy"))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def volatile(response):
    """Drop the fields that MUST differ between two runs, and only those."""
    out = {k: v for k, v in response.items() if k not in {
        "elapsed_ms", "rust_engine_time_ms", "serialization_time_ms",
        "root", "append_path", "prometheus",
    }}
    return out


class HttpTransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(PROXY):
            raise unittest.SkipTest(f"no proxy binary at {PROXY}")
        cls.http_root = tempfile.mkdtemp(prefix="httpproxy")
        cls.pipe_root = tempfile.mkdtemp(prefix="pipeproxy")
        cls.port = free_port()
        env = dict(os.environ, TS_STANDALONE="1")
        cls.http_proc = subprocess.Popen(
            [PROXY, "--serve-http", f"127.0.0.1:{cls.port}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
        )
        # Wait for the listener rather than sleeping a guessed interval.
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", cls.port), timeout=0.5):
                    break
            except OSError:
                if cls.http_proc.poll() is not None:
                    raise AssertionError(
                        "proxy exited: " + cls.http_proc.stderr.read().decode()[-2000:]
                    )
                time.sleep(0.1)
        else:
            raise AssertionError("proxy never listened")
        cls.pipe_proc = subprocess.Popen(
            [PROXY, "--serve"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
        )

    @classmethod
    def tearDownClass(cls):
        for proc in (getattr(cls, "http_proc", None), getattr(cls, "pipe_proc", None)):
            if proc is not None:
                proc.kill()
                proc.wait()

    def over_http(self, request):
        body = json.dumps(request).encode("utf-8")
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=60)
        try:
            conn.request("POST", "/", body=body,
                         headers={"Content-Type": "application/json",
                                  "Content-Length": str(len(body))})
            resp = conn.getresponse()
            self.assertEqual(resp.status, 200)
            return json.loads(resp.read())
        finally:
            conn.close()

    def over_pipe(self, request):
        self.pipe_proc.stdin.write((json.dumps(request) + "\n").encode("utf-8"))
        self.pipe_proc.stdin.flush()
        while True:
            line = self.pipe_proc.stdout.readline()
            if not line:
                raise AssertionError("pipe proxy closed: "
                                     + self.pipe_proc.stderr.read().decode()[-2000:])
            if line.strip().startswith(b"{"):
                return json.loads(line)

    # The store root is DERIVED from namespace/table -- `record_log_root` in the request is
    # ignored -- so the arms are separated by namespace, and the namespace is unique PER RUN.
    #
    # Per-run matters as much as per-arm. A fixed namespace resolves to the same directory under
    # /tmp every time, so a store written by an earlier run outlives it; a build from a different
    # lineage then refuses to load it ("wal record integrity error ... EngineWalItem.object_id:
    # invalid wire type") and this suite reports a transport failure that is really a stale store.
    # Observed exactly that way. A unique namespace makes each run start from nothing, which is
    # also what makes the two arms comparable.
    _RUN = f"{os.getpid()}{int(time.time()) % 100000}"
    HTTP_NS = f"httparm{_RUN}"
    PIPE_NS = f"pipearm{_RUN}"

    def both(self, request):
        http_req = dict(request, namespace=self.HTTP_NS, table="t")
        pipe_req = dict(request, namespace=self.PIPE_NS, table="t")
        return self.over_http(http_req), self.over_pipe(pipe_req)

    def test_health_agrees(self):
        got, want = self.both({"op": "health"})
        self.assertTrue(got["ok"], got)
        self.assertEqual(volatile(got), volatile(want))

    def test_a_write_then_a_read_agrees(self):
        write = {"op": "put_string", "key": "k1", "value": "v1"}
        got_w, want_w = self.both(write)
        self.assertTrue(got_w["ok"], got_w)
        self.assertEqual(volatile(got_w), volatile(want_w))

        read = {"op": "get_string", "key": "k1"}
        got_r, want_r = self.both(read)
        self.assertTrue(got_r["ok"], got_r)
        self.assertEqual(got_r["value"], "v1")
        self.assertEqual(volatile(got_r), volatile(want_r))

    def test_a_bad_request_fails_the_same_way(self):
        # An application-level failure must stay a 200 with ok=false, exactly as the pipe
        # reports it -- a client that changed transport must not start seeing transport errors
        # for answers the proxy actually produced.
        got, want = self.both({"op": "no_such_op"})
        self.assertFalse(got["ok"], got)
        self.assertEqual(volatile(got), volatile(want))

    def test_malformed_json_is_reported_not_dropped(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=60)
        try:
            body = b"{not json"
            conn.request("POST", "/", body=body,
                         headers={"Content-Type": "application/json",
                                  "Content-Length": str(len(body))})
            resp = conn.getresponse()
            self.assertEqual(resp.status, 200)
            payload = json.loads(resp.read())
        finally:
            conn.close()
        self.assertFalse(payload["ok"])
        self.assertIn("invalid JSON", payload.get("error", ""))

    def test_the_client_request_id_is_echoed(self):
        # The id is how a caller discards the late answer to a request it abandoned instead of
        # shifting every later reply back by one. It must survive the new transport.
        got = self.over_http({"op": "health", "namespace": self.HTTP_NS, "table": "t",
                              "client_request_id": "abc-123"})
        self.assertEqual(got.get("client_request_id"), "abc-123")

    def test_keep_alive_serves_many_requests_on_one_connection(self):
        # The transport exists to remove per-request overhead; if the server closed the
        # connection each time, the client would pay a TCP handshake per call and this would
        # fail on the second request.
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=60)
        try:
            for i in range(5):
                body = json.dumps({"op": "health", "namespace": self.HTTP_NS, "table": "t"}).encode()
                conn.request("POST", "/", body=body,
                             headers={"Content-Type": "application/json",
                                      "Content-Length": str(len(body))})
                resp = conn.getresponse()
                self.assertEqual(resp.status, 200, f"request {i}")
                self.assertTrue(json.loads(resp.read())["ok"])
        finally:
            conn.close()

    def test_concurrent_callers_all_get_answers(self):
        """Several clients at once, each on its own connection.

        With the concurrency gate OFF (the default) these serialize inside the proxy, which is
        exactly what the pipe did -- so this asserts they all COMPLETE, not that they overlap.
        A deadlock or a crossed response would show up here as a missing or wrong answer.
        """
        import threading
        results = {}

        def call(index):
            try:
                got = self.over_http({"op": "put_string", "key": f"c{index}",
                                      "value": f"v{index}",
                                      "namespace": self.HTTP_NS, "table": "t"})
                results[index] = got.get("ok")
            except Exception as exc:  # noqa: BLE001 - recorded, then asserted below
                results[index] = repr(exc)

        threads = [threading.Thread(target=call, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        self.assertEqual(results, {i: True for i in range(8)})

        for i in range(8):
            got = self.over_http({"op": "get_string", "key": f"c{i}",
                                  "namespace": self.HTTP_NS, "table": "t"})
            self.assertEqual(got["value"], f"v{i}", f"key c{i} came back wrong")


if __name__ == "__main__":
    unittest.main(verbosity=2)
