#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""POST /v1/ingest_file: stream a file body -> store to the blob tier (content-addressed,
tenant-isolated, bounded memory via a temp spool) -> invoke the same ingest handler with a
temporalstore:// pointer. Pure-Python ASGI unit test with a stubbed blob connection + backend
(no live datanode, no network)."""
import asyncio
import hashlib
import json
import unittest

try:
    from tools import matrixark_v1_gateway as gw
except ImportError:
    import matrixark_v1_gateway as gw


# ---- stub backend ------------------------------------------------------------------------------
class _FakeServer:
    def __init__(self):
        self.calls = []

    def call_tool(self, name, args):
        self.calls.append((name, dict(args)))
        return {"ok": name}

    def handle(self, body):
        return {"jsonrpc": "2.0", "id": body.get("id"), "result": {}}


# ---- fake datanode blob connection (records the streamed body) ---------------------------------
class _FakeResponse:
    def __init__(self, status, body=b""):
        self.status = status
        self._body = body

    def read(self, amt=None):
        data, self._body = self._body, b""
        return data

    def getheader(self, name):
        return None


class _FakeConn:
    last = None

    def __init__(self, response):
        self._response = response
        self.method = self.url = None
        self.headers = {}
        self.body = b""
        _FakeConn.last = self

    def putrequest(self, method, url):
        self.method, self.url = method, url

    def putheader(self, name, value):
        self.headers[name] = value

    def endheaders(self):
        pass

    def send(self, data):
        self.body += data

    def getresponse(self):
        return self._response

    def close(self):
        pass


def _factory_for(response):
    return lambda cfg: _FakeConn(response)


def drive(app, *, method="POST", path="/v1/ingest_file", chunks=None, raw=None, headers=None):
    hdrs = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {"type": "http", "method": method, "path": path, "headers": hdrs}
    if chunks is not None:
        queue = list(chunks)

        async def receive():
            if queue:
                b = queue.pop(0)
                return {"type": "http.request", "body": b, "more_body": bool(queue)}
            return {"type": "http.request", "body": b"", "more_body": False}
    else:
        payload = raw if raw is not None else b""

        async def receive():
            return {"type": "http.request", "body": payload, "more_body": False}

    sent = []

    async def send(msg):
        sent.append(msg)

    asyncio.run(app(scope, receive, send))
    start = next(x for x in sent if x["type"] == "http.response.start")
    hmap = {k.decode().lower(): v.decode() for k, v in start["headers"]}
    data = b"".join(x.get("body", b"") for x in sent if x["type"] == "http.response.body")
    return start["status"], hmap, data


def _cfg(**over):
    base = {"api_keys": {"k-acme": "acme"}, "require_auth": True}
    base.update(over)
    return gw.GatewayConfig.from_env(base)


class IngestFileRouteTest(unittest.TestCase):
    def setUp(self):
        self.s = _FakeServer()

    def _app(self, response=None, **cfgover):
        resp = response if response is not None else _FakeResponse(200, json.dumps({"stored": True}).encode())
        return gw.make_v1_app(self.s, _cfg(blob_connection_factory=_factory_for(resp), **cfgover))

    def test_requires_auth(self):
        st, _, _ = drive(self._app(), raw=b"hi")
        self.assertEqual(401, st)
        self.assertEqual([], self.s.calls)

    def test_method_not_allowed(self):
        st, _, _ = drive(self._app(), method="GET", headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(405, st)

    def test_small_file_stores_blob_and_invokes_ingest(self):
        body = b"# a small skill\nbody bytes"
        st, _, out = drive(self._app(), chunks=[b"# a small skill\n", b"body bytes"],
                           headers={"Authorization": "Bearer k-acme",
                                    "X-Resource-Kind": "skill", "X-Filename": "fix-auth.md"})
        self.assertEqual(202, st)
        sha = hashlib.sha256(body).hexdigest()
        logical_key = f"resources/{sha[:2]}/{sha}"
        # Blob was PUT to the datanode, tenant-isolated, exact bytes streamed.
        self.assertEqual("PUT", _FakeConn.last.method)
        self.assertEqual(f"/blob/acme/{logical_key}", _FakeConn.last.url)   # tenant-isolated
        self.assertEqual(body, _FakeConn.last.body)                          # exact bytes
        self.assertEqual(str(len(body)), _FakeConn.last.headers["Content-Length"])
        # Ingest handler invoked with the temporalstore:// pointer (reused, not duplicated).
        self.assertEqual(1, len(self.s.calls))
        name, args = self.s.calls[0]
        self.assertEqual("matrixark_ingest", name)
        self.assertEqual(f"temporalstore://{logical_key}", args["raw_uri"])
        self.assertEqual("skill", args["kind"])
        self.assertEqual("md", args["resource_type"])           # inferred from X-Filename
        self.assertEqual("acme", args["tenant"])                # identity resolved like /v1/ingest
        payload = json.loads(out)
        self.assertEqual(0, payload["accepted"])                # resource ingest, fast-ack

    def test_resource_type_header_wins_over_filename(self):
        st, _, _ = drive(self._app(), raw=b"data",
                         headers={"Authorization": "Bearer k-acme",
                                  "X-Resource-Type": "txt", "X-Filename": "thing.md"})
        self.assertEqual(202, st)
        self.assertEqual("txt", self.s.calls[0][1]["resource_type"])

    def test_default_kind_is_skill(self):
        drive(self._app(), raw=b"data", headers={"Authorization": "Bearer k-acme"})
        self.assertEqual("skill", self.s.calls[0][1]["kind"])

    def test_five_megabyte_body_streams_bounded_and_ingests(self):
        # ~5 MB streamed in 64 KB chunks -> spooled to disk, hashed, streamed to the blob tier.
        chunk = b"y" * 65536
        n = 80  # 80 * 64KiB = 5 MiB
        body = chunk * n
        st, _, _ = drive(self._app(), chunks=[chunk] * n,
                         headers={"Authorization": "Bearer k-acme", "X-Filename": "big.md"})
        self.assertEqual(202, st)
        sha = hashlib.sha256(body).hexdigest()
        self.assertEqual(len(body), len(_FakeConn.last.body))
        self.assertEqual(body, _FakeConn.last.body)
        self.assertEqual(f"temporalstore://resources/{sha[:2]}/{sha}", self.s.calls[0][1]["raw_uri"])

    def test_wait_header_triggers_finalize(self):
        st, _, out = drive(self._app(), raw=b"data",
                           headers={"Authorization": "Bearer k-acme", "X-Wait": "true"})
        self.assertEqual(202, st)
        # finalize path calls ingest then session_commit (batched extraction now).
        self.assertEqual(["matrixark_ingest", "matrixark_session_commit"],
                         [c[0] for c in self.s.calls])
        self.assertTrue(json.loads(out)["finalized"])

    def test_oversized_body_413_by_cap(self):
        app = self._app(max_blob_bytes=8)
        st, _, out = drive(app, chunks=[b"aaaa", b"bbbb", b"cccc"],
                           headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(413, st)
        self.assertEqual("payload_too_large", json.loads(out)["error"])
        self.assertEqual([], self.s.calls)                       # never reached ingest

    def test_oversized_body_413_by_content_length(self):
        app = self._app(max_blob_bytes=4)
        st, _, out = drive(app, raw=b"toolong",
                           headers={"Authorization": "Bearer k-acme", "Content-Length": "7"})
        self.assertEqual(413, st)
        self.assertEqual("payload_too_large", json.loads(out)["error"])

    def test_blob_store_failure_surfaces_and_skips_ingest(self):
        app = self._app(_FakeResponse(500, b"datanode boom"))
        st, _, out = drive(app, raw=b"data", headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(502, st)
        self.assertEqual("blob_store_failed", json.loads(out)["error"])
        self.assertEqual([], self.s.calls)                       # ingest not invoked on store failure

    def test_existing_ingest_route_unaffected(self):
        # Regression: the plain JSON /v1/ingest still works alongside the new route.
        app = self._app()
        st, _, out = drive(app, path="/v1/ingest", raw=json.dumps({"records": [1]}).encode(),
                           headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(202, st)
        self.assertEqual("matrixark_ingest", self.s.calls[0][0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
