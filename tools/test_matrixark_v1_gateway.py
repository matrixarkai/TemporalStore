#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Cloud API `/v1/*` gateway: auth (401), ingest 202, retrieve/commit 200, rate-limit 429,
quota 413, health probes, blob stream proxy, and `/api/*` back-compat — pure-Python unittest."""
import asyncio
import json
import unittest

try:
    from tools import matrixark_v1_gateway as gw
except ImportError:  # run from tools/ dir
    import matrixark_v1_gateway as gw


# ---- stub backend -------------------------------------------------------------------------------
class _FakeServer:
    def __init__(self):
        self.calls = []
        self.handled = []

    def call_tool(self, name, args):
        self.calls.append((name, dict(args)))
        if name == "matrixark_retrieve":
            return {"pack": [{"text": "staging = 1.9.2"}], "tokens": 7}
        return {"ok": name}

    def handle(self, body):  # used by the legacy /mcp + /v1/mcp path
        self.handled.append(body)
        return {"jsonrpc": "2.0", "id": body.get("id"), "result": {"m": body.get("method")}}


# ---- fake datanode blob connection (no network) -------------------------------------------------
class _FakeResponse:
    def __init__(self, status, body=b"", headers=None):
        self.status = status
        self._body = body
        self._pos = 0
        self._headers = headers or {}

    def read(self, amt=None):
        if amt is None:
            data, self._pos = self._body[self._pos:], len(self._body)
            return data
        data = self._body[self._pos:self._pos + amt]
        self._pos += len(data)
        return data

    def getheader(self, name):
        return self._headers.get(name.lower())


class _FakeConn:
    """Records the request line/headers/body and returns a canned response."""

    last = None

    def __init__(self, response):
        self._response = response
        self.method = None
        self.url = None
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


def _direct_factory_for(response):
    """Direct-backend connection factory (no `cfg` arg, unlike the blob factory)."""
    return lambda: _FakeConn(response)


# ---- ASGI test harness --------------------------------------------------------------------------
def drive(app, *, method="POST", path="/v1/ingest", body=None, raw=None,
          headers=None, chunks=None):
    """Run one request; return (status, headers_dict, body_bytes)."""
    if raw is not None:
        payload = raw
    else:
        payload = json.dumps(body if body is not None else {}).encode()
    hdrs = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {"type": "http", "method": method, "path": path, "headers": hdrs}

    # Optionally stream the body as multiple http.request messages.
    if chunks is not None:
        queue = list(chunks)

        async def receive():
            if queue:
                b = queue.pop(0)
                return {"type": "http.request", "body": b, "more_body": bool(queue)}
            return {"type": "http.request", "body": b"", "more_body": False}
    else:
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
    base = {"api_keys": {"k-acme": "acme", "k-globex": "globex"}, "require_auth": True}
    base.update(over)
    return gw.GatewayConfig.from_env(base)


class GatewayTest(unittest.TestCase):
    def setUp(self):
        self.s = _FakeServer()
        self.cfg = _cfg()
        self.app = gw.make_v1_app(self.s, self.cfg)

    # ---- auth -----------------------------------------------------------------------------------
    def test_missing_key_401(self):
        st, _, body = drive(self.app, path="/v1/ingest", body={"records": [1]})
        self.assertEqual(401, st)
        self.assertEqual("unauthorized", json.loads(body)["error"])
        self.assertEqual([], self.s.calls)

    def test_bad_key_401(self):
        st, _, _ = drive(self.app, path="/v1/retrieve", body={"query": "x"},
                         headers={"Authorization": "Bearer nope"})
        self.assertEqual(401, st)

    def test_anonymous_allowed_when_auth_disabled(self):
        app = gw.make_v1_app(self.s, _cfg(require_auth=False))
        st, _, _ = drive(app, path="/v1/retrieve", body={"query": "x"})
        self.assertEqual(200, st)

    # ---- happy paths ----------------------------------------------------------------------------
    def test_ingest_202_and_scope_isolation(self):
        st, hmap, body = drive(self.app, path="/v1/ingest",
                               body={"scope": "agent-7", "records": [{"t": "a"}, {"t": "b"}]},
                               headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(202, st)
        payload = json.loads(body)
        self.assertEqual(2, payload["accepted"])
        # scope is an OBJECT (backend contract); tenant isolation via tenant_id + namespace label.
        self.assertEqual({"tenant_id": "acme", "namespace": "acme/agent-7"}, payload["scope"])
        name, args = self.s.calls[0]
        self.assertEqual("matrixark_ingest", name)
        self.assertTrue(args["async_processing"])                    # fast-ack default
        self.assertEqual("acme", args["tenant"])
        self.assertEqual("k-acme", args["api_key"])
        self.assertIn("x-ratelimit-limit", hmap)                     # rate-limit headers present

    def test_ingest_finalize_triggers_batched_extraction(self):
        # A complete conversation (finalize=true) ingests AND triggers batched extraction now.
        st, _, body = drive(self.app, path="/v1/ingest",
                            body={"scope": "agent-7",
                                  "messages": [{"role": "user", "content": "hi"}],
                                  "finalize": True},
                            headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(202, st)
        payload = json.loads(body)
        self.assertTrue(payload["finalized"])
        names = [c[0] for c in self.s.calls]
        self.assertEqual(["matrixark_ingest", "matrixark_session_commit"], names)
        # extraction runs over the same isolated scope as the ingest.
        self.assertEqual({"tenant_id": "acme", "namespace": "acme/agent-7"}, self.s.calls[1][1]["scope"])

    def test_ingest_kind_conversation_also_finalizes(self):
        drive(self.app, path="/v1/ingest",
              body={"records": [1], "kind": "conversation"},
              headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(["matrixark_ingest", "matrixark_session_commit"], [c[0] for c in self.s.calls])

    def test_ingest_message_does_not_extract(self):
        # A plain streaming message buffers -- no extraction/session_commit.
        st, _, body = drive(self.app, path="/v1/ingest", body={"records": [1]},
                            headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(202, st)
        self.assertEqual(["matrixark_ingest"], [c[0] for c in self.s.calls])
        self.assertNotIn("finalized", json.loads(body))

    def test_scope_defaults_to_tenant_and_no_double_prefix(self):
        drive(self.app, path="/v1/ingest", body={"records": [1]},
              headers={"Authorization": "Bearer k-acme"})
        self.assertEqual({"tenant_id": "acme"}, self.s.calls[0][1]["scope"])
        self.s.calls.clear()
        drive(self.app, path="/v1/ingest", body={"scope": "acme/x", "records": [1]},
              headers={"Authorization": "Bearer k-acme"})
        # already-prefixed string label preserved under the object's namespace, no double-prefix.
        self.assertEqual({"tenant_id": "acme", "namespace": "acme/x"}, self.s.calls[0][1]["scope"])

    def test_retrieve_200(self):
        st, hmap, body = drive(self.app, path="/v1/retrieve", body={"query": "roll staging"},
                               headers={"X-API-Key": "k-globex"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_retrieve", self.s.calls[0][0])
        self.assertNotIn("async_processing", self.s.calls[0][1])
        self.assertEqual(7, json.loads(body)["tokens"])              # ContextPack returned directly

    def test_session_commit_200(self):
        st, _, _ = drive(self.app, path="/v1/session/commit", body={"force": True},
                         headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_session_commit", self.s.calls[0][0])

    def test_mcp_dispatch_200(self):
        st, _, body = drive(self.app, path="/v1/mcp",
                            body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                            headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual({"m": "tools/list"}, json.loads(body)["result"])

    # ---- rate limiting --------------------------------------------------------------------------
    def test_rate_limit_429_with_headers(self):
        app = gw.make_v1_app(self.s, _cfg(ingest_rps=1, ingest_burst=1))
        st1, h1, _ = drive(app, path="/v1/ingest", body={"records": [1]},
                           headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(202, st1)
        self.assertEqual("1", h1["x-ratelimit-limit"])
        self.assertEqual("0", h1["x-ratelimit-remaining"])
        st2, h2, body = drive(app, path="/v1/ingest", body={"records": [1]},
                              headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(429, st2)
        self.assertEqual("rate_limited", json.loads(body)["error"])
        self.assertIn("retry-after", h2)
        self.assertIn("x-ratelimit-remaining", h2)

    def test_rate_limit_is_per_key(self):
        app = gw.make_v1_app(self.s, _cfg(ingest_rps=1, ingest_burst=1))
        drive(app, path="/v1/ingest", body={"records": [1]}, headers={"Authorization": "Bearer k-acme"})
        # a different tenant's key still has its own bucket -> not throttled
        st, _, _ = drive(app, path="/v1/ingest", body={"records": [1]},
                         headers={"Authorization": "Bearer k-globex"})
        self.assertEqual(202, st)

    # ---- quotas ---------------------------------------------------------------------------------
    def test_oversized_body_413(self):
        app = gw.make_v1_app(self.s, _cfg(max_body_bytes=32))
        big = json.dumps({"records": [{"x": "y" * 200}]}).encode()
        st, _, body = drive(app, path="/v1/ingest", raw=big, headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(413, st)
        self.assertEqual("payload_too_large", json.loads(body)["error"])
        self.assertEqual([], self.s.calls)

    def test_oversized_batch_413(self):
        app = gw.make_v1_app(self.s, _cfg(max_batch=3))
        st, _, body = drive(app, path="/v1/ingest", body={"records": [1, 2, 3, 4]},
                            headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(413, st)
        self.assertEqual("payload_too_large", json.loads(body)["error"])
        self.assertEqual([], self.s.calls)

    def test_storage_quota_507(self):
        class _QuotaServer(_FakeServer):
            def call_tool(self, name, args):
                raise RuntimeError("tenant storage quota exceeded")
        app = gw.make_v1_app(_QuotaServer(), self.cfg)
        st, _, body = drive(app, path="/v1/ingest", body={"records": [1]},
                            headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(507, st)
        self.assertEqual("storage_quota_exceeded", json.loads(body)["error"])

    # ---- health ---------------------------------------------------------------------------------
    def test_healthz(self):
        st, _, body = drive(self.app, method="GET", path="/v1/healthz")
        self.assertEqual(200, st)
        self.assertEqual("ok", json.loads(body)["status"])

    def test_readyz(self):
        # probe factory returns a 200 /health response
        app = gw.make_v1_app(self.s, _cfg(blob_connection_factory=_factory_for(_FakeResponse(200))))
        st, _, body = drive(app, method="GET", path="/v1/readyz")
        self.assertEqual(200, st)
        payload = json.loads(body)
        self.assertTrue(payload["ready"])
        self.assertEqual("ok", payload["datanode"])

    # ---- blob proxy (streamed, monkeypatched connection) ----------------------------------------
    def test_blob_put_streams_and_returns_receipt(self):
        resp = _FakeResponse(200, body=json.dumps({"stored": True}).encode())
        app = gw.make_v1_app(self.s, _cfg(blob_connection_factory=_factory_for(resp)))
        st, _, body = drive(app, method="PUT", path="/v1/blob/report-q3.pdf",
                            chunks=[b"hello ", b"world"],
                            headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        payload = json.loads(body)
        self.assertTrue(payload["stored"])
        self.assertEqual(11, payload["bytes"])                       # hello world
        self.assertEqual("PUT", _FakeConn.last.method)
        self.assertEqual("/blob/acme/report-q3.pdf", _FakeConn.last.url)  # tenant-isolated key
        self.assertEqual(b"hello world", _extract_streamed_body(_FakeConn.last))

    def test_blob_get_streams_back(self):
        resp = _FakeResponse(200, body=b"BLOBDATA", headers={"content-length": "8"})
        app = gw.make_v1_app(self.s, _cfg(blob_connection_factory=_factory_for(resp)))
        st, hmap, body = drive(app, method="GET", path="/v1/blob/acme/x.bin",
                               headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual(b"BLOBDATA", body)
        self.assertEqual("8", hmap["content-length"])

    def test_blob_requires_auth(self):
        st, _, _ = drive(self.app, method="GET", path="/v1/blob/x")
        self.assertEqual(401, st)

    def test_oversized_blob_413_by_content_length(self):
        app = gw.make_v1_app(self.s, _cfg(max_blob_bytes=4,
                                          blob_connection_factory=_factory_for(_FakeResponse(200))))
        st, _, body = drive(app, method="PUT", path="/v1/blob/big.bin", raw=b"toolong",
                            headers={"Authorization": "Bearer k-acme", "Content-Length": "7"})
        self.assertEqual(413, st)
        self.assertEqual("payload_too_large", json.loads(body)["error"])

    # ---- back-compat ----------------------------------------------------------------------------
    def test_api_backcompat_still_routes(self):
        st, _, body = drive(self.app, path="/api/ingest",
                            body={"messages": [{"role": "user", "content": "hi"}]},
                            headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)                                    # legacy front returns 200
        self.assertEqual("matrixark_ingest", self.s.calls[0][0])
        self.assertEqual("ok", json.loads(body)["status"])

    def test_legacy_healthz_still_routes(self):
        st, _, body = drive(self.app, method="GET", path="/healthz")
        self.assertEqual(200, st)
        self.assertEqual("ok", json.loads(body)["status"])


class DirectBackendTest(unittest.TestCase):
    """Direct network path (B): /v1/ingest + /v1/retrieve POST high-level bodies to the proxy."""

    def setUp(self):
        self.s = _FakeServer()

    def _direct_app(self, response, **over):
        cfg = gw.GatewayConfig.from_env({
            "api_keys": {"k-acme": "acme"},
            "require_auth": True,
            "direct_backend": True,
            "direct_backend_url": over.pop("direct_backend_url", "http://127.0.0.1:17000"),
            "direct_connection_factory": _direct_factory_for(response),
            **over,
        })
        return gw.make_v1_app(self.s, cfg)

    def test_direct_ingest_202_and_bypasses_mcp(self):
        report = {"status": {"ok": True}, "accepted": 2, "node_hashes": [11, 12]}
        app = self._direct_app(_FakeResponse(200, body=json.dumps(report).encode()))
        st, hmap, body = drive(
            app, path="/v1/ingest",
            body={"messages": [{"role": "user", "content": "a"},
                               {"role": "assistant", "content": "b"}]},
            headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(202, st)
        payload = json.loads(body)
        self.assertEqual(2, payload["accepted"])                     # fast-ack accepted count
        self.assertEqual(report, payload["result"])                  # datanode report passed through
        self.assertEqual([], self.s.calls)                           # MCP adapter NOT used
        # POSTed the high-level {scope, messages} body to the proxy's /context/ingest.
        self.assertEqual("POST", _FakeConn.last.method)
        self.assertEqual("/context/ingest", _FakeConn.last.url)
        sent = json.loads(_FakeConn.last.body)
        self.assertEqual({"tenant_id": "acme"}, sent["scope"])       # gateway injects tenant, no hashing
        self.assertEqual(2, len(sent["messages"]))
        self.assertIn("x-ratelimit-limit", hmap)

    def test_direct_ingest_finalize_routes_to_extract(self):
        app = self._direct_app(_FakeResponse(200, body=json.dumps({"status": {"ok": True}}).encode()))
        st, _, body = drive(
            app, path="/v1/ingest",
            body={"messages": [{"role": "user", "content": "hi"}], "finalize": True},
            headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(202, st)
        self.assertTrue(json.loads(body)["finalized"])               # batched extraction now
        self.assertEqual("/context/extract", _FakeConn.last.url)     # finalize -> extract route
        self.assertEqual([], self.s.calls)

    def test_direct_session_commit_routes_to_extract(self):
        # /v1/session/commit replays + extracts the buffered raw events (scope only).
        app = self._direct_app(_FakeResponse(200, body=json.dumps({"status": {"ok": True}, "accepted": 3}).encode()))
        st, _, body = drive(app, path="/v1/session/commit", body={"force": True},
                            headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual("/context/extract", _FakeConn.last.url)
        self.assertEqual([], self.s.calls)                           # MCP adapter NOT used
        self.assertEqual({"tenant_id": "acme"}, json.loads(_FakeConn.last.body)["scope"])

    def test_direct_retrieve_200_and_bypasses_mcp(self):
        pack = {"pack": [{"text": "staging = 1.9.2"}], "tokens": 7, "status": {"ok": True}}
        app = self._direct_app(_FakeResponse(200, body=json.dumps(pack).encode()))
        st, _, body = drive(app, path="/v1/retrieve", body={"query": "roll staging"},
                            headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual(7, json.loads(body)["tokens"])              # ContextPack passed through
        self.assertEqual([], self.s.calls)                           # MCP adapter NOT used
        self.assertEqual("/context/retrieve", _FakeConn.last.url)
        sent = json.loads(_FakeConn.last.body)
        self.assertEqual({"tenant_id": "acme"}, sent["scope"])
        self.assertEqual("roll staging", sent["query"])

    def test_direct_posts_to_configured_url_with_path_prefix(self):
        client = gw.DirectBackendClient("http://10.0.0.5:18080/edge",
                                        connection_factory=_direct_factory_for(_FakeResponse(200, b"{}")))
        self.assertEqual("10.0.0.5", client.host)
        self.assertEqual(18080, client.port)
        self.assertEqual("/edge", client.base_path)
        status, data = asyncio.run(client.post_json("/context/retrieve", {"query": "x"}))
        self.assertEqual(200, status)
        self.assertEqual("/edge/context/retrieve", _FakeConn.last.url)  # base path honored

    def test_flag_off_keeps_mcp_path(self):
        # With the flag OFF the direct client is never built; retrieve uses the MCP adapter.
        cfg = gw.GatewayConfig.from_env({"api_keys": {"k-acme": "acme"}, "require_auth": True})
        app = gw.make_v1_app(self.s, cfg)
        st, _, _ = drive(app, path="/v1/retrieve", body={"query": "x"},
                         headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_retrieve", self.s.calls[0][0])


def _extract_streamed_body(conn):
    """Reassemble what the PUT streamed to the datanode (Content-Length -> raw, else de-chunk)."""
    if conn.headers.get("Content-Length") is not None:
        return conn.body
    out, data = b"", conn.body
    while data:
        size_line, _, rest = data.partition(b"\r\n")
        size = int(size_line, 16)
        if size == 0:
            break
        out += rest[:size]
        data = rest[size + 2:]
    return out


if __name__ == "__main__":
    unittest.main()
