#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Cloud API `/v1/*` gateway: auth (401), ingest 202, retrieve/commit 200, rate-limit 429,
quota 413, health probes, blob stream proxy, and `/api/*` back-compat — pure-Python unittest."""
import asyncio
import json
import os
import tempfile
import time
import unittest

try:
    from tools import matrixark_v1_gateway as gw
except ImportError:  # run from tools/ dir
    import matrixark_v1_gateway as gw


class _EnvGuard:
    """Set/clear env vars and restore them on exit (None -> unset)."""

    def __init__(self, **values):
        self._values = values
        self._saved: dict = {}

    def __enter__(self):
        for key, value in self._values.items():
            self._saved[key] = os.environ.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        return self

    def __exit__(self, *exc):
        for key, prev in self._saved.items():
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev


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
    """Run one request; return (status, headers_dict, body_bytes).

    `path` may carry a query string. A real ASGI server splits it out into scope["query_string"],
    and the routes that read query parameters look there -- so without this split a `?user_id=...`
    request would never match its route.
    """
    if raw is not None:
        payload = raw
    else:
        payload = json.dumps(body if body is not None else {}).encode()
    hdrs = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    path, _, query = path.partition("?")
    scope = {"type": "http", "method": method, "path": path,
             "query_string": query.encode("latin-1"), "headers": hdrs}

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


class EnforcedAuthTest(unittest.TestCase):
    """MATRIXARK_AUTH_ENFORCED=1: hashed per-tenant keys, demo rejected, tenant identity pinned."""

    KEY_A = "testkey_tenantA_secret"
    KEY_B = "testkey_tenantB_secret"

    def setUp(self):
        self.s = _FakeServer()
        hashed = {
            gw._secret_hash(self.KEY_A): {"tenant_id": "tenantA", "account_id": "acct_a"},
            gw._secret_hash(self.KEY_B): {"tenant_id": "tenantB", "account_id": "acct_b"},
        }
        self.cfg = gw.GatewayConfig.from_env({"enforced": True, "hashed_api_keys": hashed})
        self.app = gw.make_v1_app(self.s, self.cfg)

    def test_missing_key_rejected(self):
        st, _, _ = drive(self.app, path="/v1/retrieve", body={"query": "x"})
        self.assertEqual(401, st)
        self.assertEqual([], self.s.calls)

    def test_bad_key_rejected(self):
        st, _, _ = drive(self.app, path="/v1/retrieve", body={"query": "x"},
                         headers={"Authorization": "Bearer nope"})
        self.assertEqual(401, st)

    def test_demo_key_rejected_when_enforced(self):
        st, _, _ = drive(self.app, path="/v1/ingest", body={"records": [1]},
                         headers={"Authorization": "Bearer sk_live_demo"})
        self.assertEqual(401, st)
        self.assertEqual([], self.s.calls)

    def test_valid_key_pins_tenant_and_account(self):
        st, _, body = drive(self.app, path="/v1/ingest",
                            body={"scope": "cart", "records": [{"t": "a"}]},
                            headers={"Authorization": f"Bearer {self.KEY_A}"})
        self.assertEqual(202, st)
        scope = json.loads(body)["scope"]
        self.assertEqual("tenantA", scope["tenant_id"])
        self.assertEqual("acct_a", scope["account_id"])
        self.assertEqual("tenantA", self.s.calls[0][1]["tenant"])

    def test_two_tenants_get_distinct_scope_identity(self):
        # Same client scope string ("cart") must resolve to different tenant identities -> the
        # session/pending buffer key (account_id, tenant_id, ...) cannot collide across tenants.
        drive(self.app, path="/v1/ingest", body={"scope": "cart", "records": [1]},
              headers={"Authorization": f"Bearer {self.KEY_A}"})
        drive(self.app, path="/v1/ingest", body={"scope": "cart", "records": [1]},
              headers={"Authorization": f"Bearer {self.KEY_B}"})
        a_scope = self.s.calls[0][1]["scope"]
        b_scope = self.s.calls[1][1]["scope"]
        self.assertEqual(("tenantA", "acct_a"), (a_scope["tenant_id"], a_scope["account_id"]))
        self.assertEqual(("tenantB", "acct_b"), (b_scope["tenant_id"], b_scope["account_id"]))
        self.assertNotEqual((a_scope["tenant_id"], a_scope["account_id"]),
                            (b_scope["tenant_id"], b_scope["account_id"]))

    @staticmethod
    def _buffer_key(scope):
        # Mirrors matrixark_mcp_core_session.session_buffer_key_from_scope (the pending/session
        # buffer key); kept inline so this stays a pure-Python gateway unit test with no backend import.
        user_id = str(scope.get("user_id") or "")
        session_id = str(scope.get("session_id") or user_id or "")
        return (str(scope.get("account_id") or "acct_local"),
                str(scope.get("tenant_id") or "tenant_local_agent"), user_id, session_id)

    def test_same_tenant_users_get_distinct_buffer_keys(self):
        # Two END-USERS of ONE tenant (same API key, same client scope string "cart") must NOT share a
        # pending/session buffer. The gateway pins tenant_id/account_id from the key but PRESERVES the
        # client-supplied user_id, so `alice` and `bob` land on distinct buffer keys.
        drive(self.app, path="/v1/ingest",
              body={"scope": {"user_id": "alice", "namespace": "cart"}, "records": [1]},
              headers={"Authorization": f"Bearer {self.KEY_A}"})
        drive(self.app, path="/v1/ingest",
              body={"scope": {"user_id": "bob", "namespace": "cart"}, "records": [1]},
              headers={"Authorization": f"Bearer {self.KEY_A}"})
        a_scope = self.s.calls[0][1]["scope"]
        b_scope = self.s.calls[1][1]["scope"]
        # tenant/account pinned from the key (not client text); user_id preserved, never overwritten.
        self.assertEqual(("tenantA", "acct_a", "alice"),
                         (a_scope["tenant_id"], a_scope["account_id"], a_scope["user_id"]))
        self.assertEqual(("tenantA", "acct_a", "bob"),
                         (b_scope["tenant_id"], b_scope["account_id"], b_scope["user_id"]))
        # The resulting pending/session buffer keys differ only on the user axis -> no cross-user leak.
        ka, kb = self._buffer_key(a_scope), self._buffer_key(b_scope)
        self.assertEqual(("acct_a", "tenantA", "alice", "alice"), ka)
        self.assertEqual(("acct_a", "tenantA", "bob", "bob"), kb)
        self.assertNotEqual(ka, kb)

    def test_missing_user_id_falls_back_to_tenant_buffer(self):
        # Contract: a tenant that omits user_id/session_id legitimately shares one tenant-level buffer.
        drive(self.app, path="/v1/ingest", body={"scope": {"namespace": "cart"}, "records": [1]},
              headers={"Authorization": f"Bearer {self.KEY_A}"})
        drive(self.app, path="/v1/ingest", body={"scope": {"namespace": "cart"}, "records": [1]},
              headers={"Authorization": f"Bearer {self.KEY_A}"})
        k0 = self._buffer_key(self.s.calls[0][1]["scope"])
        k1 = self._buffer_key(self.s.calls[1][1]["scope"])
        self.assertEqual(("acct_a", "tenantA", "", ""), k0)
        self.assertEqual(k0, k1)

    def test_dev_mode_still_accepts_demo(self):
        # Enforcement OFF -> legacy behavior: demo key maps to its tenant, request succeeds.
        cfg = gw.GatewayConfig.from_env({"api_keys": {"sk_live_demo": "acme"}, "require_auth": True})
        app = gw.make_v1_app(self.s, cfg)
        st, _, _ = drive(app, path="/v1/ingest", body={"records": [1]},
                         headers={"Authorization": "Bearer sk_live_demo"})
        self.assertEqual(202, st)


class DevDefaultAuthTest(unittest.TestCase):
    """Dev DEFAULT posture: auth is OFF out of the box (anonymous allowed). Enforcement
    stays fully opt-in via MATRIXARK_REQUIRE_AUTH=1 (bad/missing key -> 401)."""

    def setUp(self):
        self.s = _FakeServer()

    def test_default_require_auth_is_false(self):
        self.assertFalse(gw._DEFAULTS["require_auth"])
        with _EnvGuard(MATRIXARK_REQUIRE_AUTH=None, MATRIXARK_AUTH_ENFORCED=None):
            cfg = gw.GatewayConfig.from_env({"api_keys": {"k-acme": "acme"}})
            self.assertFalse(cfg.require_auth)

    def test_anonymous_allowed_by_default(self):
        # No require_auth override, env cleared -> new dev default (anonymous allowed).
        with _EnvGuard(MATRIXARK_REQUIRE_AUTH=None, MATRIXARK_AUTH_ENFORCED=None):
            cfg = gw.GatewayConfig.from_env({"api_keys": {"k-acme": "acme"}})
            app = gw.make_v1_app(self.s, cfg)
            st, _, _ = drive(app, path="/v1/retrieve", body={"query": "x"})   # no Authorization
        self.assertEqual(200, st)
        self.assertEqual("matrixark_retrieve", self.s.calls[0][0])

    def test_require_auth_env_opt_in_rejects_anonymous_and_accepts_key(self):
        # HARD requirement: MATRIXARK_REQUIRE_AUTH=1 restores enforcement as opt-in.
        with _EnvGuard(MATRIXARK_REQUIRE_AUTH="1", MATRIXARK_AUTH_ENFORCED=None):
            cfg = gw.GatewayConfig.from_env({"api_keys": {"k-acme": "acme"}})
            self.assertTrue(cfg.require_auth)
            app = gw.make_v1_app(self.s, cfg)
            st_anon, _, body = drive(app, path="/v1/retrieve", body={"query": "x"})
            self.assertEqual(401, st_anon)
            self.assertEqual("unauthorized", json.loads(body)["error"])
            self.assertEqual([], self.s.calls)
            st_ok, _, _ = drive(app, path="/v1/retrieve", body={"query": "x"},
                                headers={"Authorization": "Bearer k-acme"})
            self.assertEqual(200, st_ok)
            self.assertEqual("matrixark_retrieve", self.s.calls[0][0])

    def test_no_auth_startup_warning_fires_once(self):
        # Non-blocking one-time warning when auth is effectively off.
        gw._AUTH_WARNED["done"] = False
        with _EnvGuard(MATRIXARK_REQUIRE_AUTH=None, MATRIXARK_AUTH_ENFORCED=None):
            cfg = gw.GatewayConfig.from_env({"api_keys": {"k-acme": "acme"}})
            with self.assertLogs("matrixark.gateway", level="WARNING") as cap:
                gw.make_v1_app(self.s, cfg)
            self.assertTrue(any("WITHOUT authentication" in m for m in cap.output))
        # Second build in the same process does NOT warn again (once per process).
        self.assertTrue(gw._AUTH_WARNED["done"])


class EnforcedScopeEnforcementTest(unittest.TestCase):
    """MATRIXARK_AUTH_ENFORCED=1: per-key `scopes` + `allowed_user_ids`/`allowed_session_ids`
    enforced at the edge. 401 = missing/invalid/expired key; 403 = valid key, wrong scope/user."""

    RETRIEVE_KEY = "testkey_retrieve_only"
    INGEST_KEY = "testkey_ingest_only"
    FULL_KEY = "testkey_full_scopes"
    USER_KEY = "testkey_user_locked"
    SESSION_KEY = "testkey_session_locked"

    _ALL_SCOPES = ["context:ingest", "context:retrieve", "context:feedback", "context:replay"]

    def setUp(self):
        self.s = _FakeServer()
        hashed = {
            gw._secret_hash(self.RETRIEVE_KEY): {
                "tenant_id": "t", "account_id": "acct", "scopes": ["context:retrieve"]},
            gw._secret_hash(self.INGEST_KEY): {
                "tenant_id": "t", "account_id": "acct", "scopes": ["context:ingest"]},
            gw._secret_hash(self.FULL_KEY): {
                "tenant_id": "t", "account_id": "acct", "scopes": list(self._ALL_SCOPES)},
            gw._secret_hash(self.USER_KEY): {
                "tenant_id": "t", "account_id": "acct",
                "scopes": ["context:ingest", "context:retrieve"], "allowed_user_ids": ["alice"]},
            gw._secret_hash(self.SESSION_KEY): {
                "tenant_id": "t", "account_id": "acct",
                "scopes": ["context:ingest"], "allowed_session_ids": ["sess-1"]},
        }
        self.cfg = gw.GatewayConfig.from_env({"enforced": True, "hashed_api_keys": hashed})
        self.app = gw.make_v1_app(self.s, self.cfg)

    def _bearer(self, key):
        return {"Authorization": f"Bearer {key}"}

    # ---- scope enforcement ---------------------------------------------------------------------
    def test_retrieve_only_key_allows_retrieve(self):
        st, _, _ = drive(self.app, path="/v1/retrieve", body={"query": "x"},
                         headers=self._bearer(self.RETRIEVE_KEY))
        self.assertEqual(200, st)
        self.assertEqual("matrixark_retrieve", self.s.calls[0][0])

    def test_retrieve_only_key_denies_ingest_403(self):
        st, _, body = drive(self.app, path="/v1/ingest", body={"records": [1]},
                            headers=self._bearer(self.RETRIEVE_KEY))
        self.assertEqual(403, st)
        payload = json.loads(body)
        self.assertEqual("insufficient_scope", payload["error"])
        self.assertEqual("context:ingest", payload["required"])
        self.assertEqual([], self.s.calls)                            # denied before backend dispatch

    def test_ingest_only_key_allows_ingest(self):
        st, _, _ = drive(self.app, path="/v1/ingest", body={"records": [1]},
                         headers=self._bearer(self.INGEST_KEY))
        self.assertEqual(202, st)
        self.assertEqual("matrixark_ingest", self.s.calls[0][0])

    def test_ingest_only_key_denies_retrieve_403(self):
        st, _, body = drive(self.app, path="/v1/retrieve", body={"query": "x"},
                            headers=self._bearer(self.INGEST_KEY))
        self.assertEqual(403, st)
        payload = json.loads(body)
        self.assertEqual("insufficient_scope", payload["error"])
        self.assertEqual("context:retrieve", payload["required"])
        self.assertEqual([], self.s.calls)

    def test_full_scope_key_allows_both(self):
        st_i, _, _ = drive(self.app, path="/v1/ingest", body={"records": [1]},
                           headers=self._bearer(self.FULL_KEY))
        st_r, _, _ = drive(self.app, path="/v1/retrieve", body={"query": "x"},
                           headers=self._bearer(self.FULL_KEY))
        self.assertEqual(202, st_i)
        self.assertEqual(200, st_r)
        self.assertEqual(["matrixark_ingest", "matrixark_retrieve"], [c[0] for c in self.s.calls])

    def test_session_commit_needs_ingest_scope(self):
        # /v1/session/commit is an ingest-category route -> needs context:ingest.
        st_ok, _, _ = drive(self.app, path="/v1/session/commit", body={"force": True},
                            headers=self._bearer(self.INGEST_KEY))
        self.assertEqual(200, st_ok)
        st_deny, _, body = drive(self.app, path="/v1/session/commit", body={"force": True},
                                 headers=self._bearer(self.RETRIEVE_KEY))
        self.assertEqual(403, st_deny)
        self.assertEqual("context:ingest", json.loads(body)["required"])

    def test_blob_put_needs_ingest_scope(self):
        resp = _FakeResponse(200, body=json.dumps({"stored": True}).encode())
        cfg = gw.GatewayConfig.from_env({
            "enforced": True,
            "hashed_api_keys": {
                gw._secret_hash(self.RETRIEVE_KEY): {
                    "tenant_id": "t", "account_id": "acct", "scopes": ["context:retrieve"]},
                gw._secret_hash(self.INGEST_KEY): {
                    "tenant_id": "t", "account_id": "acct", "scopes": ["context:ingest"]},
            },
            "blob_connection_factory": _factory_for(resp),
        })
        app = gw.make_v1_app(self.s, cfg)
        # retrieve-only key cannot PUT a blob (write == ingest).
        st_deny, _, body = drive(app, method="PUT", path="/v1/blob/x.bin", raw=b"data",
                                 headers=self._bearer(self.RETRIEVE_KEY))
        self.assertEqual(403, st_deny)
        self.assertEqual("context:ingest", json.loads(body)["required"])
        # retrieve-only key CAN GET a blob (read == retrieve).
        st_get, _, _ = drive(app, method="GET", path="/v1/blob/x.bin",
                             headers=self._bearer(self.RETRIEVE_KEY))
        self.assertEqual(200, st_get)

    # ---- user / session enforcement ------------------------------------------------------------
    def test_allowed_user_id_allows_matching_user(self):
        st, _, _ = drive(self.app, path="/v1/ingest",
                         body={"scope": {"user_id": "alice"}, "records": [1]},
                         headers=self._bearer(self.USER_KEY))
        self.assertEqual(202, st)
        self.assertEqual("alice", self.s.calls[0][1]["scope"]["user_id"])

    def test_allowed_user_id_denies_other_user_403(self):
        st, _, body = drive(self.app, path="/v1/ingest",
                            body={"scope": {"user_id": "bob"}, "records": [1]},
                            headers=self._bearer(self.USER_KEY))
        self.assertEqual(403, st)
        self.assertEqual("user_not_allowed", json.loads(body)["error"])
        self.assertEqual([], self.s.calls)                            # denied before backend dispatch

    def test_allowed_session_id_enforced(self):
        st_ok, _, _ = drive(self.app, path="/v1/ingest",
                            body={"scope": {"session_id": "sess-1"}, "records": [1]},
                            headers=self._bearer(self.SESSION_KEY))
        self.assertEqual(202, st_ok)
        st_deny, _, body = drive(self.app, path="/v1/ingest",
                                 body={"scope": {"session_id": "sess-9"}, "records": [1]},
                                 headers=self._bearer(self.SESSION_KEY))
        self.assertEqual(403, st_deny)
        self.assertEqual("session_not_allowed", json.loads(body)["error"])

    # ---- 401 discipline (missing/invalid/expired) ----------------------------------------------
    def test_missing_key_still_401(self):
        st, _, body = drive(self.app, path="/v1/ingest", body={"records": [1]})
        self.assertEqual(401, st)
        self.assertEqual("unauthorized", json.loads(body)["error"])

    def test_bad_key_still_401(self):
        st, _, _ = drive(self.app, path="/v1/ingest", body={"records": [1]},
                         headers=self._bearer("testkey_nope"))
        self.assertEqual(401, st)

    def test_expired_key_still_401(self):
        # A JSONL keystore record whose expires_at_ms is in the past is skipped -> 401 (not 403).
        expired_key = "testkey_expired"
        record = {
            "record_type": "matrixark_api_key",
            "api_key_hash": gw._secret_hash(expired_key),
            "tenant_id": "t", "account_id": "acct",
            "scopes": ["context:ingest"],
            "status": "active",
            "expires_at_ms": int(time.time() * 1000) - 60_000,
        }
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
            fh.write(json.dumps(record) + "\n")
            store_path = fh.name
        try:
            with _EnvGuard(MATRIXARK_AUTH_ENFORCED="1", MATRIXARK_API_KEYS_HASHED_FILE=store_path):
                cfg = gw.GatewayConfig.from_env({})
                app = gw.make_v1_app(self.s, cfg)
                st, _, body = drive(app, path="/v1/ingest", body={"records": [1]},
                                    headers=self._bearer(expired_key))
            self.assertEqual(401, st)
            self.assertEqual("unauthorized", json.loads(body)["error"])
        finally:
            os.unlink(store_path)

    # ---- backward compatibility: legacy plain keystore (no scopes) -----------------------------
    def test_legacy_plain_keystore_is_unrestricted(self):
        # Plain {sha256: {tenant_id, account_id}} form (no `scopes`) -> UNRESTRICTED: every route
        # allowed, exactly as before per-key scopes existed.
        legacy_key = "testkey_legacy_plain"
        store = {gw._secret_hash(legacy_key): {"tenant_id": "t", "account_id": "acct"}}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump(store, fh)
            store_path = fh.name
        try:
            with _EnvGuard(MATRIXARK_AUTH_ENFORCED="1", MATRIXARK_API_KEYS_HASHED_FILE=store_path):
                cfg = gw.GatewayConfig.from_env({})
                self.assertIsNone(cfg.hashed_keys[gw._secret_hash(legacy_key)]["scopes"])
                app = gw.make_v1_app(self.s, cfg)
                st_i, _, _ = drive(app, path="/v1/ingest", body={"records": [1]},
                                   headers=self._bearer(legacy_key))
                st_r, _, _ = drive(app, path="/v1/retrieve", body={"query": "x"},
                                   headers=self._bearer(legacy_key))
            self.assertEqual(202, st_i)                               # ingest allowed (no scope gate)
            self.assertEqual(200, st_r)                               # retrieve allowed (no scope gate)
        finally:
            os.unlink(store_path)

    # ---- dev mode (enforcement OFF): no scope checks -------------------------------------------
    def test_dev_mode_ignores_scopes(self):
        # MATRIXARK_AUTH_ENFORCED unset -> plaintext api_keys, no per-key record, no scope gate.
        with _EnvGuard(MATRIXARK_AUTH_ENFORCED=None, MATRIXARK_REQUIRE_AUTH="1"):
            cfg = gw.GatewayConfig.from_env({"api_keys": {"k-acme": "acme"}})
            app = gw.make_v1_app(self.s, cfg)
            st_i, _, _ = drive(app, path="/v1/ingest", body={"records": [1]},
                               headers={"Authorization": "Bearer k-acme"})
            st_r, _, _ = drive(app, path="/v1/retrieve", body={"query": "x"},
                               headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(202, st_i)
        self.assertEqual(200, st_r)


class ProvisionerScopeFlagsTest(unittest.TestCase):
    """The provisioner mints keystore records carrying scopes / allowed_user_ids /
    allowed_session_ids so restricted keys are actually issuable end-to-end."""

    def _mint(self, argv):
        try:
            from tools import matrixark_provision_api_key as prov
        except ImportError:
            import matrixark_provision_api_key as prov  # type: ignore
        with tempfile.TemporaryDirectory() as d:
            store = os.path.join(d, "keys.jsonl")
            prov.main(argv + ["--store", store])
            with open(store, "r", encoding="utf-8") as fh:
                return json.loads(fh.readline())

    def test_default_scopes_are_all_four(self):
        rec = self._mint(["--tenant-id", "t", "--account-id", "acct"])
        self.assertEqual(
            ["context:feedback", "context:ingest", "context:replay", "context:retrieve"],
            rec["scopes"])
        self.assertEqual([], rec["allowed_user_ids"])
        self.assertEqual([], rec["allowed_session_ids"])

    def test_retrieve_only_scope_flag(self):
        rec = self._mint(["--tenant-id", "t", "--scope", "context:retrieve"])
        self.assertEqual(["context:retrieve"], rec["scopes"])

    def test_comma_separated_and_repeated_flags(self):
        rec = self._mint([
            "--tenant-id", "t",
            "--scope", "context:ingest,context:retrieve",
            "--allowed-user-id", "alice,bob",
            "--allowed-session-id", "sess-1",
        ])
        self.assertEqual(["context:ingest", "context:retrieve"], rec["scopes"])
        self.assertEqual(["alice", "bob"], rec["allowed_user_ids"])
        self.assertEqual(["sess-1"], rec["allowed_session_ids"])

    def test_minted_key_verifies_at_edge(self):
        # End-to-end: mint a retrieve-only key, load its record shape at the edge, deny ingest.
        try:
            from tools import matrixark_provision_api_key as prov
        except ImportError:
            import matrixark_provision_api_key as prov  # type: ignore
        with tempfile.TemporaryDirectory() as d:
            store = os.path.join(d, "keys.jsonl")
            # Capture the plaintext key the provisioner prints once.
            import io
            import contextlib
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                prov.main(["--tenant-id", "t", "--account-id", "acct",
                           "--scope", "context:retrieve", "--store", store])
            minted = json.loads(buf.getvalue())
            plaintext = minted["api_key"]
            with _EnvGuard(MATRIXARK_AUTH_ENFORCED="1", MATRIXARK_API_KEYS_HASHED_FILE=store):
                cfg = gw.GatewayConfig.from_env({})
                app = gw.make_v1_app(_FakeServer(), cfg)
                st_ok, _, _ = drive(app, path="/v1/retrieve", body={"query": "x"},
                                    headers={"Authorization": f"Bearer {plaintext}"})
                st_deny, _, body = drive(app, path="/v1/ingest", body={"records": [1]},
                                         headers={"Authorization": f"Bearer {plaintext}"})
        self.assertEqual(200, st_ok)
        self.assertEqual(403, st_deny)
        self.assertEqual("insufficient_scope", json.loads(body)["error"])


class McpPerToolScopeTest(unittest.TestCase):
    """MATRIXARK_AUTH_ENFORCED=1: `/v1/mcp` is gated PER-TOOL via MATRIXARK_TOOL_SCOPES (the same
    map the backend enforces), not a blanket `context:retrieve`. A data-only key can call data
    tools but NOT admin tools; non-`tools/call` methods (tools/list/initialize) need no tool scope;
    user/session locks apply to the call's `params.arguments.scope`. Legacy/dev keys stay
    unrestricted; missing/bad keys still 401."""

    RETRIEVE_KEY = "testkey_mcp_retrieve_only"
    INGEST_KEY = "testkey_mcp_ingest_only"
    DATA_KEY = "testkey_mcp_data_only"          # all context scopes, NO admin scope
    USER_KEY = "testkey_mcp_user_locked"

    _DATA_SCOPES = ["context:ingest", "context:retrieve", "context:feedback", "context:replay"]

    def setUp(self):
        self.s = _FakeServer()
        hashed = {
            gw._secret_hash(self.RETRIEVE_KEY): {
                "tenant_id": "t", "account_id": "acct", "scopes": ["context:retrieve"]},
            gw._secret_hash(self.INGEST_KEY): {
                "tenant_id": "t", "account_id": "acct", "scopes": ["context:ingest"]},
            gw._secret_hash(self.DATA_KEY): {
                "tenant_id": "t", "account_id": "acct", "scopes": list(self._DATA_SCOPES)},
            gw._secret_hash(self.USER_KEY): {
                "tenant_id": "t", "account_id": "acct",
                "scopes": list(self._DATA_SCOPES), "allowed_user_ids": ["alice"]},
        }
        self.cfg = gw.GatewayConfig.from_env({"enforced": True, "hashed_api_keys": hashed})
        self.app = gw.make_v1_app(self.s, self.cfg)

    def _bearer(self, key):
        return {"Authorization": f"Bearer {key}"}

    @staticmethod
    def _call(name, arguments=None):
        params = {"name": name}
        if arguments is not None:
            params["arguments"] = arguments
        return {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": params}

    def _mcp(self, body, key):
        return drive(self.app, path="/v1/mcp", body=body, headers=self._bearer(key))

    # ---- per-tool scope ------------------------------------------------------------------------
    def test_retrieve_only_key_denies_ingest_tool_403(self):
        st, _, body = self._mcp(self._call("matrixark_ingest", {"records": [1]}), self.RETRIEVE_KEY)
        self.assertEqual(403, st)
        payload = json.loads(body)
        self.assertEqual("insufficient_scope", payload["error"])
        self.assertEqual(["context:ingest"], payload["required"])
        self.assertEqual([], self.s.handled)                          # denied before dispatch

    def test_retrieve_only_key_allows_retrieve_tool(self):
        st, _, _ = self._mcp(self._call("matrixark_retrieve", {"query": "x"}), self.RETRIEVE_KEY)
        self.assertEqual(200, st)
        self.assertEqual(1, len(self.s.handled))                      # dispatched
        self.assertEqual("matrixark_retrieve", self.s.handled[0]["params"]["name"])

    def test_ingest_scope_key_allows_ingest_tool(self):
        st, _, _ = self._mcp(self._call("matrixark_ingest", {"records": [1]}), self.INGEST_KEY)
        self.assertEqual(200, st)
        self.assertEqual(1, len(self.s.handled))

    def test_data_only_key_denies_admin_tool_403(self):
        # THE point of this change: a data-scoped key can no longer reach an admin tool via /v1/mcp.
        st, _, body = self._mcp(
            self._call("matrixark_admin_create_api_key", {"tenant_id": "t"}), self.DATA_KEY)
        self.assertEqual(403, st)
        payload = json.loads(body)
        self.assertEqual("insufficient_scope", payload["error"])
        self.assertEqual(["admin:api_key"], payload["required"])
        self.assertEqual([], self.s.handled)

    # ---- non-tools/call methods need no tool scope ---------------------------------------------
    def test_tools_list_allowed_with_any_valid_key(self):
        st, _, _ = self._mcp({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, self.RETRIEVE_KEY)
        self.assertEqual(200, st)
        self.assertEqual(1, len(self.s.handled))

    def test_initialize_allowed_with_any_valid_key(self):
        st, _, _ = self._mcp({"jsonrpc": "2.0", "id": 1, "method": "initialize"}, self.INGEST_KEY)
        self.assertEqual(200, st)
        self.assertEqual(1, len(self.s.handled))

    # ---- user/session lock on params.arguments.scope -------------------------------------------
    def test_user_locked_key_denies_other_user_403(self):
        st, _, body = self._mcp(
            self._call("matrixark_ingest", {"scope": {"user_id": "bob"}, "records": [1]}),
            self.USER_KEY)
        self.assertEqual(403, st)
        self.assertEqual("user_not_allowed", json.loads(body)["error"])
        self.assertEqual([], self.s.handled)

    def test_user_locked_key_allows_matching_user(self):
        st, _, _ = self._mcp(
            self._call("matrixark_ingest", {"scope": {"user_id": "alice"}, "records": [1]}),
            self.USER_KEY)
        self.assertEqual(200, st)
        self.assertEqual(1, len(self.s.handled))

    def test_user_locked_key_scopeless_call_not_identity_checked(self):
        # No `scope` object on the call -> nothing to check on the user axis (tools/list-style).
        st, _, _ = self._mcp(self._call("matrixark_retrieve", {"query": "x"}), self.USER_KEY)
        self.assertEqual(200, st)
        self.assertEqual(1, len(self.s.handled))

    # ---- 401 discipline (unchanged) ------------------------------------------------------------
    def test_missing_key_still_401(self):
        st, _, _ = drive(self.app, path="/v1/mcp",
                         body=self._call("matrixark_retrieve", {"query": "x"}))
        self.assertEqual(401, st)
        self.assertEqual([], self.s.handled)

    def test_bad_key_still_401(self):
        st, _, _ = self._mcp(self._call("matrixark_retrieve", {"query": "x"}), "testkey_nope")
        self.assertEqual(401, st)

    # ---- backward compatibility ----------------------------------------------------------------
    def test_legacy_plain_keystore_mcp_unrestricted(self):
        # Legacy plain {sha256: {tenant_id, account_id}} (scopes=None) -> /v1/mcp UNRESTRICTED:
        # even an admin tool dispatches, exactly as before per-tool gating existed.
        legacy_key = "testkey_mcp_legacy"
        store = {gw._secret_hash(legacy_key): {"tenant_id": "t", "account_id": "acct"}}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump(store, fh)
            store_path = fh.name
        try:
            with _EnvGuard(MATRIXARK_AUTH_ENFORCED="1", MATRIXARK_API_KEYS_HASHED_FILE=store_path):
                cfg = gw.GatewayConfig.from_env({})
                app = gw.make_v1_app(self.s, cfg)
                st, _, _ = drive(app, path="/v1/mcp",
                                 body=self._call("matrixark_admin_create_api_key", {"tenant_id": "t"}),
                                 headers=self._bearer(legacy_key))
            self.assertEqual(200, st)                                  # admin tool allowed (no gate)
            self.assertEqual(1, len(self.s.handled))
        finally:
            os.unlink(store_path)

    def test_dev_mode_mcp_no_scope_enforcement(self):
        # Enforcement OFF -> plaintext api_keys, no per-key record -> admin tool dispatches.
        with _EnvGuard(MATRIXARK_AUTH_ENFORCED=None, MATRIXARK_REQUIRE_AUTH="1"):
            cfg = gw.GatewayConfig.from_env({"api_keys": {"k-acme": "acme"}})
            app = gw.make_v1_app(self.s, cfg)
            st, _, _ = drive(app, path="/v1/mcp",
                             body=self._call("matrixark_admin_create_api_key", {"tenant_id": "t"}),
                             headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual(1, len(self.s.handled))


class UsageMeterTest(unittest.TestCase):
    """Per-key edge usage metering: authenticated (enforced) requests bump an in-process counter
    (total + ingest/retrieve split + bytes + last_used); the /v1/admin/api_key_usage read is admin-
    scope gated; dev/anonymous traffic is never metered; a metering failure never breaks a request."""

    DATA_KEY = "testkey_usage_data"          # context scopes only, NO admin scope
    ADMIN_KEY = "testkey_usage_admin"        # carries admin:api_key -> may read usage
    OTHER_KEY = "testkey_usage_other"        # a DIFFERENT tenant; no test below drives it except
                                             # the cross-tenant one, so no other count changes
    LEGACY_KEY = "testkey_usage_legacy"      # scopes=None -> unrestricted, by documented design

    _DATA_SCOPES = ["context:ingest", "context:retrieve"]

    def _hashed(self):
        return {
            gw._secret_hash(self.DATA_KEY): {
                "tenant_id": "t", "account_id": "acct", "scopes": list(self._DATA_SCOPES)},
            gw._secret_hash(self.ADMIN_KEY): {
                "tenant_id": "t", "account_id": "acct",
                "scopes": ["admin:api_key", "context:retrieve"]},
            gw._secret_hash(self.OTHER_KEY): {
                "tenant_id": "other_t", "account_id": "other_acct",
                "scopes": list(self._DATA_SCOPES)},
            gw._secret_hash(self.LEGACY_KEY): {
                "tenant_id": "t", "account_id": "acct"},        # no "scopes" -> unrestricted
        }

    def setUp(self):
        self.s = _FakeServer()
        self.cfg = gw.GatewayConfig.from_env({"enforced": True, "hashed_api_keys": self._hashed()})
        self.app = gw.make_v1_app(self.s, self.cfg)

    def _bearer(self, key):
        return {"Authorization": f"Bearer {key}"}

    def _usage(self, key):
        st, _, body = drive(self.app, method="GET", path="/v1/admin/api_key_usage",
                            headers=self._bearer(key))
        return st, (json.loads(body) if body else {})

    def test_authenticated_requests_are_counted_with_category_split(self):
        # 3 ingests + 2 retrieves under the data key -> total 5, ingest 3, retrieve 2.
        for _ in range(3):
            drive(self.app, path="/v1/ingest", body={"records": [1]}, headers=self._bearer(self.DATA_KEY))
        for _ in range(2):
            drive(self.app, path="/v1/retrieve", body={"query": "x"}, headers=self._bearer(self.DATA_KEY))
        st, payload = self._usage(self.ADMIN_KEY)
        self.assertEqual(200, st)
        self.assertEqual(1, payload["count"])                          # one distinct key metered
        entry = payload["usage"][0]
        self.assertEqual(gw._secret_hash(self.DATA_KEY), entry["api_key_hash"])
        self.assertEqual(5, entry["total"])
        self.assertEqual(3, entry["ingest"])
        self.assertEqual(2, entry["retrieve"])
        self.assertEqual("t", entry["tenant_id"])
        self.assertEqual("acct", entry["account_id"])
        self.assertGreater(entry["bytes"], 0)                          # JSON body bytes captured
        self.assertGreaterEqual(entry["last_used_at_ms"], entry["first_used_at_ms"])

    def test_key_stored_hashed_not_plaintext(self):
        drive(self.app, path="/v1/ingest", body={"records": [1]}, headers=self._bearer(self.DATA_KEY))
        _, payload = self._usage(self.ADMIN_KEY)
        blob = json.dumps(payload)
        self.assertNotIn(self.DATA_KEY, blob)                          # never leak the plaintext key
        self.assertIn(gw._secret_hash(self.DATA_KEY), blob)

    def test_usage_read_returns_only_the_callers_own_tenant(self):
        """The snapshot is deployment-wide. Returning it whole told one tenant how much traffic
        every other tenant was doing -- key hash, request counts and bytes."""
        drive(self.app, path="/v1/ingest", body={"records": [1]}, headers=self._bearer(self.DATA_KEY))
        for _ in range(4):
            drive(self.app, path="/v1/ingest", body={"records": [1, 2, 3]},
                  headers=self._bearer(self.OTHER_KEY))

        st, payload = self._usage(self.ADMIN_KEY)
        self.assertEqual(200, st)
        # The control: the caller's OWN row must still be there, or a filter returning nothing at
        # all would pass the check below without meaning anything.
        own = [r for r in payload["usage"] if r["api_key_hash"] == gw._secret_hash(self.DATA_KEY)]
        self.assertEqual(1, len(own), payload["usage"])
        self.assertEqual(1, own[0]["total"])

        foreign = [r for r in payload["usage"] if r["tenant_id"] != "t"]
        self.assertEqual([], foreign,
                         "another tenant's usage was returned to an admin of this one: %r" % foreign)
        self.assertEqual(1, payload["count"])
        blob = json.dumps(payload)
        self.assertNotIn("other_t", blob)
        self.assertNotIn(gw._secret_hash(self.OTHER_KEY), blob)

    def test_a_legacy_unrestricted_key_still_reads_the_whole_snapshot(self):
        """Unchanged on purpose: a key with no scopes is unrestricted everywhere else at this
        edge, and narrowing it here would change what those deployments see."""
        drive(self.app, path="/v1/ingest", body={"records": [1]}, headers=self._bearer(self.DATA_KEY))
        drive(self.app, path="/v1/ingest", body={"records": [1]}, headers=self._bearer(self.OTHER_KEY))
        st, payload = self._usage(self.LEGACY_KEY)
        self.assertEqual(200, st)
        self.assertEqual(2, payload["count"], payload["usage"])

    def test_non_admin_key_cannot_read_usage_403(self):
        st, payload = self._usage(self.DATA_KEY)
        self.assertEqual(403, st)
        self.assertEqual("insufficient_scope", payload["error"])
        self.assertEqual(["admin:api_key", "admin:audit"], payload["required"])

    def test_usage_read_requires_auth_401(self):
        st, _, body = drive(self.app, method="GET", path="/v1/admin/api_key_usage")
        self.assertEqual(401, st)
        self.assertEqual("unauthorized", json.loads(body)["error"])

    def test_dev_mode_requests_are_not_metered(self):
        # Enforcement OFF -> plaintext api_keys, no metering; the read returns an empty snapshot.
        with _EnvGuard(MATRIXARK_AUTH_ENFORCED=None, MATRIXARK_REQUIRE_AUTH="1"):
            cfg = gw.GatewayConfig.from_env({"api_keys": {"k-acme": "acme"}})
            app = gw.make_v1_app(self.s, cfg)
            for _ in range(4):
                st_i, _, _ = drive(app, path="/v1/ingest", body={"records": [1]},
                                   headers={"Authorization": "Bearer k-acme"})
                self.assertEqual(202, st_i)                            # request still succeeds
            st, _, body = drive(app, method="GET", path="/v1/admin/api_key_usage",
                                headers={"Authorization": "Bearer k-acme"})
            self.assertEqual(200, st)                                  # dev key unrestricted read
            self.assertEqual(0, json.loads(body)["count"])            # but nothing was metered

    def test_metering_failure_does_not_break_request(self):
        # Force the meter to raise: the request MUST still succeed (metering is best-effort).
        original = gw._UsageMeter.record

        def boom(self, *a, **k):
            raise RuntimeError("metering exploded")

        gw._UsageMeter.record = boom
        try:
            st, _, _ = drive(self.app, path="/v1/ingest", body={"records": [1]},
                             headers=self._bearer(self.DATA_KEY))
            self.assertEqual(202, st)                                  # ingest unaffected by meter error
            self.assertEqual(1, len(self.s.calls))                    # backend still dispatched
        finally:
            gw._UsageMeter.record = original

    def test_usage_flushes_to_file(self):
        # flush_every=1 -> every recorded request snapshots to MATRIXARK_API_KEY_USAGE_FILE.
        with tempfile.TemporaryDirectory() as d:
            usage_file = os.path.join(d, "usage.json")
            cfg = gw.GatewayConfig.from_env({
                "enforced": True, "hashed_api_keys": self._hashed(),
                "usage_file": usage_file, "usage_flush_every": 1,
            })
            app = gw.make_v1_app(self.s, cfg)
            drive(app, path="/v1/ingest", body={"records": [1]}, headers=self._bearer(self.DATA_KEY))
            self.assertTrue(os.path.exists(usage_file))
            with open(usage_file, "r", encoding="utf-8") as fh:
                snap = json.load(fh)
            self.assertEqual("matrixark_api_key_usage_snapshot", snap["record_type"])
            self.assertEqual(1, len(snap["keys"]))
            self.assertEqual(1, snap["keys"][0]["ingest"])

    def test_usage_file_env_var_is_read(self):
        with tempfile.TemporaryDirectory() as d:
            usage_file = os.path.join(d, "env_usage.json")
            with _EnvGuard(MATRIXARK_API_KEY_USAGE_FILE=usage_file,
                           MATRIXARK_API_KEY_USAGE_FLUSH_EVERY="1"):
                cfg = gw.GatewayConfig.from_env({})
                self.assertEqual(usage_file, cfg.usage_file)
                self.assertEqual(1, cfg.usage_flush_every)


class QuotaEnforcementTest(unittest.TestCase):
    """Per-key request QUOTA at the edge, off the usage counter. A key with request_quota=N gets N
    requests per window, then 429 quota_exceeded; a key with no quota is unlimited; dev/anonymous
    traffic is never limited; a forced quota-check error never blocks a legitimate request."""

    QUOTA_KEY = "quota-key-for-tests"        # request_quota=3 (lifetime window)
    WINDOW_KEY = "window-key-for-tests"  # request_quota=2, quota_window=100s
    FREE_KEY = "free-key-for-tests"        # no quota -> unlimited

    def _hashed(self):
        return {
            gw._secret_hash(self.QUOTA_KEY): {
                "tenant_id": "t", "account_id": "acct",
                "scopes": ["context:ingest", "context:retrieve"], "request_quota": 3},
            gw._secret_hash(self.WINDOW_KEY): {
                "tenant_id": "t", "account_id": "acct",
                "scopes": ["context:ingest"], "request_quota": 2, "quota_window": 100},
            gw._secret_hash(self.FREE_KEY): {
                "tenant_id": "t", "account_id": "acct", "scopes": ["context:ingest"]},
        }

    def setUp(self):
        self.s = _FakeServer()
        self.cfg = gw.GatewayConfig.from_env({"enforced": True, "hashed_api_keys": self._hashed()})
        self.app = gw.make_v1_app(self.s, self.cfg)

    def _bearer(self, key):
        return {"Authorization": f"Bearer {key}"}

    def test_quota_key_allows_up_to_limit_then_429(self):
        # request_quota=3 -> 3 ingests OK, 4th 429 quota_exceeded.
        for i in range(3):
            st, _, _ = drive(self.app, path="/v1/ingest", body={"records": [1]},
                             headers=self._bearer(self.QUOTA_KEY))
            self.assertEqual(202, st, f"request {i + 1} should be allowed")
        st, hdrs, body = drive(self.app, path="/v1/ingest", body={"records": [1]},
                               headers=self._bearer(self.QUOTA_KEY))
        self.assertEqual(429, st)
        payload = json.loads(body)
        self.assertEqual("quota_exceeded", payload["error"])
        self.assertEqual(3, payload["limit"])
        self.assertGreater(payload["used"], 3)
        self.assertIn("retry-after", hdrs)
        self.assertIn("x-ratelimit-quota-limit", hdrs)
        # The over-quota request must NOT reach the backend beyond the 3 allowed calls.
        self.assertEqual(3, len(self.s.calls))

    def test_quota_spans_ingest_and_retrieve(self):
        # The quota is per-request across categories, not per-category.
        drive(self.app, path="/v1/ingest", body={"records": [1]}, headers=self._bearer(self.QUOTA_KEY))
        drive(self.app, path="/v1/retrieve", body={"query": "x"}, headers=self._bearer(self.QUOTA_KEY))
        drive(self.app, path="/v1/ingest", body={"records": [1]}, headers=self._bearer(self.QUOTA_KEY))
        st, _, body = drive(self.app, path="/v1/retrieve", body={"query": "x"},
                            headers=self._bearer(self.QUOTA_KEY))
        self.assertEqual(429, st)
        self.assertEqual("quota_exceeded", json.loads(body)["error"])

    def test_windowed_quota_sets_retry_after_from_window(self):
        drive(self.app, path="/v1/ingest", body={"records": [1]}, headers=self._bearer(self.WINDOW_KEY))
        drive(self.app, path="/v1/ingest", body={"records": [1]}, headers=self._bearer(self.WINDOW_KEY))
        st, hdrs, body = drive(self.app, path="/v1/ingest", body={"records": [1]},
                               headers=self._bearer(self.WINDOW_KEY))
        self.assertEqual(429, st)
        self.assertEqual(2, json.loads(body)["limit"])
        # A finite window -> a positive Retry-After (seconds until the window rolls over).
        self.assertGreater(int(hdrs["retry-after"]), 0)

    def test_no_quota_key_is_unlimited(self):
        for _ in range(10):
            st, _, _ = drive(self.app, path="/v1/ingest", body={"records": [1]},
                             headers=self._bearer(self.FREE_KEY))
            self.assertEqual(202, st)
        self.assertEqual(10, len(self.s.calls))

    def test_dev_mode_is_never_quota_limited(self):
        # Enforcement OFF -> no metering, so no key record and no quota, even under load.
        with _EnvGuard(MATRIXARK_AUTH_ENFORCED=None, MATRIXARK_REQUIRE_AUTH="1"):
            cfg = gw.GatewayConfig.from_env({"api_keys": {"k-acme": "acme"}})
            app = gw.make_v1_app(self.s, cfg)
            for _ in range(20):
                st, _, _ = drive(app, path="/v1/ingest", body={"records": [1]},
                                 headers={"Authorization": "Bearer k-acme"})
                self.assertEqual(202, st)

    def test_quota_check_error_does_not_block_request(self):
        # A forced failure inside the metering/quota path MUST NOT block a legitimate request: the
        # helper is best-effort, so an internal meter error returns None (no quota decision).
        meter_original = gw._UsageMeter.record

        def meter_boom(self, *a, **k):
            raise RuntimeError("meter exploded")

        gw._UsageMeter.record = meter_boom
        try:
            st, _, _ = drive(self.app, path="/v1/ingest", body={"records": [1]},
                             headers=self._bearer(self.QUOTA_KEY))
            self.assertEqual(202, st)                                   # request unaffected
            self.assertEqual(1, len(self.s.calls))                     # backend still dispatched
        finally:
            gw._UsageMeter.record = meter_original

    def test_normalize_key_record_carries_quota_fields(self):
        rec = gw._normalize_key_record({"tenant_id": "t", "request_quota": 5, "quota_window": 30})
        self.assertEqual(5, rec["request_quota"])
        self.assertEqual(30.0, rec["quota_window"])
        # Legacy record (no quota fields) -> None/None -> unlimited (backward compatible).
        legacy = gw._normalize_key_record({"tenant_id": "t"})
        self.assertIsNone(legacy["request_quota"])
        self.assertIsNone(legacy["quota_window"])


class ProvisionerQuotaFlagCase(unittest.TestCase):
    """The provisioner mints keystore records carrying request_quota / quota_window, and a minted
    quota key is enforced end-to-end at the edge."""

    def _mint_record(self, argv):
        try:
            from tools import matrixark_provision_api_key as prov
        except ImportError:
            import matrixark_provision_api_key as prov  # type: ignore
        with tempfile.TemporaryDirectory() as d:
            store = os.path.join(d, "keys.jsonl")
            prov.main(argv + ["--store", store])
            with open(store, "r", encoding="utf-8") as fh:
                return json.loads(fh.readline())

    def test_request_quota_flag_written(self):
        rec = self._mint_record(["--tenant-id", "t", "--request-quota", "3", "--quota-window", "60"])
        self.assertEqual(3, rec["request_quota"])
        self.assertEqual(60.0, rec["quota_window"])

    def test_no_quota_flag_omits_field(self):
        rec = self._mint_record(["--tenant-id", "t"])
        self.assertNotIn("request_quota", rec)   # byte-identical to the pre-quota record shape
        self.assertNotIn("quota_window", rec)

    def test_minted_quota_key_enforced_at_edge(self):
        try:
            from tools import matrixark_provision_api_key as prov
        except ImportError:
            import matrixark_provision_api_key as prov  # type: ignore
        import io
        import contextlib
        with tempfile.TemporaryDirectory() as d:
            store = os.path.join(d, "keys.jsonl")
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                prov.main(["--tenant-id", "t", "--account-id", "acct",
                           "--scope", "context:ingest", "--request-quota", "2", "--store", store])
            plaintext = json.loads(buf.getvalue())["api_key"]
            with _EnvGuard(MATRIXARK_AUTH_ENFORCED="1", MATRIXARK_API_KEYS_HASHED_FILE=store):
                cfg = gw.GatewayConfig.from_env({})
                app = gw.make_v1_app(_FakeServer(), cfg)
                h = {"Authorization": f"Bearer {plaintext}"}
                st1, _, _ = drive(app, path="/v1/ingest", body={"records": [1]}, headers=h)
                st2, _, _ = drive(app, path="/v1/ingest", body={"records": [1]}, headers=h)
                st3, _, body = drive(app, path="/v1/ingest", body={"records": [1]}, headers=h)
        self.assertEqual(202, st1)
        self.assertEqual(202, st2)
        self.assertEqual(429, st3)
        self.assertEqual("quota_exceeded", json.loads(body)["error"])


class PortalPageTest(unittest.TestCase):
    """GET /v1/admin/portal returns the self-contained key-management HTML page (no auth to fetch);
    the page references the real admin JSON endpoints it drives."""

    def setUp(self):
        self.s = _FakeServer()
        self.app = gw.make_v1_app(self.s, _cfg())

    def test_portal_served_200_html_no_auth(self):
        st, hdrs, body = drive(self.app, method="GET", path="/v1/admin/portal")
        self.assertEqual(200, st)
        self.assertIn("text/html", hdrs.get("content-type", ""))
        text = body.decode("utf-8")
        self.assertIn("API Key Portal", text)
        # Key-management controls present.
        for control in ("Create key", "Rotate", "Revoke", "Admin API key"):
            self.assertIn(control, text)

    def test_portal_references_real_endpoints(self):
        _, _, body = drive(self.app, method="GET", path="/v1/admin/portal")
        text = body.decode("utf-8")
        for path in ("/api/admin/create_api_key", "/api/admin/list_api_keys",
                     "/api/admin/rotate_api_key", "/api/admin/revoke_api_key",
                     "/v1/admin/api_key_usage"):
            self.assertIn(path, text)

    def test_portal_fetch_needs_no_bearer_but_actions_are_gated(self):
        # The static page fetch is anonymous; the admin usage endpoint it calls still 401s w/o a key.
        st_page, _, _ = drive(self.app, method="GET", path="/v1/admin/portal")
        self.assertEqual(200, st_page)


if __name__ == "__main__":
    unittest.main()
