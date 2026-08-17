#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""mem0 migration compatibility.

Part A: ``normalize_envelope`` folds mem0-style TOP-LEVEL identity kwargs
(``user_id`` / ``run_id`` / ``agent_id``) into the canonical ``scope``
(``scope.user_id`` / ``scope.session_id`` / ``scope.agent_id``), with the
canonical scope field winning over an alias, and full-scope callers unaffected.

Part B: the ``matrixark_mem0_compat.Memory`` shim (drop-in for
``from mem0 import Memory``) posts the correctly-mapped body to ``/v1/ingest`` and
``/v1/retrieve`` against a tiny in-process stdlib gateway (no live services)."""
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from matrixark_mcp_core import normalize_envelope

import matrixark_mem0_compat as mem0


# --------------------------------------------------------------------------- #
# Part A: alias folding in normalize_envelope
# --------------------------------------------------------------------------- #
MESSAGES = [{"role": "user", "content": "hello"}]


class NormalizeEnvelopeAliasFoldTest(unittest.TestCase):
    def _scope(self, args: dict) -> dict:
        return normalize_envelope({"kind": "message", "messages": MESSAGES, **args},
                                  default_kind="message")["scope"]

    def test_top_level_user_id_folds_into_scope(self):
        scope = self._scope({"user_id": "u1"})
        self.assertEqual(scope["user_id"], "u1")

    def test_top_level_run_id_folds_into_session_id(self):
        scope = self._scope({"run_id": "sess-42"})
        self.assertEqual(scope["session_id"], "sess-42")

    def test_top_level_agent_id_folds_into_scope(self):
        scope = self._scope({"agent_id": "assistant"})
        self.assertEqual(scope["agent_id"], "assistant")

    def test_all_three_aliases_fold_together(self):
        scope = self._scope({"user_id": "u1", "run_id": "r1", "agent_id": "a1"})
        self.assertEqual(scope["user_id"], "u1")
        self.assertEqual(scope["session_id"], "r1")
        self.assertEqual(scope["agent_id"], "a1")

    def test_scope_run_id_also_folds_into_session_id(self):
        scope = self._scope({"scope": {"run_id": "sess-nested"}})
        self.assertEqual(scope["session_id"], "sess-nested")

    def test_explicit_scope_session_id_beats_run_id(self):
        # Canonical scope field wins over the alias.
        scope = self._scope({"run_id": "from-alias", "scope": {"session_id": "canonical"}})
        self.assertEqual(scope["session_id"], "canonical")

    def test_explicit_scope_user_id_beats_top_level_user_id(self):
        scope = self._scope({"user_id": "alias-user", "scope": {"user_id": "canonical-user"}})
        self.assertEqual(scope["user_id"], "canonical-user")

    def test_explicit_scope_agent_id_beats_top_level_agent_id(self):
        scope = self._scope({"agent_id": "alias-agent", "scope": {"agent_id": "canonical-agent"}})
        self.assertEqual(scope["agent_id"], "canonical-agent")

    def test_canonical_scope_unaffected_when_no_aliases(self):
        # A full canonical scope with no aliases is passed through byte-identically.
        full = {"tenant_id": "t1", "user_id": "u1", "session_id": "s1"}
        scope = self._scope({"scope": dict(full)})
        for key, value in full.items():
            self.assertEqual(scope[key], value)
        self.assertNotIn("agent_id", scope)  # nothing invented

    def test_no_identity_at_all_leaves_scope_empty(self):
        scope = self._scope({})
        self.assertEqual(scope, {})


# --------------------------------------------------------------------------- #
# Part B: Memory shim against an in-process mock gateway
# --------------------------------------------------------------------------- #
_POSTS: list[dict] = []


class _GatewayHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence
        pass

    def _read(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def do_POST(self):
        body = json.loads(self._read() or b"{}")
        _POSTS.append({"path": self.path, "body": body,
                       "auth": self.headers.get("Authorization") or ""})
        out = json.dumps({"ok": True, "path": self.path, "echo": body}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)


class _Gateway:
    def __init__(self):
        self.httpd = HTTPServer(("127.0.0.1", 0), _GatewayHandler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown(); self.httpd.server_close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


class MemoryAddTest(unittest.TestCase):
    def setUp(self):
        _POSTS.clear()

    def test_add_maps_identity_kwargs_to_scope(self):
        with _Gateway() as gw:
            m = mem0.Memory(base_url=gw.url, api_key="k")
            m.add(MESSAGES, user_id="u1", agent_id="assistant", run_id="sess-42",
                  metadata={"topic": "demo"})
        self.assertEqual(1, len(_POSTS))
        post = _POSTS[0]
        self.assertEqual("/v1/ingest", post["path"])
        body = post["body"]
        self.assertEqual("message", body["kind"])
        self.assertEqual(MESSAGES, body["messages"])
        self.assertEqual({"user_id": "u1", "agent_id": "assistant", "session_id": "sess-42"},
                         body["scope"])
        self.assertEqual({"topic": "demo"}, body["metadata"])
        self.assertEqual("Bearer k", post["auth"])

    def test_add_bare_string_messages_wrapped(self):
        with _Gateway() as gw:
            m = mem0.Memory(base_url=gw.url, api_key="k")
            m.add("just a string", user_id="u1")
        body = _POSTS[0]["body"]
        self.assertEqual([{"role": "user", "content": "just a string"}], body["messages"])

    def test_add_omits_empty_scope_and_metadata(self):
        with _Gateway() as gw:
            m = mem0.Memory(base_url=gw.url, api_key="k")
            m.add(MESSAGES)  # no identity, no metadata
        body = _POSTS[0]["body"]
        self.assertNotIn("scope", body)
        self.assertNotIn("metadata", body)

    def test_add_partial_identity_only_present_fields(self):
        with _Gateway() as gw:
            m = mem0.Memory(base_url=gw.url, api_key="k")
            m.add(MESSAGES, agent_id="assistant")  # only agent_id
        self.assertEqual({"agent_id": "assistant"}, _POSTS[0]["body"]["scope"])

    def test_add_openai_response_auto_sniffed(self):
        # Provider-shaped input flows through matrixark_model_adapters.to_messages.
        resp = {"choices": [{"message": {"role": "assistant", "content": "auto"}}]}
        with _Gateway() as gw:
            m = mem0.Memory(base_url=gw.url, api_key="k")
            m.add(resp, user_id="u1")
        self.assertEqual([{"role": "assistant", "content": "auto"}],
                         _POSTS[0]["body"]["messages"])

    def test_add_openai_content_parts_flattened_via_provider(self):
        req = {"messages": [{"role": "user", "content": [
            {"type": "text", "text": "a"}, {"type": "text", "text": "b"}]}]}
        with _Gateway() as gw:
            m = mem0.Memory(base_url=gw.url, api_key="k")
            m.add(req, provider="openai")
        self.assertEqual([{"role": "user", "content": "a\nb"}],
                         _POSTS[0]["body"]["messages"])

    def test_add_anthropic_blocks_via_provider(self):
        resp = {"type": "message", "role": "assistant",
                "content": [{"type": "text", "text": "claude reply"}]}
        with _Gateway() as gw:
            m = mem0.Memory(base_url=gw.url, api_key="k")
            m.add(resp, provider="anthropic")
        self.assertEqual([{"role": "assistant", "content": "claude reply"}],
                         _POSTS[0]["body"]["messages"])


class MemorySearchTest(unittest.TestCase):
    def setUp(self):
        _POSTS.clear()

    def test_search_maps_identity_and_limit(self):
        with _Gateway() as gw:
            m = mem0.Memory(base_url=gw.url, api_key="k")
            m.search("what did we decide?", user_id="u1", agent_id="assistant",
                     run_id="sess-42", limit=5)
        self.assertEqual(1, len(_POSTS))
        post = _POSTS[0]
        self.assertEqual("/v1/retrieve", post["path"])
        body = post["body"]
        self.assertEqual("what did we decide?", body["query"])
        self.assertEqual({"user_id": "u1", "agent_id": "assistant", "session_id": "sess-42"},
                         body["scope"])
        # limit -> ranking.max_selected_refs (the ref-count knob retrieve reads).
        self.assertEqual({"max_selected_refs": 5}, body["ranking"])

    def test_search_without_limit_omits_ranking(self):
        with _Gateway() as gw:
            m = mem0.Memory(base_url=gw.url, api_key="k")
            m.search("q", user_id="u1")
        body = _POSTS[0]["body"]
        self.assertNotIn("ranking", body)
        self.assertEqual({"user_id": "u1"}, body["scope"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
