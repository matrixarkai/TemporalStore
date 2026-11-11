#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Network contract tests for TemporalStore's mem0-compatible Python client."""

from __future__ import annotations

import json
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from temporalstore.llm_context import Memory


class _Handler(BaseHTTPRequestHandler):
    requests: list[dict] = []

    def log_message(self, _format: str, *_args: object) -> None:
        pass

    def _record(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b""
        body = json.loads(raw) if raw else None
        self.requests.append({
            "method": self.command,
            "path": self.path,
            "authorization": self.headers.get("Authorization"),
            "body": body,
        })
        return body or {}

    def _reply(self, body: dict) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:
        self._record()
        if self.path == "/memories":
            self._reply({"status": "accepted", "event_id_hash": 731})
        elif self.path == "/memories/search":
            self._reply({"results": [{
                "id": "731",
                "memory": "I prefer dark mode",
                "score": 0.92,
                "metadata": {"record_type": "raw_message"},
            }]})
        else:
            self._reply({"ok": True})

    def do_GET(self) -> None:
        self._record()
        self._reply({"found": True, "memory": "I prefer dark mode"})

    def do_PUT(self) -> None:
        self._record()
        self._reply({"updated": True})

    def do_DELETE(self) -> None:
        self._record()
        self._reply({"deleted": True})


class MemorySdkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _Handler.requests = []
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        host, port = cls.server.server_address
        cls.base_url = f"http://{host}:{port}"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def setUp(self) -> None:
        _Handler.requests.clear()

    def test_add_maps_identity_and_returns_mem0_shape(self) -> None:
        memory = Memory(base_url=self.base_url, api_key="secret")
        result = memory.add(
            "I prefer dark mode",
            user_id="alice",
            agent_id="assistant",
            run_id="session-7",
        )

        request = _Handler.requests[0]
        self.assertEqual("/memories", request["path"])
        self.assertEqual("Bearer secret", request["authorization"])
        self.assertEqual(
            {"user_id": "alice", "agent_id": "assistant", "session_id": "session-7"},
            request["body"]["scope"],
        )
        self.assertEqual([{"role": "user", "content": "I prefer dark mode"}],
                         request["body"]["messages"])
        self.assertTrue(request["body"]["finalize"])
        self.assertEqual("731", result["results"][0]["id"])
        self.assertEqual("ADD", result["results"][0]["event"])

    def test_add_forwards_explicit_mode_and_encoder_configuration(self) -> None:
        provider = {"provider_kind": "ollama", "embedding_model": "installed-encoder",
                    "mock_mode": False}
        messages = [{"role": "assistant", "name": "cedar", "content": "  raw memory\n"}]
        Memory(base_url=self.base_url).add(messages, user_id="alice", infer=False,
                                         provider=provider)
        request = _Handler.requests[0]
        self.assertEqual("/memories", request["path"])
        self.assertIs(request["body"]["infer"], False)
        self.assertEqual(provider, request["body"]["provider"])
        self.assertEqual(messages, request["body"]["messages"])

    def test_add_rejects_oversized_batches_without_network_calls(self) -> None:
        with self.assertRaises(ValueError):
            Memory(base_url=self.base_url).add([{"role": "user", "content": "memory"}] * 1001)
        self.assertEqual([], _Handler.requests)

    def test_search_maps_limit_and_flattens_context_pack(self) -> None:
        result = Memory(base_url=self.base_url).search(
            "theme preference", user_id="alice", limit=4
        )

        request = _Handler.requests[0]
        self.assertEqual("/memories/search", request["path"])
        self.assertEqual({"user_id": "alice"}, request["body"]["scope"])
        self.assertEqual(4, request["body"]["limit"])
        self.assertEqual("731", result["results"][0]["id"])
        self.assertEqual("I prefer dark mode", result["results"][0]["memory"])
        self.assertEqual("raw_message", result["results"][0]["metadata"]["record_type"])

    def test_addressable_memory_methods_use_expected_routes(self) -> None:
        memory = Memory(base_url=self.base_url)
        self.assertTrue(memory.get("a/b")["found"])
        self.assertTrue(memory.update("731", "Use light mode")["updated"])
        self.assertTrue(memory.delete("731")["deleted"])

        self.assertEqual(
            [("GET", "/memories/a%2Fb"), ("PUT", "/memories/731"), ("DELETE", "/memories/731")],
            [(row["method"], row["path"]) for row in _Handler.requests],
        )

    def test_temporalstore_environment_names_take_precedence(self) -> None:
        env = {
            "TEMPORALSTORE_BASE_URL": self.base_url,
            "TEMPORALSTORE_API_KEY": "temporal-token",
            "MATRIXARK_BASE_URL": "http://127.0.0.1:1",
            "MATRIXARK_API_KEY": "legacy-token",
        }
        with patch.dict(os.environ, env, clear=False):
            Memory().add("remember this", user_id="alice")

        request = _Handler.requests[0]
        self.assertEqual("Bearer temporal-token", request["authorization"])

    def test_metadata_only_update_omits_content(self) -> None:
        memory = Memory(base_url=self.base_url)
        self.assertTrue(memory.update("731", metadata={})["updated"])
        self.assertEqual({"memory_id": "731", "metadata": {}}, _Handler.requests[0]["body"])

    def test_update_accepts_text_keyword_and_preserves_whitespace(self) -> None:
        memory = Memory(base_url=self.base_url)
        self.assertTrue(memory.update("731", text="  exact text\n")["updated"])
        self.assertEqual("  exact text\n", _Handler.requests[0]["body"]["data"])

    def test_update_requires_content_or_metadata_before_network(self) -> None:
        with self.assertRaises(ValueError):
            Memory(base_url=self.base_url).update("731")
        self.assertEqual([], _Handler.requests)


if __name__ == "__main__":
    unittest.main()
