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

from temporalstore_mem0 import Memory


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
        if self.path == "/v1/ingest":
            self._reply({"status": "accepted", "event_id_hash": 731})
        elif self.path == "/v1/retrieve":
            self._reply({
                "groups": [{
                    "type": "event",
                    "items": [{"id": "731", "text": "I prefer dark mode", "score": 0.92}],
                }]
            })
        elif self.path == "/v1/update":
            self._reply({"updated": True})
        elif self.path == "/v1/delete":
            self._reply({"deleted": True})
        else:
            self._reply({"ok": True})

    def do_GET(self) -> None:
        self._record()
        self._reply({"found": True, "memory": "I prefer dark mode"})


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
        self.assertEqual("/v1/ingest", request["path"])
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

    def test_search_maps_limit_and_flattens_context_pack(self) -> None:
        result = Memory(base_url=self.base_url).search(
            "theme preference", user_id="alice", limit=4
        )

        request = _Handler.requests[0]
        self.assertEqual("/v1/retrieve", request["path"])
        self.assertEqual({"user_id": "alice"}, request["body"]["scope"])
        self.assertEqual({"max_selected_refs": 4}, request["body"]["ranking"])
        self.assertEqual("731", result["results"][0]["id"])
        self.assertEqual("I prefer dark mode", result["results"][0]["memory"])
        self.assertEqual("event", result["results"][0]["metadata"]["ref_type"])

    def test_addressable_memory_methods_use_expected_routes(self) -> None:
        memory = Memory(base_url=self.base_url)
        self.assertTrue(memory.get("a/b")["found"])
        self.assertTrue(memory.update("731", "Use light mode")["updated"])
        self.assertTrue(memory.delete("731")["deleted"])

        self.assertEqual(
            [("GET", "/v1/memory/a%2Fb"), ("POST", "/v1/update"), ("POST", "/v1/delete")],
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


if __name__ == "__main__":
    unittest.main()
