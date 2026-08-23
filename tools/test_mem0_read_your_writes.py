#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""mem0's add must be searchable when it returns.

mem0's add is synchronous by contract. Without finalize the gateway treats the ingest as
streaming and schedules an idle-commit on a debounce (stream_idle_commit_timeout_ms, 1000ms),
so add -> search inside that window returned {"results": []} while get_all already showed the
memory -- a user migrating from mem0 would conclude their memories had vanished.

Driven against the REAL /v1 gateway, not the in-process mock the other mem0 suites use: a mock
gateway only proves request shaping, and this failure lives entirely in the backend's commit
scheduling.
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path

os.environ.setdefault("MATRIXARK_REQUIRE_AUTH", "0")

import matrixark_mcp_server as mcp
import matrixark_mem0_compat as mem0

# This drives the REAL gateway over loopback, which needs an ASGI server. Where uvicorn is not
# installed -- CI installs python only -- skip rather than error: an unrunnable test reported as
# a failure is indistinguishable from a broken one, and this suite already has enough of those.
try:
    import uvicorn
except ImportError:  # pragma: no cover - depends on the environment, not the code
    uvicorn = None

try:
    from tools import matrixark_v1_gateway as gw
except ImportError:
    import matrixark_v1_gateway as gw


def live_gateway():
    adapter = mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "m.jsonl")
    server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
    app = gw.make_v1_app(server, gw.GatewayConfig.from_env())
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="critical", lifespan="on")
    srv = uvicorn.Server(config)
    threading.Thread(target=srv.run, daemon=True).start()
    for _ in range(300):
        if srv.started and srv.servers:
            break
        time.sleep(0.05)
    port = srv.servers[0].sockets[0].getsockname()[1]
    # No api_key: an unregistered key is rejected by matrixark_access with
    # "invalid or revoked MatrixArk API key", which surfaces as HTTP 500 on every route and
    # looks exactly like a broken backend.
    return adapter, srv, mem0.Memory(base_url="http://127.0.0.1:%d" % port)


@unittest.skipIf(uvicorn is None, "uvicorn is required to serve the real /v1 gateway")
class Mem0ReadYourWritesTest(unittest.TestCase):
    FACT = "I am a robotics engineer working on Aurora."

    def test_search_finds_the_memory_immediately_after_add(self):
        adapter, srv, m = live_gateway()
        try:
            m.add([{"role": "user", "content": self.FACT}], user_id="alice")
            found = json.dumps(m.search("what is my job?", user_id="alice"))
            self.assertNotEqual('{"results": []}', found,
                                "add -> search returned nothing; the commit was not inline")
            self.assertIn("Aurora", found)
        finally:
            srv.should_exit = True

    def test_get_all_and_delete_still_work(self):
        """The finalize default must not disturb the rest of the surface."""
        adapter, srv, m = live_gateway()
        try:
            m.add([{"role": "user", "content": self.FACT}], user_id="alice")
            memories = m.get_all(user_id="alice").get("memories") or []
            self.assertTrue(memories, "get_all returned nothing after add")
            # delete alone -- calling update first supersedes the id, so a later delete of the
            # ORIGINAL id legitimately matches nothing and reports deleted=False.
            result = m.delete(memories[0].get("id"))
            self.assertTrue(result.get("deleted"),
                            "delete reported nothing removed: %s" % result)
        finally:
            srv.should_exit = True

    def test_streaming_callers_can_opt_out(self):
        adapter, srv, m = live_gateway()
        try:
            m.add([{"role": "user", "content": self.FACT}], user_id="alice", finalize=False)
            self.assertTrue(adapter.read_all(), "opting out of finalize wrote nothing at all")
        finally:
            srv.should_exit = True


if __name__ == "__main__":
    unittest.main()
