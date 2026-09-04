#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Replay answers with the pack that was asked for, not with the store.

`replay` guarded itself with ``ENABLE_CONTEXT_REPLAY or bool(args.get("enable_replay"))``. The
second half comes from the caller, so a deployment that had deliberately left replay off had it on
anyway for anybody holding a key.

Past that guard, whether the answer is the compact pack-scoped payload or a debug one was decided
by ``include_debug_records`` / ``include_debug_refs`` -- also read off the request. The debug branch
returns ``read_all()``: every record in the store, ignoring both the ``context_pack_id`` that was
asked for and the tenant doing the asking.

Reproduced in enforced mode with a key carrying only ``context:replay`` and ``context:retrieve``,
scoped to tenant A:

    replay, plain                            refused, replay is disabled
    replay, caller enables it                ALLOWED, 480 chars, compact
    replay, caller asks for debug records    ALLOWED, 85,396 chars, containing tenant B's
                                             memory text

Both switches belong to whoever runs the deployment, not to whoever holds a key. They are honoured
for a dev-mode identity -- the local debugging they were written for -- and ignored for a scoped
one.

Every refusal here is paired with the same key doing something it is entitled to do, because a key
that had simply stopped working would satisfy the refusals on its own.
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer, MatrixArkError

A = {"account_id": "acct_a", "tenant_id": "tenant_a"}
B = {"account_id": "acct_b", "tenant_id": "tenant_b"}
SECRET_OF_B = "tenant B private note about the airship hull tolerances"


class ReplayIsScopedToWhatWasAskedForTest(unittest.TestCase):

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.log = Path(tmp.name) / "events.jsonl"

        dev = self._server("dev")
        self.key = dev.call_tool("matrixark_admin_create_api_key", {
            "scope": A, **A, "role": "service", "key_prefix": "sk_live",
            "scopes": ["context:replay", "context:retrieve"]})
        dev.call_tool("matrixark_ingest", {
            "scope": A, "finalize": True,
            "messages": [{"role": "user", "content": "tenant A note about balloons"}]})
        dev.call_tool("matrixark_ingest", {
            "scope": B, "finalize": True,
            "messages": [{"role": "user", "content": SECRET_OF_B}]})
        self.pack = dev.call_tool("matrixark_retrieve",
                                  {"scope": A, "query": "balloons"})["context_pack_id"]
        dev.close(timeout_s=10.0)

    def _server(self, mode: str) -> MatrixArkMcpServer:
        server = MatrixArkMcpServer(MatrixArkLocalAdapter(self.log), line_json=True,
                                    access_mode=mode)
        self.addCleanup(server.close, timeout_s=10.0)
        return server

    def _replay(self, server, **extra):
        return server.call_tool("matrixark_replay",
                                dict(extra, api_key=self.key["api_key"],
                                     context_pack_id=self.pack))

    @staticmethod
    def _replay_enabled():
        """Turn replay on the way a deployment does.

        The flag is read into a module constant at import, so setting the environment variable now
        would change nothing, and the binding that matters is the one in the module doing the
        reading rather than the one it was imported from. That module is reached through the method
        itself: importing it by name here breaks its own import cycle, and there are two constants
        of this name in the tree, only one of which `replay` reads.
        """
        module = sys.modules[MatrixArkLocalAdapter.replay.__module__]
        assert hasattr(module, "ENABLE_CONTEXT_REPLAY"), module.__name__
        return mock.patch.object(module, "ENABLE_CONTEXT_REPLAY", True)

    # ---- the control ---------------------------------------------------------------------------

    def test_the_key_works(self) -> None:
        """Otherwise every refusal below would be satisfied by a key that does nothing."""
        server = self._server("enforced")
        result = server.call_tool("matrixark_retrieve",
                                  {"api_key": self.key["api_key"], "query": "balloons"})
        self.assertTrue(result)

    # ---- what a caller may still switch on -----------------------------------------------------

    def test_enable_replay_still_works_and_buys_only_the_compact_payload(self) -> None:
        """`enable_replay` is documented in the error the guard raises and is deliberately left
        alone. On its own it gets the pack-scoped answer, which carries no memory text -- so what
        it turns on is not what the dump was."""
        server = self._server("enforced")
        result = self._replay(server, enable_replay=True)
        self.assertNotEqual("debug_full_store_scan", result.get("replay_payload_policy"))
        self.assertNotIn(SECRET_OF_B, str(result))

    # ---- and the payload is not the caller's to choose ------------------------------------------

    def test_with_replay_enabled_a_caller_still_cannot_ask_for_the_store(self) -> None:
        """The one that matters: replay is on, and the debug switch must not turn a pack-scoped
        answer into every record in the store."""
        server = self._server("enforced")
        with self._replay_enabled():
            result = self._replay(server, include_debug_records=True, include_debug_refs=True)

        self.assertNotEqual("debug_full_store_scan", result.get("replay_payload_policy"),
                            "a scoped key was handed a full store scan")
        blob = str(result)
        self.assertNotIn(SECRET_OF_B, blob,
                         "another tenant's memory text came back from a replay")

    def test_with_replay_enabled_the_pack_still_comes_back(self) -> None:
        """The fix must not turn replay off for the keys entitled to it."""
        server = self._server("enforced")
        with self._replay_enabled():
            result = self._replay(server)
        self.assertEqual(self.pack, result.get("context_pack_id"))
        self.assertIn("replay_payload_policy", result)

    # ---- the local debugging case is unchanged --------------------------------------------------

    def test_a_dev_identity_keeps_its_debug_switches(self) -> None:
        """These were written for somebody at a console with the deployment's environment, and
        that case still works: dev mode is already unrestricted everywhere else."""
        dev = self._server("dev")
        result = dev.call_tool("matrixark_replay",
                               {"context_pack_id": self.pack, "enable_replay": True,
                                "include_debug_records": True})
        self.assertEqual("debug_full_store_scan", result.get("replay_payload_policy"))


if __name__ == "__main__":
    unittest.main()
