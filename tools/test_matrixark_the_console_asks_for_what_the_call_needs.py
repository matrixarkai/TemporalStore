#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A form's declared fields are enough to make the call.

Found by driving every mem0 API one at a time. Two of them could not succeed with what the console
offers, and one of those is destructive:

* ``forget`` offered ``memory_id``. The call ignores it and requires ``confirm`` equal to the
  RESOLVED scope user, so the form could never complete -- and the description said "Stop returning
  one memory. The record stays", while one call removed 200 of 200 memories and left the scope
  empty.
* ``delete_all`` offered no ``confirm`` at all, and the deployment refuses the call without one.

Both were invisible to every existing check, because each of those checks compares the console with
the gateway's own list -- and the list was wrong in exactly the same way. Two copies of a mistake
agree with each other perfectly.

So this asks the deployment instead. For each operation, a request is built from the fields the
console declares, and the answer must not be the deployment saying a required argument is missing.
That is the only assertion that could have caught it.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402


def _adapter(tmp):
    from matrixark_mcp_backends import add_backend_arguments, build_mcp_adapter
    from matrixark_mcp_server import MatrixArkMcpServer

    parser = argparse.ArgumentParser(add_help=False)
    add_backend_arguments(parser)
    ns = parser.parse_args([])
    ns.backend = "local"
    from pathlib import Path
    # A Path, not a string: the adapter reads `.parent` off it to place its sidecar files.
    ns.event_log = Path(tmp) / "events.jsonl"
    ns.local_store = os.path.join(tmp, "store.jsonl")
    # The defaults are the live files on a developer box. A test that writes into them is not a
    # test, so this refuses rather than trusting that the assignment above took.
    for field in ("event_log", "local_store"):
        assert str(getattr(ns, field)).startswith(tmp), "%s is not isolated" % field
    return MatrixArkMcpServer(build_mcp_adapter(ns), access_mode="dev")


class TheFormAsksForWhatTheCallNeedsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.mkdtemp(prefix="mem0form")
        cls.app = gw.make_v1_app(_adapter(cls.tmp), gw.GatewayConfig.from_env())
        cls.call("POST", "/v1/ingest",
                 {"messages": [{"role": "user", "content": "we ship on Friday"}],
                  "finalize": True})

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.tmp, ignore_errors=True)

    @classmethod
    def call(cls, method, path, body=None):
        payload = json.dumps(body or {}).encode("utf-8")
        scope = {"type": "http", "method": method, "path": path, "query_string": b"",
                 "headers": [(b"content-type", b"application/json")]}
        got = {"status": 0, "body": b""}
        sent = [False]

        async def receive():
            if not sent[0]:
                sent[0] = True
                return {"type": "http.request", "body": payload, "more_body": False}
            return {"type": "http.disconnect"}

        async def send(message):
            if message["type"] == "http.response.start":
                got["status"] = message["status"]
            elif message["type"] == "http.response.body":
                got["body"] += message.get("body", b"")

        asyncio.run(asyncio.wait_for(cls.app(scope, receive, send), timeout=120))
        try:
            return got["status"], json.loads(got["body"])
        except Exception:
            return got["status"], {}

    @staticmethod
    def _op(op_id):
        return next(op for op in gw.MEM0_OPERATIONS if op["id"] == op_id)

    def _resolved_user(self):
        _, users = self.call("POST", "/v1/users", {})
        return ((users or {}).get("access") or {}).get("user_id") or "root"

    def test_forget_offers_the_argument_the_call_requires(self) -> None:
        """It offered memory_id, which forget ignores, and not confirm, which it demands."""
        names = [f["name"] for f in self._op("forget")["fields"]]
        self.assertIn("confirm", names)
        self.assertNotIn("memory_id", names,
                         "forget does not address one memory, so offering an id says it does")

    def test_forget_completes_with_what_the_form_asks_for(self) -> None:
        status, body = self.call("POST", "/v1/forget", {"confirm": self._resolved_user()})
        self.assertEqual(200, status, json.dumps(body)[:200])

    def test_delete_all_offers_a_confirm(self) -> None:
        self.assertIn("confirm", [f["name"] for f in self._op("delete_all")["fields"]])

    def test_delete_all_completes_with_what_the_form_asks_for(self) -> None:
        field = next(f for f in self._op("delete_all")["fields"] if f["name"] == "confirm")
        status, body = self.call("POST", "/v1/reset", {"confirm": field["placeholder"]})
        self.assertEqual(200, status, json.dumps(body)[:200])

    def test_the_rating_the_form_offers_is_one_the_call_accepts(self) -> None:
        """The field was a number and its sample value was 1, which the deployment refuses on every
        call: `feedback must be one of POSITIVE, NEGATIVE, VERY_NEGATIVE (got '1')`."""
        self.call("POST", "/v1/ingest", {"messages": [{"role": "user", "content": "a memory"}],
                                         "finalize": True})
        _, listed = self.call("POST", "/v1/memories", {"limit": 1})
        rows = (listed or {}).get("memories") or []
        if not rows:
            self.skipTest("nothing stored to rate")
        field = next(f for f in self._op("feedback")["fields"] if f["name"] == "rating")
        status, body = self.call("POST", "/v1/memory/feedback",
                                 {"memory_id": str(rows[0].get("id")),
                                  "rating": field["default"]})
        self.assertEqual(200, status, json.dumps(body)[:200])

    def test_the_deployment_really_does_refuse_a_missing_argument(self) -> None:
        """The floor. Every assertion above passes against a deployment that accepts anything, and
        that deployment would have accepted the broken forms too."""
        status, _ = self.call("POST", "/v1/reset", {})
        self.assertGreaterEqual(status, 400,
                                "the deployment accepted a call with its required argument "
                                "missing, so nothing above proves anything")
        # WHICH refusal it is -- a 400 naming the argument rather than a 500 blaming the backend --
        # belongs to the change that classifies it, and is asserted there. Pinning it here would
        # make this suite depend on the order those two land in.


if __name__ == "__main__":
    unittest.main()
