#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A caller's mistake is a 400 that says what to fix.

Measured by driving every mem0 API one at a time. Omit a ``memory_id`` and the answer was:

    500 {"error": "backend_error",
         "detail": "The backend could not complete this call.",
         "incident": "cd35add2689f"}

The sentence that fixes it -- ``delete requires a memory_id`` -- was discarded, and the caller was
handed a token to take to an operator, for a mistake they made themselves and could have corrected
in one read. Eleven of the sixteen generic raises in the local adapter are request validation like
that; the gateway has a 400 class and almost nothing reached it.

**The floor matters more than the change.** A 500 is still deliberately incurious: it keeps the
sentence that says nothing about the inside of the deployment, and the token that names the log
entry which does. The message is echoed only where it is ABOUT THE REQUEST. Widening that would
turn a debugging improvement into a disclosure, so it is asserted in both directions here.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402
from matrixark_mcp_core_identity import MatrixArkInvalidRequestError  # noqa: E402
from matrixark_mcp_errors import MatrixArkError  # noqa: E402
from test_matrixark_v1_gateway import _cfg  # noqa: E402

ADMIN = {"Authorization": "Bearer k-acme"}


class _RaisingServer:
    """A backend that fails the way the local adapter fails."""

    def __init__(self, exc):
        self.exc = exc

    def call_tool(self, name, args):
        raise self.exc

    def handle(self, body):
        raise self.exc


def call(exc, path="/v1/delete", body=None):
    app = gw.make_v1_app(_RaisingServer(exc), _cfg())
    payload = json.dumps(body or {}).encode("utf-8")
    scope = {"type": "http", "method": "POST", "path": path, "query_string": b"",
             "headers": [(b"content-type", b"application/json")]
                        + [(k.lower().encode(), v.encode()) for k, v in ADMIN.items()]}
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

    asyncio.run(asyncio.wait_for(app(scope, receive, send), timeout=60))
    try:
        return got["status"], json.loads(got["body"])
    except Exception:
        return got["status"], {}


class ACallersMistakeTest(unittest.TestCase):

    def test_it_is_a_400(self) -> None:
        status, _ = call(MatrixArkInvalidRequestError("delete requires a memory_id"))
        self.assertEqual(400, status)

    def test_it_says_what_to_fix(self) -> None:
        """The whole point. A status alone tells a caller they were wrong and not how."""
        _, body = call(MatrixArkInvalidRequestError("delete requires a memory_id"))
        self.assertEqual("delete requires a memory_id", body.get("detail"))

    def test_it_is_not_called_a_backend_error(self) -> None:
        """400 with `backend_error` in the body says two contradictory things about whose fault it
        was, and the reader has to pick one."""
        _, body = call(MatrixArkInvalidRequestError("delete requires a memory_id"))
        self.assertEqual("invalid_request", body.get("error"))

    def test_a_caller_is_not_handed_a_token_for_their_own_mistake(self) -> None:
        """An incident token is a handle for asking an operator about the deployment. Offering one
        for a missing parameter sends somebody to ask about a fault that is not there."""
        _, body = call(MatrixArkInvalidRequestError("delete requires a memory_id"))
        self.assertNotIn("incident", body)


class TheDeploymentsFaultIsStillItsOwnTest(unittest.TestCase):
    """The floor. Every assertion above is satisfied by a gateway that echoes every exception it
    catches, and that gateway leaks the inside of the deployment to anyone who can make it fail."""

    def test_an_unexpected_failure_is_still_a_500(self) -> None:
        status, _ = call(RuntimeError("psycopg2 connection to 10.4.2.9:5432 refused"))
        self.assertEqual(500, status)

    def test_and_still_says_nothing_about_the_inside(self) -> None:
        _, body = call(RuntimeError("psycopg2 connection to 10.4.2.9:5432 refused"))
        self.assertNotIn("psycopg2", json.dumps(body))
        self.assertNotIn("10.4.2.9", json.dumps(body))
        self.assertEqual("The backend could not complete this call.", body.get("detail"))

    def test_and_still_carries_the_token(self) -> None:
        """It is the one string that ties the caller's report to the operator's log."""
        _, body = call(RuntimeError("psycopg2 connection refused"))
        self.assertTrue(body.get("incident"))

    def test_a_generic_error_from_the_backend_is_not_the_callers(self) -> None:
        """`MatrixArkError` is the base class, raised for deployment-side trouble as well -- so it
        must NOT be swept into 400 along with its invalid-request subclass."""
        status, body = call(MatrixArkError("forget could not resolve the subject scope"))
        self.assertEqual(500, status)
        self.assertEqual("backend_error", body.get("error"))


if __name__ == "__main__":
    unittest.main()
