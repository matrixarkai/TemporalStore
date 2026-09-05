#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A recorded failure names the log entry that explains it.

Every backend failure mints an incident token: the caller is told it, and the exception is logged
under it. The panel listing recent failures did not carry it, so an operator reading *"500 on
/v1/memories, 40 seconds ago"* had to guess at timestamps in a log that may hold many.

The token exists by the time the failure is recorded -- it is minted inside the handler, and the
request is recorded in the wrapper's ``finally`` after that handler returns -- but nothing carried it
the few frames up the stack. A context variable does, the same way the request's ``Accept-Encoding``
reaches the response helpers, and it is cleared at the top of every request: a connection serves
several, and a token left behind would attach one failure's log entry to another's row. That is
asserted, not assumed.

The column is always present, with a dash where there is none. Most rows in this ring are refusals
-- a 401 for a wrong key mints no incident, because nothing went wrong inside -- and a column that
came and went with the data would be the shape-shifting table its neighbour was just fixed for.

This file also gives ``failures_panel_harness.js`` an owner. It had none: every other harness in
that directory is run by a test named after it, and this one sat with fifteen assertions that never
executed. It passes against the current pages, so it was not stale -- just unreachable.
"""
from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import shutil
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_metrics as gwm  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402

PORTAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal")
HARNESS = os.path.join(PORTAL, "failures_panel_harness.js")
PAGE = os.path.join(PORTAL, "setup_portal.html")

SECRET = "/srv/matrixark/store/shard-7/pages.db"


class _Boom:
    """A backend that fails the way a real one does: with something in the message."""

    def call_tool(self, name, args):
        raise OSError("[Errno 13] Permission denied: '%s'" % SECRET)

    def handle(self, body):
        return {"jsonrpc": "2.0", "id": body.get("id"), "result": {}}


class _Quiet:
    def call_tool(self, name, args):
        return {"ok": name}

    def handle(self, body):
        return {"jsonrpc": "2.0", "id": body.get("id"), "result": {}}


def _drive(*args, **kwargs):
    from test_matrixark_v1_gateway import drive
    return drive(*args, **kwargs)


def _two_requests_in_one_task(app, first, second):
    """Serve two requests without a fresh context between them.

    `drive` uses asyncio.run per call, so every request it makes starts from a clean context. That
    is the wrong shape for asking whether something is left behind BETWEEN requests, so these two
    are awaited in one task -- which is what a server reusing a task for a keep-alive connection
    gives you.
    """
    bodies = []

    async def once(method, path, payload):
        sent = []

        async def receive():
            return {"type": "http.request", "body": json.dumps(payload).encode(),
                    "more_body": False}

        async def send(message):
            sent.append(message)

        await app({"type": "http", "method": method, "path": path,
                   "query_string": b"", "headers": []}, receive, send)
        bodies.append(b"".join(m.get("body", b"") for m in sent
                               if m["type"] == "http.response.body"))

    async def both():
        await once(*first)
        await once(*second)

    asyncio.run(both())
    return bodies


def _app(server):
    from test_matrixark_v1_gateway import _cfg
    return gw.make_v1_app(server, _cfg(require_auth=False))


class _CaptureLog:
    def __enter__(self):
        self.stream = io.StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.logger = logging.getLogger("matrixark.gateway")
        self.previous = self.logger.level
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.ERROR)
        return self

    def __exit__(self, *_exc):
        self.logger.removeHandler(self.handler)
        self.logger.setLevel(self.previous)
        return False

    @property
    def text(self):
        return self.stream.getvalue()


def _failure_rows():
    return gwm.METRICS.snapshot().get("recent_failures", [])


class TheRecordedFailureCarriesTheTokenTest(unittest.TestCase):

    def test_the_caller_the_panel_and_the_log_agree(self) -> None:
        """Three copies of one token. Any two disagreeing would be worse than none, because a
        mismatched token looks like an answer."""
        with _CaptureLog() as log:
            _st, _h, payload = _drive(_app(_Boom()), method="POST", path="/v1/memories",
                                      body={"scope": {}})
        told = json.loads(payload)["incident"]
        rows = [r for r in _failure_rows() if r.get("incident") == told]
        self.assertTrue(rows, "no recorded failure carries the token the caller was given")
        self.assertIn(told, log.text)
        self.assertEqual("/v1/memories", rows[0]["route"])

    def test_a_refusal_carries_none(self) -> None:
        """Nothing went wrong inside, so there is no log entry to name. An empty token here would
        point at nothing."""
        _st, _h, _b = _drive(_app(_Quiet()), method="GET", path="/v1/nope")
        for row in _failure_rows():
            if row.get("status") == 404 and row.get("route") not in ("", None):
                self.assertNotIn("incident", row,
                                 "a refusal was given a token that names no log entry")

    def test_one_request_does_not_inherit_anothers(self) -> None:
        """A connection serves several requests. A token left in place would attach the previous
        failure's log entry to this row, and an operator following it would find the wrong thing.

        Driven through one task on purpose. The suite's own `drive` calls asyncio.run per request,
        which hands each one a fresh context -- so it cannot show a token surviving into the next
        request, which is exactly what the reset prevents. Two awaits in one task share a context,
        the way a server reusing a task for a keep-alive connection would.
        """
        app = _app(_Boom())
        with _CaptureLog():
            first, second = _two_requests_in_one_task(
                app,
                ("POST", "/v1/memories", {"scope": {}}),
                ("GET", "/v1/nope", {}))
        told = json.loads(first)["incident"]
        self.assertTrue(told, "the first request did not mint one, so this proves nothing")
        stale = [r for r in _failure_rows()
                 if r.get("status") == 404 and r.get("incident") == told]
        self.assertEqual([], stale, "a later failure inherited an earlier request's token")


class TheRecorderStillWorksWithoutOneTest(unittest.TestCase):

    def test_record_takes_it_optionally(self) -> None:
        """Anything already calling record() keeps working; the argument is new and defaults."""
        gwm.METRICS.record("/v1/retrieve", "POST", 500, 0.01)
        self.assertTrue(any(r["status"] == 500 for r in _failure_rows()))

    def test_a_failure_recorded_without_one_has_no_token(self) -> None:
        gwm.METRICS.record("/v1/retrieve", "POST", 502, 0.01)
        rows = [r for r in _failure_rows() if r["status"] == 502]
        self.assertTrue(rows)
        self.assertNotIn("incident", rows[0])


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class ThePanelShowsItTest(unittest.TestCase):
    """Also the owner this harness never had: it carried fifteen assertions and no test ran it."""

    def _run(self):
        return subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=300)

    def test_the_harness_passes(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_a_failure_that_carries_a_token_shows_it(self) -> None:
        self.assertIn("ok   a failure that carries a token shows it", self._run().stdout)

    def test_the_column_does_not_come_and_go(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   the column is there even for the rows without one", out)
        self.assertIn("ok   a row without one shows a dash rather than an empty cell", out)

    def test_the_assertions_it_already_had_still_run(self) -> None:
        """The point of giving it an owner: these were never executed before."""
        out = self._run().stdout
        for line in ("ok   the failures panel drew the failures",
                     "ok   newest is first",
                     "ok   the tail is shown, not just the mean",
                     "ok   no identity is rendered"):
            with self.subTest(line=line):
                self.assertIn(line, out)


if __name__ == "__main__":
    unittest.main()
