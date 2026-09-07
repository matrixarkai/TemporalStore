#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A stream that breaks says why, the same as one that ends on purpose.

The planned ending already carries a reason -- *"Say why before going, so a reconnect is not
mistaken for a fault"* is the comment on it. A stream that breaks on the server's side said
nothing at all: the body generator raised, the response ended mid-flight, and the browser saw
silence.

EventSource then reconnects on the ``retry`` cadence the stream itself set, and breaks again, for
as long as the fault lasts. So a persistent backend failure is a reconnect every three seconds
that nothing counts, and that no page can tell from a network that died -- the two look identical
from the client, and only one of them is the deployment's fault.

The reason goes out with the token that names the log entry, not the fault: a portal reader is not
told the inside of the deployment, and that is the shape every other refusal here uses.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_metrics as gwm  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import _FakeServer, _cfg  # noqa: E402

ADMIN = {"Authorization": "Bearer k-acme"}


def drive(app, *, fail_after=1, timeout=10.0):
    """Open a stream, let it emit, then make the NEXT tick raise. Returns what was sent."""
    scope = {"type": "http", "method": "GET", "path": "/v1/admin/events",
             "query_string": b"",
             "headers": [(k.lower().encode(), v.encode()) for k, v in ADMIN.items()]}
    sent = []
    seen = {"count": 0}
    disconnect = asyncio.Event()

    async def receive():
        if not seen.get("opened"):
            seen["opened"] = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)
        body = message.get("body") or b""
        if body.startswith(b"event: status") or body.startswith(b": keepalive"):
            seen["count"] += 1
            if seen["count"] >= fail_after:
                # From here on the frame builder is broken, the way a backend outage breaks it.
                gw._event_frame = _explode
        if body.startswith(b"event: bye"):
            disconnect.set()

    async def run():
        await asyncio.wait_for(app(scope, receive, send), timeout=timeout)

    asyncio.run(run())
    return b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")


async def _explode(*args, **kwargs):
    raise RuntimeError("the backend is not answering")


class AStreamThatBreaksSaysWhyTest(unittest.TestCase):

    def setUp(self) -> None:
        self.original = gw._event_frame
        self.addCleanup(setattr, gw, "_event_frame", self.original)
        self.app = gw.make_v1_app(_FakeServer(), _cfg())

    def _bye(self, body: bytes) -> dict:
        self.assertIn(b"event: bye", body, "the stream ended without saying anything")
        chunk = body.split(b"event: bye", 1)[1]
        line = [l for l in chunk.split(b"\n") if l.startswith(b"data:")][0]
        return json.loads(line[5:].strip())

    def test_it_says_the_server_broke(self) -> None:
        said = self._bye(drive(self.app))
        self.assertEqual("server_error", said.get("reason"))

    def test_it_carries_the_token_that_names_the_log_entry(self) -> None:
        """The one string a reader can take to an operator. Without it the page has a sentence and
        no way to get any further, and the operator has a log they cannot tie to the report."""
        said = self._bye(drive(self.app))
        self.assertTrue(said.get("incident"), "no incident token on the reason")

    def test_it_does_not_leak_what_actually_broke(self) -> None:
        """A portal reader is not told the inside of the deployment -- the token is the handle."""
        body = drive(self.app)
        self.assertNotIn(b"the backend is not answering", body)
        self.assertNotIn(b"RuntimeError", body)
        self.assertNotIn(b"Traceback", body)

    def test_a_planned_ending_is_still_told_apart(self) -> None:
        """Both endings are a bye, so the reason is the only thing separating them -- and the page
        treats one as routine and the other as a fault."""
        self.assertNotEqual("stream_max_age", self._bye(drive(self.app)).get("reason"))

    def test_a_stream_that_is_not_broken_says_no_such_thing(self) -> None:
        """The floor. A stream that always announced a server error would pass every assertion
        above, and would tell every page the deployment is faulty on every rotation."""
        scope = {"type": "http", "method": "GET", "path": "/v1/admin/events",
                 "query_string": b"",
                 "headers": [(k.lower().encode(), v.encode()) for k, v in ADMIN.items()]}
        sent = []
        seen = {"count": 0}
        disconnect = asyncio.Event()

        async def receive():
            if not seen.get("opened"):
                seen["opened"] = True
                return {"type": "http.request", "body": b"", "more_body": False}
            await disconnect.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            sent.append(message)
            body = message.get("body") or b""
            if body.startswith(b"event: status") or body.startswith(b": keepalive"):
                seen["count"] += 1
                if seen["count"] >= 2:
                    disconnect.set()

        asyncio.run(asyncio.wait_for(self.app(scope, receive, send), timeout=10.0))
        body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
        self.assertNotIn(b"server_error", body)
        self.assertIn(b"event: status", body, "the stream sent nothing at all, so this proves little")


class HowTheStreamsEndedIsCountedTest(unittest.TestCase):
    """A stream is one request that lasts minutes, so the request counter cannot tell a deployment
    whose tabs are open from one breaking a stream every three seconds -- both are one request on
    /v1/admin/events. Without a count of the reasons, a reconnect storm is invisible in every
    series the gateway publishes."""

    def setUp(self) -> None:
        self.original = gw._event_frame
        self.addCleanup(setattr, gw, "_event_frame", self.original)
        self.app = gw.make_v1_app(_FakeServer(), _cfg())

    @staticmethod
    def _scrape() -> str:
        return gwm.prometheus_text({"extraction": {"provider": "deterministic"},
                                    "embedding": {"provider": "deterministic"},
                                    "warnings": []})

    @staticmethod
    def _count(reason: str) -> int:
        return gwm.METRICS.stream_ends().get(reason, 0)

    def test_a_broken_stream_is_counted_as_one(self) -> None:
        before = self._count("server_error")
        drive(self.app)
        self.assertEqual(before + 1, self._count("server_error"))

    def test_an_unknown_reason_cannot_invent_a_label(self) -> None:
        """The reason is a metric label, and one taken from an open value is a cardinality bomb in
        a dict that lives as long as the process -- on a path any client can trigger."""
        before = self._count("other")
        gwm.METRICS.note_stream_end("something nobody wrote")
        self.assertEqual(before + 1, self._count("other"))
        self.assertNotIn("something nobody wrote", self._scrape())

    def test_every_reason_is_published_even_at_zero(self) -> None:
        """A counter that appears only once it fires cannot be alerted on until it has fired,
        which is exactly the moment the alert was worth having."""
        text = self._scrape()
        for reason in gwm.STREAM_END_REASONS:
            with self.subTest(reason=reason):
                self.assertIn('matrixark_gateway_event_stream_ended_total{reason="%s"}' % reason,
                              text)

    def test_it_is_in_the_scrape_at_all(self) -> None:
        """The positive control: every assertion above passes on a build that publishes nothing."""
        self.assertIn("matrixark_gateway_event_stream_ended_total", self._scrape())


if __name__ == "__main__":
    unittest.main()
