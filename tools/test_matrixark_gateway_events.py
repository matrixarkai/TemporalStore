#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The live event stream.

Three pages were each polling three endpoints on their own timers, so an import that finished
between two polls left a stale bar on screen until the next one. One stream carries the same state
and the server builds it once.

A stream needs its own driver: the ordinary harness runs a request to completion, and this one does
not complete — that is the point. These connect, read frames, and disconnect, which is also the
only way to prove the server notices a client going away rather than pushing into a dead socket
until the age limit.
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


def stream(app, *, path="/v1/admin/events", headers=None, frames=2, timeout=10.0):
    """Open a stream, read `frames` EMISSIONS, then disconnect.

    Emissions, not data frames. A tick whose content matches the last one sends a keepalive comment
    instead of republishing it, so on a quiet deployment most ticks carry no frame -- and a driver
    waiting for data frames would be waiting for something deliberately not sent.

    Returns (status, headers, data frames, emissions).
    """
    hdrs = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {"type": "http", "method": "GET", "path": path,
             "query_string": b"", "headers": hdrs}
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
        if message.get("type") == "http.response.body" and message.get("body"):
            body = message["body"]
            # Count what the server actually pushed. A keepalive is a tick that had nothing to say,
            # and it is still evidence the connection is alive and the loop is running.
            if body.startswith(b"event: status") or body.startswith(b": keepalive"):
                seen["count"] += 1
                if seen["count"] >= frames:
                    disconnect.set()

    async def run():
        await asyncio.wait_for(app(scope, receive, send), timeout=timeout)

    asyncio.run(run())
    start = next((m for m in sent if m["type"] == "http.response.start"), None)
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    payloads = []
    for block in body.decode("utf-8").split("\n\n"):
        for line in block.splitlines():
            if line.startswith("data: "):
                payloads.append(json.loads(line[len("data: "):]))
    headers_map = {}
    if start:
        headers_map = {k.decode().lower(): v.decode() for k, v in start["headers"]}
    return (start or {}).get("status"), headers_map, payloads, seen["count"]


class EventStreamTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _FakeServer()
        self.app = gw.make_v1_app(self.server, _cfg())
        self._tick = gw.EVENT_TICK_S
        gw.EVENT_TICK_S = 0.05  # the cadence is not what is under test
        # The live caches are process-wide and outlive a connection, which is the point of them --
        # a new tab reuses a recent read rather than asking again. A test that counts backend work
        # therefore has to say where it is starting from, or it measures the previous test.
        gw._reset_live_cache()
        self.addCleanup(gw._reset_live_cache)

    def tearDown(self) -> None:
        gw.EVENT_TICK_S = self._tick

    def test_it_needs_a_key(self) -> None:
        status, _headers, _frames, _ticks = stream(self.app, frames=1)
        self.assertEqual(401, status)

    def test_it_is_served_as_an_event_stream(self) -> None:
        status, headers, _frames, _ticks = stream(self.app, headers=ADMIN, frames=1)
        self.assertEqual(200, status)
        self.assertTrue(headers["content-type"].startswith("text/event-stream"))
        self.assertIn("no-cache", headers["cache-control"])
        # Nginx buffers a proxied response by default, which turns a live stream into one delivery
        # when it finishes. This header is the difference between live and not.
        self.assertEqual("no", headers["x-accel-buffering"])

    def test_frames_carry_the_live_state(self) -> None:
        _st, _h, frames, _ticks = stream(self.app, headers=ADMIN, frames=2)
        # One frame is enough to show the shape. A quiet deployment sends the first and then says
        # nothing, which is the point of the change rather than a gap in the test.
        self.assertGreaterEqual(len(frames), 1)
        for frame in frames:
            self.assertIn("ts", frame)
            self.assertIn("traffic", frame)
            self.assertIn("imports", frame)
            self.assertIn("warnings", frame)

    def test_it_keeps_sending_rather_than_answering_once(self) -> None:
        # The whole difference from a poll: the connection stays open and the server keeps
        # pushing. What it pushes on a quiet tick is a keepalive rather than a repeat of the last
        # frame, so this counts emissions -- the property is that they keep coming.
        _st, _h, frames, ticks = stream(self.app, headers=ADMIN, frames=3)
        self.assertGreaterEqual(ticks, 3)
        self.assertGreaterEqual(len(frames), 1)
        if len(frames) >= 2:
            self.assertLessEqual(frames[0]["ts"], frames[-1]["ts"])

    def test_it_stops_when_the_client_goes_away(self) -> None:
        # Proved by the driver returning at all: it disconnects after N frames, and a server that
        # ignored the disconnect would run to its ten-minute age limit and time this out.
        _st, _h, _frames, ticks = stream(self.app, headers=ADMIN, frames=2, timeout=5.0)
        self.assertGreaterEqual(ticks, 2)

    def test_the_embedding_count_rides_at_its_own_cadence(self) -> None:
        # It walks the record log, so it must not be recomputed on every tick.
        calls = []
        original = self.server.call_tool

        def counted(name, args):
            calls.append(name)
            return original(name, args)

        self.server.call_tool = counted  # type: ignore[assignment]
        stream(self.app, headers=ADMIN, frames=4)
        embedding_calls = [name for name in calls if name == "matrixark_embedding_status"]
        self.assertEqual(1, len(embedding_calls),
                         "the record-log walk ran %d times across four frames"
                         % len(embedding_calls))

    def test_a_backend_that_cannot_answer_leaves_the_field_empty_not_wrong(self) -> None:
        # "Nothing pending" and "I could not find out" are different answers; a stream that
        # invented the first would say retrieval was ready when nobody knows.
        def explode(_name, _args):
            raise RuntimeError("backend down")

        self.server.call_tool = explode  # type: ignore[assignment]
        _st, _h, frames, _ticks = stream(self.app, headers=ADMIN, frames=2)
        self.assertIsNone(frames[0]["embedding"])

    def test_a_stream_is_not_counted_as_latency(self) -> None:
        # A subscription that lasts minutes is one request, not a slow one. Left in the histogram
        # it poisons every quantile drawn across routes: a p99 of ten minutes, describing nothing
        # anyone waited for.
        self.assertIn("/v1/admin/events", gwm.STREAMING_ROUTES)
        metrics = gwm.GatewayMetrics()
        metrics.record("/v1/admin/events", "GET", 200, 600.0, 0, 4096)
        metrics.record("/v1/retrieve", "POST", 200, 0.02)
        snapshot = metrics.snapshot()
        self.assertEqual(0.0, snapshot["routes"]["/v1/admin/events"]["max_ms"])
        # The request and its bytes are still counted -- only the clock is dropped.
        self.assertEqual(1, snapshot["routes"]["/v1/admin/events"]["requests"])
        self.assertEqual(4096, snapshot["routes"]["/v1/admin/events"]["response_bytes"])


class AnUnchangedFrameIsNotRepublishedTest(unittest.TestCase):
    """A quiet deployment published the same 325 bytes every two seconds, per viewer, for ever.

    Measured before the change: five of five consecutive frames were byte-identical once the
    timestamp was removed. Now such a tick sends an SSE comment instead -- which the strip's own
    parser already drops, because it ignores every line that does not begin with `data:`.
    """

    def setUp(self) -> None:
        self.server = _FakeServer()
        self.app = gw.make_v1_app(self.server, _cfg())
        self._tick = gw.EVENT_TICK_S
        gw.EVENT_TICK_S = 0.02
        gw._reset_live_cache()
        self.addCleanup(gw._reset_live_cache)

    def tearDown(self) -> None:
        gw.EVENT_TICK_S = self._tick

    def test_a_quiet_deployment_sends_one_frame_and_then_keeps_quiet(self) -> None:
        _st, _h, frames, ticks = stream(self.app, headers=ADMIN, frames=8)
        self.assertGreaterEqual(ticks, 8)
        self.assertEqual(1, len(frames),
                         "%d frames over %d ticks with nothing changing: the skip is not firing "
                         "and every tick republishes the same answer" % (len(frames), ticks))

    def test_the_first_frame_is_always_sent(self) -> None:
        """A viewer that has just arrived has nothing on screen, however long the lull has been.

        The traffic counter has to be frozen for this to mean anything. Watching the stream is
        itself a request, so without pinning it the second viewer sees a different frame simply
        because the first viewer existed -- and the test passes whether or not the comparison is
        per connection. It did exactly that until a mutation that shared the comparison across
        connections failed to fail.
        """
        import matrixark_gateway_metrics as gwm

        frozen = dict(gwm.METRICS.snapshot())
        gwm.METRICS.snapshot = lambda: dict(frozen)   # type: ignore[assignment]
        self.addCleanup(setattr, gwm.METRICS, "snapshot", gwm.METRICS.__class__.snapshot.__get__(
            gwm.METRICS, gwm.METRICS.__class__))
        gw._reset_live_cache()

        first = stream(self.app, headers=ADMIN, frames=6)
        self.assertGreaterEqual(len(first[2]), 1, "the first viewer got no frame either")
        _st, _h, frames, _ticks = stream(self.app, headers=ADMIN, frames=3)
        self.assertGreaterEqual(len(frames), 1,
                                "a new viewer got no frame at all: the comparison is being shared "
                                "across connections, so it inherited somebody else's state")

    def test_a_change_is_published_immediately(self) -> None:
        """Skipping must not mean missing: the next different frame goes out on its own tick."""
        import matrixark_gateway_metrics as gwm

        real = gwm.METRICS.snapshot
        state = {"n": 0}

        def moving():
            state["n"] += 1
            out = dict(real())
            out["total_requests"] = state["n"]
            return out

        gwm.METRICS.snapshot = moving          # type: ignore[assignment]
        self.addCleanup(setattr, gwm.METRICS, "snapshot", real)
        gw._reset_live_cache()

        _st, _h, frames, ticks = stream(self.app, headers=ADMIN, frames=6)
        self.assertGreaterEqual(len(frames), 2,
                                "the traffic count changed every tick and only %d frames were "
                                "sent over %d ticks" % (len(frames), ticks))

    def test_the_keepalive_is_a_line_the_browser_ignores(self) -> None:
        """The strip drops every line that is not `data:`. A keepalive must be exactly that."""
        sent: list = []
        real_stream = gw._event_stream

        hdrs = [(k.lower().encode(), v.encode()) for k, v in ADMIN.items()]
        scope = {"type": "http", "method": "GET", "path": "/v1/admin/events",
                 "query_string": b"", "headers": hdrs}
        seen = {"n": 0, "opened": False}
        disconnect = asyncio.Event()

        async def receive():
            if not seen["opened"]:
                seen["opened"] = True
                return {"type": "http.request", "body": b"", "more_body": False}
            await disconnect.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            body = message.get("body") or b""
            if body:
                sent.append(body)
                seen["n"] += 1
                if seen["n"] >= 6:
                    disconnect.set()

        asyncio.run(asyncio.wait_for(self.app(scope, receive, send), timeout=20))
        keepalives = [b for b in sent if b.startswith(b":")]
        self.assertTrue(keepalives, "no keepalive was sent at all")
        for body in keepalives:
            text = body.decode("utf-8")
            self.assertTrue(text.startswith(": "), repr(text))
            self.assertTrue(text.endswith("\n\n"), repr(text))
            for line in text.splitlines():
                self.assertFalse(line.startswith("data:"),
                                 "a keepalive carrying a data line would be rendered as a frame")


if __name__ == "__main__":
    unittest.main()
