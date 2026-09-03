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
    """Open a stream, read `frames` data frames, then disconnect. Returns (status, [frames])."""
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
            if b"data:" in message["body"]:
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
    return (start or {}).get("status"), headers_map, payloads


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
        status, _headers, _frames = stream(self.app, frames=1)
        self.assertEqual(401, status)

    def test_it_is_served_as_an_event_stream(self) -> None:
        status, headers, _frames = stream(self.app, headers=ADMIN, frames=1)
        self.assertEqual(200, status)
        self.assertTrue(headers["content-type"].startswith("text/event-stream"))
        self.assertIn("no-cache", headers["cache-control"])
        # Nginx buffers a proxied response by default, which turns a live stream into one delivery
        # when it finishes. This header is the difference between live and not.
        self.assertEqual("no", headers["x-accel-buffering"])

    def test_frames_carry_the_live_state(self) -> None:
        _st, _h, frames = stream(self.app, headers=ADMIN, frames=2)
        self.assertGreaterEqual(len(frames), 2)
        for frame in frames:
            self.assertIn("ts", frame)
            self.assertIn("traffic", frame)
            self.assertIn("imports", frame)
            self.assertIn("warnings", frame)

    def test_it_keeps_sending_rather_than_answering_once(self) -> None:
        # The whole difference from a poll: the connection stays open and frames keep arriving.
        _st, _h, frames = stream(self.app, headers=ADMIN, frames=3)
        self.assertGreaterEqual(len(frames), 3)
        self.assertLessEqual(frames[0]["ts"], frames[-1]["ts"])

    def test_it_stops_when_the_client_goes_away(self) -> None:
        # Proved by the driver returning at all: it disconnects after N frames, and a server that
        # ignored the disconnect would run to its ten-minute age limit and time this out.
        _st, _h, frames = stream(self.app, headers=ADMIN, frames=2, timeout=5.0)
        self.assertGreaterEqual(len(frames), 2)

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
        _st, _h, frames = stream(self.app, headers=ADMIN, frames=2)
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


if __name__ == "__main__":
    unittest.main()
