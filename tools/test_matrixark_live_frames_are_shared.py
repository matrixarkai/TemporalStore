#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""One live frame per tick for the deployment, not one per viewer.

The stream built every part of every frame inside each connection's own loop, so the cost was
exactly linear in the number of open browser tabs. Measured against the real stream:

    viewers  frames  embedding  config  imports  metrics
          1       5          1       5        5        5
         16      80         16      80       80       80

Sixteen tabs meant sixteen backend reads and sixteen redacted-configuration builds for state that
is identical for all of them. Within a frame, building that configuration was 68.6% of the cost
(52.6 of 76.6 us) and read about thirty environment variables, so the stream could take one integer
out of it -- the number of warnings.

What may be shared is not a tuning question, and that is what most of this file is about:

* traffic, imports and the warning count are deployment-wide aggregates, the same answer for every
  viewer, so they are built once per tick;
* the embedding backlog is read with the caller's identity applied, so it is cached per identity
  and never across one. Sharing that would be one tenant reading another's backlog, and it would
  look exactly like a working optimisation.
"""
from __future__ import annotations

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import _FakeServer, _cfg  # noqa: E402


async def _viewer(app, headers, frames=3):
    hdrs = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    scope = {"type": "http", "method": "GET", "path": "/v1/admin/events",
             "query_string": b"", "headers": hdrs}
    seen = {"count": 0, "opened": False}
    disconnect = asyncio.Event()

    async def receive():
        if not seen["opened"]:
            seen["opened"] = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        body = message.get("body") or b""
        # Emissions, not data frames: a tick with nothing new to say sends a keepalive, so a driver
        # counting frames would wait for something deliberately not sent.
        if body.startswith(b"event: status") or body.startswith(b": keepalive"):
            seen["count"] += 1
            if seen["count"] >= frames:
                disconnect.set()

    await asyncio.wait_for(app(scope, receive, send), timeout=30)
    return seen["count"]


class SharedAcrossViewersTest(unittest.TestCase):
    """What every viewer sees the same answer for is built once."""

    def setUp(self) -> None:
        self.server = _FakeServer()
        self.app = gw.make_v1_app(self.server, _cfg())
        self._tick = gw.EVENT_TICK_S
        gw.EVENT_TICK_S = 0.02
        # The caches outlive a connection on purpose, so a test that counts work must say where it
        # is starting from.
        gw._reset_live_cache()
        self.addCleanup(gw._reset_live_cache)
        self.calls: list = []
        original = self.server.call_tool

        def counted(name, args):
            self.calls.append((name, args.get("scope", {}).get("tenant_id")))
            return original(name, args)

        self.server.call_tool = counted  # type: ignore[assignment]

    def tearDown(self) -> None:
        gw.EVENT_TICK_S = self._tick

    def _run(self, *header_sets, frames=3):
        async def go():
            return await asyncio.gather(
                *[_viewer(self.app, h, frames) for h in header_sets])
        return asyncio.run(go())

    def _backend_reads(self, tenant=None):
        return [c for c in self.calls
                if c[0] == "matrixark_embedding_status" and (tenant is None or c[1] == tenant)]

    def test_one_viewer_reads_the_backend_once(self) -> None:
        self._run({"Authorization": "Bearer k-acme"})
        self.assertEqual(1, len(self._backend_reads()),
                         "a single viewer should walk the record log once, not once per frame")

    def test_eight_viewers_on_one_key_still_read_the_backend_once(self) -> None:
        """The whole point. Before this they arrived together and each started its own read."""
        headers = [{"Authorization": "Bearer k-acme"}] * 8
        self._run(*headers)
        reads = self._backend_reads()
        self.assertEqual(1, len(reads),
                         "eight tabs on one key caused %d backend reads for one answer"
                         % len(reads))

    def test_the_shared_parts_are_built_once_per_tick(self) -> None:
        built = {"n": 0}
        real = gw._model_config_snapshot

        def counted():
            built["n"] += 1
            return real()

        gw._model_config_snapshot = counted
        self.addCleanup(setattr, gw, "_model_config_snapshot", real)
        headers = [{"Authorization": "Bearer k-acme"}] * 8
        self._run(*headers, frames=3)
        self.assertLessEqual(built["n"], 6,
                             "the configuration was rebuilt %d times for 8 viewers over 3 frames, "
                             "so it is still per viewer" % built["n"])

    def test_a_frame_still_carries_every_field(self) -> None:
        """Sharing must not quietly drop a field: the strip renders absent and zero differently."""
        gw._reset_live_cache()

        async def go():
            frame = await gw._event_frame(self.server, _cfg(), "k-acme", "acme", None,
                                          {"total": 3})
            return frame

        frame = asyncio.run(go())
        for field in ("ts", "traffic", "imports", "warnings", "embedding"):
            self.assertIn(field, frame)
        for field in ("total_requests", "total_errors", "in_flight", "routes"):
            self.assertIn(field, frame["traffic"])

    def test_each_frame_carries_its_own_timestamp(self) -> None:
        """Shared state must not mean a shared clock: frames would stop being distinguishable."""
        async def go():
            first = await gw._event_frame(self.server, _cfg(), "k-acme", "acme", None, None)
            await asyncio.sleep(0.01)
            second = await gw._event_frame(self.server, _cfg(), "k-acme", "acme", None, None)
            return first, second

        first, second = asyncio.run(go())
        self.assertNotEqual(first["ts"], second["ts"],
                            "two frames share a timestamp, so the shared part froze the clock")


class NotSharedAcrossIdentitiesTest(unittest.TestCase):
    """The boundary. Everything above is worthless if it leaks one tenant's state to another."""

    def setUp(self) -> None:
        self.server = _FakeServer()
        self.app = gw.make_v1_app(self.server, _cfg())
        self._tick = gw.EVENT_TICK_S
        gw.EVENT_TICK_S = 0.02
        gw._reset_live_cache()
        self.addCleanup(gw._reset_live_cache)

    def tearDown(self) -> None:
        gw.EVENT_TICK_S = self._tick

    def test_a_second_identity_is_read_separately(self) -> None:
        seen: list = []
        real = gw._read_embedding

        async def counted(server, cfg, key, tenant, account):
            seen.append((key, tenant, account))
            return await real(server, cfg, key, tenant, account)

        gw._read_embedding = counted
        self.addCleanup(setattr, gw, "_read_embedding", real)

        async def go():
            await gw._embedding_for(self.server, _cfg(), "k-acme", "acme", None)
            await gw._embedding_for(self.server, _cfg(), "k-acme", "acme", None)
            await gw._embedding_for(self.server, _cfg(), "k-globex", "globex", None)

        asyncio.run(go())
        self.assertEqual(2, len(seen),
                         "expected one read per identity and got %r -- either the second identity "
                         "was served the first one's answer, or nothing is being shared" % (seen,))
        self.assertIn(("k-globex", "globex", None), seen)

    def test_the_cache_key_is_the_whole_identity(self) -> None:
        """Keying on the tenant alone would serve one account another's backlog."""
        seen: list = []
        real = gw._read_embedding

        async def counted(server, cfg, key, tenant, account):
            seen.append((key, tenant, account))
            return await real(server, cfg, key, tenant, account)

        gw._read_embedding = counted
        self.addCleanup(setattr, gw, "_read_embedding", real)

        async def go():
            await gw._embedding_for(self.server, _cfg(), "k-acme", "acme", "account-a")
            await gw._embedding_for(self.server, _cfg(), "k-acme", "acme", "account-b")

        asyncio.run(go())
        self.assertEqual(2, len(seen),
                         "two accounts under one tenant shared a cached backlog")


class ReconnectsAreSpreadTest(unittest.TestCase):
    """A fixed age limit makes every stream opened together end together, for ever."""

    def test_the_age_limit_is_not_the_same_for_every_connection(self) -> None:
        source = gw._event_stream.__doc__ or ""
        import inspect
        body = inspect.getsource(gw._event_stream)
        self.assertIn("max_age", body,
                      "the stream uses the fixed ceiling, so every stream opened together also "
                      "ends together and the reconnects arrive as a herd")
        self.assertIn("random", body)

    def test_the_spread_stays_within_a_sensible_band(self) -> None:
        """Spread, not unpredictability: a lifetime nobody can reason about is its own problem."""
        import inspect
        body = inspect.getsource(gw._event_stream)
        self.assertIn("EVENT_STREAM_MAX_S * (0.9", body,
                      "the jitter band is not the documented one")


if __name__ == "__main__":
    unittest.main()
