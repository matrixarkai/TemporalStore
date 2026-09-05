#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The per-identity live cache forgets the identities that stopped watching.

``_LIVE_EMBEDDING`` is keyed on the identity triple ``(key, tenant, account)`` and holds one
identity's encoding backlog so its other tabs reuse the read. Nothing removed an entry.
``_reset_live_cache`` exists but is for tests.

There were two such caches when this was written. The other held a frame's signature, and it is
gone: caching that answer per identity meant returning one computed from a DIFFERENT frame, which
is a correctness question rather than a memory one -- see
``test_matrixark_a_signature_describes_its_own_frame``.

So a worker's resident memory grew with the number of distinct keys that had **ever** opened a
status stream, not the number watching one. Measured at **2,433 bytes per identity**, kept for the
life of the process -- and every byte of it already too stale for its own reader to serve. On a
production portal an identity is a customer's API key, so this is a per-customer cost that is never
paid back.

The sweep uses each reader's own staleness test, called with the same arguments: an entry is dropped
exactly when it had stopped being an answer. That is what the floor below is for -- a sweep that
simply cleared both dicts would pass every leak test here while throwing away the sharing the caches
exist for, so this file asserts in both directions.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402


def _server_and_cfg():
    from test_matrixark_v1_gateway import _FakeServer, _cfg
    return _FakeServer(), _cfg()


def _identity(n: int) -> tuple:
    return ("k-%06d" % n, "t-%06d" % n, "a-%06d" % n)


class _LiveCacheTest(unittest.TestCase):

    def setUp(self) -> None:
        self.server, self.cfg = _server_and_cfg()
        gw._reset_live_cache()
        self.addCleanup(gw._reset_live_cache)

    def watch(self, count: int, offset: int = 0) -> None:
        """Serve `count` distinct identities the way a stream tick does."""
        async def run() -> None:
            for n in range(count):
                key, tenant, account = _identity(n + offset)
                embedding = await gw._embedding_for(self.server, self.cfg, key, tenant, account)
                await gw._event_frame(self.server, self.cfg, key, tenant, account, embedding, "ok")
        asyncio.run(run())

    def tick(self) -> None:
        """The next tick's rebuild, which is where the sweep runs."""
        gw._LIVE_SHARED = None
        self.watch(1, offset=900000)

    def age_everything(self) -> None:
        """Put every entry past its own reader's staleness test, without waiting for a clock."""
        now = time.time()
        for identity, (_at, value) in list(gw._LIVE_EMBEDDING.items()):
            gw._LIVE_EMBEDDING[identity] = (now - gw._embedding_refresh_interval(value) - 1.0,
                                            value)


class TheCachesForgetWhoLeftTest(_LiveCacheTest):

    def test_the_sweep_has_something_to_sweep(self) -> None:
        """A leak test over an empty cache would pass while nothing was ever cached."""
        self.watch(20)
        self.assertEqual(20, len(gw._LIVE_EMBEDDING))

    def test_an_embedding_does_not_outlive_its_own_interval(self) -> None:
        self.watch(20)
        self.age_everything()
        self.tick()
        left = [i for i in gw._LIVE_EMBEDDING if i[0].startswith("k-0000")]
        self.assertEqual([], left, "backlog reads too stale to be served were kept anyway")

    def test_a_worker_does_not_grow_with_the_identities_that_left(self) -> None:
        """The defect itself, at the scale that makes it one: a portal whose customers come and go.

        Two thousand identities watch, all of them leave, and one arrives. What is retained should
        describe who is watching now, not everyone who ever did.
        """
        self.watch(2000)
        self.assertEqual(2000, len(gw._LIVE_EMBEDDING))
        self.age_everything()
        self.tick()
        self.assertLess(len(gw._LIVE_EMBEDDING), 10,
                        "the worker still holds a backlog read for every key that ever watched")


class NothingStillWorthServingIsDroppedTest(_LiveCacheTest):
    """The floor. Clearing the dict on every tick would pass every test above and destroy the only
    reason the cache exists."""

    def test_a_fresh_embedding_survives_a_tick(self) -> None:
        self.watch(3)
        self.tick()
        kept = [i for i in gw._LIVE_EMBEDDING if i[0].startswith("k-0000")]
        self.assertEqual(3, len(kept),
                         "a backlog read the reader would still have served was dropped")

    def test_a_second_viewer_of_one_identity_still_reuses_the_read(self) -> None:
        """What the embedding cache is for. If a tick evicted it, every open tab would put its own
        walk of the record log on the backend."""
        watched = _identity(0)[0]
        reads = []
        original = gw._read_embedding

        async def counting(server, cfg, key, *args, **kwargs):
            # Counted per identity: a tick serves whoever is connected, and this asks about one of
            # them. Counting every read would attribute the tick's own viewer to this one.
            if key == watched:
                reads.append(key)
            return await original(server, cfg, key, *args, **kwargs)

        gw._read_embedding = counting
        self.addCleanup(setattr, gw, "_read_embedding", original)

        self.watch(1)
        self.assertEqual(1, len(reads), "the first viewer did not read, so this proves nothing")
        self.tick()
        self.watch(1)
        self.assertEqual(1, len(reads),
                         "a second viewer of the same identity read the backlog again")


class TheInflightMapDropsDeadLoopsTest(_LiveCacheTest):

    def test_a_task_from_a_closed_loop_is_dropped(self) -> None:
        """`_embedding_for` refuses to await a task from another loop, so such an entry is
        unreachable rather than merely stale -- nothing will ever pop it."""
        loop = asyncio.new_event_loop()

        async def nothing():
            return None

        task = loop.create_task(nothing())
        loop.run_until_complete(task)
        loop.close()
        gw._LIVE_EMBEDDING_INFLIGHT[_identity(1)] = (loop, task)

        self.tick()
        self.assertNotIn(_identity(1), gw._LIVE_EMBEDDING_INFLIGHT,
                         "an entry no caller can ever await was kept")

    def test_a_live_loops_entry_is_left_alone(self) -> None:
        loop = asyncio.new_event_loop()
        self.addCleanup(loop.close)

        async def nothing():
            return None

        task = loop.create_task(nothing())
        loop.run_until_complete(task)
        gw._LIVE_EMBEDDING_INFLIGHT[_identity(2)] = (loop, task)

        self.tick()
        self.assertIn(_identity(2), gw._LIVE_EMBEDDING_INFLIGHT,
                      "an entry belonging to a live loop was dropped")


if __name__ == "__main__":
    unittest.main()
