#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A frame's signature describes THAT frame.

The status stream sends a frame only when what it says differs from what this connection was last
sent. That comparison is made on a signature: the frame serialised with the timestamp left out,
because a timestamp changes every tick by definition and a comparison including it would never find
two frames equal.

The signature was cached per identity for one tick, so that two viewers on the same key did not each
serialise the same answer. **On a cache hit it returned the signature of a frame built earlier and
ignored the frame it was handed.** Two plainly different frames therefore got the same signature,
and a viewer could be told nothing had changed while holding a frame that had -- waiting a further
tick for state it already possessed.

That is the shape below: two viewers on one key, ticking at an offset, and a change that lands
between them. The second is the one that misses it.

The cache saved 54 microseconds a call, measured on a frame from a busy deployment (4,301 bytes,
fourteen routes and a full failure ring). That is 0.27 ms of CPU per second at ten viewers on one
identity and 2.7 ms at a hundred. A status stream arriving a tick late is the thing it exists not to
be, so the cache is gone and the signature is derived from the frame every time.

Removing it also takes a process-wide dict with it -- one that had to be swept, and is now not
there to sweep. The eviction suite next door covers the one cache that remains.
"""
from __future__ import annotations

import inspect
import json
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402


def frame(warnings: int, at: float = None) -> dict:
    """A frame's content as the emit loop would have built it at this instant."""
    return {"ts": time.time() if at is None else at,
            "warnings": warnings, "traffic": {}, "imports": {}, "settings_waiting": 0}


class ASignatureIsAboutItsOwnFrameTest(unittest.TestCase):

    def test_two_different_frames_do_not_agree(self) -> None:
        """The defect at its smallest. These differ in every field but the clock."""
        first = gw._frame_signature(frame(0))
        second = gw._frame_signature(frame(9))
        self.assertNotEqual(first, second)

    def test_the_signature_says_what_the_frame_says(self) -> None:
        """Not merely different -- the right one. Two frames could differ consistently and still
        both be described wrongly."""
        signature = json.loads(gw._frame_signature(frame(7)).decode("utf-8"))
        self.assertEqual(7, signature["warnings"])

    def test_the_clock_is_still_left_out(self) -> None:
        """The reason the signature exists rather than comparing frames directly. Including the
        timestamp would make every tick look like a change, and the check would never once skip a
        send."""
        early, late = frame(3, at=1000.0), frame(3, at=9999.0)
        self.assertNotEqual(early["ts"], late["ts"])
        self.assertEqual(gw._frame_signature(early), gw._frame_signature(late))

    def test_it_does_not_ask_who_wants_to_know(self) -> None:
        """There is no answer here that depends on the viewer, and taking an identity is what let
        one viewer be handed another's answer."""
        parameters = list(inspect.signature(gw._frame_signature).parameters)
        self.assertEqual(["frame"], parameters,
                         "the signature takes something other than the frame: %r" % parameters)


class TheViewerThatUsedToMissAChangeTest(unittest.TestCase):
    """The emit loop's decision, run: it sends when the signature differs from the one it last
    sent, and keeps quiet otherwise."""

    class Viewer:
        def __init__(self) -> None:
            self.last = None
            self.sent = []

        def tick(self, content) -> str:
            signature = gw._frame_signature(content)
            if signature == self.last:
                return "keepalive"
            self.last = signature
            self.sent.append(content["warnings"])
            return "sent"

    def setUp(self) -> None:
        gw._reset_live_cache()
        self.addCleanup(gw._reset_live_cache)

    def test_a_second_viewer_on_one_key_sees_a_change_that_lands_between_ticks(self) -> None:
        """Two tabs on the same key, ticking half a tick apart. A warning appears just after the
        first has ticked, so only the second is holding a frame that shows it.

        This is what a cached signature got wrong: the second was handed the answer computed for
        the first, before the change, and stayed quiet.
        """
        first, second = self.Viewer(), self.Viewer()
        self.assertEqual("sent", first.tick(frame(2)))
        self.assertEqual("sent", second.tick(frame(2)))

        # Nothing has changed for the first viewer, so it says nothing. That entry is what the
        # second used to be given.
        self.assertEqual("keepalive", first.tick(frame(2)))

        self.assertEqual("sent", second.tick(frame(3)),
                         "a viewer holding a changed frame was told nothing had changed")
        self.assertIn(3, second.sent)

    def test_a_viewer_still_keeps_quiet_when_nothing_changed(self) -> None:
        """The floor. A signature that never matched would make every tick a send, which is the
        cost the comparison exists to avoid -- and every test above would still pass."""
        viewer = self.Viewer()
        self.assertEqual("sent", viewer.tick(frame(2)))
        self.assertEqual("keepalive", viewer.tick(frame(2)))
        self.assertEqual("keepalive", viewer.tick(frame(2)))
        self.assertEqual([2], viewer.sent)

    def test_the_two_viewers_are_independent(self) -> None:
        """A viewer that has just arrived has nothing on screen, so its first frame must be sent
        however long the deployment has been quiet."""
        first = self.Viewer()
        first.tick(frame(2))
        first.tick(frame(2))
        arriving = self.Viewer()
        self.assertEqual("sent", arriving.tick(frame(2)))


class NothingCachesTheSignatureAnyMoreTest(unittest.TestCase):

    def test_the_module_has_no_signature_cache(self) -> None:
        """A dict left behind would be swept for nothing, and would invite the same mistake back."""
        self.assertFalse(hasattr(gw, "_LIVE_SIGNATURE"),
                         "the per-identity signature cache is back")

    def test_the_cache_that_remains_is_still_swept(self) -> None:
        """Removing one cache must not take the eviction of the other with it."""
        self.assertTrue(hasattr(gw, "_forget_idle_identities"))
        body = inspect.getsource(gw._forget_idle_identities)
        self.assertIn("_LIVE_EMBEDDING", body)
        self.assertNotIn("_LIVE_SIGNATURE", body)


if __name__ == "__main__":
    unittest.main()
