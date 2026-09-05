#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A frame delivers every field it builds.

``_shared_live_parts`` computes, once per tick for the whole deployment::

    traffic, imports, warnings, settings_waiting, config_changed_at

and ``_event_frame`` copied out four of those five. ``settings_waiting`` was computed and dropped.

So the status strip's *"N settings awaiting restart"* segment could never appear, on any deployment.
Save a setting that is read once at startup and the Setup page says so plainly -- *"restart the
gateway for it to take effect"* -- while the strip, which is on all seven panels and is what a
customer sees from anywhere, showed nothing. Found by driving a real gateway: after writing
``extraction.provider`` the gateway reported ``pending_restart: ['extraction.provider']`` and the
frame delivered to the page had no such field.

It was not free either. The count comes from ``pending_restart_keys()``, which exists in that shape
*because* it runs on every tick -- the work was being done and discarded every two seconds.

The behaviour is one line. The sweep at the bottom is the point: the shared half of a frame and the
delivered half are two lists that have to agree, and nothing said so, which is how a field came to
be built for nobody.
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_config as gwconfig  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402

# Shared-frame keys that are deliberately not delivered. Empty, and an entry here needs a reason:
# the whole point of the sweep is that a field built for nobody is invisible.
NOT_DELIVERED: dict = {}


def _frame(embedding=None, datanode=None):
    from test_matrixark_v1_gateway import _FakeServer, _cfg
    return asyncio.run(gw._event_frame(_FakeServer(), _cfg(), "k-acme", "acme", None,
                                       embedding, datanode))


def _fresh_shared():
    """A frame's shared half, rebuilt rather than served from the tick cache."""
    gw._LIVE_SHARED = None
    return gw._shared_live_parts()


class EveryBuiltFieldIsDeliveredTest(unittest.TestCase):

    def test_the_shared_half_builds_something(self) -> None:
        """A sweep over an empty dict would pass while delivering nothing."""
        self.assertGreaterEqual(len(_fresh_shared()), 4, sorted(_fresh_shared()))

    def test_nothing_is_built_for_nobody(self) -> None:
        shared = _fresh_shared()
        delivered = _frame()
        missing = sorted(k for k in shared if k not in delivered and k not in NOT_DELIVERED)
        self.assertEqual([], missing,
                         "these are computed on every tick and never reach a viewer: %r" % missing)

    def test_the_allowlist_is_honest(self) -> None:
        """An entry that no longer names a real field would let a dropped one hide behind it."""
        shared = _fresh_shared()
        stale = sorted(k for k in NOT_DELIVERED if k not in shared)
        self.assertEqual([], stale, stale)


class TheStripCanLearnAboutAPendingRestartTest(unittest.TestCase):

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        previous = os.environ.get("MATRIXARK_RUNTIME_CONFIG_FILE")

        def restore() -> None:
            if previous is None:
                os.environ.pop("MATRIXARK_RUNTIME_CONFIG_FILE", None)
            else:
                os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = previous

        self.addCleanup(restore)
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(tmp.name, "runtime.json")
        self.assertTrue(gwconfig.config_path().startswith(tmp.name),
                        "the config path did not move; this test would rewrite a live config")
        # What the gateway does at startup, and without it nothing is ever pending: the record of
        # what this process began with is built by apply_boot, and a module that was merely
        # imported has none. Skipping it here made this test read as "the frame says nothing"
        # when the frame was right and the test was not a real process.
        gwconfig.apply_boot()

    def test_the_field_arrives(self) -> None:
        self.assertIn("settings_waiting", _frame())

    def test_it_counts_a_setting_that_needs_a_restart(self) -> None:
        """The one the Setup page already explains in words. The strip is what a customer sees
        from the other six panels, and it had nothing to say."""
        before = _frame()["settings_waiting"] or 0
        gwconfig.update({"extraction.provider": "openai_compatible"}, actor="test")
        gw._LIVE_SHARED = None
        after = _frame()["settings_waiting"] or 0
        self.assertGreater(after, before,
                           "a setting read once at startup was written and the frame said nothing")

    def test_a_quiet_deployment_reports_zero_rather_than_nothing(self) -> None:
        """The strip hides the segment on a falsy value, so zero and absent look the same there --
        but a panel reading the frame directly can tell them apart, and should be able to."""
        gw._LIVE_SHARED = None
        value = _frame()["settings_waiting"]
        self.assertIsNotNone(value)
        self.assertIsInstance(value, int)


class TheStripRendersItTest(unittest.TestCase):
    """The consumer half: the segment exists and reads from this field."""

    def test_the_strip_has_a_segment_for_it(self) -> None:
        portal = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal")
        with open(os.path.join(portal, "setup_portal.html"), encoding="utf-8") as handle:
            page = handle.read()
        self.assertIn("liveWaiting", page)
        self.assertIn("frame.settings_waiting", page)
        self.assertIn("awaiting restart", page)


if __name__ == "__main__":
    unittest.main()
