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

    # The names that shadow extraction.provider. A value in either of these is the launcher's and
    # outranks anything the portal stores, so a test that leaves one set makes the next one's write
    # unobservable -- which is exactly what happened to this file.
    SHADOWING = ("MATRIXARK_UNDERSTANDING_PROVIDER", "MATRIXARK_EXTRACTION_PROVIDER")

    def setUp(self) -> None:
        # apply_boot() writes to os.environ and rebuilds _BOOT_EFFECTIVE, both of them process-wide
        # and neither scoped to a test. Restored here in full: without this the test inherits
        # whatever the previous one left behind AND leaves its own behind for the next, which is
        # how it passed alone and failed in the suite.
        environment = dict(os.environ)

        def restore_environment() -> None:
            os.environ.clear()
            os.environ.update(environment)

        self.addCleanup(restore_environment)

        boot = dict(gwconfig._BOOT_EFFECTIVE)

        def restore_boot() -> None:
            gwconfig._BOOT_EFFECTIVE.clear()
            gwconfig._BOOT_EFFECTIVE.update(boot)

        self.addCleanup(restore_boot)

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(tmp.name, "runtime.json")
        self.assertTrue(gwconfig.config_path().startswith(tmp.name),
                        "the config path did not move; this test would rewrite a live config")
        for name in self.SHADOWING:
            os.environ.pop(name, None)

        # A frame is built from a cache shared by the whole process and held for a tick, so without
        # this the FIRST reading below is whatever the previous test left there rather than
        # anything about this configuration. That is what failed in CI while passing alone: a
        # neighbour had left a frame reporting three settings waiting, so this test wrote one and
        # compared it against three.
        gw._reset_live_cache()
        self.addCleanup(gw._reset_live_cache)

        # What the gateway does at startup, and without it nothing is ever pending: the record of
        # what this process began with is built by apply_boot, and a module that was merely
        # imported has none. Skipping it here made this test read as "the frame says nothing"
        # when the frame was right and the test was not a real process.
        gwconfig.apply_boot()

    def _a_provider_other_than_the_one_we_booted_with(self) -> str:
        """Chosen against the boot record rather than written as a constant.

        A fixed value can be the value already in effect, and then the write changes nothing and the
        assertion fails while the code is fine.
        """
        setting = gwconfig.SETTINGS_BY_KEY["extraction.provider"]
        booted = gwconfig._BOOT_EFFECTIVE.get(setting.key)
        for choice in setting.choices or []:
            if choice != booted:
                return choice
        self.fail("extraction.provider offers no value other than %r, so nothing can be "
                  "changed here" % booted)

    def test_the_field_arrives(self) -> None:
        self.assertIn("settings_waiting", _frame())

    def test_it_counts_a_setting_that_needs_a_restart(self) -> None:
        """The one the Setup page already explains in words. The strip is what a customer sees
        from the other six panels, and it had nothing to say."""
        gwconfig.update(
            {"extraction.provider": self._a_provider_other_than_the_one_we_booted_with()},
            actor="test")
        gw._LIVE_SHARED = None

        # Against the configuration's own answer rather than against an earlier reading of the
        # frame. A before/after comparison asks what changed in a number that the whole process
        # shares, so it inherits whatever a neighbouring test left in the tick cache; this asks the
        # question that actually matters -- the strip reports what the settings say is waiting.
        waiting = gwconfig.pending_restart_keys()
        self.assertTrue(waiting, "the write did not leave anything pending, so this proves nothing")
        self.assertEqual(len(waiting), _frame()["settings_waiting"],
                         "the frame does not report what the settings say is waiting: %r"
                         % (waiting,))

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
