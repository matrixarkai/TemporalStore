#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A launch artifact says which local settings it does not carry.

The plan composes topology and credentials -- shape, storage tier, key NAMES. It carries none of
the storage tuning a customer set on this deployment's Setup page, and that is the right default:
tuning is sized for the box it was measured on, and copying a cache figure onto different hardware
is worse than starting from the engine's own numbers.

It is a surprise if nobody says it. The customer tuned a store, asked this page for a box, and got
one that ignores every value they chose. So the plan now names them.

Disclosure only. The environment the plan produces must be byte-identical whether or not anything
is configured locally -- a note that changed the deployment would be a much worse bug than the
silence it replaces.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_deployment_plan as dp  # noqa: E402

VALID = {"shape": "onebox", "storage": "ebs", "root": "/srv/temporalstore"}


def _disclosures(plan) -> list:
    return [note for note in plan["notes"] if "NOT carried" in note]


class ALaunchSaysWhatItDoesNotCarryTest(unittest.TestCase):

    def test_a_configured_setting_is_named(self) -> None:
        plan = dp.plan(configured_engine_settings=["TS_PAGE_INDEX_CACHE_BYTES"], **VALID)
        notes = _disclosures(plan)
        self.assertEqual(1, len(notes), plan["notes"])
        self.assertIn("TS_PAGE_INDEX_CACHE_BYTES", notes[0])
        self.assertIn("engine default", notes[0],
                      "the note must say what the new node uses instead")

    def test_nothing_configured_says_nothing(self) -> None:
        """A page that warns unconditionally trains people to ignore it."""
        self.assertEqual([], _disclosures(dp.plan(**VALID)))

    def test_the_disclosure_does_not_change_the_deployment(self) -> None:
        """The invariant that matters: this reports, it does not configure."""
        plain = dp.plan(**VALID)
        disclosed = dp.plan(
            configured_engine_settings=["TS_PAGE_INDEX_CACHE_BYTES", "TS_VECTOR_SCALED"],
            **VALID)
        self.assertEqual(plain["env"], disclosed["env"],
                         "the environment changed when a setting was disclosed; a note must not "
                         "configure anything")
        self.assertEqual(plain["ok"], disclosed["ok"])
        self.assertEqual(plain["blocking"], disclosed["blocking"])

    def test_names_are_deduplicated_and_ordered(self) -> None:
        plan = dp.plan(
            configured_engine_settings=["TS_VECTOR_SCALED", "TS_PAGE_INDEX_CACHE_BYTES",
                                        "TS_VECTOR_SCALED"],
            **VALID)
        notes = _disclosures(plan)
        self.assertEqual(2, len(notes), "a repeated name produced a repeated note")
        self.assertIn("TS_PAGE_INDEX_CACHE_BYTES", notes[0], "notes are not in a stable order")


class TheRouteSuppliesNamesOnlyTest(unittest.TestCase):

    def test_only_names_ever_leave_the_gateway(self) -> None:
        """The plan and its artifact travel. A value must not ride along with the name."""
        import matrixark_v1_gateway as gateway

        names = gateway._configured_engine_settings()
        self.assertIsInstance(names, list)
        for name in names:
            self.assertIsInstance(name, str)
            self.assertTrue(name.startswith("TS_"),
                            "%r is not an engine variable name" % name)
            self.assertNotIn("=", name, "%r looks like it carries a value" % name)

    def test_it_survives_a_config_it_cannot_read(self) -> None:
        """A plan is still worth producing when the settings cannot be loaded."""
        import matrixark_v1_gateway as gateway

        saved = os.environ.get("MATRIXARK_RUNTIME_CONFIG_FILE")
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = "/nonexistent/dir/does-not-exist.json"
        try:
            self.assertIsInstance(gateway._configured_engine_settings(), list)
        finally:
            if saved is None:
                os.environ.pop("MATRIXARK_RUNTIME_CONFIG_FILE", None)
            else:
                os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = saved


if __name__ == "__main__":
    unittest.main()
