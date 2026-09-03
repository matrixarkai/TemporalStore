#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A setting the engine clamps must say so on the page.

Four of the storage tuning knobs are read as `parse_*(...).max(1024)`. A customer who sets 512 gets
1024, and nothing on the page or in the response says the value was changed -- which is the same
shape as every other defect found in this area: something accepted that silently resolves to
something else.

The floors are derived from the accessor rather than written down here. A list of "these four clamp"
would be correct today and wrong the moment a fifth gains a floor or one loses it, and the failure
mode of that staleness is silence -- the same silence the check exists to remove.
"""
from __future__ import annotations

import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_config as cfgmod  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TUNING = os.path.join(ROOT, "crates", "temporalstore-rust", "src", "storage_config.rs")

# `field: parse_usize(get(TS_NAME), defaults.field,)  .max(1024),`  -- the clamp may sit on the
# next line, so the pattern spans whitespace rather than assuming a layout.
CLAMPED = re.compile(
    r"get\((TS_[A-Z0-9_]+)\)[^;]*?\)\s*\.\s*(?:max|min|clamp)\(\s*([0-9_]+)",
    re.S)


def declared_floors() -> dict:
    """env name -> the floor its accessor applies."""
    if not os.path.exists(TUNING):
        return {}
    with open(TUNING, encoding="utf-8", errors="replace") as handle:
        text = handle.read()
    start = text.find("pub fn from_getter")
    if start < 0:
        return {}
    body = text[start:text.find("\n    }\n", start)]
    floors = {}
    for match in CLAMPED.finditer(body):
        floors[match.group(1)] = int(match.group(2).replace("_", ""))
    return floors


def _env_for_constant(identifier: str) -> str:
    """The variable a knob constant holds, which is not always its identifier."""
    with open(TUNING, encoding="utf-8", errors="replace") as handle:
        text = handle.read()
    match = re.search(r'pub const %s\s*:\s*&(?:\'static\s+)?str\s*=\s*"(TS_[A-Z0-9_]+)"'
                      % re.escape(identifier), text)
    return match.group(1) if match else identifier


class AClampedSettingSaysSoTest(unittest.TestCase):

    def setUp(self) -> None:
        self.floors = {_env_for_constant(name): floor
                       for name, floor in declared_floors().items()}
        self.by_env = {s.env: s for s in cfgmod.SETTINGS if s.env}

    def test_the_scan_found_clamps(self) -> None:
        """Without a floor to find, every assertion below passes by checking nothing."""
        self.assertGreaterEqual(
            len(self.floors), 3,
            "found %d clamped knobs in the tuning accessor; the pattern has stopped matching and "
            "the checks below would pass silently" % len(self.floors))

    def test_every_clamped_setting_mentions_its_floor(self) -> None:
        silent = []
        for env, floor in sorted(self.floors.items()):
            setting = self.by_env.get(env)
            if setting is None:
                continue          # not offered on the page; nothing to mislead a customer with
            help_text = (setting.help or "").lower()
            readable = "1 kib" if floor == 1024 else str(floor)
            if "raised" not in help_text or readable not in help_text:
                silent.append("%s (floor %d)" % (env, floor))
        self.assertEqual(
            [], silent,
            "the engine raises these to a floor and the page does not say so, so a customer who "
            "sets a smaller value is told nothing and gets a different one: %s" % ", ".join(silent))

    def test_an_unclamped_setting_does_not_claim_a_floor(self) -> None:
        """The opposite error: text promising a clamp that the engine does not apply."""
        wrong = []
        for env, setting in sorted(self.by_env.items()):
            if env in self.floors or not env.startswith("TS_"):
                continue
            if "raised to it" in (setting.help or "").lower():
                wrong.append(env)
        self.assertEqual(
            [], wrong,
            "these promise the engine raises small values and it does not: %s" % ", ".join(wrong))


class TheMinimumsMapMatchesTheEngineTest(unittest.TestCase):
    """`_ENGINE_MINIMUMS` exists so a write can report a raised value at the moment it happens.

    It is a transcription of what the accessor does, and a transcription drifts. Comparing it to
    the floors derived from the Rust source is what stops that -- without this the map could keep
    reporting 1024 long after the engine moved, and the report would be confidently wrong, which is
    worse than the silence it replaced.
    """

    def setUp(self) -> None:
        self.derived = {_env_for_constant(name): floor
                        for name, floor in declared_floors().items()}

    def test_the_map_and_the_engine_agree(self) -> None:
        mapped = dict(getattr(cfgmod, "_ENGINE_MINIMUMS", {}))
        self.assertEqual(
            self.derived, mapped,
            "the floors the portal reports and the floors the engine applies have diverged. "
            "Engine: %r. Portal: %r." % (self.derived, mapped))

    def test_the_map_is_not_empty(self) -> None:
        self.assertTrue(getattr(cfgmod, "_ENGINE_MINIMUMS", {}),
                        "no floors are recorded, so no write can report a raised value")


class AWriteReportsARaisedValueTest(unittest.TestCase):

    def setUp(self) -> None:
        import shutil
        import tempfile

        directory = tempfile.mkdtemp(prefix="matrixark-clamp-test-")
        path = os.path.join(directory, "runtime_config.json")
        # The WHOLE environment, not just the config path. update() sets os.environ for every
        # setting it writes, and `unittest discover` runs the suite in one process -- leaving
        # TS_CONTEXT_PAGE_TARGET_BYTES behind made the portal's export test see a configured
        # setting where it asserts there are none. Isolating the config file is not enough when
        # the thing under test also writes the environment.
        self._saved_environ = dict(os.environ)
        self._saved = os.environ.get("MATRIXARK_RUNTIME_CONFIG_FILE")
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = path
        resolved = cfgmod.config_path()
        if resolved != path:
            raise AssertionError("config isolation failed: %r" % resolved)

        def restore():
            os.environ.clear()
            os.environ.update(self._saved_environ)
            shutil.rmtree(directory, ignore_errors=True)

        self.addCleanup(restore)
        self.key = next(s.key for s in cfgmod.SETTINGS
                        if s.env in getattr(cfgmod, "_ENGINE_MINIMUMS", {}))
        self.floor = cfgmod._ENGINE_MINIMUMS[
            next(s.env for s in cfgmod.SETTINGS if s.key == self.key)]

    def _row(self, value):
        result = cfgmod.update({self.key: value}, actor="test")
        return next(r for r in result["applied"] if r["key"] == self.key)

    def test_a_value_below_the_floor_is_reported(self) -> None:
        row = self._row(str(self.floor // 2))
        self.assertEqual(
            self.floor, row.get("raised_to"),
            "a value below the engine's floor was accepted with no indication it would be raised")

    def test_a_value_at_or_above_the_floor_is_not(self) -> None:
        self.assertIsNone(self._row(str(self.floor)).get("raised_to"))
        self.assertIsNone(self._row(str(self.floor * 64)).get("raised_to"))

    def test_the_write_is_still_accepted(self) -> None:
        """Reporting, not refusing. The engine takes the value and raises it; this file must not
        invent a stricter rule than the thing it configures."""
        row = self._row(str(self.floor // 2))
        self.assertTrue(row.get("in_effect") or row.get("applies") == "restart")
        self.assertEqual(str(self.floor // 2),
                         os.environ.get(row["env"]),
                         "the value written to the environment was altered; this must report what "
                         "the engine will do, not do it here")


if __name__ == "__main__":
    unittest.main()
