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

Derived from ONE accessor, though, and only where the argument is a literal. There is a second
floor further down the same file, and it is the one the write path uses:

    pub fn effective_slab_target_bytes(self) -> u64 {
        self.block_slab_target_bytes.max(self.stream_max_blob_size)
    }

A slab has to be able to hold the largest blob it stores, so `TS_STREAM_MAX_BLOB_SIZE` is a floor
under `TS_BLOCK_SLAB_TARGET_BYTES`, and its shipped default is 10 MiB -- 10,240 times the literal
1024 this file was finding. `block_store/append.rs` and `block_store.rs` call
`effective_block_slab_target_bytes()`, so that is the number the write path actually seals at.

Measured, with the portal's own `update()` on one side and `storage_config.rs` built on its own on
the other:

    written        portal said raised_to      engine write path used
    512            1024                       10485760
    1024           (nothing)                  10485760
    65536          (nothing)                  10485760
    1048576        (nothing)                  10485760
    10485760       (nothing)                  10485760

`AWriteReportsARaisedValueTest` below passed the whole time: it asks whether a raise is REPORTED,
and one was. `test_the_scan_found_clamps` passed too -- it wanted three and found three. A floor
that is met is not evidence that a scan can see everything it should, which is why the cross-field
clamp is now derived as well and asserted in both directions.
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


#: `self.a.max(self.b)` -- one tuning field clamped by ANOTHER, anywhere in the file rather than
#: inside one accessor, and with a field name where CLAMPED wants a number. Both of those are why
#: the scan above could not see it.
CROSS_CLAMPED = re.compile(
    r"self\.(?P<field>[a-z0-9_]+)\s*\n?\s*\.(?P<op>max|min)\(\s*self\.(?P<other>[a-z0-9_]+)\s*\)")

#: `field: parse_usize(get(TS_CONST)` in `from_getter` -- what names each struct field.
FIELD_SOURCE = re.compile(r"(?P<field>[a-z0-9_]+):\s*parse_[a-z0-9]+\(\s*\n?\s*get\((?P<konst>[A-Za-z0-9_]+)\)")


def _tuning_text() -> str:
    if not os.path.exists(TUNING):
        return ""
    with open(TUNING, encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _field_to_env() -> dict:
    """struct field -> the variable that fills it."""
    return {m.group("field"): _env_for_constant(m.group("konst"))
            for m in FIELD_SOURCE.finditer(_tuning_text())}


def cross_field_floors() -> dict:
    """env name -> the env name of the setting that floors it."""
    fields = _field_to_env()
    out = {}
    for match in CROSS_CLAMPED.finditer(_tuning_text()):
        if match.group("op") != "max":
            continue
        subject, other = match.group("field"), match.group("other")
        if subject == other or subject not in fields or other not in fields:
            continue
        out[fields[subject]] = fields[other]
    return out


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


class AFloorThatIsAnotherSettingIsReportedTooTest(unittest.TestCase):
    """The floor that decides the write path is not a number in an accessor.

    Asserted in BOTH directions on purpose. A new cross-field clamp must arrive with its portal
    entry, and one that goes away must take its entry with it -- otherwise the portal starts
    reporting a raise the engine no longer applies, which is the same defect pointing the other
    way.
    """

    def setUp(self) -> None:
        self.cross = cross_field_floors()
        self.by_env = {s.env: s for s in cfgmod.SETTINGS if s.env}

    def test_the_field_mapping_resolved(self) -> None:
        """Without it every assertion below has nothing to decide about."""
        fields = _field_to_env()
        self.assertGreaterEqual(
            len(fields), 8,
            "resolved %d struct fields to variables; the accessor's shape has changed and the "
            "cross-field scan is deciding nothing" % len(fields))

    def test_the_scan_found_the_cross_field_clamp(self) -> None:
        self.assertGreaterEqual(
            len(self.cross), 1,
            "found no field clamped by another field. If the engine really stopped doing that, "
            "delete _ENGINE_FLOOR_FROM_SETTING with this assertion; until then the scan has "
            "stopped matching and the checks below pass on an empty set")

    def test_the_portal_knows_every_cross_field_floor(self) -> None:
        mapped = dict(getattr(cfgmod, "_ENGINE_FLOOR_FROM_SETTING", {}))
        self.assertEqual(
            self.cross, mapped,
            "the engine floors one setting with another and the portal's map of that has "
            "diverged. Engine: %r. Portal: %r." % (self.cross, mapped))

    def test_both_settings_name_each_other(self) -> None:
        """A customer reading either page entry has to be able to find the other one. Neither
        said anything: the slab knob offered a 1 KiB floor and the blob knob called itself a
        ceiling, and between them they hid that the first was pinned by the second."""
        silent = []
        for subject, other in sorted(self.cross.items()):
            for name, must_name in ((subject, other), (other, subject)):
                setting = self.by_env.get(name)
                if setting is None:
                    continue
                if must_name not in (setting.help or ""):
                    silent.append("%s does not name %s" % (name, must_name))
        self.assertEqual([], silent, "; ".join(silent))

    def test_the_reported_raise_is_the_number_the_engine_uses(self) -> None:
        """The whole point. `engine_minimum` used to answer with the literal from the accessor,
        which is not the floor anything applies once the second one is bigger."""
        for subject, other in sorted(self.cross.items()):
            other_setting = self.by_env.get(other)
            if other_setting is None:
                continue
            ceiling = int(other_setting.default)
            reported = cfgmod.engine_minimum(subject)
            self.assertEqual(
                ceiling, reported,
                "%s is floored by %s (%d at its shipped default) and the portal reports %r"
                % (subject, other, ceiling, reported))
            self.assertGreater(
                ceiling, cfgmod._ENGINE_MINIMUMS.get(subject, 0),
                "this test proves nothing unless the second floor is the larger one")


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
        # A knob whose ONLY floor is the literal one. Picking the first knob in declaration order
        # picked one that also has a cross-field floor the moment one was added, and then
        # `self.floor` was the smaller of two numbers and the assertions below decided the wrong
        # thing -- quietly, because writing the literal floor still produces a report.
        cross = getattr(cfgmod, "_ENGINE_FLOOR_FROM_SETTING", {})
        self.key = next(s.key for s in cfgmod.SETTINGS
                        if s.env in getattr(cfgmod, "_ENGINE_MINIMUMS", {})
                        and s.env not in cross)
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
