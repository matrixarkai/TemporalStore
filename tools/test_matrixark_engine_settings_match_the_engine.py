#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every engine knob the portal offers must show the engine's own default.

The portal prints a default next to each setting, and a customer reads it as "what happens if I
leave this alone". For the `TS_*` knobs that default lives in Rust, in the accessor that reads the
variable -- so the portal's copy is a transcription, and a transcription drifts.

Rather than trusting the number I typed, this derives the default from the accessor:

* `.unwrap_or(N)` on a parsed integer means N.
* A boolean that tests its value against `"0" | "false" | "no" | "off"` is ON unless switched off,
  so its default is 1.
* One testing against `"1" | "true" | "yes" | "on"` is OFF unless switched on, so its default is 0.

If the engine changes a default and the portal is not updated, this fails and names both values.
"""
from __future__ import annotations

import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_config as cfgmod  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUST_SRC = os.path.join(ROOT, "crates", "temporalstore-rust", "src")

FALSEY = ('"0" | "false" | "no" | "off"', '"0" | "false" | "no" | "off"')
TRUTHY = ('"1" | "true" | "yes" | "on"', '"1" | "true" | "yes" | "on"')


def _rust_files() -> list:
    out = []
    for root, _dirs, files in os.walk(RUST_SRC):
        if "tests" in root.replace(os.sep, "/").split("/"):
            continue
        out.extend(os.path.join(root, name) for name in files if name.endswith(".rs"))
    return out


def _enclosing_body(text: str, at: int) -> str:
    """The body of the fn containing `at`, by brace matching from its opening brace."""
    fn_at = text.rfind("fn ", 0, at)
    if fn_at < 0:
        return ""
    open_at = text.find("{", fn_at)
    if open_at < 0 or open_at > at:
        return ""
    depth = 0
    for i in range(open_at, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[open_at:i + 1]
    return text[open_at:]


SIZE_CONST = re.compile(r"pub const (DEFAULT_[A-Z0-9_]+)\s*:\s*\w+\s*=\s*([^;]+);")


def _evaluate(expr: str):
    """Evaluate a default constant's initialiser: integers, `*`, `<<`, and the two bool words.

    Restricted to that character set on purpose -- these come from source this test reads, and a
    guard that evaluates arbitrary text from a file it scans is a worse problem than the drift it
    is trying to catch.
    """
    text = expr.strip()
    if text in ("true", "false"):
        return "1" if text == "true" else "0"
    if not re.fullmatch(r"[0-9_ ()*<+]+", text):
        return None
    try:
        return str(eval(text, {"__builtins__": {}}, {}))  # noqa: S307 - charset restricted above
    except Exception:
        return None


def tuning_defaults() -> dict:
    """Knobs read through a getter closure, whose default lives in a DEFAULT_* constant.

    `StorageTuningConfig::from_getter` passes a name CONSTANT to a closure, so these knobs never
    appear beside `env::var` and no scan that looks there can find them. Their defaults come from
    `StorageTuningConfig::default()`, field by field, from `DEFAULT_<KNOB>`.
    """
    defaults = {}
    for path in _rust_files():
        with open(path, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
        constants = {name: _evaluate(value) for name, value in SIZE_CONST.findall(text)}
        # (identifier, env name it holds) -- these are not always the same, and the default
        # constant is named after the IDENTIFIER while the portal must show the NAME.
        declared = re.findall(
            r'pub const (TS_[A-Z0-9_]+)\s*:\s*&(?:\'static\s+)?str\s*=\s*"(TS_[A-Z0-9_]+)"',
            text)
        for identifier, env_name in declared:
            value = constants.get("DEFAULT_" + identifier[len("TS_"):])
            if value is not None:
                defaults[env_name] = value
    return defaults


def engine_defaults() -> dict:
    """env name -> the default its Rust accessor applies, as the portal would print it."""
    defaults = {}
    for path in _rust_files():
        with open(path, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
        for match in re.finditer(r'(?:std::)?env::var(?:_os)?\(\s*"(TS_[A-Z0-9_]+)"', text):
            name = match.group(1)
            if name in defaults:
                continue
            body = _enclosing_body(text, match.start())
            if not body:
                continue
            numeric = re.search(r"\.unwrap_or\((\d+)\)", body)
            if numeric:
                defaults[name] = numeric.group(1)
                continue
            squashed = " ".join(body.split())
            if '"0" | "false" | "no" | "off"' in squashed:
                defaults[name] = "1"     # matches the OFF words, so it is on unless switched off
            elif '"1" | "true" | "yes" | "on"' in squashed:
                defaults[name] = "0"     # matches the ON words, so it is off unless switched on
    return defaults


class TheEnginesDefaultIsWhatThePortalShowsTest(unittest.TestCase):

    def setUp(self) -> None:
        self.engine_settings = [s for s in cfgmod.SETTINGS
                                if s.env and s.env.startswith("TS_")]
        self.derived = dict(tuning_defaults())
        self.derived.update(engine_defaults())

    def test_the_portal_offers_engine_knobs_at_all(self) -> None:
        self.assertGreaterEqual(
            len(self.engine_settings), 5,
            "the portal offers almost no engine knobs, so this file checks nothing")

    def test_the_source_scan_found_defaults(self) -> None:
        """Without this the comparison below skips every setting and reports success."""
        self.assertGreater(
            len(self.derived), 10,
            "derived defaults for only %d engine knobs; the accessor scan has stopped working and "
            "every comparison below would be skipped" % len(self.derived))

    def test_every_offered_engine_knob_shows_the_engines_default(self) -> None:
        checked = 0
        for setting in self.engine_settings:
            derived = self.derived.get(setting.env)
            if derived is None:
                continue
            checked += 1
            with self.subTest(env=setting.env):
                self.assertEqual(
                    derived, setting.default,
                    "%s: the engine defaults to %r and the portal shows %r. A customer reads that "
                    "number as what happens if they leave the setting alone."
                    % (setting.env, derived, setting.default))
        self.assertGreater(
            checked, 4,
            "only %d offered engine knobs could be matched to an accessor, so this test is close "
            "to vacuous" % checked)

    def test_no_offered_engine_knob_escapes_the_derivation(self) -> None:
        """Every knob on the page must have a default this test can check.

        Without this, adding a setting whose default cannot be derived quietly reduces coverage --
        the loop above skips it and still reports success, which is how an unverified number gets
        onto a customer-facing page.
        """
        unchecked = [s.env for s in self.engine_settings if s.env not in self.derived]
        self.assertEqual(
            [], unchecked,
            "these engine knobs are offered and their default cannot be derived from the source, "
            "so nothing checks the number shown: %s" % ", ".join(unchecked))

    def test_every_offered_engine_knob_is_actually_read_by_the_engine(self) -> None:
        # Two ways a knob reaches the environment, and the second is the one that matters here.
        # A literal `env::var("TS_X")` is easy to see. The storage-tuning family is read through
        # `StorageTuningConfig::from_getter`, which passes a name CONSTANT to a closure that calls
        # `env::var(name)` -- so those names never appear beside `env::var` at all. A declared
        # `pub const TS_X: &str = "TS_X"` exists to name a knob, so it counts as a read.
        read = set()
        for path in _rust_files():
            with open(path, encoding="utf-8", errors="replace") as handle:
                text = handle.read()
            read.update(re.findall(r'(?:std::)?env::var(?:_os)?\(\s*"(TS_[A-Z0-9_]+)"', text))
            # the NAME the constant holds, not the identifier that holds it
            read.update(re.findall(
                r'pub const TS_[A-Z0-9_]+\s*:\s*&(?:\'static\s+)?str\s*=\s*"(TS_[A-Z0-9_]+)"',
                text))
        for setting in self.engine_settings:
            with self.subTest(env=setting.env):
                self.assertIn(
                    setting.env, read,
                    "%s is offered on the portal and the engine never reads it, so setting it "
                    "does nothing" % setting.env)


if __name__ == "__main__":
    unittest.main()
