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
        self.derived = engine_defaults()

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

    def test_every_offered_engine_knob_is_actually_read_by_the_engine(self) -> None:
        read = set()
        for path in _rust_files():
            with open(path, encoding="utf-8", errors="replace") as handle:
                text = handle.read()
            read.update(re.findall(r'(?:std::)?env::var(?:_os)?\(\s*"(TS_[A-Z0-9_]+)"', text))
        for setting in self.engine_settings:
            with self.subTest(env=setting.env):
                self.assertIn(
                    setting.env, read,
                    "%s is offered on the portal and the engine never reads it, so setting it "
                    "does nothing" % setting.env)


if __name__ == "__main__":
    unittest.main()
