#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An engine setting must offer the default the engine actually uses.

mx#959 found the portal advertising retrieval budgets 10x to 156x larger than any deployment
gets, because the display was derived from a registry the serving path had been taught to
distrust. The display and the code that consumes it were both correct in isolation; nothing
compared them. This asks the same question one layer down, of the storage engine.

`crates/temporalstore-rust/src/storage_config.rs` is the whole answer for this family: it declares
the env NAME and the DEFAULT as neighbouring consts, and `from_getter` reads one with the other.
So the engine's default is derivable, and the portal's copy of it can be checked rather than
trusted.

The pairing is on the IDENTIFIER, not the string, and that distinction is the point:

    pub const TS_BLOCK_SLAB_TARGET_BYTES: &str = "TS_BLOCK_SEGMENT_TARGET_BYTES";
    pub const DEFAULT_BLOCK_SLAB_TARGET_BYTES: u64 = 1 << 30;

The variable an operator sets is `TS_BLOCK_SEGMENT_TARGET_BYTES`; the constants around it are
named `SLAB`. Matching the portal's env name against the const IDENTIFIER would find nothing here
and quietly pass, so the identifier pairs the two consts and the string is what the portal must
match.

Only literal const expressions are evaluated -- integers, `*`, `<<`, parentheses, and the two
booleans. Anything else is left UNCOMPARED rather than guessed at, so a wrong reading cannot
present itself as a finding.
"""
from __future__ import annotations

import os
import re
import unittest
from typing import Dict, Optional

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
STORAGE_CONFIG = os.path.join(
    REPO, "crates", "temporalstore-rust", "src", "storage_config.rs")

_NAME_CONST = re.compile(
    r'pub const (TS_[A-Z0-9_]+)\s*:\s*&str\s*=\s*"([A-Z0-9_]+)"\s*;')
_DEFAULT_CONST = re.compile(
    r'pub const DEFAULT_([A-Z0-9_]+)\s*:\s*[a-z0-9]+\s*=\s*([^;]+);')

_LITERAL = re.compile(r"^[\d_ ()*<]+$")

# 9 name/default pairs when this was written.
EXPECTED_PAIR_FLOOR = 8


def _value(expr: str) -> Optional[str]:
    expr = expr.strip()
    if expr in ("true", "false"):
        return "1" if expr == "true" else "0"
    if not _LITERAL.match(expr):
        return None
    try:
        return str(eval(expr.replace("_", ""), {"__builtins__": {}}, {}))  # noqa: S307
    except Exception:
        return None


def _engine_defaults() -> Dict[str, str]:
    """env name -> the default the engine uses, paired through the const identifier."""
    with open(STORAGE_CONFIG, encoding="utf-8") as handle:
        source = handle.read()
    defaults = {}
    for match in _DEFAULT_CONST.finditer(source):
        value = _value(match.group(2))
        if value is not None:
            defaults[match.group(1)] = value
    out = {}
    for match in _NAME_CONST.finditer(source):
        identifier, env = match.group(1), match.group(2)
        suffix = identifier[len("TS_"):]
        if suffix in defaults:
            out[env] = defaults[suffix]
    return out


class EngineSettingsOfferTheEngineDefaultTest(unittest.TestCase):

    def test_the_storage_config_is_where_this_says_it_is(self) -> None:
        self.assertTrue(
            os.path.exists(STORAGE_CONFIG),
            "%s is gone, so every assertion below compares an empty set" % STORAGE_CONFIG)

    def test_the_scan_still_pairs_names_with_defaults(self) -> None:
        pairs = _engine_defaults()
        self.assertGreaterEqual(
            len(pairs), EXPECTED_PAIR_FLOOR,
            "paired %d env names with a default, expected at least %d -- if the const shape "
            "changed, the comparison below runs on nothing"
            % (len(pairs), EXPECTED_PAIR_FLOOR))

    def test_the_pairing_survives_a_name_that_differs_from_its_identifier(self) -> None:
        # The case this file exists to not miss: identifier SLAB, variable SEGMENT.
        pairs = _engine_defaults()
        self.assertIn(
            "TS_BLOCK_SEGMENT_TARGET_BYTES", pairs,
            "the slab/segment pair is no longer resolved. Either it was renamed -- fine, say so "
            "here -- or the pairing has quietly gone back to matching on the env string, which "
            "would drop every const whose identifier differs from the variable it names.")

    def test_the_portal_offers_what_the_engine_uses(self) -> None:
        import matrixark_gateway_config as cfgmod
        pairs = _engine_defaults()
        checked = 0
        for setting in cfgmod.SETTINGS:
            engine = pairs.get(setting.env)
            if engine is None:
                continue
            checked += 1
            with self.subTest(env=setting.env):
                self.assertEqual(
                    engine, setting.default,
                    "the portal offers %s as the default for %s; the engine uses %s when nothing "
                    "is set" % (setting.default, setting.env, engine))
        self.assertTrue(
            checked, "no engine setting on the portal matched a name in storage_config.rs, so "
                     "this compared nothing at all")


if __name__ == "__main__":
    unittest.main()
