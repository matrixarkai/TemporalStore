#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A config key that names a variable has to actually set it.

`config/temporalstore.toml` is written as `key = value  # TS_SOMETHING  what it does`, and
`matrixark_load_config.ENV_MAP` turns `SECTION.key` into that variable in `os.environ`. A key with
no entry in the map sets nothing: the file documents a knob, an operator turns it, and the engine
never hears about it.

Two were in that state and neither was visible:

  * `[wal] commit_delay_us`, ACTIVE, naming `TS_WAL_COMMIT_DELAY_US` -- while the ENGINE
    documents the config key as the way to set it (`group_commit_delay`, whose doc comment reads
    "`TS_WAL_COMMIT_DELAY_US` (config `[wal] commit_delay_us`)"), and both of its neighbours in
    the same section were mapped.
  * `[storage] index_catalog_fold`, commented out, naming a flag that defaults ON and is read in
    eight places -- so uncommenting it would have done nothing. (That flag has since been retired:
    the fold is unconditional, and the key and its mapping went with it.)

Commented keys count. A commented line is an offer, and the only reason to write one is that
somebody may uncomment it.
"""
from __future__ import annotations

import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
sys.path.insert(0, TOOLS)

import matrixark_load_config  # noqa: E402

CONFIG = os.path.join(REPO, "config", "temporalstore.toml")

_SECTION = re.compile(r"^\s*\[([a-z0-9_.]+)\]")
_LINE = re.compile(
    r"^\s*(#?)\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+?)\s+#\s*"
    r"((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)")

#: A scan that finds nothing passes every assertion below, so it is floored. The file names well
#: over a hundred keys; this is far under that and only catches the shape changing.
EXPECTED_KEY_FLOOR = 60


def _declared():
    """(SECTION.key, variable named in the comment, line number, active?) for the config file."""
    section = ""
    with open(CONFIG, encoding="utf-8") as handle:
        for number, raw in enumerate(handle, 1):
            found = _SECTION.match(raw)
            if found:
                section = found.group(1)
                continue
            match = _LINE.match(raw)
            if match:
                yield ("%s.%s" % (section, match.group(2)), match.group(4), number,
                       not match.group(1))


class TheLoaderMapsEveryConfigKeyTest(unittest.TestCase):

    def test_the_scan_still_finds_the_config_keys(self) -> None:
        keys = list(_declared())
        self.assertGreaterEqual(
            len(keys), EXPECTED_KEY_FLOOR,
            "found %d keys naming a variable, expected at least %d -- if the line shape changed, "
            "the assertions below run on an empty set" % (len(keys), EXPECTED_KEY_FLOOR))

    def test_every_key_that_names_a_variable_is_mapped(self) -> None:
        unmapped = [
            "%s -> %s (%s, line %d)"
            % (key, variable, "active" if active else "commented", number)
            for key, variable, number, active in _declared()
            if key not in matrixark_load_config.ENV_MAP
        ]
        self.assertEqual(
            [], unmapped,
            "these config keys name a variable the loader does not map, so setting them does "
            "nothing: %s" % unmapped)

    def test_the_map_does_not_carry_keys_the_config_stopped_offering(self) -> None:
        """The other direction: an entry for a key no longer in the file is dead weight, and it
        makes the map look like it covers more of the file than it does."""
        declared = {key for key, _, _, _ in _declared()}
        # Keys the loader accepts from a deployment file rather than from the shipped one are not
        # in the tree to be found; only complain about a SECTION the shipped file still has.
        sections = {key.split(".", 1)[0] for key in declared}
        orphaned = sorted(
            key for key in matrixark_load_config.ENV_MAP
            if key.split(".", 1)[0] in sections and key not in declared)
        self.assertEqual(
            [], orphaned,
            "the loader maps keys the shipped config no longer offers: %s" % orphaned)


if __name__ == "__main__":
    unittest.main()
