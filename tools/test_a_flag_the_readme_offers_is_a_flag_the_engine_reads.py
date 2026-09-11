#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A flag the engine README offers is a flag the engine reads.

`crates/temporalstore-rust/README.md` lists, under the sentence "`StorageTuningConfig::from_env()`
reads:", the tuning knobs a deployment may set. That list is an OFFER: an operator reads it, sets
one, and expects the engine to honour it.

`TS_STORAGE_ZONE_SIZE` sat in that list after the flag was retired. The engine named it in no
source file, the `storage_band_size` field it fed was gone, and the shipped config had stopped
declaring it -- so the one place still offering it was the document telling operators it was read.
That is the same defect as a portal field nothing reads, in its documentation form, and nothing in
the tree would have said so: a stale markdown line breaks no test.

What is checked is the OFFER, not the prose. Every `TS_*` name in that list must appear in the
engine's own source. The reverse is deliberately NOT checked -- the engine has hundreds of flags
and this list is a curated subset, so requiring every engine flag to be documented here would be a
different and much larger claim.

The scan is anchored on the sentence rather than on line numbers, and a floor asserts the list was
actually found: a parser that matched nothing would otherwise pass over an empty set, which is how
a guard comes to assert nothing at all.
"""
from __future__ import annotations

import os
import re
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
README = os.path.join(REPO, "crates", "temporalstore-rust", "README.md")
ENGINE_SRC = os.path.join("crates", "temporalstore-rust", "src")

#: Eight when the list was written, seven after the retired one left. A floor, not a count.
EXPECTED_OFFERED_FLOOR = 5

_ANCHOR = "`StorageTuningConfig::from_env()` reads:"
_BULLET = re.compile(r"^- `(TS_[A-Z0-9_]+)`")


def _offered() -> list:
    """The TS_* names the README offers under the from_env sentence.

    Read as the bullet list immediately following the anchor sentence and stopping at the first
    line that is neither a bullet nor a continuation, so prose after the list is not scanned for
    flag-shaped text.
    """
    with open(README, encoding="utf-8", errors="replace") as handle:
        lines = handle.read().splitlines()
    try:
        start = next(index for index, line in enumerate(lines) if _ANCHOR in line)
    except StopIteration:
        return []
    found = []
    for line in lines[start + 1:]:
        stripped = line.strip()
        if not stripped:
            continue
        if line.startswith("- "):
            match = _BULLET.match(line)
            if match:
                found.append(match.group(1))
            continue
        if line.startswith(" "):
            continue                      # a wrapped bullet
        break                             # the list is over
    return found


def _named_in_engine(flag: str) -> bool:
    result = subprocess.run(["git", "grep", "-l", "--fixed-strings", flag, "--", ENGINE_SRC],
                            cwd=REPO, capture_output=True, text=True, check=False)
    return bool(result.stdout.strip())


class AFlagTheReadmeOffersIsAFlagTheEngineReadsTest(unittest.TestCase):

    def test_every_offered_flag_is_named_in_the_engine(self) -> None:
        """The rule. A name in that list is an offer to an operator, and an offer the engine
        cannot honour is worse than no documentation: it is acted on."""
        missing = [flag for flag in _offered() if not _named_in_engine(flag)]
        self.assertEqual(
            [], missing,
            "the engine README offers these under \"from_env() reads\" and the engine names them "
            "in no source file, so an operator who sets one gets nothing and is told otherwise: "
            "%s" % missing)

    def test_the_list_was_actually_found(self) -> None:
        """A floor. With the anchor moved or the list reformatted, the check above would pass by
        having nothing to look at -- which is the failure mode of every scan in this tree."""
        offered = _offered()
        self.assertGreaterEqual(
            len(offered), EXPECTED_OFFERED_FLOOR,
            "found %d offered flags, below the floor of %d -- the anchor sentence or the list "
            "shape changed and this file is now asserting nothing" % (len(offered),
                                                                      EXPECTED_OFFERED_FLOOR))

    def test_the_engine_lookup_can_fail(self) -> None:
        """A control for the lookup itself. If `_named_in_engine` answered True for everything --
        a bad path, a git invocation that errors into an empty grep -- the rule above would pass
        for a README full of retired flags."""
        self.assertFalse(_named_in_engine("TS_A_FLAG_THAT_DOES_NOT_EXIST_ANYWHERE"),
                         "the engine lookup answers True for a name that cannot be there")
        self.assertTrue(_named_in_engine("TS_CONTEXT_PAGE_TARGET_BYTES"),
                        "the engine lookup answers False for a flag the engine certainly reads")

    def test_the_retired_flag_is_not_offered_again(self) -> None:
        """The instance this came from, pinned by name. Re-adding the bullet is the regression,
        and it would otherwise be caught only by the general rule above -- which is right, but
        says nothing about why this particular name is absent."""
        self.assertNotIn(
            "TS_STORAGE_ZONE_SIZE", _offered(),
            "TS_STORAGE_ZONE_SIZE is retired: the engine names it in no source file and the "
            "shipped config does not declare it. It must not be offered as a knob again")


if __name__ == "__main__":
    unittest.main()
