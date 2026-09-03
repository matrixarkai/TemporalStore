#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A byte-valued setting is shown in units a person reads, and only where that is true.

Nine settings on the Setup page hold a byte count, and they were rendered as raw integers.
1073741824 and 268435456 differ by a factor of four and look the same at a glance, which is how a
cache gets sized wrong by someone reading carefully.

Run rather than grepped. The built page containing `byteHint` proves the function is present, not
that it fires: a hint appended to a variable nothing renders, or a pattern that never matches the
variable names actually shipped, leave identical source text behind. The harness extracts the
function from the built page and calls it.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "byte_hint_harness.js")
PAGE = os.path.join(PORTAL, "setup_portal.html")


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page's own JS cannot be run")
class AByteSettingReadsInUnitsTest(unittest.TestCase):

    def test_the_harness_passes_every_case(self) -> None:
        proc = subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=120)
        self.assertEqual(0, proc.returncode,
                         "the Setup page's byteHint did not render every case as expected:\n%s%s"
                         % (proc.stdout, proc.stderr))
        self.assertIn("PASS", proc.stdout)

    def test_the_harness_checks_a_meaningful_number_of_cases(self) -> None:
        """A harness that asserts nothing passes everything."""
        proc = subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=120)
        checked = sum(1 for line in proc.stdout.splitlines() if line.startswith("ok "))
        self.assertGreaterEqual(
            checked, 8,
            "the harness reported only %d checks, so passing it says little" % checked)


class EveryByteSettingIsCoveredTest(unittest.TestCase):
    """The hint keys off a `_BYTES` suffix, so a byte setting named otherwise gets nothing."""

    def test_no_byte_shaped_setting_is_missed_by_the_suffix_rule(self) -> None:
        import matrixark_gateway_config as cfgmod

        suspicious = []
        for setting in cfgmod.SETTINGS:
            if setting.kind != "int" or not setting.env:
                continue
            if setting.env.endswith("_BYTES"):
                continue
            # A setting whose help talks about bytes but whose name does not end in _BYTES would
            # render as a bare integer with no hint, and nothing else would notice.
            help_text = (setting.help or "").lower()
            if "bytes" in help_text and "per" not in help_text:
                suspicious.append("%s (%s)" % (setting.key, setting.env))
        self.assertEqual(
            [], suspicious,
            "these look like byte counts but do not end in _BYTES, so they render without a "
            "readable size: %s" % ", ".join(suspicious))


if __name__ == "__main__":
    unittest.main()
