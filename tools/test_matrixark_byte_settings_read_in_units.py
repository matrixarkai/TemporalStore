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
import re
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
    """Every byte-valued setting renders with a readable size -- checked by rendering it.

    The hint keys off a `_BYTES` suffix plus a short list of names that lack one, so a byte
    setting named otherwise gets a bare integer. Two did: `TS_STORAGE_ZONE_SIZE` (1 GiB) and
    `TS_STREAM_MAX_BLOB_SIZE` (10 MiB).

    This test existed to catch that and did not, because it asked whether a setting's help
    contained the word "bytes" -- and both of them say "1 KiB". A detector that misses every
    member of the class it guards passes exactly as loudly as one that works.

    So it stops pattern-matching prose and renders instead: it finds the byte-shaped settings by
    unit words in their help, then calls the built page's own `byteHint` on each. A name the list
    forgets comes back empty, and empty fails.
    """

    UNIT = re.compile(r"\b(bytes?|[KMGT]iB)\b")

    def _byte_shaped(self):
        import matrixark_gateway_config as cfgmod

        found = []
        for setting in cfgmod.SETTINGS:
            if setting.kind != "int" or not setting.env:
                continue
            # By NAME or by what the help calls it. Only four settings name a unit in their help,
            # so the help alone is far too narrow a scan to rest an "everything is covered" claim
            # on -- and the suffix alone is what missed these two in the first place.
            if setting.env.endswith("_BYTES") or self.UNIT.search(setting.help or ""):
                found.append(setting)
        return found

    def test_the_scan_finds_the_byte_settings(self) -> None:
        """A scan that found none would make the check below pass over an empty list."""
        found = self._byte_shaped()
        self.assertGreaterEqual(
            len(found), 8,
            "only %d byte-shaped settings found; the scan is broken" % len(found))
        self.assertTrue(
            any(not s.env.endswith("_BYTES") for s in found),
            "no byte setting outside the _BYTES suffix was found, which is the case this exists "
            "for -- the scan has stopped seeing them")

    @unittest.skipUnless(shutil.which("node"), "node is not installed; the page's own JS cannot be run")
    def test_every_byte_shaped_setting_renders_with_a_size(self) -> None:
        missing = []
        for setting in self._byte_shaped():
            value = setting.default or "1073741824"
            script = (
                "const fs=require('fs');"
                "const page=fs.readFileSync(process.argv[1],'utf8');"
                "const start=page.indexOf('function byteHint');"
                "const end=page.indexOf('function fieldHtml');"
                "const fn=new Function(page.slice(start,end)+'; return byteHint;')();"
                "process.stdout.write(fn({env:process.argv[2],value:process.argv[3]}));"
            )
            proc = subprocess.run(["node", "-e", script, PAGE, setting.env, str(value)],
                                  capture_output=True, text=True, timeout=120)
            self.assertEqual(0, proc.returncode,
                             "rendering %s failed: %s" % (setting.env, proc.stderr))
            if not proc.stdout.strip():
                missing.append("%s (%s = %s)" % (setting.key, setting.env, value))
        self.assertEqual(
            [], missing,
            "these hold byte counts and render as bare integers, with no readable size beside "
            "them:\n  %s" % "\n  ".join(missing))


if __name__ == "__main__":
    unittest.main()
