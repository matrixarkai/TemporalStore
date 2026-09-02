#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run the engine-dashboard conformance validator, because nothing did.

`tools/validate_grafana_metrics_conformance.py` checks the engine dashboard and its alert rules
against the Rust sources that emit the series. It existed, it was failing, and no test or workflow
invoked it -- the only file that mentioned it was another validator. An unrun validator is an
unwired knob with a different shape: the machinery is there and nothing consults it.

Two things it was wrong about, both fixed alongside this file:

* **It scanned ten hand-listed Rust files while twenty emit series**, so it reported conformance
  over 15% of its subject and never saw `bin/server/metrics.rs`, `engine/prometheus_metrics.rs`,
  `meta/subsystem_metrics.rs` or `proxy/prometheus.rs`. One of the ten paths did not even exist --
  `bin/matrixark_rust_proxy_impl.rs`, where the file is not under `bin/` -- and the loader skipped
  it silently with `if path.exists()`.
* **It measured families against a document that has never existed**, turning one missing file into
  ten separate "family is undocumented" failures.

Discovery replaced the list, the document was written, and 33 reported failures became 1 -- a real
one, kept visible rather than resolved away.
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

TOOLS = os.path.dirname(os.path.abspath(__file__))
VALIDATOR = os.path.join(TOOLS, "validate_grafana_metrics_conformance.py")


def _run():
    return subprocess.run([sys.executable, VALIDATOR], cwd=TOOLS,
                          capture_output=True, text=True, timeout=300)


class TheValidatorRunsAndPassesTest(unittest.TestCase):

    def test_it_passes(self) -> None:
        proc = _run()
        self.assertEqual(0, proc.returncode,
                         "engine dashboard conformance failed:\n%s\n%s"
                         % (proc.stderr[-2000:], proc.stdout[-2000:]))

    def test_it_still_reports_the_known_gap_rather_than_hiding_it(self) -> None:
        import json
        proc = _run()
        report = json.loads(proc.stdout)
        self.assertIn("storage_cache:rust:temporalstore_block_store_extent_bytes",
                      report["missing"],
                      "the known blank panel vanished from the report; if it was fixed, remove it "
                      "from KNOWN_UNEMITTED so the list does not go stale")


class TheScanCoversWhatItClaimsTest(unittest.TestCase):
    """A source-scanning guard that quietly narrows reports success over less and less."""

    def setUp(self) -> None:
        import validate_grafana_metrics_conformance as validator
        self.validator = validator

    def test_it_scans_every_rust_file_that_emits_a_series(self) -> None:
        import re
        emitting = set()
        root = self.validator.RUST_SRC_ROOT
        for base, _dirs, files in os.walk(root):
            for name in files:
                if not name.endswith(".rs"):
                    continue
                path = os.path.join(base, name)
                parts = os.path.relpath(path, root).split(os.sep)
                if "tests" in parts or name.startswith("test_"):
                    continue
                try:
                    with open(path, encoding="utf-8") as handle:
                        text = handle.read()
                except OSError:
                    continue
                if re.search(r'#\s*(?:HELP|TYPE)\s+(?:temporalstore|matrixark)_', text) or \
                   re.search(r'"(?:temporalstore|matrixark)_[a-z0-9_]+(?:\{|\s)', text):
                    emitting.add(os.path.relpath(path, root))
        scanned = {os.path.relpath(str(p), str(root)) for p in self.validator.RUST_SOURCES}
        missed = sorted(emitting - scanned)
        self.assertEqual([], missed,
                         "these files emit Prometheus series and are not scanned, so the validator "
                         "reports conformance over a subset of its subject: %s" % missed)
        self.assertGreater(len(emitting), 10,
                           "found almost no emitting files, so this comparison proves nothing")

    def test_the_floor_is_still_discovered(self) -> None:
        # Every file the hand-maintained list named must still be found by discovery. If one stops
        # appearing, the scan has narrowed and the report would not say so.
        self.assertEqual([], self.validator.check_scan_extent())

    def test_every_floor_entry_actually_exists(self) -> None:
        # The list this replaced named a path that was never there, and `if path.exists()` turned
        # that into a silent omission rather than an error.
        root = self.validator.RUST_SRC_ROOT
        for name in self.validator.RUST_SOURCE_FLOOR:
            with self.subTest(path=name):
                self.assertTrue((root / name).exists(),
                                "%s is named as a floor entry and does not exist" % name)


if __name__ == "__main__":
    unittest.main()
