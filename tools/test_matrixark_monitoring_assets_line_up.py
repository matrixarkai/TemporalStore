#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every monitoring asset the portal offers is served, described, and actually there.

Three ways this can be wrong, and a customer meets all three as the same thing -- a download that
does not work:

* served but not described: the file downloads and the page never mentions it, so nobody finds it.
* described but not served: the page lists it and the endpoint 404s.
* described, served, and the file is absent: the page lists it, the endpoint is wired, and the read
  fails at request time.

The third is not hypothetical here. Three conformance validators in this repository point at
`tools/run_matrixark_rust_scale_report.py`, a file that has never existed, and each died at the
moment it tried to read it rather than when it was wired up. A path is a claim about the
filesystem, and claims want checking.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gateway  # noqa: E402

TOOLS = os.path.dirname(os.path.abspath(gateway.__file__))


class TheMonitoringAssetsLineUpTest(unittest.TestCase):

    def setUp(self) -> None:
        self.served = dict(gateway._GRAFANA_ASSETS)
        self.described = {entry["asset"]: entry for entry in gateway._MONITORING_ASSETS}
        self.assertGreaterEqual(len(self.served), 4,
                                "almost nothing is served, so these comparisons prove little")

    def test_everything_served_is_described(self) -> None:
        orphans = sorted(set(self.served) - set(self.described))
        self.assertEqual(
            [], orphans,
            "these download but the page never mentions them, so a customer has no way to find "
            "them: %s" % ", ".join(orphans))

    def test_everything_described_is_served(self) -> None:
        missing = sorted(set(self.described) - set(self.served))
        self.assertEqual(
            [], missing,
            "the page offers these and the endpoint does not serve them: %s" % ", ".join(missing))

    def test_every_served_asset_exists_on_disk(self) -> None:
        absent = []
        for name, (path, _content_type) in sorted(self.served.items()):
            full = os.path.normpath(os.path.join(TOOLS, path))
            if not os.path.exists(full):
                absent.append("%s -> %s" % (name, path))
        self.assertEqual(
            [], absent,
            "these are wired to a path that is not there, so the download fails at request time "
            "rather than when it was wired: %s" % ", ".join(absent))

    def test_every_description_names_the_file_it_serves(self) -> None:
        """A filename shown to a customer that does not match what downloads is worse than none."""
        wrong = []
        for name, entry in sorted(self.described.items()):
            filename = entry.get("filename", "")
            path = self.served.get(name, ("", ""))[0]
            if not filename or not path.endswith(filename):
                wrong.append("%s says %r, serves %r" % (name, filename, path))
        self.assertEqual([], wrong, "; ".join(wrong))

    def test_every_description_names_a_scrape_target(self) -> None:
        """Importing a dashboard against the wrong process yields blank panels, which reads as a
        quiet system rather than as a query aimed at the wrong place."""
        for name, entry in sorted(self.described.items()):
            with self.subTest(asset=name):
                self.assertTrue(entry.get("scrape"),
                                "%s does not say which process exports the series it queries" % name)
                self.assertTrue(entry.get("covers"),
                                "%s does not say what it covers" % name)


if __name__ == "__main__":
    unittest.main()
