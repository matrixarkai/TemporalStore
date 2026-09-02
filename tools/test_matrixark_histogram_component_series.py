#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A histogram is declared once and published as three series.

The conformance validator fails a dashboard or alert that names a metric nothing declares. That is
the right rule and it had a hole on the correct side: Prometheus renders a histogram as `_bucket`,
`_sum` and `_count` from a SINGLE `# TYPE ... histogram` declaration, so comparing raw names
reports every proper use of a histogram as undeclared.

Nothing queried the one histogram in the engine yet, so the check was not failing -- it was waiting
to fail the first person who added a latency percentile panel. The obvious way to make that
failure go away is to delete the panel, which is the opposite of what the validator exists to do.

A guard that rejects correct usage does more damage than one that misses a defect, because the
person it stops is doing the right thing.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import validate_grafana_metrics_conformance as conformance  # noqa: E402


class AHistogramPublishesThreeSeriesTest(unittest.TestCase):

    def setUp(self) -> None:
        self.rust = conformance.rust_metric_text()
        self.base = conformance.declared_metric_names(self.rust)
        self.kinds = conformance.declared_kinds(self.rust)
        self.expanded = conformance.expand_component_series(self.base, self.kinds)

    def test_the_engine_declares_at_least_one_histogram(self) -> None:
        """Without one, everything below passes by having nothing to expand."""
        shaped = {name: kind for name, kind in self.kinds.items()
                  if kind in ("histogram", "summary")}
        self.assertTrue(
            shaped,
            "no histogram or summary is declared, so the expansion this file checks is a no-op "
            "and its other tests prove nothing")

    def test_every_component_series_of_a_histogram_is_accepted(self) -> None:
        for name, kind in sorted(self.kinds.items()):
            if kind not in ("histogram", "summary"):
                continue
            for suffix in conformance.COMPONENT_SUFFIXES:
                with self.subTest(metric=name + suffix):
                    self.assertIn(
                        name + suffix, self.expanded,
                        "%s%s is how Prometheus publishes a %s, and the validator would report a "
                        "panel using it as querying an undeclared metric"
                        % (name, suffix, kind))

    def test_a_component_suffix_on_an_undeclared_family_is_still_rejected(self) -> None:
        # The expansion must not become a blanket amnesty for anything ending in _bucket.
        self.assertNotIn("temporalstore_not_a_real_family_bucket", self.expanded)
        self.assertNotIn("temporalstore_not_a_real_family_sum", self.expanded)

    def test_a_gauge_does_not_gain_component_series(self) -> None:
        gauges = [name for name, kind in self.kinds.items() if kind == "gauge"]
        self.assertTrue(gauges, "no gauge declared, so this check is vacuous")
        for name in gauges[:20]:
            with self.subTest(metric=name):
                self.assertNotIn(name + "_bucket", self.expanded,
                                 "%s is a gauge; it publishes no bucket series" % name)

    def test_expansion_only_adds(self) -> None:
        self.assertTrue(self.base.issubset(self.expanded),
                        "expansion dropped a base declaration, which would newly reject panels "
                        "that were previously fine")


if __name__ == "__main__":
    unittest.main()
