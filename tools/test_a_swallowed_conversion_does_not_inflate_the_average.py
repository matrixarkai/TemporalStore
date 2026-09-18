#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A swallowed conversion must not leave the retention average out of step.

`source_event_lineage_summary` accumulates a retained-text sum, a retained-line sum and one count
across every selection source it sees, and reports sum/count. The three blocks that fill those
accumulators used to advance a sum, then convert a second value that can raise, then advance the
count:

    memory_selection_retained_text_ratio_sum += float(source.get("retained_text_ratio"))
    memory_selection_retained_line_ratio_sum += float(source.get("retained_line_ratio"))   # raises
    memory_selection_retained_ratio_count += 1                                             # skipped

so one malformed line ratio left the TEXT sum holding a sample the count never counted. With one
good source and one half-malformed one the reported average came out at **1.4** -- a retention
ratio above 1.0, claiming more text was kept than existed -- and the swallow meant nothing said so.

The tree already had the safe shape in `matrixark_mcp_session_runtime`: convert into locals first,
so a bad value fails before anything is mutated. These blocks now match it.

Why this is asserted on `source_event_lineage_summary` and not on `context_source_lineage`, which
carries the same shape: there the selection is a single dict, so a partial failure leaves the count
at 0 and the average falls back to 1.0 either way. The corruption needs a LOOP, where other
iterations supply the non-zero denominator. Fixing that site too is correctness housekeeping; only
this one can be shown to produce a wrong number.

The controls matter: an average of 0.5 must come from the good source ALONE, so the test also
pins what a clean pair reports and what a fully-malformed source does, or "0.5" could be a
coincidence of two arithmetic errors.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matrixark_mcp_local_adapter as adapter

TEXT_AVG = "source_memory_selection_retained_text_ratio_avg"
LINE_AVG = "source_memory_selection_retained_line_ratio_avg"


def _record(policy: str, text_ratio, line_ratio) -> dict:
    return {"codex_memory_selection": {"policy": policy,
                                       "retained_text_ratio": text_ratio,
                                       "retained_line_ratio": line_ratio}}


class ASwallowedConversionDoesNotInflateTheAverage(unittest.TestCase):
    def test_a_malformed_line_ratio_does_not_add_its_text_ratio(self) -> None:
        summary = adapter.source_event_lineage_summary([
            _record("good", 0.5, 0.5),
            _record("half-bad", 0.9, "not-a-number"),
        ])
        self.assertLessEqual(
            summary[TEXT_AVG], 1.0,
            "a retention ratio above 1.0 claims more text was kept than existed: %r"
            % summary[TEXT_AVG])
        self.assertEqual(0.5, summary[TEXT_AVG],
                         "the half-malformed source contributed its text ratio anyway")
        self.assertEqual(0.5, summary[LINE_AVG])

    def test_a_clean_pair_averages_both(self) -> None:
        # Control: without it, a fix that discarded EVERY source would also pass the test above.
        summary = adapter.source_event_lineage_summary([
            _record("a", 0.5, 0.5),
            _record("b", 0.9, 0.7),
        ])
        self.assertEqual(0.7, summary[TEXT_AVG])
        self.assertEqual(0.6, summary[LINE_AVG])

    def test_a_fully_malformed_source_contributes_nothing(self) -> None:
        summary = adapter.source_event_lineage_summary([
            _record("good", 0.5, 0.5),
            _record("bad", "x", "y"),
        ])
        self.assertEqual(0.5, summary[TEXT_AVG])
        self.assertEqual(0.5, summary[LINE_AVG])

    def test_no_sources_still_reports_fully_retained(self) -> None:
        # Control: the count-is-zero fallback is 1.0 and must stay that way.
        summary = adapter.source_event_lineage_summary([{}])
        self.assertEqual(1.0, summary[TEXT_AVG])
        self.assertEqual(1.0, summary[LINE_AVG])


if __name__ == "__main__":
    unittest.main()
