#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The one-box retrieval profile is visible to monitoring.

``MATRIXARK_ONEBOX_EMBEDDING_FIRST`` decides whether a candidate's score is its vector similarity
alone or a 0.72/0.28 blend with a lexical match. It is **on by default**, it decides every result
the deployment returns, and nothing published it -- so "why did the answers change?" had no answer
in any series the gateway emits, and two deployments answering differently looked identical on
every dashboard.

**The first version of this gauge lied.** It read the value by importing the retrieval module,
which is a circular import -- the adapter imports the retrieval mixin and the mixin imports the
adapter -- so the import raised, an ``except`` caught it, and the gauge published ``0``. A gauge
saying "blended scoring" about a deployment running the profile ON is worse than no gauge at all:
every dashboard reading it describes the opposite of what happened.

So it reads the environment, and the default it falls back to is asserted here against the
retrieval module's own. Two copies of a default agree until one is edited.
"""
from __future__ import annotations

import io
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_metrics as gwm  # noqa: E402

TOOLS = os.path.dirname(os.path.abspath(__file__))


def gauge(lines, name: str):
    for line in lines:
        if line.startswith(name + " "):
            return line.split(" ", 1)[1].strip()
    return None


class TheProfileIsPublishedTest(unittest.TestCase):

    def setUp(self) -> None:
        self.original = os.environ.get("MATRIXARK_ONEBOX_EMBEDDING_FIRST")
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        if self.original is None:
            os.environ.pop("MATRIXARK_ONEBOX_EMBEDDING_FIRST", None)
        else:
            os.environ["MATRIXARK_ONEBOX_EMBEDDING_FIRST"] = self.original

    def test_it_reports_the_profile_that_is_actually_on(self) -> None:
        os.environ["MATRIXARK_ONEBOX_EMBEDDING_FIRST"] = "1"
        self.assertEqual("1", gauge(gwm.onebox_lines(),
                                    "matrixark_gateway_onebox_embedding_first"))

    def test_it_reports_the_profile_that_is_actually_off(self) -> None:
        os.environ["MATRIXARK_ONEBOX_EMBEDDING_FIRST"] = "0"
        self.assertEqual("0", gauge(gwm.onebox_lines(),
                                    "matrixark_gateway_onebox_embedding_first"))

    def test_with_nothing_set_it_reports_the_default_which_is_on(self) -> None:
        """The case that matters most, because it is the case almost every deployment is in -- and
        the case the first version of this got backwards."""
        os.environ.pop("MATRIXARK_ONEBOX_EMBEDDING_FIRST", None)
        self.assertEqual("1", gauge(gwm.onebox_lines(),
                                    "matrixark_gateway_onebox_embedding_first"))

    def test_the_default_here_is_the_default_there(self) -> None:
        """The duplication this rests on, held to the original.

        The metrics module cannot import the retrieval module to ask -- that import is circular --
        so it carries its own copy of the default. Read out of the source rather than imported, for
        the same reason.
        """
        with io.open(os.path.join(TOOLS, "matrixark_local_adapter_retrieval.py"),
                     encoding="utf-8") as handle:
            source = handle.read()
        match = re.search(r'ONEBOX_EMBEDDING_FIRST_DEFAULT\s*=\s*"([^"]*)"', source)
        self.assertIsNotNone(match, "the retrieval module no longer names its default")
        self.assertEqual(match.group(1), gwm.ONEBOX_PROFILE_DEFAULT,
                         "the two copies of the one-box default have drifted apart")

    def test_the_return_all_state_is_published_too(self) -> None:
        """A dashboard that shows how scoring works without showing whether ranking is allowed to
        drop anything explains half of a changed answer."""
        lines = gwm.onebox_lines()
        for name in ("matrixark_gateway_return_all_candidates",
                     "matrixark_gateway_return_all_candidate_threshold"):
            with self.subTest(series=name):
                self.assertIsNotNone(gauge(lines, name))

    def test_every_gauge_is_emitted_even_at_its_default(self) -> None:
        """A gauge that appears only once somebody changes something cannot be alerted on before
        they do, which is the moment the alert was worth having."""
        os.environ.pop("MATRIXARK_ONEBOX_EMBEDDING_FIRST", None)
        lines = gwm.onebox_lines()
        self.assertEqual(3, len([l for l in lines if l.startswith("matrixark_gateway_")]))

    def test_it_reaches_the_scrape(self) -> None:
        """The positive control: every assertion above passes on a function nothing calls."""
        text = gwm.prometheus_text({"extraction": {"provider": "deterministic"},
                                    "embedding": {"provider": "deterministic"}, "warnings": []})
        self.assertIn("matrixark_gateway_onebox_embedding_first", text)


if __name__ == "__main__":
    unittest.main()
