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

It now reads the profile by calling the accessor the serving path calls, which lives in
matrixark_retrieval_effective -- a module with no cycle to enter. The copy of the default this
file used to pin against the original is gone, and so is the copy of the PARSE that sat beside it
and was wrong: it read ON only for `1 true yes on` against the serving path's ON-unless-off-word,
so five values in sixteen published the opposite of what was served.

test_matrixark_the_page_reports_what_a_retrieve_applies carries that value-space check, and the
one-box page's panels. What is left here is this gauge's own contract: it is emitted, it is
emitted at its default, and it reaches the scrape.
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

    def test_there_is_no_second_copy_of_the_default_to_drift(self) -> None:
        """This used to pin two copies of the default against each other. There is one now.

        The copies were never the problem -- they agreed to the end. The problem was that having
        two seemed normal, so nothing asked whether the PARSE beside each one agreed too, and it
        did not. This asserts the condition that made the question possible to forget: that the
        serving path declares the default and nobody else declares one.
        """
        with io.open(os.path.join(TOOLS, "matrixark_local_adapter_retrieval.py"),
                     encoding="utf-8") as handle:
            source = handle.read()
        self.assertIsNone(
            re.search(r'^ONEBOX_EMBEDDING_FIRST_DEFAULT\s*=\s*"', source, re.M),
            "the retrieval module declares its own copy of the one-box default again; it should "
            "import the one in matrixark_retrieval_effective, which is what the gauge reads")
        import matrixark_retrieval_effective as eff
        self.assertIs(gwm.ONEBOX_PROFILE_DEFAULT, eff.ONEBOX_EMBEDDING_FIRST_DEFAULT)

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
        # Five: the profile, whether the profile could be READ, return-all, its threshold, and how
        # many tenants the return-all pair does not speak for.
        #
        # Two of the five exist only to say how far the others can be trusted, which is the shape
        # this file keeps arriving at. The readable one is here because a caught ImportError once
        # became a confident 0; the tenant count is here because return-all resolves per tenant
        # and the gauge reports the deployment default, so two tenants running return-all showed
        # up as "off on this deployment".
        self.assertEqual(5, len([l for l in lines if l.startswith("matrixark_gateway_")]))

    def test_it_reaches_the_scrape(self) -> None:
        """The positive control: every assertion above passes on a function nothing calls."""
        text = gwm.prometheus_text({"extraction": {"provider": "deterministic"},
                                    "embedding": {"provider": "deterministic"}, "warnings": []})
        self.assertIn("matrixark_gateway_onebox_embedding_first", text)


if __name__ == "__main__":
    unittest.main()
