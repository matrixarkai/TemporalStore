#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`return_all_candidates` returns facts that ranking was dropping.

A recall comparison, not a presence check. The policy-wiring guard asked for exactly this, and said
why: *"a knob that silently narrows or widens results looks like it works either way."* So the same
store is asked the same question twice, and the assertion is on **which facts came back**.

Measured on the real thing while building it, at the deployment's own defaults -- 80 short facts,
one question, an 8000-token budget:

    knob off   40 of 80 came back
    knob on    79 of 80

Eighty facts of that length is about a thousand tokens, so the budget was never what cut it.
`max_selected_refs` was -- 64 by default, and one of the five knobs frozen at import, so a
deployment cannot raise it without a restart.

This suite sets that cap per request instead of writing eighty facts, so the same mechanism is
exercised in a second rather than a minute. The cap is what the knob lifts; where it comes from
does not change what is being tested.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

FACTS = [
    ("the office fridge is emptied on Fridays", "fridge"),
    ("Priya owns the billing integration", "billing"),
    ("the staging cluster lives in Frankfurt", "staging"),
    ("invoices over 10000 need a second approver", "invoices"),
    ("the retro moved to Thursday in December", "retro"),
    ("Marcus prefers async standups", "standups"),
    ("the mobile build signs with the 2027 certificate", "mobile"),
    ("support rotas are published a fortnight ahead", "rotas"),
    ("the archive bucket is in eu-west-2", "archive"),
    ("data retention for logs is ninety days", "retention"),
]
CAP = 2


def _server(tmp):
    from matrixark_mcp_backends import add_backend_arguments, build_mcp_adapter
    from matrixark_mcp_server import MatrixArkMcpServer
    from pathlib import Path

    parser = argparse.ArgumentParser(add_help=False)
    add_backend_arguments(parser)
    ns = parser.parse_args([])
    ns.backend = "local"
    # Both of this backend's files default to the LIVE ones on a developer box, and only one has an
    # environment override -- so both are set here and the run refuses if either did not take.
    ns.event_log = Path(tmp) / "events.jsonl"
    ns.local_store = os.path.join(tmp, "store.jsonl")
    for field in ("event_log", "local_store"):
        assert str(getattr(ns, field)).startswith(tmp), "%s is not isolated" % field
    return MatrixArkMcpServer(build_mcp_adapter(ns), access_mode="dev")


class ASmallStoreCanReturnEverythingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.mkdtemp(prefix="returnall")
        cls.server = _server(cls.tmp)
        cls.scope = {"user_id": "recall"}
        for text, _tag in FACTS:
            cls.server.call_tool("matrixark_ingest", {
                "scope": cls.scope,
                "messages": [{"role": "user", "content": text}],
                "finalize": True,
            })

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _packed(self, **policy) -> dict:
        """The facts AND the pack id, so a cache hit can be told from an equal answer."""
        answer = self._ask(**policy)
        text = json.dumps(answer, default=str).lower()
        return {"facts": {tag for _t, tag in FACTS if tag in text},
                "pack_id": (answer or {}).get("context_pack_id")}

    def _ask(self, **policy):
        """One retrieve with `policy` applied to this tenant, and the environment put back."""
        original = {}
        for name, value in policy.items():
            original[name] = os.environ.get(name)
            os.environ[name] = str(value)
        try:
            answer = self.server.call_tool("matrixark_retrieve", {
                "scope": self.scope,
                "query": "when is the office fridge emptied?",
                "max_budget_tokens": 8000,
                # The cap the knob lifts, set per request so the mechanism bites on ten facts
                # rather than eighty. It is the same cap either way.
                "ranking": {"max_selected_refs": CAP},
            })
        finally:
            for name, value in original.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
        return answer

    def _recall(self, **policy) -> set:
        """Which of the facts came back, with `policy` applied to this tenant."""
        text = json.dumps(self._ask(**policy), default=str).lower()
        return {tag for _t, tag in FACTS if tag in text}

    def test_the_cap_really_is_dropping_facts(self) -> None:
        """The floor. If ranking returned everything anyway, the comparison below would pass on a
        knob that does nothing at all -- which is the failure this whole suite is for."""
        kept = self._recall()
        self.assertLess(len(kept), len(FACTS),
                        "nothing was being dropped, so there is nothing for the knob to recover")

    def test_it_returns_more_than_ranking_did(self) -> None:
        before = self._recall()
        after = self._recall(MATRIXARK_RETURN_ALL_CANDIDATES="1")
        self.assertGreater(len(after), len(before),
                           "the knob is on and the same facts came back: %s" % sorted(after))

    def test_what_it_adds_is_what_ranking_dropped(self) -> None:
        """Not merely 'more'. The facts it recovers must be the ones the cap was cutting, and it
        must not lose any it was already returning."""
        before = self._recall()
        after = self._recall(MATRIXARK_RETURN_ALL_CANDIDATES="1")
        self.assertEqual(set(), before - after, "it dropped a fact ranking was keeping")
        self.assertTrue(after - before, "it recovered nothing")

    def test_the_threshold_reaches_the_same_answer_on_a_small_store(self) -> None:
        """The self-tuning form. A store under the threshold should behave as if the knob were on
        without anyone setting it."""
        wide_open = self._recall(MATRIXARK_RETURN_ALL_CANDIDATES="1")
        by_threshold = self._recall(MATRIXARK_RETURN_ALL_CANDIDATE_THRESHOLD="100000")
        self.assertEqual(wide_open, by_threshold)

    def test_a_threshold_below_the_store_leaves_ranking_alone(self) -> None:
        """The other half of the threshold, and the one that keeps it honest: above it, the indexed
        and scored path is what a large store still gets."""
        self.assertEqual(self._recall(), self._recall(MATRIXARK_RETURN_ALL_CANDIDATE_THRESHOLD="1"))

    def test_turning_it_on_is_not_answered_from_the_cache(self) -> None:
        """The knob was unusable without this, and the reason was invisible.

        Retrieval caches a built pack, keyed on the scope, the query, the budgets and several
        policies. The return-all policy was not among them, so a tenant who turned the knob on and
        asked the same question again was handed the pack ranking had already built: same pack id,
        same two facts. It looked exactly like a knob that does nothing, and the only way to see
        otherwise was to reword the question.

        Asserted on the pack id as well as the answer, because equal answers would also be
        produced by a cache that was correctly missed on a store where the knob changes nothing.
        """
        cold = self._packed()
        warm = self._packed(MATRIXARK_RETURN_ALL_CANDIDATES="1")
        self.assertNotEqual(cold["pack_id"], warm["pack_id"],
                            "the same pack was served for two different policies")
        self.assertGreater(len(warm["facts"]), len(cold["facts"]))

    def test_the_default_is_still_the_default(self) -> None:
        """Nothing changes for a deployment that sets neither knob."""
        self.assertEqual(self._recall(), self._recall(MATRIXARK_RETURN_ALL_CANDIDATES="0"))


if __name__ == "__main__":
    unittest.main()
