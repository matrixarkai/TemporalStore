# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The near-duplicate setting reaches both packers, including the one the gateway uses.

`matrixark_gateway_config` offers `retrieval.near_duplicate_overlap_threshold`, describes it as
"A candidate this similar to an already-selected, higher-ranked one is dropped. Stops a pack paying
twice for one fact.", and defaults it to 0.85 -- on. `matrixark_load_config` maps it to
`MATRIXARK_NEAR_DUPLICATE_OVERLAP_THRESHOLD` and applies it to the environment.

`matrixark_mcp_budget_pack`, which the gateway reaches through `matrixark_mcp_budget_policies` from
`matrixark_mcp_server`, had no near-duplicate logic at all -- the word did not appear in the file.
Measured on four candidates, three of them near-duplicates of one another:

    matrixark_mcp_core_ref_selection   selected ['d', 'a']              near_duplicate dropped 2
    matrixark_mcp_budget_pack          selected ['d', 'a', 'c', 'b']    no such reason

So a deployment that read its own settings page believed near-duplicate suppression was on, and on
the gateway path it was not. This is the shape of mx#959 -- a surface advertising a knob nothing
reads -- and the check below is written to catch it the same way: not "does the packer have a
threshold parameter" but "does the SETTING the gateway offers change what the packer selects".
"""
from __future__ import annotations

import importlib
import unittest

SETTING = "retrieval.near_duplicate_overlap_threshold"
ENV = "MATRIXARK_NEAR_DUPLICATE_OVERLAP_THRESHOLD"

SHARED = "the deploy went out on tuesday and p99 latency improved to 41 milliseconds"

#: Three of these are near-duplicates of one another; "distinct" is not, and is what shows the
#: suppression is selective rather than simply dropping candidates.
CANDIDATES = [
    {"ref_id": "first", "text": SHARED, "score": 0.90},
    {"ref_id": "reworded", "text": SHARED + " roughly", "score": 0.85},
    {"ref_id": "abbreviated",
     "text": "the deploy went out on tuesday and p99 latency improved to 41 ms", "score": 0.80},
    {"ref_id": "distinct", "text": "shard rebalance completed with no errors", "score": 0.75},
]


def _import(name: str):
    try:
        return importlib.import_module("tools." + name)
    except ImportError:
        return importlib.import_module(name)


def _candidates():
    return [dict(c, ref_hash="h_" + c["ref_id"], ref_type="event", context_class="event",
                 memory_scope="session", session_continuity="same_session",
                 metadata={"ref_type": "event"})
            for c in CANDIDATES]


def _select(fn, **kwargs):
    selected, _tokens, audit = fn(_candidates(), [], max_context_tokens=4000,
                                  auxiliary_quota=0, question_type="fact", **kwargs)
    return [ref["ref_id"] for ref in selected], audit


class TheNearDuplicateSettingReachesBothPackersTest(unittest.TestCase):

    def setUp(self) -> None:
        _import("matrixark_mcp_local_adapter")            # settles the circular imports
        self.gateway_packer = _import("matrixark_mcp_budget_pack").select_token_budgeted_refs
        self.retrieve_packer = _import(
            "matrixark_mcp_core_ref_selection").select_token_budgeted_refs

    def test_the_gateway_packer_drops_near_duplicates(self) -> None:
        selected, audit = _select(self.gateway_packer)
        self.assertEqual(
            ["distinct", "first"], selected,
            "the packer the gateway reaches selected %s. It kept refs that near-duplicate a "
            "higher-ranked one, which is exactly what the setting it advertises says it does not "
            "do" % selected)
        self.assertEqual(2, audit.get("near_duplicate"),
                         "the drop was not attributed to near_duplicate, so an operator reading "
                         "the audit cannot see why the pack is smaller")

    def test_both_packers_select_the_same_refs(self) -> None:
        gateway, _ = _select(self.gateway_packer)
        retrieve, _ = _select(self.retrieve_packer)
        self.assertEqual(
            retrieve, gateway,
            "the two live packers select different refs for the same candidates, so a request "
            "served through the gateway gets a different pack from one served through retrieve")

    def test_the_threshold_is_what_decides_it(self) -> None:
        """Control. Without this, the assertions above pass for a packer that always drops."""
        off, off_audit = _select(self.gateway_packer, near_duplicate_overlap_threshold=0.0)
        self.assertEqual(
            ["distinct", "first", "abbreviated", "reworded"], off,
            "a threshold of 0 must turn the suppression OFF -- that is how an operator disables "
            "it, and if it drops anyway the setting is decorative in the other direction")
        self.assertEqual(0, off_audit.get("near_duplicate"))

        exact, _ = _select(self.gateway_packer, near_duplicate_overlap_threshold=1.0)
        self.assertEqual(
            len(CANDIDATES), len(exact),
            "a threshold of 1 requires identical token sets, and none of these are identical, so "
            "nothing should be dropped -- if something is, the comparison is not the ratio it "
            "claims to be")

    def test_the_default_the_packer_uses_is_the_one_the_gateway_advertises(self) -> None:
        """The mx#959 shape: a settings page and the code that consumes it must not disagree."""
        import inspect

        config = _import("matrixark_gateway_config")
        declared = None
        for setting in getattr(config, "SETTINGS", []):
            if getattr(setting, "key", None) == SETTING:
                declared = setting
                break
        self.assertIsNotNone(
            declared, "%s is no longer offered by matrixark_gateway_config. If the setting was "
                      "withdrawn, this file should go with it; if it was renamed, the packers "
                      "need to follow it" % SETTING)
        self.assertEqual(
            ENV, getattr(declared, "env", None),
            "the setting no longer maps to %s, which is the variable the default is read from" % ENV)

        runtime = _import("matrixark_mcp_runtime_config")
        self.assertAlmostEqual(
            float(declared.default), runtime.DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD, places=6,
            msg="the gateway advertises a default of %s and the code applies %s"
                % (declared.default, runtime.DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD))

        for name, fn in (("gateway", self.gateway_packer), ("retrieve", self.retrieve_packer)):
            default = inspect.signature(fn).parameters["near_duplicate_overlap_threshold"].default
            self.assertAlmostEqual(
                runtime.DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD, float(default), places=6,
                msg="the %s packer defaults its threshold to %s rather than to the value the "
                    "setting resolves to" % (name, default))

    def test_one_module_owns_the_comparison(self) -> None:
        scoring = _import("matrixark_mcp_scoring")
        selection = _import("matrixark_mcp_core_ref_selection")
        budget = _import("matrixark_mcp_budget_pack")
        for module, label in ((selection, "core_ref_selection"), (budget, "budget_pack")):
            self.assertIs(
                scoring.normalized_token_set, module.normalized_token_set,
                "%s uses its own normalized_token_set, so the two packers can tokenise the same "
                "text differently and disagree about what a duplicate is" % label)
            self.assertIs(
                scoring.near_duplicate_overlap_ratio, module.near_duplicate_overlap_ratio,
                "%s uses its own near_duplicate_overlap_ratio" % label)


if __name__ == "__main__":
    unittest.main()
