# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The near-duplicate setting reaches both packers, including the one the gateway uses.

`matrixark_gateway_config` OFFERED `retrieval.near_duplicate_overlap_threshold`, described it as
"A candidate this similar to an already-selected, higher-ranked one is dropped. Stops a pack paying
twice for one fact.", and defaulted it to 0.85 -- on. `matrixark_load_config` mapped it to
`MATRIXARK_NEAR_DUPLICATE_OVERLAP_THRESHOLD` and applied it to the environment.

BOTH ARE RETIRED IN matrixarkai#1829 and this file did NOT go with them, although one assertion in it
used to say it should. What that assertion protected is the mx#959 shape -- a surface advertising a
knob nothing reads -- and the surface is what went. The threshold is still a parameter of both
packers, still defaults to the build constant, and still decides what a pack contains, so every rule
here has the subject it always had and is now asked of
`matrixark_mcp_runtime_config.DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD` instead of a portal row.
The one thing no longer true is that a deployment can change it.

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

    def test_the_setting_is_not_advertised_any_more(self) -> None:
        """The mx#959 shape from the other side. It read "a page and the code must not disagree";
        the page has no such row since matrixarkai#1829, so what must hold is that NOTHING offers it --
        a row left behind in either registry would advertise a control that cannot be reached."""
        config = _import("matrixark_gateway_config")
        self.assertNotIn(
            SETTING, {getattr(s, "key", None) for s in getattr(config, "SETTINGS", [])},
            "%s is offered again. It was retired along with its variable, so a row here "
            "advertises a knob nothing reads -- the defect this file exists for" % SETTING)
        loader = _import("matrixark_load_config")
        self.assertNotIn(
            ENV, set(getattr(loader, "ENV_MAP", {}).values()),
            "%s is mapped by the config loader again" % ENV)

    def test_the_default_the_packers_use_is_the_build_constant(self) -> None:
        """What the retired row used to anchor: both packers default to the one number, and that
        number is the build constant now rather than a declared default."""
        import inspect

        runtime = _import("matrixark_mcp_runtime_config")
        self.assertAlmostEqual(
            0.85, runtime.DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD, places=6,
            msg="the build constant is %s. It is 0.85 because that is what the shipped config "
                "pinned and what both packers were measured at"
                % runtime.DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD)

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
