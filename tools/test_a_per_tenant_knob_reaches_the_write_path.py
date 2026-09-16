#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A per-tenant knob has to reach the write path, and reach it per tenant.

``matrixark_tenant_policy.KNOBS`` holds the knobs a tenant may set, and ``_knob_settings()`` emits
a ``behaviour.<knob>`` row for each so the portal offers it. Two of those knobs resolved correctly
and changed nothing: ``collapse_pipeline_task_rows`` and ``dedupe_index_postings`` had gates that
read the environment variable directly, so ``resolve()`` returned exactly what the tenant asked for
and the write path never asked. ``test_matrixark_policy_gates_wired`` recorded both -- the first in
``KNOWN_UNREAD_KNOBS``, the second in ``KNOWN_UNWIRED`` -- and asked to be struck off when wired.

These tests assert the behaviour rather than the wiring, because the wiring is what a refactor
changes and the behaviour is what a tenant buys. Each one runs a batch that carries TWO tenants
with OPPOSITE policies, so a gate that resolves once for the whole batch fails just as loudly as
one that never resolves at all -- which is the mutation a "simplification" of this code would make.

The environment layer is asserted separately and must not move: this change adds a layer above it,
it does not replace it.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_index_growth_bound as growth  # noqa: E402
import matrixark_pipeline_task_slim as slim  # noqa: E402
import matrixark_tenant_policy as policy  # noqa: E402

POLICY_PATH = "MATRIXARK_TENANT_POLICY_PATH"


class _PolicyFixture(unittest.TestCase):
    """Writes a tenant policy file and restores the environment it found."""

    KNOB = ""          # set by the subclass
    ENV = ""           # the knob's environment variable

    def setUp(self) -> None:
        self.assertTrue(self.KNOB and self.ENV, "subclass must name the knob and its variable")
        self._saved = {k: os.environ.get(k) for k in (POLICY_PATH, self.ENV)}
        self._dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self._dir.cleanup)
        self.addCleanup(self._restore)
        os.environ.pop(self.ENV, None)

    def _restore(self) -> None:
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _write_policy(self, tenants: dict) -> None:
        """Point the resolver at a fresh file. A new path is a cache miss, so this takes effect."""
        path = os.path.join(self._dir.name, f"policy-{len(os.listdir(self._dir.name))}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"defaults": {}, "tenants": tenants, "users": {}}, handle)
        os.environ[POLICY_PATH] = path

    def assert_registry_offers_it(self) -> None:
        """If the knob stopped being tenant-settable these tests are asserting nothing."""
        self.assertIn(self.KNOB, policy.KNOBS, f"{self.KNOB} is no longer a registered knob")
        self.assertEqual(policy.KNOBS[self.KNOB].env, self.ENV,
                         f"{self.KNOB} no longer owns {self.ENV}")


def _posting(tenant: str, ref: str, *, updated: int) -> dict:
    return {
        "record_type": "context_index",
        "scope": {"tenant_id": tenant, "scope_key": f"t={tenant}"},
        "scope_key": f"t={tenant}",
        "index_name": "entity",
        "ref_type": "event",
        "capability": "",
        "data_model": "",
        "ref_hashes": [ref],
        "updated_at_ms": updated,
        "created_at_ms": updated,
    }


class DedupeIndexPostingsIsPerTenant(_PolicyFixture):
    KNOB = "dedupe_index_postings"
    ENV = "MATRIXARK_DEDUPE_INDEX_POSTINGS"

    def test_one_tenant_can_turn_it_off_while_another_keeps_it_on(self) -> None:
        self.assert_registry_offers_it()
        self._write_policy({"keeps": {self.KNOB: False}, "drops": {self.KNOB: True}})

        records = [
            _posting("keeps", "r1", updated=1), _posting("keeps", "r1", updated=2),
            _posting("drops", "r1", updated=1), _posting("drops", "r1", updated=2),
        ]
        out = growth.dedupe_index_postings(records)
        kept = [r for r in out if r["scope"]["tenant_id"] == "keeps"]
        dropped = [r for r in out if r["scope"]["tenant_id"] == "drops"]

        self.assertEqual(len(kept), 2,
                         "the tenant who turned dedup OFF must keep both of its postings")
        self.assertEqual(len(dropped), 1,
                         "the tenant who left dedup ON must have its duplicate collapsed")
        self.assertEqual(dropped[0]["updated_at_ms"], 2, "the newest posting is the one kept")

    def test_the_environment_layer_still_decides_when_no_policy_names_the_knob(self) -> None:
        """The fix adds a layer above the variable; it must not take the variable away."""
        os.environ.pop(POLICY_PATH, None)
        records = [_posting("solo", "r1", updated=1), _posting("solo", "r1", updated=2)]

        os.environ[self.ENV] = "0"
        self.assertIs(growth.dedupe_index_postings(records), records,
                      "the variable set OFF must still disable the lever")

        os.environ[self.ENV] = "1"
        self.assertEqual(len(growth.dedupe_index_postings(records)), 1,
                         "the variable set ON must still enable the lever")

        os.environ[self.ENV] = ""
        self.assertEqual(len(growth.dedupe_index_postings(records)), 1,
                         "an EMPTY value means absent, so the default (ON) applies")


def _task(tenant: str, task_hash: int, status: str, *, updated: int) -> dict:
    return {
        "record_type": slim.PIPELINE_TASK_RECORD_TYPE,
        "task_hash": task_hash,
        "event_id_hash": 900 + task_hash,
        "scope": {"tenant_id": tenant, "scope_key": f"t={tenant}"},
        "scope_key": f"t={tenant}",
        "status": status,
        "idle_commit_deadline_ms": 5,
        "updated_at_ms": updated,
    }


class CollapsePipelineTaskRowsIsPerTenant(_PolicyFixture):
    KNOB = "collapse_pipeline_task_rows"
    ENV = "MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS"

    def test_one_tenant_can_turn_it_off_while_another_keeps_it_on(self) -> None:
        self.assert_registry_offers_it()
        self._write_policy({"keeps": {self.KNOB: False}, "drops": {self.KNOB: True}})

        records = [
            _task("keeps", 1, "summary_completed", updated=10),
            _task("keeps", 1, "summary_completed", updated=20),
            _task("drops", 2, "summary_completed", updated=10),
            _task("drops", 2, "summary_completed", updated=20),
        ]
        out = slim.collapse_pipeline_task_rows(records)
        kept = [r for r in out if r["scope"]["tenant_id"] == "keeps"]
        dropped = [r for r in out if r["scope"]["tenant_id"] == "drops"]

        self.assertEqual(len(kept), 2,
                         "the tenant who turned collapse OFF must keep both re-stamps")
        self.assertEqual(len(dropped), 1,
                         "the tenant who left collapse ON must have its re-stamp collapsed")
        self.assertEqual(dropped[0]["updated_at_ms"], 20, "the newest stamp is the one kept")

    def test_the_environment_layer_still_decides_when_no_policy_names_the_knob(self) -> None:
        os.environ.pop(POLICY_PATH, None)
        records = [_task("solo", 1, "summary_completed", updated=10),
                   _task("solo", 1, "summary_completed", updated=20)]

        os.environ[self.ENV] = "0"
        self.assertIs(slim.collapse_pipeline_task_rows(records), records,
                      "the variable set OFF must still disable the lever")

        os.environ[self.ENV] = "1"
        self.assertEqual(len(slim.collapse_pipeline_task_rows(records)), 1,
                         "the variable set ON must still enable the lever")

        os.environ[self.ENV] = ""
        self.assertEqual(len(slim.collapse_pipeline_task_rows(records)), 1,
                         "an EMPTY value means absent, so the default (ON) applies")


class TheRecordOfUnwiredGatesStaysHonest(unittest.TestCase):
    """The two knobs above must not still be listed as unread, and the list must not be empty.

    ``test_matrixark_policy_gates_wired`` fails in both directions on its own. This asserts the
    half that matters here from the other side, so deleting that file does not quietly delete the
    record along with it.
    """

    def test_the_two_wired_knobs_are_struck_off_and_the_rest_remain(self) -> None:
        import test_matrixark_policy_gates_wired as wired

        for knob in ("collapse_pipeline_task_rows", "dedupe_index_postings"):
            self.assertNotIn(knob, wired.KNOWN_UNREAD_KNOBS,
                             f"{knob} is wired now; it must not still be recorded as unread")
        self.assertNotIn("dedupe_index_postings_enabled", wired.KNOWN_UNWIRED,
                         "the dedupe gate is wired now; it must not still be recorded as unwired")
        # A list that empties itself stops being evidence of anything. `slim_terminal_pipeline_tasks`
        # and `write_secondary_index` are deliberately still there -- see that file for why.
        self.assertTrue(wired.KNOWN_UNREAD_KNOBS,
                        "the unread-knob record went empty; that is a rewrite, not a fix")
        self.assertIn("slim_terminal_pipeline_tasks", wired.KNOWN_UNREAD_KNOBS,
                      "this one is NOT offered on the portal and was left for a product call")

    def test_the_gate_reached_only_through_a_chain_is_counted_and_nothing_else_is(self) -> None:
        """The chain-following in `_callers` has to find lever 0 and nothing but lever 0.

        Vacuity and over-reach are the two ways a widened detector goes wrong, so both are pinned:
        the gate that motivated it must be found AND found only through the chain, and every gate
        still listed as unwired must still be unwired.
        """
        import test_matrixark_policy_gates_wired as wired

        callers = wired._callers()
        sites = callers.get("dedupe_index_postings_enabled") or []
        self.assertTrue(sites,
                        "lever 0's gate is reached from production; it must show a caller")
        self.assertTrue(all("(via " in site for site in sites),
                        "it is reached ONLY through the in-module chain -- a direct caller means "
                        "the shape this widening exists for is gone: %s" % sites)
        self.assertTrue(any("enforce_secondary_index_bounds" in site for site in sites),
                        "the bridge is enforce_secondary_index_bounds: %s" % sites)

        still_unwired = {gate for gate in wired.KNOWN_UNWIRED if not callers.get(gate)}
        self.assertEqual(
            still_unwired, wired.KNOWN_UNWIRED,
            "following the chain reclassified a gate that is still genuinely unwired: %s"
            % sorted(wired.KNOWN_UNWIRED - still_unwired))


if __name__ == "__main__":
    unittest.main()
