# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""CPython sizes a dict by KEY COUNT in steps, so a record's container cost is a cliff, not a slope.

Measured on the read cache, the record dicts themselves are 46.8% of live heap -- more than any
value they hold. A record sitting one key over a step pays the whole next one: `resource_import_task`
was at 44 keys with the step at 43, so two keys holding the constants 10 and 10 cost 1,096 B per row.

These tests pin the record types that sit closest to a boundary, so the next field added to one of
them fails here instead of silently costing ~50-85% more container memory per row.

The step boundaries are MEASURED from the running interpreter rather than tabulated, so the guard
stays true if CPython changes its growth policy.
"""
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

try:  # package path
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
except ImportError:
    from matrixark_mcp_local_adapter import MatrixArkLocalAdapter

SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}


def dict_bytes(key_count: int) -> int:
    """What this interpreter charges for a dict of ``key_count`` keys."""
    probe = {}
    for index in range(key_count):
        probe["k%06d" % index] = index
    return sys.getsizeof(probe)


def step_ceiling(key_count: int) -> int:
    """The largest key count that costs the same as ``key_count`` -- the room left before a jump."""
    size = dict_bytes(key_count)
    ceiling = key_count
    while dict_bytes(ceiling + 1) == size:
        ceiling += 1
    return ceiling


def skill_text(index: int) -> str:
    lines = ["# Runbook %d" % index, "", "Draining a stuck queue for case %d." % index, ""]
    for step in range(12):
        lines += ["## Step %d" % step, "",
                  "Check the queue depth for case %d step %d and drain it." % (index, step), ""]
    return "\n".join(lines)


class RecordStaysUnderItsDictStepTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(tempfile.mkdtemp(prefix="dict_step_"))
        log = cls.root / "events.jsonl"
        writer = MatrixArkLocalAdapter(log)
        for index in range(3):
            writer.ingest({
                "kind": "skill", "scope": SCOPE, "text": skill_text(index),
                "metadata": {"raw_uri": "file:///s/r-%d.md" % index, "title": "r-%d" % index},
            })
        writer.close(timeout_s=600)
        cls.records = MatrixArkLocalAdapter(log).read_all()

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.root, ignore_errors=True)

    def rows(self, record_type: str) -> list:
        found = [r for r in self.records if str(r.get("record_type") or "") == record_type]
        # An upper bound on key count passes hardest when there are no rows at all, so require the
        # type to be present before believing anything the bound says about it.
        self.assertGreater(len(found), 0, "no %s rows: the bound below would pass vacuously"
                           % record_type)
        return found

    def test_the_step_helper_actually_sees_steps(self):
        # Control for the helpers themselves: if dict sizing were linear, every assertion built on
        # step_ceiling would be meaningless.
        self.assertGreater(step_ceiling(11), 11, "no headroom found at 11 keys -- helper is wrong")
        self.assertLess(dict_bytes(step_ceiling(11)), dict_bytes(step_ceiling(11) + 1),
                        "crossing a ceiling did not cost more, so there is no step to guard")

    def test_the_import_task_stays_below_its_step(self):
        rows = self.rows("resource_import_task")
        widest = max(len(r) for r in rows)
        ceiling = step_ceiling(widest)
        self.assertLessEqual(
            widest, ceiling,
            "resource_import_task has %d keys; %d is the last that costs %d B, and one more costs "
            "%d B per row" % (widest, ceiling, dict_bytes(ceiling), dict_bytes(ceiling + 1)))
        # It sat at 44 against a step at 43. Pin that it is back under, naming the number, so a
        # regression says what it cost rather than just that a count changed.
        self.assertLessEqual(widest, 42,
                             "resource_import_task is back over CPython's 43-key step: %d keys "
                             "costs %d B per row against %d at 42"
                             % (widest, dict_bytes(widest), dict_bytes(42)))

    def test_nothing_was_lost_when_the_caps_moved_out(self):
        # The two removed keys are still on the record, inside its own metrics dict. Dropping data
        # would be a different change from dropping a duplicate.
        for row in self.rows("resource_import_task"):
            metrics = row.get("metrics")
            self.assertIsInstance(metrics, dict, "the task lost its metrics dict")
            for field in ("index_cap_per_chunk", "index_cap_per_fact"):
                self.assertIn(field, metrics,
                              "%s is not in metrics, so removing the top-level copy lost it"
                              % field)
            self.assertNotIn("index_cap_per_chunk", row,
                             "the top-level duplicate is back")

    def test_the_two_most_numerous_types_have_headroom(self):
        # skill_section and resource_chunk are the bulk of any corpus and sit at 21 keys, which is
        # the LAST slot before the next step. One added field costs them ~84% more container each.
        for record_type in ("skill_section", "resource_chunk"):
            rows = self.rows(record_type)
            widest = max(len(r) for r in rows)
            ceiling = step_ceiling(widest)
            self.assertLessEqual(
                widest, ceiling,
                "%s has %d keys, over the %d that cost %d B; the next step is %d B per row"
                % (record_type, widest, ceiling, dict_bytes(ceiling), dict_bytes(ceiling + 1)))


if __name__ == "__main__":
    unittest.main()
