# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A cached record should not carry a dict table sized for twice the keys it holds.

CPython grows a dict's table on insert and never shrinks it, and the table a dict ends on depends on
HOW it was built rather than on what it holds. Expansion builds one the expensive way: copy the
encoded record, drop the intern token, then put the bundle's fields back. A record that reaches 21
keys that way keeps a 64-slot table and costs 1,176 B, where one built straight to the same 21 keys
gets 32 slots and costs 640.

That is 536 B on every cached record, for identical keys and identical values -- about 20% of the
whole read cache on a 1 MB skill corpus, where skill_section and resource_chunk are 99.2% of rows.

These tests pin the record as right-sized, and pin the reason, because the fix is one that a later
refactor could undo without changing any behaviour a normal test would see.
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


def right_sized_bytes(key_count: int) -> int:
    """What a dict BUILT to this many keys costs on this interpreter."""
    probe = {}
    for index in range(key_count):
        probe["k%06d" % index] = index
    return sys.getsizeof(probe)


def skill_text(index: int, sections: int = 60) -> str:
    lines = ["# Runbook %d" % index, "", "Draining a queue for case %d." % index, ""]
    for step in range(sections):
        lines += ["## Step %d" % step, "",
                  "Check the queue depth for case %d step %d and drain the oldest partition."
                  % (index, step), ""]
    return "\n".join(lines)


class CachedRecordIsRightSizedTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(tempfile.mkdtemp(prefix="right_sized_"))
        log = cls.root / "events.jsonl"
        writer = MatrixArkLocalAdapter(log)
        for index in range(2):
            writer.ingest({
                "kind": "skill", "scope": SCOPE, "text": skill_text(index),
                "metadata": {"raw_uri": "file:///s/r-%d.md" % index, "title": "r-%d" % index},
            })
        writer.close(timeout_s=1800)
        cls.records = MatrixArkLocalAdapter(log).read_all()

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.root, ignore_errors=True)

    def test_the_interpreter_still_sizes_tables_by_how_they_were_built(self):
        # The control for everything below. If a future CPython shrank tables on delete, or sized
        # them purely by occupancy, this whole file would be pinning something that no longer
        # exists -- and every assertion would pass while measuring nothing.
        grown = {}
        for index in range(40):
            grown["field_%02d" % index] = index
        for index in range(19):
            del grown["field_%02d" % (39 - index)]
        self.assertEqual(len(grown), 21)
        self.assertGreater(
            sys.getsizeof(grown), right_sized_bytes(21),
            "this interpreter no longer over-allocates a grown-then-pruned dict, so the saving "
            "these tests pin does not exist here")

    def test_the_bulk_records_carry_no_spare_table(self):
        for record_type in ("skill_section", "resource_chunk"):
            rows = [r for r in self.records
                    if str(r.get("record_type") or "") == record_type]
            # Named before it is bounded: an assertion about every row passes hardest over none.
            self.assertGreater(len(rows), 8, "no %s rows to check" % record_type)
            for row in rows[:40]:
                self.assertEqual(
                    sys.getsizeof(row), right_sized_bytes(len(row)),
                    "%s holds %d keys in a table sized for more: %d B against %d B right-sized"
                    % (record_type, len(row), sys.getsizeof(row), right_sized_bytes(len(row))))

    def test_right_sizing_changed_no_content(self):
        # The saving must come from the table, never from the contents. A rebuild of a right-sized
        # record is equal to it AND the same size.
        rows = [r for r in self.records
                if str(r.get("record_type") or "") == "skill_section"]
        self.assertGreater(len(rows), 8)
        for row in rows[:20]:
            rebuilt = dict(row.items())
            self.assertEqual(rebuilt, row, "a rebuild changed the record's contents")
            self.assertEqual(sys.getsizeof(rebuilt), sys.getsizeof(row),
                             "the served record is not already right-sized")

    def test_the_interned_fields_really_are_present(self):
        # The oversize came from putting the bundle back after the copy. If the bundle were no
        # longer expanded onto the record, these rows would be right-sized for a different and
        # much worse reason -- the fields would simply be missing.
        rows = [r for r in self.records
                if str(r.get("record_type") or "") == "skill_section"]
        self.assertGreater(len(rows), 8)
        carrying = [r for r in rows if r.get("storage_route")]
        self.assertGreater(len(carrying), 0,
                           "no skill_section carries storage_route, so the record is small "
                           "because expansion stopped, not because the table was right-sized")


if __name__ == "__main__":
    unittest.main()
