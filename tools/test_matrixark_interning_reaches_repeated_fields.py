# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Interning covers the fields that repeat, and still refuses the ones raw-log paths match on.

A census of the event log found 30.4% of its bytes in fields holding a single value across every
record of their type -- on a corpus varied across tenants, users and sessions so uniformity could
not manufacture the result. Interning was already enabled and not reaching them, because it reads
INTERN_METADATA_FIELDS and that list held five routing fields.

The exclusions matter more than the additions, so they are asserted by name here: a field matched
on the UNEXPANDED log cannot be interned, because the token is what the matcher would see.
"""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter as adapter_module


class InterningCoversTheRepeatedFields(unittest.TestCase):
    def test_it_never_interns_what_a_raw_path_matches_on(self):
        """Each of these is read off the log BEFORE expansion, so a token would break the match."""
        fields = set(adapter_module.INTERN_METADATA_FIELDS)
        self.assertNotIn("scope_key", fields,
                         "scope-level tombstones match scope_key on unexpanded records in "
                         "purge_tombstones; interning it makes them miss and tombstoned records "
                         "survive a purge")
        self.assertNotIn("record_type", fields,
                         "every raw filter, including the tombstone scan, matches record_type")
        for identity in ("model_kind", "model_ref", "model_name", "model_hash",
                         "provider", "execution_mode"):
            self.assertNotIn(identity, fields,
                             "_seed_model_registry_seen_locked reads %s off the unexpanded log"
                             % identity)
        self.assertNotIn(adapter_module.INTERN_BUNDLE_TOKEN_KEY, fields,
                         "the bundle's own token cannot be part of what it names")

    def test_the_list_reaches_past_routing(self):
        """The defect: the list held only routing fields while the repetition was elsewhere."""
        fields = set(adapter_module.INTERN_METADATA_FIELDS)
        for repeated in ("storage_record_kind", "storage_part", "source_ref_type"):
            self.assertIn(repeated, fields, "%s repeats on every record of its type" % repeated)

    def test_a_log_reads_back_exactly_what_went_in(self):
        """Interning is only ever a wire format; the served records must be unchanged."""
        with tempfile.TemporaryDirectory() as store:
            log = Path(store) / "events.jsonl"
            adapter = adapter_module.MatrixArkLocalAdapter(log)
            for i in range(40):
                adapter.ingest({
                    "kind": "message",
                    "scope": {"tenant_id": "t%d" % (i % 3), "user_id": "u%d" % (i % 5),
                              "session_id": "s%d" % (i % 7)},
                    "messages": [{"role": ["user", "assistant", "tool"][i % 3],
                                  "content": "a sentence with enough words to be extracted %d" % i}],
                })
            served = adapter.read_all()
            self.assertTrue(served, "nothing was ingested")
            for record in served:
                self.assertNotIn(adapter_module.INTERN_BUNDLE_TOKEN_KEY, record,
                                 "a token reached a served record; expansion is incomplete")

            with adapter_module._LOCAL_READ_CACHE_LOCK:
                adapter_module._LOCAL_READ_CACHE.clear()
            cold = adapter_module.MatrixArkLocalAdapter(log).read_all()
            self.assertEqual(
                sorted(json.dumps(r, sort_keys=True, default=str) for r in served),
                sorted(json.dumps(r, sort_keys=True, default=str) for r in cold),
                "a reader with no process state saw different records")

    def test_the_sidecar_count_stays_far_below_the_record_count(self):
        """The failure mode to watch. Fields share ONE bundle, so a field that varies per record
        multiplies sidecars instead of removing bytes -- that would make the log bigger."""
        with tempfile.TemporaryDirectory() as store:
            log = Path(store) / "events.jsonl"
            adapter = adapter_module.MatrixArkLocalAdapter(log)
            for i in range(40):
                adapter.ingest({
                    "kind": "message",
                    "scope": {"tenant_id": "t%d" % (i % 3), "user_id": "u%d" % (i % 5),
                              "session_id": "s%d" % (i % 7)},
                    "messages": [{"role": ["user", "assistant", "tool"][i % 3],
                                  "content": "a sentence with enough words to be extracted %d" % i}],
                })
            sidecars = data = 0
            for line in log.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                record = json.loads(line)
                if str(record.get("record_type") or "") == adapter_module.INTERN_DICT_RECORD_TYPE:
                    sidecars += 1
                else:
                    data += 1
            self.assertGreater(data, 0)
            self.assertLess(sidecars, data / 4.0,
                            "%d sidecars for %d records -- a field in the bundle is varying per "
                            "record, which costs bytes instead of saving them" % (sidecars, data))


if __name__ == "__main__":
    unittest.main()
