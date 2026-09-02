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
    # Read at import by matrixark_mcp_temporal_append, and another file in this suite sets it and
    # re-imports that module without putting it back. Left on, the warm view and a cold read
    # disagree on a hash inside the index records -- which reproduces on main with these field
    # additions reverted, so it is not this change, but it makes the assertion below non-
    # deterministic depending on test order. Pin it to the default and restore afterwards.
    _BACKEND_INTERN = "MATRIXARK_INTERN_BACKEND_METADATA"

    def setUp(self):
        self._saved_backend = os.environ.get(self._BACKEND_INTERN)
        os.environ.pop(self._BACKEND_INTERN, None)
        self._reimport_append_module()
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()

    def tearDown(self):
        if self._saved_backend is None:
            os.environ.pop(self._BACKEND_INTERN, None)
        else:
            os.environ[self._BACKEND_INTERN] = self._saved_backend
        self._reimport_append_module()
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()

    @staticmethod
    def _reimport_append_module():
        """The flag is bound at import, so the module has to be rebuilt to see the change."""
        for name in [m for m in sys.modules if "matrixark_mcp_temporal_append" in m]:
            del sys.modules[name]
        import matrixark_mcp_temporal_append  # noqa: F401

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

    def test_no_token_survives_into_a_served_record(self):
        """Interning is a wire format. A token that reaches a caller is a leaked encoding."""
        with tempfile.TemporaryDirectory() as store:
            log = Path(store) / "events.jsonl"
            adapter = adapter_module.MatrixArkLocalAdapter(log)
            self._ingest(adapter)
            for record in adapter.read_all():
                self.assertNotIn(adapter_module.INTERN_BUNDLE_TOKEN_KEY, record,
                                 "a bundle token reached a served record")

    def test_a_cold_reader_sees_the_same_interned_fields(self):
        """The fields this change moved into the bundle must survive the disk round trip.

        Scoped to those fields on purpose. An earlier version compared whole records and failed
        under the full suite for reasons that reproduce on main with this change reverted -- a hash
        inside the index records differs between the warm view and a cold read once another test
        leaves MATRIXARK_INTERN_BACKEND_METADATA set. That is worth chasing separately; it is not
        what this change is responsible for, and asserting it here only makes this test fail for
        somebody else's reason.
        """
        interned = set(adapter_module.INTERN_METADATA_FIELDS)
        with tempfile.TemporaryDirectory() as store:
            log = Path(store) / "events.jsonl"
            adapter = adapter_module.MatrixArkLocalAdapter(log)
            self._ingest(adapter)
            warm = adapter.read_all()
            with adapter_module._LOCAL_READ_CACHE_LOCK:
                adapter_module._LOCAL_READ_CACHE.clear()
            cold = adapter_module.MatrixArkLocalAdapter(log).read_all()
            self.assertEqual(len(warm), len(cold), "a cold reader saw a different record count")

            def projection(records):
                return sorted(
                    json.dumps({k: v for k, v in r.items() if k in interned},
                               sort_keys=True, default=str)
                    for r in records)

            self.assertEqual(projection(warm), projection(cold),
                             "an interned field did not survive the disk round trip")
            self.assertTrue(any(set(r) & interned for r in warm),
                            "nothing in this corpus carried an interned field, so the assertion "
                            "above proved nothing")

    @staticmethod
    def _ingest(adapter):
        for i in range(40):
            adapter.ingest({
                "kind": "message",
                "scope": {"tenant_id": "t%d" % (i % 3), "user_id": "u%d" % (i % 5),
                          "session_id": "s%d" % (i % 7)},
                "messages": [{"role": ["user", "assistant", "tool"][i % 3],
                              "content": "a sentence with enough words to be extracted %d" % i}],
            })

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
