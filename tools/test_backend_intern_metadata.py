#!/usr/bin/env python3
"""Tests for the backend interning codec.

A codec that loses a field on a crash is worse than the bytes it saves, so these check the
properties the JSONL codec claims and that this one must also hold:

  1. flag OFF  -> input returned unchanged (byte-identical to today)
  2. flag ON   -> the field is replaced by a token and a sidecar carries the value
  3. round-trip -> encode then expand yields the ORIGINAL record
  4. sidecar FIRST -> the dict record precedes every record referencing it
  5. dedup -> one sidecar per distinct value, not per record
  6. expansion is unconditional -> a token written while ON still expands after the flag goes OFF
  7. no-op on old data -> a record with no token key is untouched
  8. sidecars are storage -> they never surface as data records
"""
import io
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def fresh(flag):
    """Reimport the module with the flag set, since it is read at import time."""
    os.environ["MATRIXARK_INTERN_BACKEND_METADATA"] = flag
    for mod in list(sys.modules):
        if "matrixark_mcp_temporal_append" in mod:
            del sys.modules[mod]
    import matrixark_mcp_temporal_append as m
    return m


OPTS = {"route": "shared_store_async", "tier": "hot", "replicas": 3}


class BackendInternTest(unittest.TestCase):

    def test_flag_off_is_identity(self):
        m = fresh("0")
        records = [{"record_type": "context_entity", "storage_options": dict(OPTS)}]
        self.assertEqual(m.backend_intern_records(records, set()), records)

    def test_flag_on_replaces_field_with_token(self):
        m = fresh("1")
        out = m.backend_intern_records(
            [{"record_type": "context_entity", "storage_options": dict(OPTS)}], set())
        sidecars = [r for r in out if r["record_type"] == m.BACKEND_INTERN_DICT_RECORD_TYPE]
        data = [r for r in out if r["record_type"] != m.BACKEND_INTERN_DICT_RECORD_TYPE]
        self.assertEqual(len(sidecars), 1)
        self.assertEqual(sidecars[0]["bi_value"], OPTS)
        self.assertNotIn("storage_options", data[0])
        self.assertIn(m.BACKEND_INTERN_TOKEN_KEY, data[0])

    def test_round_trip_restores_the_original(self):
        m = fresh("1")
        original = {"record_type": "context_entity", "entity_hash": 7,
                    "storage_options": dict(OPTS)}
        encoded = m.backend_intern_records([dict(original)], set())
        expanded = m.backend_expand_records(encoded)
        self.assertEqual(expanded, [original])

    def test_sidecar_precedes_every_referencing_record(self):
        m = fresh("1")
        out = m.backend_intern_records(
            [{"record_type": "context_entity", "storage_options": dict(OPTS)} for _ in range(5)],
            set())
        first_data = next(i for i, r in enumerate(out)
                          if r["record_type"] != m.BACKEND_INTERN_DICT_RECORD_TYPE)
        last_sidecar = max(i for i, r in enumerate(out)
                           if r["record_type"] == m.BACKEND_INTERN_DICT_RECORD_TYPE)
        self.assertLess(last_sidecar, first_data)

    def test_one_sidecar_per_distinct_value(self):
        m = fresh("1")
        out = m.backend_intern_records(
            [{"record_type": "context_entity", "storage_options": dict(OPTS)} for _ in range(20)],
            set())
        sidecars = [r for r in out if r["record_type"] == m.BACKEND_INTERN_DICT_RECORD_TYPE]
        self.assertEqual(len(sidecars), 1, "20 identical values must share ONE sidecar")

    def test_expansion_still_works_after_the_flag_goes_off(self):
        m_on = fresh("1")
        encoded = m_on.backend_intern_records(
            [{"record_type": "context_entity", "storage_options": dict(OPTS)}], set())
        m_off = fresh("0")
        expanded = m_off.backend_expand_records(encoded)
        self.assertEqual(expanded[0].get("storage_options"), OPTS)

    def test_records_without_a_token_are_untouched(self):
        m = fresh("1")
        old = [{"record_type": "context_entity", "storage_options": dict(OPTS)}]
        self.assertEqual(m.backend_expand_records([dict(old[0])]), old)

    def test_sidecars_never_surface_as_data(self):
        m = fresh("1")
        encoded = m.backend_intern_records(
            [{"record_type": "context_entity", "storage_options": dict(OPTS)}], set())
        expanded = m.backend_expand_records(encoded)
        self.assertTrue(all(r["record_type"] != m.BACKEND_INTERN_DICT_RECORD_TYPE
                            for r in expanded))
        self.assertEqual(len(expanded), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
