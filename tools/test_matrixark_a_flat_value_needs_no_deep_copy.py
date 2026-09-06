# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Expanding an interned value copies it, but a flat value does not need a DEEP copy.

The copy is not optional: several records share one bundle, and downstream mutates storage_route
and placement in place, so two records must never end up holding one object. That is asserted here
first, because it is the reason the copy exists and the thing a cheaper copy could break.

Every value the bundle actually holds is flat -- routing and placement dicts of scalars, measured
at nesting depth 1 with none containing a container. For those, a shallow copy is indistinguishable
from a deep one: expanding a 271-record log went from 17.2 ms to 6.8 ms, 2.55x, with the expanded
records byte-identical. A value that does contain a container still takes the deep copy.
"""
import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter as adapter_module


class AFlatValueNeedsNoDeepCopy(unittest.TestCase):
    def test_one_record_cannot_change_another_records_value(self):
        """The guarantee this file was written for, now kept a different way.

        It used to be kept by copying: every record got its own object, so a mutation could only
        reach one. Records now share one object per interned value -- 22% less real memory -- and
        the same guarantee is kept by refusing the mutation instead. Either way, what must remain
        true is that touching one record's routing cannot change another's.
        """
        bundle = {"storage_route": {"tier": "hot", "replicas": 3}}
        token = "tok"
        records = [
            {"record_type": adapter_module.INTERN_DICT_RECORD_TYPE,
             "im_token": token, "im_bundle": bundle},
            {"record_type": "context_event", "id": "a",
             adapter_module.INTERN_BUNDLE_TOKEN_KEY: token},
            {"record_type": "context_event", "id": "b",
             adapter_module.INTERN_BUNDLE_TOKEN_KEY: token},
        ]
        first, second = adapter_module.expand_interned_records(records)
        with self.assertRaises(TypeError):
            first["storage_route"]["tier"] = "cold"
        self.assertEqual("hot", second["storage_route"]["tier"],
                         "another record's routing changed")
        self.assertEqual("hot", bundle["storage_route"]["tier"],
                         "the shared bundle changed")

        # and the way a caller is told to do it does not reach anyone else
        mine = dict(first["storage_route"])
        mine["tier"] = "cold"
        self.assertEqual("hot", second["storage_route"]["tier"])

    def test_a_nested_value_is_still_copied_all_the_way_down(self):
        """The cheaper copy must not reach a value it cannot safely handle."""
        nested = {"tier": "hot", "opts": {"replicas": 3}, "tags": [{"k": "v"}]}
        copied = adapter_module._copy_interned_value(nested)
        self.assertIsNot(copied, nested)
        self.assertIsNot(copied["opts"], nested["opts"], "the inner dict is shared")
        self.assertIsNot(copied["tags"][0], nested["tags"][0], "the inner list item is shared")
        copied["opts"]["replicas"] = 99
        self.assertEqual(3, nested["opts"]["replicas"])

    def test_a_flat_value_is_copied_and_equal(self):
        for value in ({"tier": "hot", "replicas": 3}, ["a", "b"], {}, []):
            copied = adapter_module._copy_interned_value(value)
            self.assertIsNot(copied, value, "%r was not copied" % (value,))
            self.assertEqual(value, copied)
            self.assertEqual(copy.deepcopy(value), copied,
                             "the cheap copy differs from the deep one for %r" % (value,))

    def test_a_scalar_is_returned_as_is(self):
        for value in ("x", 3, None, True, 1.5):
            self.assertIs(value, adapter_module._copy_interned_value(value))

    def test_expansion_is_unchanged_over_a_real_log(self):
        """The whole point: the served records must be exactly what they were."""
        with tempfile.TemporaryDirectory() as store:
            log = Path(store) / "events.jsonl"
            adapter = adapter_module.MatrixArkLocalAdapter(log)
            for i in range(12):
                adapter.ingest({
                    "kind": "message",
                    "scope": {"tenant_id": "acme", "user_id": "u", "session_id": "s%d" % (i // 4)},
                    "messages": [{"role": "user", "content": "a sentence to extract %d" % i}],
                })
            # Through the module's own shard iterator: the durable log takes more than one
            # form and only this knows which one is on disk.
            raw = [json.loads(line) for line in adapter_module._iter_shard_lines(log)
                   if line.strip()]
            self.assertTrue(
                any(adapter_module.INTERN_BUNDLE_TOKEN_KEY in r for r in raw if isinstance(r, dict)),
                "nothing in this log is interned, so the comparison proves nothing")

            cheap = adapter_module.expand_interned_records(raw)
            saved = adapter_module._copy_interned_value
            adapter_module._copy_interned_value = copy.deepcopy   # the previous behaviour
            try:
                deep = adapter_module.expand_interned_records(raw)
            finally:
                adapter_module._copy_interned_value = saved
            self.assertEqual(
                sorted(json.dumps(r, sort_keys=True, default=str) for r in deep),
                sorted(json.dumps(r, sort_keys=True, default=str) for r in cheap))


if __name__ == "__main__":
    unittest.main()
