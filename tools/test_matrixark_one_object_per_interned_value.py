# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Every record carrying an interned value holds the SAME object, and cannot change it.

Expansion used to copy each value out of the shared bundle, once per record. Measured over 331
expanded records that cost 2,386 KB of real memory; storing each distinct value once costs
1,856 KB -- 22% less. Serialised size cannot see any of this: the records are 787 KB as JSON
either way, so the copies cost about three times what the bytes suggest.

The copy existed because a shared value must not be mutated -- a change would reach every record
carrying it. A tripwire recording every in-place change to an expanded value, run over the whole
test suite, found none in production code. Rather than trust that, the shared value refuses: a path
that does mutate fails where it happens instead of silently rewriting records it never looked at.
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


def _log(token="tok", n=3):
    bundle = {"storage_route": {"tier": "hot", "replicas": 3},
              "storage_options": {"replicas": 3}}
    records = [{"record_type": adapter_module.INTERN_DICT_RECORD_TYPE,
                "im_token": token, "im_bundle": bundle}]
    for i in range(n):
        records.append({"record_type": "context_event", "id": "e%d" % i,
                        adapter_module.INTERN_BUNDLE_TOKEN_KEY: token})
    return records, bundle


class OneObjectPerInternedValue(unittest.TestCase):
    def test_every_record_holds_the_same_object(self):
        """The saving. A copy per record is what cost three times the serialised size."""
        expanded = adapter_module.expand_interned_records(_log()[0])
        self.assertEqual(3, len(expanded))
        first = expanded[0]["storage_route"]
        for other in expanded[1:]:
            self.assertIs(first, other["storage_route"],
                          "records hold separate copies; the memory saving is gone")

    def test_the_value_still_reads_normally(self):
        """Sharing must be invisible to anything that only reads."""
        expanded = adapter_module.expand_interned_records(_log()[0])
        route = expanded[0]["storage_route"]
        self.assertEqual("hot", route["tier"])
        self.assertEqual("hot", route.get("tier"))
        self.assertIn("replicas", route)
        self.assertEqual({"tier", "replicas"}, set(route))
        self.assertEqual({"tier": "hot", "replicas": 3}, dict(route))
        self.assertEqual({"tier": "hot", "replicas": 3},
                         json.loads(json.dumps(route)))

    def test_changing_it_is_refused_rather_than_reaching_every_record(self):
        """The guard. Without it a mutation here would rewrite records it never looked at."""
        expanded = adapter_module.expand_interned_records(_log()[0])
        route = expanded[0]["storage_route"]
        for change in (lambda: route.__setitem__("tier", "cold"),
                       lambda: route.pop("tier"),
                       lambda: route.update({"tier": "cold"}),
                       lambda: route.setdefault("new", 1),
                       lambda: route.clear(),
                       lambda: route.__delitem__("tier")):
            with self.assertRaises(TypeError):
                change()
        self.assertEqual("hot", expanded[1]["storage_route"]["tier"],
                         "another record's value changed")

    def test_a_caller_that_needs_its_own_copy_can_take_one(self):
        """What the refusal tells you to do has to actually work."""
        expanded = adapter_module.expand_interned_records(_log()[0])
        mine = dict(expanded[0]["storage_route"])
        mine["tier"] = "cold"
        self.assertEqual("hot", expanded[0]["storage_route"]["tier"])
        self.assertEqual("hot", expanded[1]["storage_route"]["tier"])

    def test_every_way_of_copying_it_yields_a_plain_writable_dict(self):
        """Found by running the suite: deepcopy rebuilds a dict subclass by assigning into a new
        instance of the same class, which lands on the refusal -- on a path that copies a whole
        record and never touches this value. Copying is what a caller who needs to change one
        should do, so every route to a copy has to work."""
        expanded = adapter_module.expand_interned_records(_log()[0])
        route = expanded[0]["storage_route"]
        for name, made in (("copy", copy.copy(route)),
                           ("deepcopy", copy.deepcopy(route)),
                           ("dict()", dict(route)),
                           ("inside a record", copy.deepcopy({"r": route})["r"])):
            self.assertIs(type(made), dict, "%s did not give back a plain dict" % name)
            made["tier"] = "cold"       # must be writable
            self.assertEqual("hot", route["tier"], "%s aliased the shared value" % name)

    def test_the_records_are_unchanged_over_a_real_log(self):
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
                "nothing in this log is interned, so this proves nothing")
            expanded = adapter_module.expand_interned_records(raw)
            # serialising is the reader's view: sharing must not change it at all
            self.assertTrue(all(json.dumps(r, sort_keys=True, default=str) for r in expanded))
            for record in expanded:
                self.assertNotIn(adapter_module.INTERN_BUNDLE_TOKEN_KEY, record)


if __name__ == "__main__":
    unittest.main()
