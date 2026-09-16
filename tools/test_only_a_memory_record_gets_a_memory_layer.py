#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`attach_memory_layer` decided from ref_type, so anything carrying one got a memory layer.

`candidate_memory_layer_name` reads `ref_type`, NOT `record_type`. The live
`matrixark_codex_hook.attach_memory_layer` stamped `memory_layer` on whatever it was handed as long
as that name came back something other than "unknown" -- so a `matrixark_async_pipeline_task`
carrying `ref_type: "event"` came back stamped `session_neutral_event`. Measured over the cross
product of a non-memory record_type with the fields that drive the layer: **5,760 of 6,720**.

Nothing was mis-stamped, because both callers pass a memory record: the `context_embedding` built
for event text, and the `context_event` at the fast-hook projection -- whose sibling
`matrixark_async_pipeline_task`, built three lines later, is pointedly NOT passed through. The
guard is what keeps that true of the next caller.

IT WAS ALREADY WRITTEN. `matrixark_mcp_local_batch_extract_runtime` holds a copy of this function
with exactly this allowlist, and production cannot reach that module. This is the case the tree
warns about in [read the orphan before deleting it, it may be the complete one]: the unreachable
copy was the more complete one, and the diverged-copy ratchet had recorded the pair for months
without anyone asking which way the difference went.

ASSERTED IN BOTH DIRECTIONS, because only one of them is the interesting failure:

  * a non-memory record carrying a ref_type is returned UNTOUCHED -- the property this creates;
  * every one of the six memory types still gets its layer -- because a guard that also stops the
    real callers working would pass the first test and break the product. The two live call sites
    are named explicitly so a reader can check them against the source.
"""
from __future__ import annotations

import itertools
import unittest

try:
    from tools import matrixark_mcp_core  # noqa: F401  - enter the import cycle from the core side
    from tools.matrixark_codex_hook import attach_memory_layer, candidate_memory_layer_name
except ImportError:  # run from tools/
    import matrixark_mcp_core  # noqa: F401
    from matrixark_codex_hook import attach_memory_layer, candidate_memory_layer_name

#: The six the guard admits. Kept here so a change to the allowlist has to change this list too.
MEMORY_RECORD_TYPES = ("context_event", "context_entity", "context_segment", "context_summary",
                       "context_compression_event", "context_embedding")

#: Record types that are not memory. `matrixark_async_pipeline_task` is the one that matters: it is
#: built three lines after the second call site, from the same data.
OTHER_RECORD_TYPES = ("matrixark_async_pipeline_task", "matrixark_idempotency",
                      "context_summary_dirty", "model_registry", "matrixark_api_key",
                      "context_node", "serving_record", "", "something_added_later")

REF_TYPES = ("event", "entity", "segment", "summary", "compression")
MEMORY_SCOPES = (None, "session", "cross_session", "tenant")


class OnlyAMemoryRecordGetsAMemoryLayer(unittest.TestCase):

    def test_the_layer_name_is_decided_by_ref_type_not_record_type(self):
        """The reason the guard is needed, asserted rather than described.

        If this ever stops being true -- if the namer starts reading record_type -- the guard is
        belt and braces and this file is worth less than it costs.
        """
        as_task = candidate_memory_layer_name(
            {"record_type": "matrixark_async_pipeline_task", "ref_type": "event"})
        as_event = candidate_memory_layer_name(
            {"record_type": "context_event", "ref_type": "event"})
        self.assertEqual(
            as_event, as_task,
            "candidate_memory_layer_name now distinguishes record_type (%r vs %r), so the guard in "
            "attach_memory_layer is no longer load-bearing" % (as_event, as_task))
        self.assertNotEqual(
            "unknown", as_task,
            "a non-memory record with ref_type 'event' no longer resolves to a real layer, so "
            "there is nothing for the guard to prevent")

    def test_a_non_memory_record_is_returned_untouched(self):
        """The property this creates."""
        stamped = []
        checked = 0
        for record_type, ref_type, scope in itertools.product(
                OTHER_RECORD_TYPES, REF_TYPES, MEMORY_SCOPES):
            record = {"record_type": record_type, "ref_type": ref_type}
            if scope is not None:
                record["memory_scope"] = scope
            checked += 1
            out = attach_memory_layer(dict(record))
            if "memory_layer" in out:
                stamped.append((record, out["memory_layer"]))
        self.assertGreater(
            checked, 50,
            "only %d shapes tried, so 'nothing was stamped' would be nearly free to satisfy"
            % checked)
        self.assertEqual(
            [], stamped[:5],
            "%d of %d non-memory records were given a memory_layer. attach_memory_layer decides "
            "from ref_type, so it must check record_type first."
            % (len(stamped), checked))

    def test_every_memory_record_still_gets_its_layer(self):
        """The other direction: a guard that stops the real callers would pass the test above.

        Both live call sites in matrixark_codex_hook pass one of these -- the context_embedding
        built for event text, and the context_event at the fast-hook projection.
        """
        missing = []
        for record_type in MEMORY_RECORD_TYPES:
            record = {"record_type": record_type, "ref_type": "event", "memory_scope": "session"}
            out = attach_memory_layer(dict(record))
            if "memory_layer" not in out:
                missing.append(record_type)
        self.assertEqual(
            [], missing,
            "the guard now also blocks %s, which are the records this function exists to stamp"
            % ", ".join(missing))

    def test_the_record_is_not_mutated(self):
        """It returns a new dict; the caller's record must come back as it went in."""
        record = {"record_type": "context_event", "ref_type": "event"}
        before = dict(record)
        attach_memory_layer(record)
        self.assertEqual(before, record, "attach_memory_layer mutated the record it was given")


if __name__ == "__main__":
    unittest.main()
