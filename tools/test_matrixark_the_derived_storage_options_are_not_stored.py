#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`storage_options` carried a copy of a pure function of its own inputs.

The tail of `normalize_storage_options` merges `canonical_storage_route(normalized)` back into the
options, so every stored record spelled out sixteen fields that its own kept fields already imply.
On the one-box log `storage_options` is 6.53% of every byte written, and 17 of its 25 fields never
take a second value.

Ten of those are PURE outputs -- names `canonical_storage_route` produces and never reads back --
and only those are dropped. The other six (`route`, `storage_family`, `write_mode`, `durability`,
`read_preference`, `background_write`) are read back as INPUTS, so dropping them would silently
override an explicit caller value; `test_an_explicit_input_survives` is what holds that line.

The invariant that matters is not "the field is absent" but "the field is recoverable", so the
central test re-derives every dropped name and demands an exact match across a matrix of option
shapes, not one fixture.
"""
import itertools
import unittest

try:
    from tools.matrixark_mcp_temporal_append import (
        _OPTIONS_KEYS_DERIVED_FROM_THE_REST,
        slim_persisted_storage_options,
    )
    from tools.matrixark_mcp_storage_options import (
        canonical_storage_route,
        normalize_storage_options,
    )
except ImportError:  # run from tools/
    from matrixark_mcp_temporal_append import (
        _OPTIONS_KEYS_DERIVED_FROM_THE_REST,
        slim_persisted_storage_options,
    )
    from matrixark_mcp_storage_options import (
        canonical_storage_route,
        normalize_storage_options,
    )

DERIVED = set(_OPTIONS_KEYS_DERIVED_FROM_THE_REST)


def _options(**kwargs):
    """A realistic block, built by the same function that builds the stored ones."""
    return normalize_storage_options({"storage_options": dict(kwargs)})


def _shapes():
    """A matrix of option shapes, so the invariant is not asserted on one lucky fixture."""
    for mode, write, family in itertools.product(
        ("default", "shared_store", "raft"),
        ("async", "sync"),
        ("default", "shared_store", "raft"),
    ):
        if len({m for m in (mode, family) if m in {"shared_store", "raft"}}) > 1:
            continue        # normalize refuses to mix two families in one request
        opts = {"storage_mode": mode, "write_mode": write}
        if family != "default":
            opts["storage_family"] = family
        try:
            yield _options(**opts)
        except Exception:
            continue


class DerivedStorageOptionsTests(unittest.TestCase):
    def test_every_dropped_field_is_recoverable_exactly(self):
        """The load-bearing invariant: what is dropped must come back byte-identical."""
        checked = 0
        for options in _shapes():
            record = slim_persisted_storage_options({"storage_options": options})
            kept = record["storage_options"]
            rebuilt = canonical_storage_route(kept)
            for field in DERIVED & set(options):
                self.assertEqual(
                    options[field], rebuilt.get(field),
                    f"{field} did not come back from {kept}")
                checked += 1
        self.assertGreater(checked, 20, "the shape matrix produced almost nothing to check")

    def test_the_derived_fields_are_actually_gone(self):
        """Without this the recovery test above would pass on an unchanged record."""
        options = _options(storage_mode="default", write_mode="async")
        present = DERIVED & set(options)
        self.assertTrue(present, "the fixture never had the derived fields")
        kept = slim_persisted_storage_options({"storage_options": options})["storage_options"]
        self.assertEqual(set(), DERIVED & set(kept))

    def test_an_explicit_input_survives(self):
        """`background_write` is an input as well as an output, so it must NOT be dropped.

        An async write derives background_write=True. A caller who explicitly asked for False would
        have that silently reversed if the field were treated as purely derived.
        """
        options = _options(storage_mode="default", write_mode="async", background_write=False)
        kept = slim_persisted_storage_options({"storage_options": options})["storage_options"]
        self.assertIn("background_write", kept)
        self.assertIs(False, kept["background_write"])
        self.assertIs(True, canonical_storage_route({"write_mode": "async"})["background_write"],
                      "positive control: the default really is the opposite value")

    def test_a_record_without_options_is_untouched(self):
        record = {"record_type": "context_event", "text": "x"}
        self.assertIs(record, slim_persisted_storage_options(record))

    def test_an_already_slim_record_is_untouched(self):
        record = {"storage_options": {"storage_mode": "default", "oplog_mode": "async"}}
        self.assertIs(record, slim_persisted_storage_options(record))

    def test_the_rest_of_the_record_is_not_disturbed(self):
        options = _options(storage_mode="default", write_mode="async")
        record = {"record_type": "context_event", "text": "hello", "node_hash": 7,
                  "storage_options": options}
        slim = slim_persisted_storage_options(record)
        self.assertEqual(
            {k: v for k, v in record.items() if k != "storage_options"},
            {k: v for k, v in slim.items() if k != "storage_options"})
        self.assertIn("storage_options", record, "the input record was mutated in place")
        self.assertEqual(options, record["storage_options"])

    def test_a_non_dict_is_returned_as_is(self):
        self.assertEqual("nope", slim_persisted_storage_options("nope"))

    def test_it_saves_a_material_share_of_the_block(self):
        """A guard on the reason this exists, so a future narrowing shows up as a failure."""
        import json
        options = _options(storage_mode="default", write_mode="async")
        before = len(json.dumps(options, separators=(",", ":")))
        after = len(json.dumps(
            slim_persisted_storage_options({"storage_options": options})["storage_options"],
            separators=(",", ":")))
        self.assertLess(after, before * 0.7,
                        f"expected the block to shrink materially, got {before} -> {after}")


class EnvelopeStorageOptionsTests(unittest.TestCase):
    """The block lives in two places on a record; trimming one is not trimming it."""

    def setUp(self):
        self.options = _options(storage_mode="default", write_mode="async")
        self.assertTrue(DERIVED & set(self.options), "fixture has no derived fields")

    def test_the_envelopes_own_block_is_trimmed(self):
        record = {"record_type": "context_event",
                  "envelope": {"messages": [], "storage_options": dict(self.options)}}
        slim = slim_persisted_storage_options(record)
        self.assertEqual(set(), DERIVED & set(slim["envelope"]["storage_options"]))

    def test_the_envelopes_dropped_fields_are_recoverable(self):
        record = {"envelope": {"storage_options": dict(self.options)}}
        kept = slim_persisted_storage_options(record)["envelope"]["storage_options"]
        rebuilt = canonical_storage_route(kept)
        for field in DERIVED & set(self.options):
            self.assertEqual(self.options[field], rebuilt.get(field), field)

    def test_both_copies_are_trimmed_together(self):
        record = {"storage_options": dict(self.options),
                  "envelope": {"storage_options": dict(self.options)}}
        slim = slim_persisted_storage_options(record)
        self.assertEqual(set(), DERIVED & set(slim["storage_options"]))
        self.assertEqual(set(), DERIVED & set(slim["envelope"]["storage_options"]))

    def test_the_input_record_is_not_mutated(self):
        envelope = {"storage_options": dict(self.options)}
        record = {"envelope": envelope}
        slim_persisted_storage_options(record)
        self.assertTrue(DERIVED & set(envelope["storage_options"]),
                        "the caller's envelope was trimmed in place")
        self.assertIs(envelope, record["envelope"])

    def test_the_rest_of_the_envelope_survives(self):
        record = {"envelope": {"messages": [{"role": "user"}], "metadata": {"a": 1},
                               "storage_options": dict(self.options)}}
        slim = slim_persisted_storage_options(record)
        self.assertEqual([{"role": "user"}], slim["envelope"]["messages"])
        self.assertEqual({"a": 1}, slim["envelope"]["metadata"])

    def test_a_record_with_neither_block_is_returned_as_is(self):
        record = {"record_type": "context_event", "envelope": {"messages": []}}
        self.assertIs(record, slim_persisted_storage_options(record))

    def test_a_non_dict_envelope_is_ignored(self):
        record = {"storage_options": dict(self.options), "envelope": "not a dict"}
        slim = slim_persisted_storage_options(record)
        self.assertEqual("not a dict", slim["envelope"])
        self.assertEqual(set(), DERIVED & set(slim["storage_options"]))


if __name__ == "__main__":
    unittest.main()
