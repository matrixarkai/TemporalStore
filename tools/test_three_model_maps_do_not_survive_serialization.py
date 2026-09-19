#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Three model maps do not survive serialization, and the rebuild re-derives one of them.

A dump manifest carries a serialized `ShardState`. Three of the model maps that
`visit_model_live_blocks` walks are declared `#[serde(default, skip_serializing)]`, so a manifest
index DECODES WITH THEM EMPTY while its `bucket_index.bucket_map` -- which does serialize -- still
names every page they owned.

That asymmetry is load-bearing in two places, and they are guarded differently:

- `rebuild_bucket_block_ownership` clears `bucket_map` and repopulates it FROM the model maps, so
  run against a freshly decoded index it deletes exactly the pages only the index still knew
  about. `install_bucket_dump_manifest` and the default recovery arm in `load_shard_with` both
  call it.
- the manifest cross-check derives the object lifecycle twice, once from the bucket index and
  once from the model maps, and refuses an install when the two disagree.

`rebuild_unserialized_model_maps_from_bucket_index` exists to close the first, and re-derives
`hashes` ONLY. The other two are keyed by a `u64` the bucket index does not carry in its
`component`, so they stay dropped -- a hole `load_shard_with` documents in place rather than
fixes, and the one the two corpus suites fail on today.

So this file pins the three facts that make that hole exactly the size it is, and nothing about
whether it SHOULD be closed. Each can change silently and each changes what recovery loses:

1. exactly three `ShardState` fields are `skip_serializing`, and they are the three named here.
   A FOURTH widens the hole to a map nobody has weighed -- a field added with the attribute
   copied from its neighbour is how that happens, and it compiles.
2. all three are walked by `visit_model_live_blocks`, which is what makes their emptiness visible
   to the lifecycle cross-check at all. A map that stops being walked stops being checked.
3. the rebuild re-derives exactly one of them. Widening it is the fix, and it must arrive with a
   decision about the keys rather than as a quiet edit -- re-deriving a map with keys the index
   cannot supply would repair the disagreement the cross-check exists to detect.

`object_block_lookup` also carries the attribute but lives on `CoreIndex`, not `ShardState`, and
is a derived lookup rebuilt from the bucket map on load. The scan is scoped to the `ShardState`
body for that reason; a file-wide grep reports four and is wrong about this class.
"""
from __future__ import annotations

import io
import os
import re
import unittest
from typing import Dict, List, Set

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
ENGINE = os.path.join(REPO, "crates", "temporalstore-rust", "src", "engine")
STATE = os.path.join(ENGINE, "state.rs")
INTERNALS = os.path.join(ENGINE, "storage_bucket_internals.rs")

# The three maps that do not survive serialization, and the one the rebuild re-derives.
UNSERIALIZED = frozenset({"hashes", "context_events", "context_indexes"})
REBUILT = frozenset({"hashes"})

# ShardState carried 60+ fields when this was written. A body that yields fewer than this was not
# parsed -- the scan would then report "no unserialized maps" and pass while seeing nothing.
MINIMUM_SHARD_STATE_FIELDS = 40

# `skip_serializing` as a WHOLE attribute word: `skip_serializing_if = "..."` is a different
# thing, it serializes the field whenever the predicate is false, and several fields use it.
_SKIP_SERIALIZING = re.compile(r"\bskip_serializing\b(?!_if)")
_FIELD = re.compile(r"^\s*pub(?:\(super\)|\(crate\))?\s+([a-z_][a-z0-9_]*)\s*:", re.M)
_ATTRIBUTE = re.compile(r"^\s*#\[")


def _read(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def _braced_body(source: str, opener: str, what: str) -> str:
    """The text between the braces that follow `opener`, by brace balance."""
    start = re.search(opener, source)
    if start is None:
        raise AssertionError("%s is not declared" % what)
    open_brace = source.index("{", start.end())
    depth = 0
    for position in range(open_brace, len(source)):
        if source[position] == "{":
            depth += 1
        elif source[position] == "}":
            depth -= 1
            if depth == 0:
                return source[open_brace + 1:position]
    raise AssertionError("%s has no closing brace" % what)


def _struct_body(source: str, name: str) -> str:
    return _braced_body(
        source, r"\bstruct\s+%s\b" % re.escape(name), "struct %s" % name)


def _fn_body(source: str, name: str) -> str:
    return _braced_body(source, r"\bfn\s+%s\b" % re.escape(name), "fn %s" % name)


def _unserialized_fields(body: str) -> Dict[str, int]:
    """Field name -> 1-based line within the body, for every `skip_serializing` field.

    An attribute applies to the next FIELD, so the walk carries the flag forward across further
    attribute lines and comments and clears it once a field consumes it. Matching an attribute
    against the immediately following line instead would miss any field whose attributes are
    split over two lines.
    """
    found: Dict[str, int] = {}
    pending = False
    for number, line in enumerate(body.splitlines(), start=1):
        if _SKIP_SERIALIZING.search(line):
            pending = True
            continue
        field = _FIELD.match(line)
        if field is not None:
            if pending:
                found[field.group(1)] = number
            pending = False
            continue
        stripped = line.strip()
        if _ATTRIBUTE.match(line) or not stripped or stripped.startswith("//"):
            continue
        pending = False
    return found


def _all_fields(body: str) -> List[str]:
    return [match.group(1) for match in _FIELD.finditer(body)]


class ThreeModelMapsDoNotSurviveSerialization(unittest.TestCase):
    def setUp(self) -> None:
        self.state = _read(STATE)
        self.internals = _read(INTERNALS)
        self.body = _struct_body(self.state, "ShardState")

    def test_the_scan_read_a_shard_state_body(self) -> None:
        """Vacuity: a body that did not parse yields no fields and every test below passes."""
        fields = _all_fields(self.body)
        self.assertGreaterEqual(
            len(fields), MINIMUM_SHARD_STATE_FIELDS,
            "only %d ShardState fields parsed (floor %d); the struct scan is not reading the "
            "body, so an empty result below would mean nothing." % (
                len(fields), MINIMUM_SHARD_STATE_FIELDS))

    def test_exactly_three_maps_do_not_survive_serialization(self) -> None:
        found = _unserialized_fields(self.body)
        self.assertEqual(
            UNSERIALIZED, set(found),
            "the set of ShardState fields that do not survive serialization has changed to %r. "
            "Each one decodes EMPTY from a dump manifest while the bucket index still names its "
            "pages, and `rebuild_bucket_block_ownership` repopulates the bucket map from these "
            "maps -- so a field added here is a page class the recovery arm drops. Weigh it "
            "against `rebuild_unserialized_model_maps_from_bucket_index`, which re-derives %s."
            % (sorted(found), sorted(REBUILT)))

    def test_every_unserialized_map_is_walked_as_a_model_map(self) -> None:
        walk = _fn_body(self.internals, "visit_model_live_blocks")
        for field in sorted(UNSERIALIZED):
            self.assertIn(
                "shard.%s" % field, walk,
                "`%s` does not survive serialization but `visit_model_live_blocks` no longer "
                "walks it, so the manifest cross-check that compares the bucket-index lifecycle "
                "against the model-map one can no longer see it is empty -- the refusal that "
                "currently stops a lossy install would stop happening." % field)

    def test_the_rebuild_re_derives_exactly_one_of_them(self) -> None:
        rebuild = _fn_body(
            self.internals, "rebuild_unserialized_model_maps_from_bucket_index")
        assigned: Set[str] = {
            field for field in UNSERIALIZED
            if re.search(r"shard\.%s\s*=" % re.escape(field), rebuild)}
        self.assertEqual(
            REBUILT, assigned,
            "the rebuild now re-derives %s rather than %s. Widening it is the fix for what the "
            "two corpus suites fail on, and it is welcome -- but the other two maps are keyed by "
            "a u64 the bucket index does not carry in its `component`, so re-deriving them needs "
            "a key from somewhere. Inventing one would make the lifecycle cross-check agree with "
            "itself while the map stayed wrong, which is the disagreement it exists to detect. "
            "Update this file with where the keys came from."
            % (sorted(assigned), sorted(REBUILT)))


if __name__ == "__main__":
    unittest.main()
