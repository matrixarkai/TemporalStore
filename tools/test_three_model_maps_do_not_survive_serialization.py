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

   THE SHAPE OF THAT RE-DERIVATION IS NO LONGER ONE SHAPE, and this file was blinded once by
   assuming it was. `hashes` is a `RecordedHashContainer` now -- a type that owns the map together
   with the durable record of its mutations, so the inner map is private and the rebuild cannot
   assign the field. It calls the container's named reconcile instead. The other two maps are still
   plain maps and are still re-derived, if ever, by a wholesale assignment. So the scan below
   recognises BOTH shapes, and names the method it accepts rather than accepting any call, because
   `shard.hashes.len()` is a read.

`object_block_lookup` also carries the attribute but lives on `CoreIndex`, not `ShardState`, and
is a derived lookup rebuilt from the bucket map on load. The scan is scoped to the `ShardState`
body for that reason; a file-wide grep reports four and is wrong about this class.

THE THIRD ENTRY IS NOW A PRIVATE FIELD, AND THE SCAN HAD TO LEARN TO SEE ONE. `context_events`
became `context_events_compat` when the event series moved to an owned sorted array: the
compatibility field holds what a stored image carries, and the live array field beside it is
`#[serde(skip)]`. The map still does not survive serialization -- the hole is the same hole, the
same size -- but the field is private, because that is what stops anything outside `state.rs`
writing it, and this scan required `pub`. It therefore reported TWO maps where there are three,
which is the direction that would pass a genuine fourth map added privately. The `pub` requirement
was harmless for as long as `ShardState` had no private fields and #2155 added the first one, so
this is a dated regression rather than a sloppy matcher -- tracked as #2167, which also records that
the `serde(skip)` exclusion below is CORRECT and must not be "fixed".

AND `serde(skip)` IS NOT THIS CLASS, which is the distinction worth keeping straight here because
the two look alike and behave differently on the half nobody can observe from a stored image:

  * `#[serde(skip)]` supplies `Default` on decode, so an image lacking the key loads fine.
  * `#[serde(skip_serializing)]` WITHOUT `default` does not -- an image lacking the key fails to
    decode outright, so the first image written without the key is unopenable thereafter.

Both write byte-identical images. No check over a stored image can tell them apart; only the source
can, which is what #2163 enforces. The live array field is `skip` and the compatibility field is
`skip_serializing` WITH `default`, so both halves are the safe variant -- and the scan below counts
only `skip_serializing`, because a `serde(skip)` field is derived in-memory state and a different,
much larger class.
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
#
# STILL THREE. `context_events` was RENAMED to `context_events_compat` and made PRIVATE when the
# event series moved to an owned sorted array: the compatibility field holds what the stored image
# carries and the live field beside it is `#[serde(skip)]`. The event map therefore does not survive
# serialization exactly as it did not before -- same hole, same size -- so this is a rename here and
# not a fourth entry.
UNSERIALIZED = frozenset({"hashes", "context_events_compat", "context_indexes"})
REBUILT = frozenset({"hashes"})

# How `visit_model_live_blocks` reaches each one, because a field is not always reached BY ITS NAME
# any more. The event map is read through an accessor -- the field is private precisely so nothing
# outside `state.rs` can write it -- so looking for `shard.context_events_compat` in the walk would
# report "no longer walked" about a map that is walked on the next line. Keys are asserted equal to
# UNSERIALIZED below so the two cannot drift apart.
WALKED_AS = {
    "hashes": "shard.hashes",
    "context_indexes": "shard.context_indexes",
    "context_events_compat": "shard.context_events()",
}

# ShardState carried 60+ fields when this was written. A body that yields fewer than this was not
# parsed -- the scan would then report "no unserialized maps" and pass while seeing nothing.
MINIMUM_SHARD_STATE_FIELDS = 40

# `skip_serializing` as a WHOLE attribute word: `skip_serializing_if = "..."` is a different
# thing, it serializes the field whenever the predicate is false, and several fields use it.
_SKIP_SERIALIZING = re.compile(r"\bskip_serializing\b(?!_if)")
# `pub` is OPTIONAL, and that is a fix rather than a loosening. `ShardState` had no private
# fields when this was written, so requiring `pub` cost nothing; the compatibility fields the
# consolidation introduces are private ON PURPOSE -- that is what stops anything outside `state.rs`
# writing them -- and a private field carrying `skip_serializing` widens the recovery hole exactly
# as a public one does. Requiring `pub` made this guard report TWO maps where there were three,
# which is the direction that passes a real fourth map rather than failing on a rename.
_FIELD = re.compile(
    r"^\s*(?:pub(?:\([a-z]+\))?\s+)?([a-z_][a-z0-9_]*)\s*:", re.M)
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



# The two shapes that count as re-deriving a model map in the rebuild, and nothing else.
#
# `shard.X = ...` is how a plain map is replaced. `shard.X.<named reconcile>(...)` is how a map
# behind a type that owns its own mutation record is replaced -- the inner map is private, so the
# field cannot be assigned. The methods are NAMED rather than matched as "any call", because
# `shard.hashes.len()` is a read and "any call" would count it.
REDERIVE_METHODS = ("reconcile_from_durable",)


def _rederived_fields(body: str) -> Set[str]:
    found: Set[str] = set()
    for field in UNSERIALIZED:
        if re.search(r"shard\.%s\s*=(?!=)" % re.escape(field), body):
            found.add(field)
            continue
        for method in REDERIVE_METHODS:
            pattern = r"shard\s*\.\s*%s\s*\.\s*%s\s*\(" % (
                re.escape(field), re.escape(method))
            if re.search(pattern, body):
                found.add(field)
                break
    return found


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

    def test_the_field_scan_sees_a_private_field(self) -> None:
        """The matcher that failed, with a positive and a negative control.

        It required `pub` and therefore stopped seeing `context_events_compat` entirely, reporting
        two unserialized maps where there are three. A scan that cannot see a private field would
        pass a genuine fourth map added as one, which is the failure this whole file exists to
        prevent -- so the fix gets controls rather than a comment.
        """
        self.assertEqual(
            {"private_map": 2},
            _unserialized_fields(
                "    #[serde(default, skip_serializing)]\n    private_map: HashMap<A, B>,\n"),
            "the scan must see a PRIVATE field carrying the attribute")
        self.assertEqual(
            {"public_map": 2},
            _unserialized_fields(
                "    #[serde(default, skip_serializing)]\n"
                "    pub(super) public_map: HashMap<A, B>,\n"),
            "and must still see a public one")
        self.assertEqual(
            {},
            _unserialized_fields(
                "    #[serde(skip_serializing_if = \"Option::is_none\")]\n"
                "    pub(super) sometimes: Option<u8>,\n"),
            "`skip_serializing_if` is a different thing and must not be counted")
        self.assertEqual(
            {},
            _unserialized_fields(
                "    #[serde(skip)]\n    pub(super) derived: HashMap<A, B>,\n"),
            "`serde(skip)` is NOT this class: it supplies Default on decode, so an image lacking "
            "the key loads. `skip_serializing` WITHOUT `default` does not -- an image lacking the "
            "key fails to decode outright. Both write byte-identical images, so no check over a "
            "stored image can tell them apart; only the source can, which is what #2163 enforces")

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
        self.assertEqual(
            UNSERIALIZED, frozenset(WALKED_AS),
            "WALKED_AS and UNSERIALIZED have drifted apart: %r vs %r. A field missing from the "
            "mapping would be skipped by the loop below and silently stop being checked."
            % (sorted(WALKED_AS), sorted(UNSERIALIZED)))
        for field in sorted(UNSERIALIZED):
            self.assertIn(
                WALKED_AS[field], walk,
                "`%s` does not survive serialization but `visit_model_live_blocks` no longer "
                "walks it, so the manifest cross-check that compares the bucket-index lifecycle "
                "against the model-map one can no longer see it is empty -- the refusal that "
                "currently stops a lossy install would stop happening." % field)

    def test_the_rebuild_re_derives_exactly_one_of_them(self) -> None:
        rebuild = _fn_body(
            self.internals, "rebuild_unserialized_model_maps_from_bucket_index")
        # The body has to have been parsed. An empty body yields no matches and the scan would then
        # report "re-derives nothing" for the wrong reason.
        self.assertGreater(
            len(rebuild), 400,
            "`rebuild_unserialized_model_maps_from_bucket_index` parsed to %d characters, which is "
            "not its body -- every count below would be a zero presented as a finding."
            % len(rebuild))
        assigned = _rederived_fields(rebuild)

        # CONTROLS on the matcher itself, because this one was blinded once: it must see both
        # re-derivation shapes and must not see a read.
        self.assertEqual(
            {"context_events_compat"},
            _rederived_fields("shard.context_events_compat = derive(bucket_index);"),
            "the matcher no longer sees a wholesale assignment, which is how the two plain maps "
            "would be re-derived")
        self.assertEqual(
            {"hashes"},
            _rederived_fields("shard.hashes.reconcile_from_durable(derived, &live, &mut n);"),
            "the matcher no longer sees the container's named reconcile, which is how `hashes` is "
            "re-derived now that its inner map is private -- this is exactly the blindness that "
            "made this guard fail on a change that kept the fact it pins")
        self.assertEqual(
            set(),
            _rederived_fields("let n = shard.hashes.len(); if shard.hashes.is_empty() {}"),
            "the matcher counts a READ as a re-derivation, so it would pass on a rebuild that "
            "only looked at the map")

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
