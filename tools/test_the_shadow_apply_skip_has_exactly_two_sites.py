#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The optimisation that skips applying into a peer is implemented TWICE, and both sites matter.

Each `RaftNode` owns a `TemporalEngine`, so applying a committed entry into every node this
process holds drives one engine WAL -- and one durability barrier -- per node. In a deployed
process the peers are shadows: the real followers apply in their own processes off AppendEntries,
so those applies are pure cost. The optimisation skips them, advancing log and commit bookkeeping
for every node but applying only into the local one.

It lives at TWO independent sites, under two different names:

| file | binding | what it governs |
| --- | --- | --- |
| `raft/cluster_membership.rs` | `skip_shadow_apply` | the apply inside `catch_up` |
| `raft.rs` | `apply_local_only` | the apply inside `propose_one` |

**Both, not one.** That is the whole reason this file exists. The two govern disjoint behaviour,
so removing either one on its own changes only its own half, and an experiment that mutates one
site and sees a partial result reads exactly like a refutation of a shared cause. Measured: three
`--bins` tests fail on this optimisation, one from the `catch_up` site and two from the
`propose_one` site, and mutating either site alone fixes only its own.

So the count is the thing to hold. A third copy, or a silent loss of one, changes what a recovery
or a write leaves applied, and neither shows up in CI -- the Rust test step is
`continue-on-error`, so the suite that would catch it reports success while failing.

## What is checked, and what is deliberately crude

Two exact claims, and one population count as a tripwire:

1. each binding exists exactly once, in its own file, and is derived from `local_node_id`;
2. neither name appears anywhere else under `raft`;
3. the set of raft functions that BOTH call `apply_committed(` AND mention `local_node_id` is
   exactly four -- the two real gates above, plus two that merely mention it.

Claim 3 over-reports on purpose. Asking "is this apply GUARDED by that name" needs real control
flow, and a guard that answers it approximately would be wrong in a way nobody could see. Instead
the whole population is pinned, so any movement in it -- including a third copy of the
optimisation under a new name, which claims 1 and 2 would sail straight past -- fails here and
asks a person to classify it.

The two non-gating members are named below rather than skipped silently, because an exemption
list is a hiding place unless each entry says why it is exempt:

- `restore_single_shard_from_wal` -- applies at one point, then mentions `local_node_id: None`
  much later as a struct field. Unrelated to the apply.
- `receive_append_entries` -- applies gated on `replica_role.can_serve_data()`, then later reads
  `local_node_id` to refresh pipeline state. Unrelated to the apply.

Both were read and classified by hand, not inferred from the rule.
"""
from __future__ import annotations

import io
import os
import re
import unittest
from typing import Dict, List, Set, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
SRC = os.path.join(REPO, "crates", "temporalstore-rust", "src")
RAFT_RS = os.path.join(SRC, "raft.rs")
RAFT_DIR = os.path.join(SRC, "raft")

# The two sites, and the file each belongs to.
GATES = {
    "skip_shadow_apply": os.path.join("raft", "cluster_membership.rs"),
    "apply_local_only": "raft.rs",
}

# Functions that call apply_committed AND mention local_node_id. The first two gate the apply on
# it; the last two only mention it, for the reasons in the docstring.
EXPECTED_POPULATION = {
    ("raft.rs", "propose_one"),
    (os.path.join("raft", "cluster_membership.rs"), "catch_up"),
    ("raft.rs", "restore_single_shard_from_wal"),
    (os.path.join("raft", "cluster_replication.rs"), "receive_append_entries"),
}

# Functions calling apply_committed at all, gated or not. Eight when this was written; a floor,
# because the scan reporting "none" must not read as "nothing gates an apply".
MINIMUM_APPLY_SITES = 6

_FN = re.compile(
    r"^\s*(?:pub(?:\([a-z()]+\))?\s+)?(?:async\s+)?fn\s+([a-z_][a-z0-9_]*)", re.M)


def _read(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def _sources() -> List[Tuple[str, str]]:
    """(path relative to src/, text) for raft.rs and every file under raft/."""
    out = [("raft.rs", _read(RAFT_RS))]
    for name in sorted(os.listdir(RAFT_DIR)):
        if not name.endswith(".rs"):
            continue
        out.append((os.path.join("raft", name), _read(os.path.join(RAFT_DIR, name))))
    return out


def _function_bodies(source: str) -> List[Tuple[str, str]]:
    """(name, body) for each `fn`, by brace balance.

    Nested functions appear under their own name as well as inside the enclosing body, which
    would double count. The population below is a SET of (file, name), so a duplicate collapses.
    """
    bodies: List[Tuple[str, str]] = []
    for match in _FN.finditer(source):
        brace = source.find("{", match.end())
        if brace < 0:
            continue
        depth = 0
        for position in range(brace, len(source)):
            if source[position] == "{":
                depth += 1
            elif source[position] == "}":
                depth -= 1
                if depth == 0:
                    bodies.append((match.group(1), source[brace + 1:position]))
                    break
    return bodies


class TheShadowApplySkipHasExactlyTwoSites(unittest.TestCase):
    def setUp(self) -> None:
        self.sources = _sources()
        self.by_path: Dict[str, str] = dict(self.sources)

    def test_the_scan_reads_the_raft_sources(self) -> None:
        """Vacuity: no sources, or no apply sites, makes every test below pass on nothing."""
        self.assertGreaterEqual(
            len(self.sources), 5,
            "only %d raft sources read; the scan is not finding the tree." % len(self.sources))
        applying = [
            (path, name)
            for path, text in self.sources
            for name, body in _function_bodies(text)
            if "apply_committed(" in body]
        self.assertGreaterEqual(
            len(set(applying)), MINIMUM_APPLY_SITES,
            "only %d functions call apply_committed (floor %d); the body scan is not working, so "
            "an empty population below would mean nothing." % (len(set(applying)),
                                                               MINIMUM_APPLY_SITES))

    def test_each_gate_exists_once_in_its_own_file(self) -> None:
        for binding, expected_path in sorted(GATES.items()):
            text = self.by_path.get(expected_path)
            self.assertIsNotNone(text, "%s is missing from the tree" % expected_path)
            declarations = re.findall(r"let %s\b" % re.escape(binding), text or "")
            self.assertEqual(
                1, len(declarations),
                "`%s` is declared %d times in %s, expected exactly 1. This is one of the two "
                "sites that skip applying into a peer; a second declaration in the same file is "
                "a third copy of the optimisation." % (binding, len(declarations), expected_path))
            self.assertIn(
                "local_node_id", text or "",
                "`%s` no longer sits in a file that reads `local_node_id`, so it cannot still be "
                "deriving the skip from which node this process owns." % binding)

    def test_neither_gate_name_appears_elsewhere(self) -> None:
        for binding, expected_path in sorted(GATES.items()):
            elsewhere = [
                path for path, text in self.sources
                if binding in text and path != expected_path]
            self.assertEqual(
                [], elsewhere,
                "`%s` also appears in %r. It belongs only in %s; another copy is another place "
                "a peer silently stops applying." % (binding, elsewhere, expected_path))

    def test_the_population_that_could_hide_a_third_copy_is_unchanged(self) -> None:
        population: Set[Tuple[str, str]] = set()
        for path, text in self.sources:
            for name, body in _function_bodies(text):
                if "apply_committed(" in body and "local_node_id" in body:
                    population.add((path, name))
        self.assertEqual(
            EXPECTED_POPULATION, population,
            "the set of raft functions that both apply committed entries and mention "
            "`local_node_id` has changed.\n  added:   %r\n  removed: %r\nTwo of these gate the "
            "apply on it (%s) and two only mention it. A NEW member is the case this test exists "
            "for: a third copy of the skip, which the name checks above would not see. Read it, "
            "classify it, and update this file saying which it is."
            % (sorted(population - EXPECTED_POPULATION),
               sorted(EXPECTED_POPULATION - population),
               ", ".join(sorted(GATES))))


if __name__ == "__main__":
    unittest.main()
