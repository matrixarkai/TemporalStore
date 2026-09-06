# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An audit that counts a drop says which ref was dropped.

`record_dropped_candidate` and `dropped_candidate_audit_ref` were each implemented twice, and the
two copies had drifted in OPPOSITE directions, so neither was the complete one:

  * `matrixark_mcp_core_ref_selection.record_dropped_candidate` recorded every memory candidate --
    events, entities, segments, summaries, compressions, resource chunks, skill sections, and the
    matching context classes. `matrixark_mcp_recall_scoring`'s recorded only a resource/skill
    candidate or a stale entity.
  * `matrixark_mcp_recall_scoring.dropped_candidate_audit_ref` emitted six fields the other did not
    -- entity_name, entity_type, memory_scope, session_continuity, profile_shadowed_by_ref_hash,
    profile_shadowed_reason -- and missed none of the other's.

`matrixark_mcp_budget_pack`, which the gateway reaches through `matrixark_mcp_budget_policies`,
resolved both names to `matrixark_mcp_recall_scoring`. So a gateway pack's audit reported
`cross_session_budget: 2` beside `refs: []`: the count said two refs were dropped and the list said
which of them for none. Measured, on three cross-session entities.

The surviving pair takes the wider rule and the richer record, and lives in
`matrixark_mcp_recall_scoring` because that module does not import `matrixark_mcp_core`.

The check is behavioural rather than a definition count, because a definition count cannot tell a
delegate from a divergence -- and the corpus is pinned, because a corpus of only resource and skill
candidates agreed under BOTH copies and would have passed all along.
"""
from __future__ import annotations

import ast
import importlib
import os
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HOME = "matrixark_mcp_recall_scoring"
NAMES = ("record_dropped_candidate", "dropped_candidate_audit_ref")

#: The six fields the richer record carries. They are what an operator reads to tell one dropped
#: entity from another.
RICH_FIELDS = ("entity_name", "entity_type", "memory_scope", "session_continuity")


def _import(name: str):
    try:
        return importlib.import_module("tools." + name)
    except ImportError:
        return importlib.import_module(name)


def _candidates():
    """Cross-session entities: memory candidates, and NOT resource or skill ones.

    That is the discriminating shape. The narrow rule recorded a resource or skill candidate and
    a stale entity; a corpus made of those agrees under either copy.
    """
    out = []
    for index, score in enumerate((0.7, 0.6, 0.5)):
        out.append({
            "ref_id": "e%d" % index, "ref_hash": "h_e%d" % index, "ref_type": "entity",
            "context_class": "entity_state", "score": score, "text": "entity %d" % index,
            "entity_name": "name%d" % index, "entity_type": "preference",
            "memory_scope": "user_profile", "session_continuity": "cross_session",
            "metadata": {"ref_type": "entity"},
        })
    return out


def _select(fn):
    _selected, _tokens, audit = fn(_candidates(), [], max_context_tokens=40,
                                   auxiliary_quota=0, question_type="fact")
    return audit


def _modules_defining(name: str) -> list:
    listed = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    found = []
    for rel in listed:
        if os.path.basename(rel).startswith("test_"):
            continue
        try:
            tree = ast.parse((REPO / rel).read_text(encoding="utf-8", errors="replace"))
        except (SyntaxError, OSError):
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                found.append(os.path.basename(rel)[:-3])
    return sorted(found)


class AnAuditThatCountsADropSaysWhichTest(unittest.TestCase):

    def setUp(self) -> None:
        _import("matrixark_mcp_local_adapter")
        self.gateway = _import("matrixark_mcp_budget_pack").select_token_budgeted_refs
        self.retrieve = _import(
            "matrixark_mcp_core_ref_selection").select_token_budgeted_refs

    def test_only_one_module_defines_each(self) -> None:
        for name in NAMES:
            self.assertEqual(
                [HOME], _modules_defining(name),
                "%s is defined in more than one module again. The copies drifted in opposite "
                "directions last time -- one recorded more candidates, the other more fields -- "
                "so neither was the one to keep" % name)

    def test_the_corpus_is_one_the_narrow_rule_would_have_skipped(self) -> None:
        """Control. A resource or skill candidate agreed under both copies."""
        recall = _import(HOME)
        for candidate in _candidates():
            self.assertFalse(
                recall.is_resource_or_skill_candidate(candidate),
                "the corpus became resource/skill candidates, which the narrow rule recorded "
                "anyway -- the comparison below would pass whichever rule were in force")

    def test_a_counted_drop_is_a_recorded_drop(self) -> None:
        for label, fn in (("gateway", self.gateway), ("retrieve", self.retrieve)):
            audit = _select(fn)
            counted = sum(value for key, value in audit.items()
                          if isinstance(value, int) and key != "refs" and value)
            recorded = len(audit.get("refs") or [])
            self.assertTrue(counted, "%s dropped nothing, so this asserts nothing" % label)
            self.assertEqual(
                counted, recorded,
                "the %s audit counts %d drops and names %d. An operator reading it sees that "
                "something was dropped and cannot see what" % (label, counted, recorded))

    def test_both_paths_produce_the_same_audit(self) -> None:
        gateway, retrieve = _select(self.gateway), _select(self.retrieve)
        self.assertEqual(
            [r.get("ref_hash") for r in (retrieve.get("refs") or [])],
            [r.get("ref_hash") for r in (gateway.get("refs") or [])],
            "the two live packers record different refs for the same drops")

    def test_the_record_carries_the_fields_that_tell_two_entities_apart(self) -> None:
        refs = _select(self.gateway).get("refs") or []
        self.assertTrue(refs, "nothing recorded, so the fields below are not being checked")
        for field in RICH_FIELDS:
            self.assertIn(
                field, refs[0],
                "the audit record lost %s. Without it two dropped entities read alike, which is "
                "the half of this the other copy had and this one did not" % field)


if __name__ == "__main__":
    unittest.main()
