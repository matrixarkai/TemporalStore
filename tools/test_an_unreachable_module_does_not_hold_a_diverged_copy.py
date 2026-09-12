# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An unreachable module holding a DIVERGED copy of a live name is a wrong answer waiting to be read.

42 modules are recorded as unreachable from any production entry point. Between them they define 129
names that a reachable module also defines. 76 of those are verbatim copies -- dead weight, and only
that. **53 have different bodies**, and nothing in either file says which one is current.

That is not a theoretical hazard. Three taken at random:

* ``compact_ws`` -- the live copy normalises line endings, collapses runs of spaces and tabs, and
  caps consecutive blank lines. The orphan collapses ALL whitespace, newlines included, into single
  spaces: it destroys paragraph structure the live one preserves.
* ``candidate_memory_layer_name`` -- 7,606 characters live against 5,190 in the orphan, which is
  missing the explicit ``memory_layer`` handling and the ``context_event`` -> ``event`` normalisation.
* ``dedupe_entities`` -- the orphan omits ``drop_directive_duplicates``.

Consolidating two copies on this tree has already kept the wrong one and lost audit fields. The cost
is paid by whoever reads next, and reading is not something a test can prevent -- so this guards the
only thing it can: **the number may not grow.**

WHY A COUNT AND NOT A LIST. A guard that writes down names feeds the guards that count mentions --
that has happened three times here, most recently when a recorded module list made an orphan check
treat those names as referenced. So nothing is recorded but an integer; the names are derived at run
time and printed only when the assertion fails.

WHAT THE DETECTOR MUST GET RIGHT, learned by getting it wrong: the reachability record holds
``LIVE_ROOTS`` as well as ``UNREACHABLE``. Collecting module names from every binding merges them
and inverts the question -- an earlier version of this scan reported ``matrixark_mcp_core``, the
module every live index-posting caller resolves to, as an orphan holding 31 shadowed names. Read one
named binding.

Docstrings are stripped before comparing, so re-wording a docstring is not divergence; anything else
is.
"""
from __future__ import annotations

import ast
import pathlib
import unittest
from collections import defaultdict

#: Diverged shadows. May only go DOWN. Raising it needs a reason in the message.
#:
#: 53 on 2026-09-06, 51 now. The two that left are `oss_model_memory_segments` and
#: `semantic_saliency_score`: matrixark_mcp_segments held a copy of each, and both copies were the
#: STALE side, so the module re-exports matrixark_mcp_core's instead -- which is the pattern that
#: file already used for `detect_memory_segments` and `build_segment_prompt`, with the comment
#: "the implementation lives in matrixark_mcp_core; this module re-exports it" written above them.
#:
#: 51 -> 48 for the three `*_candidates_from_query` functions, whose copies in matrixark_mcp_query
#: differed from matrixark_mcp_core_query_analysis's by ONE token -- a private `_ordered_unique`
#: defined in that file, a nine-line duplicate of matrixark_mcp_indexing.ordered_unique, where the
#: live copies call the shared one. Same pattern again: that module was ALREADY re-exporting two
#: names from the same live file, with the same comment above them.
#:
#: 48 -> 46 for `summary_provider` and `synthesize_context_node_summary` in matrixark_mcp_summaries,
#: a module ALREADY re-exporting four names from matrixark_mcp_core. Third module, same shape: the
#: copies differed by which spelling of require_oss_understanding they called, and in one case by a
#: lazy-import shim for a name core resolves at module scope. Same implementation, different
#: plumbing -- which is the hardest kind to read, because the diff is real and means nothing.
#:
#: 46 -> 44 for the same reason one file along: matrixark_mcp_oss_understanding kept copies of
#: oss_encoder_memory_segments and oss_encoder_extract_batch_entities that differed only by
#: reaching their helpers through a lazy `core.` accessor. That accessor looked like cycle
#: avoidance and was not -- the file already binds core at module scope, and core does not
#: import it at all -- so it went with them.
#:
#: 44 -> 40 for six more in matrixark_mcp_query, the same accessor one more time. Four of the
#: six were diverged and two were verbatim, which is why the total falls by six and this number
#: by four. The accessor STAYS there: candidate_index_terms still calls it and diverges by more
#: than a spelling, so it is not part of that move.
RECORDED_DIVERGED = 40

#: Total shadowed names (diverged + verbatim), recorded for the same reason.
#:
#: 129 on 2026-09-06 and 63 now, and only two of that fall are from the change that lowered the
#: number above -- 66 verbatim copies went with work that landed since and did not bank this line.
#: Banked here, because a ceiling left sixty-six above the truth is not a ratchet, it is a number
#: that will pass whatever happens next.
RECORDED_SHADOWED = 50

_CACHE: dict[str, object] = {}


def _tools() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent


def _unreachable_modules() -> set[str]:
    """Names in the reachability record's UNREACHABLE binding, read as data.

    One binding on purpose: the same file holds LIVE_ROOTS, and a scan that takes both calls the
    most reachable modules in the tree unreachable.
    """
    record = _tools() / "test_a_module_only_tests_reach_is_not_live.py"
    try:
        tree = ast.parse(record.read_text(encoding="utf-8", errors="replace"))
    except (OSError, SyntaxError):
        return set()
    names: set[str] = set()
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(isinstance(t, ast.Name) and t.id == "UNREACHABLE" for t in targets):
            continue
        for inner in ast.walk(node):
            if isinstance(inner, ast.Constant) and isinstance(inner.value, str):
                value = inner.value.strip()
                if value.startswith("matrixark_") and " " not in value and "\n" not in value:
                    names.add(value)
    return names


def _definitions() -> dict[str, dict[str, str]]:
    """name -> {module: normalised body}, over every non-test module."""
    if "defs" in _CACHE:
        return _CACHE["defs"]  # type: ignore[return-value]
    found: dict[str, dict[str, str]] = defaultdict(dict)
    for path in sorted(_tools().glob("*.py")):
        if path.name.startswith("test_"):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except (OSError, SyntaxError):
            continue
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            body = list(node.body)
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                body = body[1:]
            found[node.name][path.stem] = ast.dump(ast.Module(body=body, type_ignores=[]))
    _CACHE["defs"] = found
    return found


def _shadows() -> tuple[list[str], list[str]]:
    """(diverged, verbatim) names defined by both a live and an unreachable module."""
    unreachable = _unreachable_modules()
    diverged: list[str] = []
    verbatim: list[str] = []
    for name, holders in _definitions().items():
        live = [m for m in holders if m not in unreachable]
        dead = [m for m in holders if m in unreachable]
        if not (live and dead):
            continue
        bodies = {holders[m] for m in live} | {holders[m] for m in dead}
        (verbatim if len(bodies) == 1 else diverged).append(name)
    return sorted(diverged), sorted(verbatim)


class OrphanModulesDoNotHoldMoreDivergedCopiesTest(unittest.TestCase):

    def test_the_reachability_record_yields_the_unreachable_list_only(self) -> None:
        """Control on the input. Reading the wrong binding inverts every answer below."""
        unreachable = _unreachable_modules()
        self.assertTrue(
            unreachable,
            "parsed no module names out of UNREACHABLE -- every check below would then find no "
            "orphan at all and pass by finding nothing")
        self.assertNotIn(
            "matrixark_mcp_core", unreachable,
            "matrixark_mcp_core is recorded as unreachable, which cannot be true -- it is the "
            "module every live caller of context_index_posting_record resolves to. The scan is "
            "reading LIVE_ROOTS as well as UNREACHABLE.")

    def test_the_detector_finds_some_divergence(self) -> None:
        """Positive control. A detector that finds none would satisfy the ratchet forever."""
        diverged, verbatim = _shadows()
        self.assertTrue(
            diverged or verbatim,
            "no shadowed names found at all -- the comparison is not running, and the ratchet "
            "below would pass however many diverged copies were added")

    def test_no_new_diverged_copy_appears_in_an_unreachable_module(self) -> None:
        """The ratchet. Reading is what costs; all a test can do is stop the pile growing."""
        diverged, _ = _shadows()
        self.assertLessEqual(
            len(diverged), RECORDED_DIVERGED,
            "unreachable modules now hold %d diverged copies of live names, up from %d. Each one "
            "compiles, looks current, and says nothing about which way it diverged -- and "
            "consolidating two copies here has already kept the wrong one. Names now: %s"
            % (len(diverged), RECORDED_DIVERGED, ", ".join(diverged)))

    def test_the_total_shadowed_count_does_not_grow_either(self) -> None:
        """Verbatim copies are only dead weight, but they are how a diverged one starts."""
        diverged, verbatim = _shadows()
        total = len(diverged) + len(verbatim)
        self.assertLessEqual(
            total, RECORDED_SHADOWED,
            "unreachable modules now redefine %d live names, up from %d"
            % (total, RECORDED_SHADOWED))


if __name__ == "__main__":
    unittest.main()
