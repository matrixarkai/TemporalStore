# SPDX-License-Identifier: Apache-2.0
"""There is one index normaliser, and a non-ASCII term survives it.

`normalized_index_value` was implemented twice. `matrixark_mcp_core`'s kept CJK and other non-ASCII
characters through an explicit character class; `matrixark_mcp_indexing`'s replaced everything
outside `[a-z0-9_.:/-]` with `_`. Because the result is then stripped of `_`, and
`context_index_name` returns `""` for an empty value, a term written only in Chinese, Japanese or
Korean normalised to the EMPTY STRING through the plainer copy. The term did not rank lower. It
disappeared.

`café` lost its accent the same way, so this is not only a CJK question -- any non-ASCII letter was
dropped.

WHAT THIS DOES NOT CLAIM, having checked: no live path could hit the divergence. Both copies are
reached -- `matrixark_mcp_core_query_analysis` and `matrixark_mcp_core_codex_outcome` reach core's,
and `matrixark_mcp_indexing`'s own term builders reach its own -- but every LIVE entry into this
module passes values that are ASCII by construction. The only live text-taking entry is
`benchmark_quality_index_terms`, reached from `matrixark_mcp_core` and
`matrixark_mcp_local_adapter`, and its terms are fixed benchmark and metric names plus a workload
capture whose character class is `[a-z0-9_.:/ -]`. Measured: `benchmark_quality_index_terms`
("workload: 内存测试 p99") yields `['metric:p99_latency']` and no workload term at all.

`matrixark_mcp_query`, which does call the term builders with arbitrary text, is reached by nothing
outside `tools/test_*`.

So the divergence was latent, and the reason it was latent is an accident of which builders exist
today -- it stops being true the moment a term builder takes user text. A copy that is both wrong
and unreachable-in-practice is the combination worth removing, which is why this is a change rather
than a note.

The rule is the strong form -- not "the two must agree" but "there must not be two". Every ASCII
input agrees under either copy, so a same-answer check written against ordinary test data would
have passed indefinitely; the first probe of this pair used seven ASCII strings and reported no
difference at all.
"""
from __future__ import annotations

import ast
import importlib
import inspect
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SELF = Path(__file__).name

HOME = "matrixark_mcp_indexing"


def _import(name: str):
    """Import a tools module whichever way the suite is being run.

    The suite runs both from tools/ (bare names) and from the repository root (as
    `tools.<module>`), and every module in the tree carries the same try/except for this reason.
    A guard that picks one path fails on the OTHER path, which reads as a defect in the code it
    is guarding rather than in how it was invoked.
    """
    try:
        return importlib.import_module("tools." + name)
    except ImportError:
        return importlib.import_module(name)


def _module_name(obj) -> str:
    """The defining module's name without its package prefix.

    The suite runs both from tools/ and as `tools.<module>`, so the same function reports
    `matrixark_mcp_scoring` in one and `tools.matrixark_mcp_scoring` in the other. Comparing whole
    dotted names makes this file fail on the import path rather than on what it is checking.
    """
    module = inspect.getmodule(obj)
    return getattr(module, "__name__", "").rsplit(".", 1)[-1]


#: Reached from a production entry point.
LIVE_CALLERS = (
    "matrixark_mcp_core",
    "matrixark_mcp_core_query_analysis",
    "matrixark_mcp_core_codex_outcome",
)

#: Reached by nothing outside `tools/test_*`. Checked all the same, so a second implementation
#: cannot appear here and be adopted later -- but a pass on THESE is not evidence about a live
#: lookup, which is the distinction this list exists to keep visible.
#:
#: `matrixark_mcp_extraction_normalization` is not an index path at all: its use is the dedup key in
#: `codex_outcome_fact_entities`, where two distinct facts written in Chinese shared a key and the
#: second was dropped. It sits in a seven-module island that imports only itself.
TEST_ONLY_CALLERS = (
    "matrixark_mcp_query",
    "matrixark_mcp_extraction_normalization",
)

CALLERS = LIVE_CALLERS + TEST_ONLY_CALLERS

#: Scripts written in these must survive normalisation, because a term made only of them would
#: otherwise normalise to nothing at all.
NON_ASCII_TERMS = ("内存", "检索延迟 p99", "メモリ", "메모리", "café")


def _modules_defining(name: str) -> list[str]:
    listed = subprocess.run(
        ["git", "ls-files", "tools/*.py"], cwd=REPO, capture_output=True, text=True).stdout.split()
    found = []
    for rel in listed:
        if Path(rel).name == SELF:
            continue
        try:
            tree = ast.parse((REPO / rel).read_text(encoding="utf-8", errors="replace"))
        except (SyntaxError, OSError):
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                found.append(Path(rel).stem)
    return sorted(found)


class ThereIsOneIndexNormaliserTest(unittest.TestCase):

    def test_only_one_module_defines_each(self) -> None:
        for name in ("normalized_index_value", "context_index_name"):
            self.assertEqual(
                [HOME], _modules_defining(name),
                "%s is defined in more than one module again. A second copy will agree on every "
                "ASCII input, so nothing routine will catch it -- re-export instead" % name)

    def test_every_caller_reaches_that_one(self) -> None:
        for module_name in CALLERS:
            module = _import(module_name)
            for name in ("normalized_index_value", "context_index_name"):
                fn = getattr(module, name, None)
                if fn is None:
                    continue
                self.assertEqual(
                    HOME, _module_name(fn),
                    "%s reaches a %s from %s rather than %s, so the writer and the reader can "
                    "normalise differently"
                    % (module_name, name, _module_name(fn), HOME))

    def test_a_non_ascii_term_survives(self) -> None:
        indexing = _import(HOME)
        for term in NON_ASCII_TERMS:
            self.assertNotEqual(
                "", indexing.normalized_index_value(term),
                "%r normalises to the empty string, so a term written in it cannot be indexed or "
                "looked up at all" % term)
            self.assertNotEqual(
                "", indexing.context_index_name("keyword", term),
                "context_index_name is empty for %r, which is how the term disappears rather than "
                "merely changing" % term)

    def test_the_writer_and_the_reader_agree_on_the_same_term(self) -> None:
        """One term, two modules, one index name.

        `matrixark_mcp_query` is reached only by tests, so this is not asserting a lookup that
        happens today -- it is asserting that the two modules cannot drift apart again.
        """
        core = _import("matrixark_mcp_core")
        query = _import("matrixark_mcp_query")
        for term in NON_ASCII_TERMS + ("plain ascii term",):
            written = core.context_index_name("keyword", term)
            read = query.context_index_name("keyword", term)
            self.assertEqual(
                written, read,
                "%r is indexed as %r and looked up as %r" % (term, written, read))

    def test_two_distinct_non_ascii_facts_do_not_share_a_dedup_key(self) -> None:
        """The third copy was not an index normaliser at all, and lost data rather than lookups.

        `codex_outcome_fact_entities` keys `seen` on the normalised fact. Under a normaliser that
        drops every non-ASCII character, two unrelated Chinese facts both normalise to "" and the
        second is discarded as a duplicate.
        """
        norm = _import("matrixark_mcp_extraction_normalization")
        first = norm.normalized_index_value("内存不足")
        second = norm.normalized_index_value("延迟过高")
        self.assertNotEqual("", first)
        self.assertNotEqual(
            first, second,
            "two distinct facts share a dedup key, so one of them is dropped by the extraction")

    def test_an_index_term_list_never_contains_the_empty_string(self) -> None:
        """The other way an index term stops being a name.

        `context_index_name` returns "" for anything that normalises to nothing, and the term
        builders append its result unconditionally. matrixark_mcp_indexing had its own
        `_ordered_unique` that neither trimmed nor dropped the empty string, so
        `metadata_index_terms({})` returned [""]. An index name of "" is not a name -- every
        record producing one posts under the same key.
        """
        indexing = _import(HOME)
        core = _import("matrixark_mcp_core")
        self.assertIs(
            core.ordered_unique, indexing.ordered_unique,
            "ordered_unique is two objects again, and the copies disagreed about the empty string")
        for metadata in ({}, {"keywords": []}, {"keywords": ["", " ", "k"]}):
            for terms in (indexing.metadata_index_terms(metadata),
                          core.metadata_index_terms(metadata)):
                self.assertNotIn(
                    "", terms,
                    "metadata_index_terms(%r) returned an empty index term: %r"
                    % (metadata, terms))

    def test_ascii_is_unchanged(self) -> None:
        """Control. This passes under either copy, and is here to say so.

        On its own it is not evidence of anything -- it is the assertion that made the original
        divergence invisible.
        """
        indexing = _import(HOME)
        self.assertEqual("hello_world", indexing.normalized_index_value("Hello World"))
        self.assertEqual("matrixark-2026", indexing.normalized_index_value("MatrixArk-2026"))
        self.assertEqual("", indexing.normalized_index_value(""))


if __name__ == "__main__":
    unittest.main()
