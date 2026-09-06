# SPDX-License-Identifier: Apache-2.0
"""There is one index normaliser, and a non-ASCII term survives it.

`normalized_index_value` was implemented twice. `matrixark_mcp_core`'s kept CJK and other non-ASCII
characters through an explicit character class; `matrixark_mcp_indexing`'s replaced everything
outside `[a-z0-9_.:/-]` with `_`. Because the result is then stripped of `_`, and
`context_index_name` returns `""` for an empty value, a term written only in Chinese, Japanese or
Korean normalised to the EMPTY STRING through the plainer copy. The term did not rank lower. It
disappeared.

The two copies sat on opposite sides of the same lookup: `matrixark_mcp_core_query_analysis` and
`matrixark_mcp_core_codex_outcome` reached `matrixark_mcp_core`, while `matrixark_mcp_query`
reached `matrixark_mcp_indexing`. A term indexed under one could not be found by a query normalised
through the other.

`café` lost its accent the same way, so this is not only a CJK question -- any non-ASCII letter was
dropped.

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


#: Everything that normalises an index value, on either side of the lookup.
CALLERS = (
    "matrixark_mcp_core",
    "matrixark_mcp_core_query_analysis",
    "matrixark_mcp_core_codex_outcome",
    "matrixark_mcp_query",
    # Not an index path: its use is a dedup key. It had the strictest copy of the three, so two
    # distinct facts written in Chinese shared a key and the second was dropped as a duplicate.
    "matrixark_mcp_extraction_normalization",
)

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
        """The property that actually matters: one term, two paths, one index name."""
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
