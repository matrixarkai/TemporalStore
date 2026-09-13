#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An import inside a function must name the module both ways, or it raises under one entry style.

Every module-level import in this tree is written

    try:
        from tools.matrixark_x import y
    except ImportError:
        from matrixark_x import y

because a module is loaded as `tools.X` by some entry points and as bare `X` by others. An import
INSIDE A FUNCTION that uses only one spelling raises `ModuleNotFoundError` under the other, and it
raises when the function is CALLED rather than when the module loads -- so nothing at import time
reveals it.

Three did, and were fixed in the commit that added this file. Reproduced before the fix with the
repo root on `sys.path` and `tools/` not, which is the package entry style:

    >>> import tools.matrixark_mcp_core as core
    >>> core.embedding_model_name()
    ModuleNotFoundError: No module named 'matrixark_mcp_embeddings'

and after it returns `'matrixark-local-token-hash-v1'`. The other two were
`MatrixArkMcpServer.__init__` reaching `matrixark_mcp_audit_queue` -- a CONSTRUCTOR, so every
server built under that entry style would have raised -- and `close()` reaching
`matrixark_mcp_shutdown`.

The denominator matters: there are ~193 in-function sibling imports and all but three already do
it correctly, so this is a ratchet on the exception and not a sweeping rule. The set is asserted
EXACTLY, so a new one fails here and a fixed one fails here too.
"""
from __future__ import annotations

import ast
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(TOOLS)

#: (module, function) -> why this one does not need the fallback.
ALLOWED = {
    ("matrixark_codex_hook", "load_matrixark"):
        "it does sys.path.insert(0, str(root)) on the line before, so the `tools.` spelling is "
        "guaranteed by construction rather than by luck",
    ("run_matrixark_mcp_scale_failover_test", "run_rust_gateway_failover"):
        "a run_* harness, invoked only as a script from tools/, where the bare spelling is the "
        "one that resolves",
    ("run_matrixark_message_pdf_debug_trace", "vector_preview"):
        "a run_* harness, invoked only as a script from tools/",
}

_IMPORT_ERRORS = ("ImportError", "ModuleNotFoundError", "Exception")


def _tracked():
    out = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=ROOT,
                         capture_output=True, text=True).stdout.split()
    return [p for p in out if not os.path.basename(p).startswith("test_")]


def _handles_import_error(node):
    for handler in node.handlers:
        if handler.type is None:
            return True
        if isinstance(handler.type, ast.Name) and handler.type.id in _IMPORT_ERRORS:
            return True
        if isinstance(handler.type, ast.Tuple) and any(
                isinstance(e, ast.Name) and e.id in _IMPORT_ERRORS for e in handler.type.elts):
            return True
    return False


def _scan():
    """(module, function) -> [imported module] for in-function sibling imports with no fallback."""
    unguarded, total = {}, 0
    for rel in _tracked():
        stem = os.path.basename(rel)[:-3]
        try:
            with open(os.path.join(ROOT, rel), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except SyntaxError:  # pragma: no cover - an unparseable module fails elsewhere
            continue
        for function in ast.walk(tree):
            if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            guarded = set()
            for node in ast.walk(function):
                if isinstance(node, ast.Try) and _handles_import_error(node):
                    for sub in ast.walk(node):
                        if isinstance(sub, (ast.Import, ast.ImportFrom)):
                            guarded.add(id(sub))
            for node in ast.walk(function):
                if not isinstance(node, ast.ImportFrom) or not node.module:
                    continue
                if not node.module.startswith(("matrixark_", "tools.matrixark_")):
                    continue
                total += 1
                if id(node) not in guarded:
                    unguarded.setdefault((stem, function.name), []).append(node.module)
    return unguarded, total


class ASiblingImportInsideAFunctionNeedsBothSpellings(unittest.TestCase):

    def test_there_are_sibling_imports_to_check(self) -> None:
        """A floor. If the scan sees nothing, the assertion below passes over an empty set."""
        _unguarded, total = _scan()
        self.assertGreater(
            total, 100,
            "only %d in-function sibling imports were found, so this file is checking almost "
            "nothing -- the scan has probably stopped matching the import shape" % total)

    def test_exactly_the_recorded_ones_lack_a_fallback(self) -> None:
        """Asserted in BOTH directions.

        A new one fails here, which is the point: the failure happens when the function is CALLED,
        so nothing at import time would reveal it. One that gets fixed fails here too, so the
        allowlist cannot rot into furniture.
        """
        unguarded, _total = _scan()
        found = sorted(unguarded)
        recorded = sorted(ALLOWED)
        self.assertEqual(
            recorded, found,
            "the set of in-function sibling imports with no `tools.`/bare fallback changed.\n"
            "  now:      %s\n  recorded: %s\n"
            "A new one raises ModuleNotFoundError under one entry style, when the function runs."
            % (found, recorded))

    def test_the_three_that_were_fixed_now_name_both_spellings(self) -> None:
        """The specific regressions this file was written for."""
        expected = {
            "matrixark_mcp_core.py": ("tools.matrixark_mcp_embeddings", "matrixark_mcp_embeddings"),
            "matrixark_mcp_server.py": ("tools.matrixark_mcp_audit_queue",
                                        "matrixark_mcp_audit_queue"),
        }
        for module, spellings in sorted(expected.items()):
            with self.subTest(module=module):
                with open(os.path.join(TOOLS, module), encoding="utf-8",
                          errors="replace") as handle:
                    body = handle.read()
                for spelling in spellings:
                    self.assertIn(
                        "from %s import" % spelling, body,
                        "%s no longer imports %s; if the call moved, this entry needs restating"
                        % (module, spelling))

    def test_the_server_shutdown_import_names_both_spellings(self) -> None:
        """`close()` reaches matrixark_mcp_shutdown, and used to name only one spelling."""
        with open(os.path.join(TOOLS, "matrixark_mcp_server.py"), encoding="utf-8",
                  errors="replace") as handle:
            body = handle.read()
        for spelling in ("tools.matrixark_mcp_shutdown", "matrixark_mcp_shutdown"):
            with self.subTest(spelling=spelling):
                self.assertIn("from %s import close_server_within_budget" % spelling, body)


if __name__ == "__main__":
    unittest.main()
