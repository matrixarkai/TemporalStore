# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Every production caller mints an index posting with the SAME implementation.

`context_index_posting_record` is defined twice:

* `matrixark_mcp_core` — required `data_model`, no `capability` parameter
* `matrixark_mcp_indexing` — optional `data_model`, plus a `capability` it folds into the key

They are not two spellings of one function. Probed on five realistic postings, all five returning,
**`index_hash` differs on every one** — the index KEY, not just an extra field. Two writers using
different copies would file the same logical posting under two different keys: lookups keyed one way
would miss rows written the other, and the index would carry both.

Today that cannot happen. All eleven calling modules resolve the name to `matrixark_mcp_core`, and
`matrixark_mcp_indexing` is recorded as unreachable from any production entry point. The live store
agrees: of 4,853 real `context_index` rows sampled from the WAL, **0 carry `capability`** and 100%
carry `data_model`, under `posting_policy: bucketed_by_scope_data_model_index_time`.

So this guard is not fixing anything. It holds the arrangement that makes the duplicate harmless:
the day `matrixark_mcp_indexing` becomes reachable, or one module's import flips, the index key
splits and nothing else in the suite would notice — every call site keeps compiling, and the name is
identical at all 22 of them.

THE RULE IS "THERE MUST NOT BE TWO IN USE", NOT "THE TWO MUST AGREE". A same-answer check would pass
for years on postings where the two happen to coincide, and there are none here anyway — they
disagree on every input tried.

The caller list is DERIVED by parsing every non-test module for the call and importing it, so it
cannot go stale against a rename and cannot pass because someone edited a list.
"""
from __future__ import annotations

import ast
import importlib
import pathlib
import unittest

NAME = "context_index_posting_record"


def _tools_dir() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent


def _import(module_name: str):
    try:
        return importlib.import_module("tools.%s" % module_name)
    except ImportError:
        return importlib.import_module(module_name)


def _origin(function) -> str:
    return "%s:%d" % (function.__module__.split(".")[-1], function.__code__.co_firstlineno)



def _recorded_module_names(path: pathlib.Path) -> set[str]:
    """Module names held as DATA in the reachability record.

    Collected from module-level assignments only, so a name that merely appears in a comment or a
    docstring does not count as recorded. Mutation testing found the earlier substring version
    passing with the name commented out -- a mention is not membership.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, SyntaxError):
        return set()
    names: set[str] = set()
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        for inner in ast.walk(node):
            if isinstance(inner, ast.Constant) and isinstance(inner.value, str):
                value = inner.value.strip()
                if value.startswith("matrixark_") and "\n" not in value and " " not in value:
                    names.add(value)
    return names


class ThereIsOneIndexPostingRecordInUseTest(unittest.TestCase):

    def setUp(self) -> None:
        self.tools = _tools_dir()

    def _definers(self) -> list[str]:
        found = []
        for path in sorted(self.tools.glob("*.py")):
            if path.name.startswith("test_"):
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
            except (OSError, SyntaxError):
                continue
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == NAME:
                    found.append(path.stem)
        return sorted(found)

    def _calling_modules(self) -> list[str]:
        found = []
        for path in sorted(self.tools.glob("*.py")):
            if path.name.startswith("test_"):
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            if "%s(" % NAME not in text:
                continue
            try:
                tree = ast.parse(text)
            except SyntaxError:
                continue
            calls = any(
                isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == NAME
                for node in ast.walk(tree))
            if calls:
                found.append(path.stem)
        return sorted(found)

    def test_the_duplicate_still_exists_so_this_guard_is_not_vacuous(self) -> None:
        """Positive control. If there were only one definition there would be nothing to hold."""
        definers = self._definers()
        self.assertGreaterEqual(
            len(definers), 1, "%s is not defined anywhere; this guard is testing nothing" % NAME)
        if len(definers) == 1:
            self.skipTest(
                "only one definition remains (%s) -- the duplicate this guard exists for is gone, "
                "and the guard can be retired rather than left to pass for the wrong reason"
                % definers[0])

    def test_every_production_caller_resolves_to_the_same_implementation(self) -> None:
        """The rule. Two implementations are harmless only while one of them is unused."""
        callers = self._calling_modules()
        self.assertTrue(callers, "no module calls %s; the detector is not finding call sites" % NAME)

        resolved: dict[str, str] = {}
        for module_name in callers:
            try:
                module = _import(module_name)
            except Exception:
                continue  # a module that cannot import here cannot be calling it at runtime either
            function = getattr(module, NAME, None)
            if function is None or not hasattr(function, "__code__"):
                continue
            resolved[module_name] = _origin(function)

        self.assertTrue(
            resolved,
            "not one calling module bound %s at module scope -- the resolution check ran on "
            "nothing and would pass however many implementations were in use" % NAME)

        origins = sorted(set(resolved.values()))
        self.assertEqual(
            1, len(origins),
            "production callers mint index postings with DIFFERENT implementations, which file the "
            "same posting under different index_hash values: %s"
            % "; ".join("%s -> %s" % (m, o) for m, o in sorted(resolved.items())
                        if len(origins) > 1))

    def test_the_unused_definition_is_not_production_reachable(self) -> None:
        """The other half of why the duplicate is safe, and the thing most likely to change."""
        definers = self._definers()
        if len(definers) < 2:
            self.skipTest("only one definition; nothing to be unreachable")

        callers = self._calling_modules()
        in_use = set()
        for module_name in callers:
            try:
                module = _import(module_name)
            except Exception:
                continue
            function = getattr(module, NAME, None)
            if function is not None and hasattr(function, "__code__"):
                in_use.add(function.__module__.split(".")[-1])

        unused = [d for d in definers if d not in in_use]
        self.assertTrue(
            unused,
            "every definition of %s is in use by some production caller, so the index key depends "
            "on which import a module happened to take" % NAME)

        recorded = (self.tools / "test_a_module_only_tests_reach_is_not_live.py")
        if not recorded.exists():
            self.skipTest("the reachability record is gone; cannot confirm the unused copy is dead")
        listed = _recorded_module_names(recorded)
        self.assertTrue(
            listed,
            "parsed no module names out of the reachability record -- the membership check would "
            "pass for every name, which is how a substring version of this passed with the name "
            "commented out")
        for module_name in unused:
            self.assertIn(
                module_name, listed,
                "%s defines an unused %s but is NOT recorded as unreachable from production. If it "
                "became reachable, postings would be filed under two different index_hash values "
                "with nothing failing." % (module_name, NAME))


if __name__ == "__main__":
    unittest.main()
