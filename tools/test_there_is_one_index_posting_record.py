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

    def test_the_unused_definition_has_no_caller_at_all(self) -> None:
        """Why the duplicate is safe -- and it is a THINNER margin than "the module is dead".

        `matrixark_mcp_indexing` is a LIVE module: it is one of the seeds in the reachability
        record's LIVE_ROOTS. So the unused copy is not protected by sitting in dead code. It is
        protected only by the fact that nothing calls it -- not one production module, and not even
        the module that defines it. One `from matrixark_mcp_indexing import
        context_index_posting_record` is all it would take to split the index key.
        """
        definers = self._definers()
        if len(definers) < 2:
            self.skipTest("only one definition; nothing to be unused")

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
            "every definition of %s is in use by some production caller, so which index_hash a "
            "posting gets depends on which import its module happened to take" % NAME)

        # The defining module must not call its own copy either -- that would make it a caller,
        # and the key would then depend on which module did the minting.
        for module_name in unused:
            self.assertNotIn(
                module_name, callers,
                "%s defines an unused %s and also CALLS it, so it mints postings under a "
                "different index_hash than every other module" % (module_name, NAME))


if __name__ == "__main__":
    unittest.main()
