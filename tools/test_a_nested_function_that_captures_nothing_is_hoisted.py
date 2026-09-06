# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A nested function that captures nothing belongs at module scope.

`retrieve` in `matrixark_local_adapter_retrieve` is the live retrieval entry point and the least
readable code on that path: 3,718 lines, nesting depth 16, 744 branches. Most of its twenty nested
`def`s genuinely close over its locals and cannot move -- relocating one of those would be a
behaviour change dressed as tidying, and this guard does not ask for it.

What it does ask is that a nested function which closes over NOTHING does not sit inside that body.
Such a function is a plain module-level function that happens to be written 1,300 lines into a
closure: it cannot be imported, cannot be tested on its own, and adds to the depth a reader has to
hold. Three were found and moved (`first_explicit_bool`, `scope_from_node_path`,
`stored_encoder_name`); this keeps the next one from accumulating.

THE LIST IS DERIVED, NOT WRITTEN. The test recomputes which nested functions capture nothing, so it
cannot go stale against a rename and cannot pass because someone edited a list. A named exemption
would defeat it -- if a genuinely free function must stay nested, the honest fix is to give it a
reason in code, not an entry here.

The detector has one trap, and a static-only version gets it wrong: this module does
`from ...matrixark_mcp_core import *`, so `Json`, `Any` and `scope_matches` are module-level at
RUNTIME but invisible to an AST scan of the file. Reading the module's real namespace is what makes
the answer right -- a static version reported all twenty as capturing something and would have
passed vacuously forever.
"""
from __future__ import annotations

import ast
import builtins
import importlib
import inspect
import unittest


def _module():
    try:
        return importlib.import_module("tools.matrixark_local_adapter_retrieve")
    except ImportError:
        return importlib.import_module("matrixark_local_adapter_retrieve")


def _bound_within(fn: ast.FunctionDef) -> set[str]:
    """Every name the function binds itself: parameters, assignments, imports, except-as."""
    names = {a.arg for a in fn.args.args + fn.args.kwonlyargs + fn.args.posonlyargs}
    if fn.args.vararg:
        names.add(fn.args.vararg.arg)
    if fn.args.kwarg:
        names.add(fn.args.kwarg.arg)
    for node in ast.walk(fn):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            names.add(node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node is not fn:
            names.add(node.name)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            names.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add((alias.asname or alias.name).split(".")[0])
    return names


class NestedFunctionsThatCaptureNothingAreHoistedTest(unittest.TestCase):

    def setUp(self) -> None:
        self.module = _module()
        source = inspect.getsource(self.module)
        self.tree = ast.parse(source)
        self.retrieve = next(
            (node for node in ast.walk(self.tree)
             if isinstance(node, ast.FunctionDef) and node.name == "retrieve"), None)
        self.assertIsNotNone(self.retrieve, "retrieve is gone; this guard is testing nothing")
        # The module's REAL namespace, which is what makes star-imported names resolvable.
        self.module_scope = set(dir(self.module)) | set(dir(builtins))

    def _free_of_capture(self, parent: ast.FunctionDef) -> list[tuple[str, int]]:
        free = []
        for node in parent.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            bound = _bound_within(node)
            loaded = {n.id for n in ast.walk(node)
                      if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
            if not (loaded - bound - self.module_scope):
                free.append((node.name, node.lineno))
        return free

    def test_the_detector_can_see_a_captured_name(self) -> None:
        """Positive control: without it, an over-broad module scope would pass everything."""
        nested = [n for n in self.retrieve.body
                  if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
        self.assertTrue(nested, "no nested defs found; the detector is not looking at retrieve")
        captured_somewhere = False
        for node in nested:
            bound = _bound_within(node)
            loaded = {n.id for n in ast.walk(node)
                      if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
            if loaded - bound - self.module_scope:
                captured_somewhere = True
                break
        self.assertTrue(
            captured_somewhere,
            "not one nested function reads an enclosing local, which means the module scope used "
            "here is too broad and the guard below would pass vacuously")

    def test_no_nested_function_in_retrieve_captures_nothing(self) -> None:
        """The rule. A closure over nothing is a module-level function in the wrong place."""
        free = self._free_of_capture(self.retrieve)
        self.assertEqual(
            [], free,
            "these nested functions in retrieve close over nothing, so they are plain functions "
            "written inside a 3,700-line body -- unimportable, untestable on their own, and paid "
            "for by every reader: %s. Move them to module scope."
            % ", ".join("%s (line %d)" % (name, lineno) for name, lineno in free))

    def test_the_hoisted_three_are_importable_and_behave(self) -> None:
        """They were moved to be usable; check they actually are, and still answer correctly."""
        for name in ("first_explicit_bool", "scope_from_node_path", "stored_encoder_name"):
            self.assertTrue(
                callable(getattr(self.module, name, None)),
                "%s is not importable at module scope" % name)

        first_explicit_bool = self.module.first_explicit_bool
        self.assertIsNone(
            first_explicit_bool("k", {}, {"k": None}, {"k": ""}),
            "absent, None and empty must all read as 'nobody said', not as False -- the caller "
            "distinguishes those to know whether it may apply its own default")
        self.assertIs(True, first_explicit_bool("k", {"k": "yes"}))
        self.assertIs(False, first_explicit_bool("k", {"k": "off"}))
        self.assertIs(True, first_explicit_bool("k", {"k": None}, {"k": 1}),
                      "a later source must still be able to answer after an unstated one")

        scope_from_node_path = self.module.scope_from_node_path
        self.assertEqual(
            {"tenant_id": "t1", "session_id": "s9"},
            scope_from_node_path(["tenant:t1", "user:", "session:s9"]),
            "an empty segment must be dropped, not stored blank: a caller comparing scopes has to "
            "tell an absent field from an empty one")
        self.assertEqual({}, scope_from_node_path("not-a-list"))

        stored_encoder_name = self.module.stored_encoder_name
        self.assertEqual(
            "e5", stored_encoder_name({"embedding_meta": {"model": "e5"}}),
            "the encoder rides under embedding_meta on owner records; reading only the top level "
            "found nothing on every record a current ingest writes")
        self.assertEqual("top", stored_encoder_name({"model": "top"}))
        self.assertEqual("", stored_encoder_name({}))


if __name__ == "__main__":
    unittest.main()
