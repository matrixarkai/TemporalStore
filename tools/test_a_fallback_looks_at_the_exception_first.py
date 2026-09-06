# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A fallback that degrades must look at the exception first.

`matrixark_codex_hook.fast_async_hook_ingest` called the adapter twice:

    try:
        result = session_commit(args, hook=hook)
    except TypeError:
        result = session_commit(args)
    except Exception as exc:
        result = {"status": "error", ...}

which reads as tolerance for an adapter whose `session_commit` predates the `hook` parameter. Every
`session_commit` in this tree takes `hook` -- the mixin and the module-level one both -- so it could
not fire for the reason it exists. Anything it DID catch came from inside the commit, and the
response was to run the whole commit again without the hook, silently, with the structured error
below never reached.

Its three siblings all check the message and re-raise:
`matrixark_mcp_core_session.adapter_ensure_backend_ready`, the native retriever in
`matrixark_temporal_direct_read`, and `append_records` in `matrixark_temporal_direct_backend`. This
was the one that did not.

WHAT THIS FILE DOES NOT ASSERT, and why. `matrixark_mcp_temporal_adapters.close` has the same shape
around `super_close(timeout_s=...)` and is left alone: `MatrixArkRustCdylibClient.close` and
`MatrixArkRustProxyClient.close` genuinely take no `timeout_s`, so that fallback fires for the
reason it exists. The shape is not the defect -- a degrading fallback whose parameter every
implementation already accepts is.

So the rule below is the narrow one that actually decides it: if every `session_commit` accepts
`hook`, the fallback can only be catching something else, and it must re-raise that. If an
implementation without `hook` ever appears, this fails and says to widen the handler rather than
narrow it -- which is the correct answer in that direction, and the reason this is asserted rather
than the fallback simply deleted.
"""
from __future__ import annotations

import ast
import importlib
import os
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
METHOD = "session_commit"
PARAMETER = "hook"

#: The call sites the rule is about.
CALL_SITES = "matrixark_codex_hook.py"


def _implementations():
    """(where, accepts the parameter) for every session_commit defined in the tree."""
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
        for node in ast.walk(tree):
            owner = None
            if isinstance(node, ast.ClassDef):
                owner = node.name
                candidates = node.body
            elif isinstance(node, ast.Module):
                candidates = node.body
            else:
                continue
            for item in candidates:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                        and item.name == METHOD:
                    params = {a.arg for a in item.args.args + item.args.kwonlyargs}
                    found.append(("%s:%s" % (os.path.basename(rel), owner or "<module>"),
                                  PARAMETER in params))
    return found


def _degrading_handlers():
    """Handlers in the call-site module that call session_commit with fewer arguments."""
    tree = ast.parse((REPO / "tools" / CALL_SITES).read_text(encoding="utf-8"))
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        tried = [c for stmt in node.body for c in ast.walk(stmt)
                 if isinstance(c, ast.Call) and getattr(c.func, "id", None) == METHOD]
        if not tried:
            continue
        for handler in node.handlers:
            fell = [c for stmt in handler.body for c in ast.walk(stmt)
                    if isinstance(c, ast.Call) and getattr(c.func, "id", None) == METHOD]
            if not fell:
                continue
            if max(len(c.args) + len(c.keywords) for c in tried) <= \
                    max(len(c.args) + len(c.keywords) for c in fell):
                continue
            checks = any(isinstance(inner, ast.Raise) for stmt in handler.body
                         for inner in ast.walk(stmt))
            out.append((handler.lineno, checks))
    return out


class AFallbackLooksAtTheExceptionFirstTest(unittest.TestCase):

    def test_there_are_handlers_to_check(self) -> None:
        """Extent. Two when this was written; a scan finding none passes vacuously."""
        handlers = _degrading_handlers()
        self.assertGreaterEqual(
            len(handlers), 2,
            "found %d degrading %s handlers in %s, expected at least 2 -- if the call sites were "
            "restructured, point this file at where they went"
            % (len(handlers), METHOD, CALL_SITES))

    def test_every_implementation_takes_the_parameter(self) -> None:
        """The premise. If this stops holding, the fallback becomes necessary and must widen."""
        found = _implementations()
        self.assertGreaterEqual(len(found), 2, "the scan no longer finds the implementations")
        missing = [where for where, takes in found if not takes]
        self.assertEqual(
            [], missing,
            "%s does not take `%s`, so the fallback in %s can now fire for the reason it was "
            "written for. WIDEN the handler rather than narrowing it, and change this file to say "
            "so" % (", ".join(missing), PARAMETER, CALL_SITES))

    def test_each_handler_reraises_what_it_did_not_expect(self) -> None:
        offenders = [line for line, checks in _degrading_handlers() if not checks]
        self.assertEqual(
            [], offenders,
            "the handler at line %s degrades without looking at the exception. Every %s in this "
            "tree takes `%s`, so it cannot be catching a signature mismatch -- it is catching a "
            "failure inside the commit and running the commit again without the hook"
            % (offenders, METHOD, PARAMETER))

    def test_the_check_is_the_one_the_siblings_use(self) -> None:
        """Not just any raise: the same message test the other three fallbacks apply."""
        source = (REPO / "tools" / CALL_SITES).read_text(encoding="utf-8")
        self.assertGreaterEqual(
            source.count('if "unexpected keyword argument" not in str(exc)'), 2,
            "the handlers no longer test the exception the way "
            "matrixark_mcp_core_session.adapter_ensure_backend_ready does, so a TypeError from "
            "inside the commit can be misread as a missing parameter again")


if __name__ == "__main__":
    unittest.main()
