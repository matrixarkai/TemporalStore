#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two modules answer "which backend, and is it allowed" differently, and both are live.

matrixark_mcp_server defines backend_ready_required, default_mcp_backend and
validate_mcp_backend_policy, and matrixark_mcp_backends defines all three as well. The server's are
STRICTER, and they are not a stale copy -- they carry a policy the other never got, spelled out in
its own error message: "MatrixArk MCP no longer supports local JSONL serving backends".

On the same machine, same environment:

    matrixark_mcp_server.default_mcp_backend()    ->  "temporalstore-direct"
    matrixark_mcp_backends.default_mcp_backend()  ->  "local"

Which answer a caller gets depends on which module it imported from, and both are imported by live
code: matrixark_v1_gateway and matrixark_asgi take default_mcp_backend from matrixark_mcp_backends,
while matrixark_mcp_server re-exports its own through __all__.

WHAT THEY DISAGREE ABOUT

    default_mcp_backend          backends keeps a dev fallback -- no rust binary configured and not
                                 a production profile gives "local". The server has no fallback.
    validate_mcp_backend_policy  backends refuses a local backend only under a production profile
                                 and only when MATRIXARK_ALLOW_LOCAL_BACKEND is unset. The server
                                 refuses anything that is not one of the three temporalstore
                                 backends, profile and permission irrelevant.
    backend_ready_required       the server's set also contains "temporalstore-rust-direct".

THIS FILE DOES NOT PICK A WINNER. Which policy is right is a decision about whether a dev machine
may still serve from JSONL, and it is not one a test should make quietly. What it does is stop the
divergence from being invisible: it fails if the two ever AGREE (the decision was taken and one
side should have been deleted), and it fails if matrixark_mcp_server imports the names it defines,
which is how the divergence hid -- three imported names, shadowed six hundred lines later, dead
from the moment they were bound.
"""
from __future__ import annotations

import ast
import io
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

SHADOWED = ("backend_ready_required", "default_mcp_backend", "validate_mcp_backend_policy")


def _definitions(name):
    src = io.open(os.path.join(TOOLS, name), encoding="utf-8").read()
    lines = src.splitlines(True)
    out = {}
    for node in ast.parse(src).body:
        if getattr(node, "name", "") in SHADOWED:
            body = node.body[1:] if (node.body and isinstance(node.body[0], ast.Expr)
                                     and isinstance(node.body[0].value, ast.Constant)) else node.body
            out[node.name] = ast.dump(ast.Module(body=body, type_ignores=[]))
    return out


def _imported_names(name):
    src = io.open(os.path.join(TOOLS, name), encoding="utf-8").read()
    out = set()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.ImportFrom) and node.module and "matrixark_mcp_backends" in node.module:
            for alias in node.names:
                out.add(alias.asname or alias.name)
    return out


class TwoBackendPoliciesAreDeclaredTest(unittest.TestCase):

    def test_both_modules_still_define_all_three(self) -> None:
        """Control on the input. If one side stops defining them this file is asserting nothing."""
        server, backends = _definitions("matrixark_mcp_server.py"), _definitions("matrixark_mcp_backends.py")
        for name in SHADOWED:
            with self.subTest(name=name):
                self.assertIn(name, server, "matrixark_mcp_server no longer defines %s" % name)
                self.assertIn(name, backends, "matrixark_mcp_backends no longer defines %s" % name)

    def test_the_server_does_not_import_what_it_defines(self) -> None:
        """The shadowing itself, which is how two policies stayed invisible.

        Three names were imported from matrixark_mcp_backends and redefined six hundred lines
        later. The imports were dead at the moment they were bound, and a reader landing on the
        import block would conclude the backends policy was the one in force. It is not.
        """
        imported = _imported_names("matrixark_mcp_server.py")
        clash = sorted(set(SHADOWED) & imported)
        self.assertEqual(
            [], clash,
            "matrixark_mcp_server imports %s from matrixark_mcp_backends and defines them itself, "
            "so the import is dead and the file reads as though the other policy applies"
            % ", ".join(clash))

    def test_the_two_policies_still_differ(self) -> None:
        """Fails if they AGREE -- which would mean the decision was made and a copy is now dead.

        This is the unusual direction for a guard, and it is the right one here: the danger is not
        that they diverge, it is that somebody deletes one side to make a diff tidy without
        deciding which behaviour the product wants.
        """
        server, backends = _definitions("matrixark_mcp_server.py"), _definitions("matrixark_mcp_backends.py")
        same = [name for name in SHADOWED if server.get(name) == backends.get(name)]
        self.assertEqual(
            [], same,
            "%s now agree between matrixark_mcp_server and matrixark_mcp_backends. If that was "
            "deliberate, delete the duplicate definition and this entry; if it was a tidy-up, it "
            "changed which backends a deployment will accept." % ", ".join(same))

    def test_the_disagreement_is_reachable_and_not_theoretical(self) -> None:
        """The two answer the same question differently on the same machine.

        Executed rather than read: an environment with no MATRIXARK_MCP_BACKEND and no rust CLI
        configured is the ordinary developer case, and it is exactly where they part.
        """
        import matrixark_mcp_backends as backends
        import matrixark_mcp_server as server

        saved = {k: os.environ.get(k) for k in ("MATRIXARK_MCP_BACKEND",
                                                "MATRIXARK_TEMPORALSTORE_RUST_CLI")}
        for key in saved:
            os.environ.pop(key, None)
        try:
            self.assertNotEqual(
                server.default_mcp_backend(), backends.default_mcp_backend(),
                "the two default_mcp_backend implementations now agree with no backend configured; "
                "see the note above this test before making them agree")
        finally:
            for key, value in saved.items():
                if value is not None:
                    os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
