#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A setting says when it takes effect, and the code has to agree.

`applies: live` means the value is read inside the function that needs it, so a write to the
environment is in force at once. `applies: restart` means it was resolved once at import and a
write cannot reach it. The portal uses that field to decide whether it tells the operator the
change is in force now, and the help underneath is what they read.

Forty-one settings said both. The derived settings were generated with a fixed sentence, "Frozen
when the process starts.", in every help, while `applies` was computed per read site -- and the
sentence was never conditioned on the answer, so wherever the computation said `live` the help said
the opposite.

Which half was wrong differed:

    MATRIXARK_EMBEDDING_CACHE_ENTRIES  matrixark_mcp_embeddings:38  inside a function -> live
    MATRIXARK_INGEST_TIMEOUT_MS        matrixark_mcp_server:242     CLASS BODY        -> frozen

The class-body case is what the derivation missed. `DEFAULT_REQUEST_DEADLINES_MS` and
`DEFAULT_OPERATION_CONCURRENCY` are dicts built in the body of `MatrixArkMcpServer`: resolved once
at import exactly as a module-level constant is, but not AT module level, and the rule only looked
for that. Five settings were declared live on that basis and take a restart.

Both halves are checked here, because they fail independently: a help that contradicts its own
field, and a field that contradicts the code.
"""
from __future__ import annotations

import ast
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

FROZEN_CLAIM = "frozen when the process starts"

#: The calls that read a variable out of the environment. A read through any of these resolves the
#: value at the point it runs, which is the only thing that decides live from restart.
READERS = {"get", "getenv", "env_bool", "bool_env", "_env_bool", "_bool_env", "env_int",
           "env_float", "env_str", "env_lower", "env_text", "live_int", "live_float",
           "positive_int_env", "_positive_int_env", "_env_int", "_env_float", "_env"}


def _settings():
    import matrixark_gateway_config

    return [s for s in matrixark_gateway_config.SETTINGS if getattr(s, "env", "")]


def _read_scopes() -> dict:
    """{variable: {"function", "module"}} over production modules.

    A class body counts as "module": it runs once at import, so a value resolved there is frozen
    just as a module-level constant is. Missing that is what put five settings on the wrong side.
    """
    listed = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=REPO,
                            capture_output=True, text=True, check=False).stdout.split()
    out: dict = {}
    for rel in listed:
        if os.path.basename(rel).startswith("test_"):
            continue
        try:
            with open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue
        inside = set()
        for func in ast.walk(tree):
            if isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                for node in ast.walk(func):
                    inside.add(id(node))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else (
                func.attr if isinstance(func, ast.Attribute) else "")
            if name not in READERS:
                continue
            try:
                variable = ast.literal_eval(node.args[0])
            except (ValueError, SyntaxError):
                continue
            if not isinstance(variable, str) or not variable.startswith(("MATRIXARK_", "TS_")):
                continue
            out.setdefault(variable, set()).add("function" if id(node) in inside else "module")
    return out


class ASettingSaysWhenItTakesEffectAndTheCodeAgreesTest(unittest.TestCase):

    def test_no_live_setting_claims_to_be_frozen(self) -> None:
        """The two halves of one record must not contradict each other. Whichever an operator
        believes, the other one is telling them something false about the same field."""
        contradictory = sorted(
            "%s (%s)" % (setting.key, setting.env)
            for setting in _settings()
            if setting.applies == "live" and FROZEN_CLAIM in (setting.help or "").lower())
        self.assertEqual(
            [], contradictory,
            "these settings are declared live and their help says they are frozen until a "
            "restart; one of the two is wrong on a field the portal offers")

    def test_no_live_setting_is_resolved_once_at_import(self) -> None:
        """The field against the code. A variable read only at module or CLASS-body scope is
        resolved at import, so a write cannot reach it and `live` is a promise the process cannot
        keep."""
        scopes = _read_scopes()
        frozen = []
        for setting in _settings():
            if setting.applies != "live":
                continue
            seen = scopes.get(setting.env)
            if seen and "function" not in seen:
                frozen.append("%s (%s)" % (setting.key, setting.env))
        self.assertEqual(
            [], sorted(frozen),
            "these settings are declared live and every read of them happens once at import, so "
            "changing one does nothing until the process restarts")

    def test_the_scan_finds_the_settings_and_the_reads(self) -> None:
        """A floor. With either side empty both assertions above pass over nothing."""
        settings = _settings()
        scopes = _read_scopes()
        self.assertGreater(len(settings), 150, "the Setting scan came back nearly empty")
        self.assertGreater(len(scopes), 200, "the read scan came back nearly empty")
        self.assertGreater(
            sum(1 for s in settings if s.applies == "live"), 50,
            "almost nothing is declared live, so the checks above are about nothing")

    def test_a_class_body_read_counts_as_frozen(self) -> None:
        """The discriminator, on the real example that was missed. `MATRIXARK_INGEST_TIMEOUT_MS` is
        read in the body of `MatrixArkMcpServer`, not at module level and not inside a function. If
        the scan counted that as a function read, the check above would accept the declaration it
        exists to refuse."""
        seen = _read_scopes().get("MATRIXARK_INGEST_TIMEOUT_MS", set())
        self.assertEqual(
            {"module"}, seen,
            "a value built in a class body must count as resolved at import, not per call")


if __name__ == "__main__":
    unittest.main()
