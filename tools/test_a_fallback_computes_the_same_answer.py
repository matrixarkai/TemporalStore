#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A fallback definition must answer what the thing it falls back to answers.

The idiom is everywhere in this tree:

    try:
        from matrixark_mcp_core_identity import stable_hash
    except ModuleNotFoundError:
        def stable_hash(value):      # the fallback
            ...

`matrixark_skill_discovery`'s fallback returned `int(sha1(value)[:15], 16)` where the real one
returns the first eight bytes of sha256 masked to 63 bits. Different algorithm, different digest,
different width -- the two agree on NO input -- and that branch computes the skill, node, raw-uri,
section and content hashes.

Nothing persisted had diverged: `append_discovered_skill_records` raises when the builders are
missing, so no record is written in that mode. But `content_hash` is computed above that gate, and
the containment rests on one `if`. A fallback whose answer differs from the thing it falls back to
is not a fallback; it is a second implementation waiting for someone to read its output as the
real one.

WHY NO GUARD SAW IT. `test_there_is_one_copy_of_each_helper` skips bodies under four statements,
deliberately -- that floor is what lets a re-export off the list, and these hashes are two lines.
And every scan that enumerates definitions from `tree.body` misses this one entirely, because it
is nested inside a module-level `try`. It sat in the blind spot of both.

WHAT THIS FILE ASSERTS. Not that the sources match -- a fallback may legitimately be written
differently, and pinning text would fail on a rename. It runs BOTH implementations over the same
inputs and requires the same answer, which is the only thing a caller depends on.
"""
from __future__ import annotations

import ast
import hashlib
import io
import os
import textwrap
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

#: (module, name, module holding the implementation it falls back to). Each entry is a definition
#: nested in a module-level try/except that shadows a live name.
FALLBACKS = (
    ("matrixark_skill_discovery", "stable_hash", "matrixark_mcp_core_identity"),
    ("matrixark_provision_api_key", "secret_hash", "matrixark_mcp_core_identity"),
)

#: Inputs the two must agree on. Short, odd and empty strings, because a width difference shows
#: up at the extremes and a digest difference shows up everywhere.
SAMPLES = ("", "a", "abc", "skill:deploy", "discovered_skill:slug:ab12cd34",
           "x" * 200, "中文", "with spaces and : colons")


def _nested_definition(module: str, name: str):
    """The fallback, compiled from its source -- it is inside a try, so it cannot be imported."""
    src = io.open(os.path.join(TOOLS, module + ".py"), encoding="utf-8").read()
    lines = src.splitlines(True)
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            body = textwrap.dedent("".join(lines[node.lineno - 1:node.end_lineno]))
            namespace: dict = {"hashlib": hashlib, "Any": object}
            exec(compile(body, module + ".py", "exec"), namespace)
            return namespace[name]
    return None


class AFallbackComputesTheSameAnswerTest(unittest.TestCase):

    def test_every_recorded_fallback_still_exists(self) -> None:
        """Control on the input. A renamed or deleted fallback must not pass by being absent."""
        for module, name, _live in FALLBACKS:
            with self.subTest(module=module, name=name):
                self.assertIsNotNone(
                    _nested_definition(module, name),
                    "%s no longer defines a fallback %s; drop the entry or point it at the new "
                    "name, because a list allowed to go stale asserts nothing" % (module, name))

    def test_the_fallback_answers_what_the_real_one_answers(self) -> None:
        import sys
        sys.path.insert(0, TOOLS)
        for module, name, live_module in FALLBACKS:
            fallback = _nested_definition(module, name)
            live = getattr(__import__(live_module), name)
            for sample in SAMPLES:
                with self.subTest(module=module, name=name, sample=sample[:20]):
                    self.assertEqual(
                        live(sample), fallback(sample),
                        "%s.%s and %s.%s disagree on %r. The fallback is not a fallback -- it is a "
                        "second implementation, and whichever one a caller gets depends on whether "
                        "an import happened to succeed."
                        % (live_module, name, module, name, sample[:40]))

    def test_the_samples_would_catch_a_different_algorithm(self) -> None:
        """Positive control on the inputs, not on the code.

        The defect this was written for was sha1-truncated against sha256-masked. If the sample set
        ever stopped separating those two, every check above would pass on a real divergence.
        """
        def sha1_style(value):
            return int(hashlib.sha1(str(value).encode("utf-8")).hexdigest()[:15], 16)

        def sha256_style(value):
            return int.from_bytes(hashlib.sha256(str(value).encode("utf-8")).digest()[:8],
                                  "big") & 0x7FFF_FFFF_FFFF_FFFF
        differing = [s for s in SAMPLES if sha1_style(s) != sha256_style(s)]
        self.assertEqual(
            list(SAMPLES), differing,
            "these samples no longer distinguish the two algorithms this file was written for, so "
            "the equality checks above could pass on a genuine divergence: %s"
            % [s for s in SAMPLES if s not in differing])


if __name__ == "__main__":
    unittest.main()
