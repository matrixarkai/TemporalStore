#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An empty pack says it is empty -- on one of the two assemblies.

`retrieve` has two. The Python assembly ends by setting ``"insufficient_context": not selected``,
so a pack with nothing in it says so. The native assembly returns the engine's pack through a
block of `setdefault` calls that fill in five fields -- `context_pack_assembly`,
`context_pack_cache_hit`, `pre_retrieval_summary_refresh`, `remote_context_refs`,
`selected_ref_counts` -- and `insufficient_context` is not one of them. The block never mentions
it, and compaction downstream only PROPAGATES the flag (`if pack.get("insufficient_context")`) or
strips it when false; neither originates it.

So an empty answer from the engine carries no marker at all.

## Why that matters, from an incident rather than from theory

A one-box returned `HTTP 200`, 141 bytes, `groups: []`, **no warnings**, from a store holding
3,045 `context_event` records, and stayed that way across restarts. The response was:

    {"context_pack_id": "rust-native-...", "groups": [], "tokens": {},
     "served_by": {"assembly": "native_backend"}}

Nothing in it distinguishes "the engine searched and found nothing" from "the engine answered
with nothing". The third possibility, a shed, is the only empty pack that DOES announce itself --
it carries `service_backpressure` in `warnings` and sets `partial` and `insufficient_context` --
which is why the Explore page can name shedding as a cause and cannot name this.

## Recorded, not fixed

Setting the flag on the native path is a one-line `setdefault`, and every reader already defaults
it, so it is tempting. It is still a decision: `insufficient_context` means "we looked and there
was not enough", and an engine that answered with nothing may not have looked. Marking a possible
engine failure as a legitimate empty result would make the incident above HARDER to spot, not
easier. Which of the two an empty native pack is -- and whether it needs a third word rather than
this one -- is a serving question.

So this asserts the difference exactly, in both directions, the way matrixarkai#1872, #1875 and
#1909 record a diverged pair. If the native path starts marking empty packs this fails and
somebody confirms that was meant; if the Python path stops, it fails too.
"""
from __future__ import annotations

import ast
import io
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

RETRIEVE_SOURCE = os.path.join(TOOLS, "matrixark_local_adapter_retrieve.py")

#: The fields the native block fills in for itself today. Recorded so that a field appearing or
#: disappearing is visible here, not only the one this file is about.
NATIVE_SETDEFAULTS = frozenset({
    "context_pack_assembly", "context_pack_cache_hit", "pre_retrieval_summary_refresh",
    "remote_context_refs", "selected_ref_counts",
})


def _native_block() -> str:
    """The source of `if native_pack is not None:` up to the return that ends it."""
    with io.open(RETRIEVE_SOURCE, encoding="utf-8") as handle:
        source = handle.read()
    start = source.index("if native_pack is not None:")
    end = source.index("return compact_context_pack_for_serving", start)
    return source[start:end]


class AnEmptyPackSaysItIsEmptyTest(unittest.TestCase):

    def test_the_python_assembly_marks_an_empty_pack(self) -> None:
        """Measured by retrieving from an empty store, not read out of the source."""
        import matrixark_mcp_server as mcp

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "empty.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            scope = {"account_id": "acct_local", "tenant_id": "emptypack", "user_id": "u",
                     "session_id": "s0", "agent_name": "probe"}
            pack = server.call_tool("matrixark_retrieve",
                                    {"scope": scope, "query": "nothing is stored yet"})

        self.assertEqual([], pack.get("groups") or [],
                         "the fixture is meant to retrieve from an EMPTY store; it found "
                         "something, so the check below is not about an empty pack")
        self.assertEqual("python_local_adapter",
                         (pack.get("served_by") or {}).get("assembly"),
                         "this fixture is meant to exercise the PYTHON assembly")
        self.assertTrue(pack.get("insufficient_context"),
                        "the Python assembly stopped marking an empty pack. If that was "
                        "deliberate, the two assemblies now agree by omission and this record "
                        "should go -- but an empty pack that says nothing is what made the "
                        "native incident undiagnosable.")

    def test_the_native_assembly_does_not(self) -> None:
        """Structural: the engine is not running here, so the source is the evidence available."""
        block = _native_block()
        self.assertNotIn(
            "insufficient_context", block,
            "the native block now mentions insufficient_context. If it sets it, the divergence "
            "this file records is resolved and the file should go with it -- after deciding "
            "whether an empty engine answer is 'not enough context' or a failure.")

    def test_the_fields_the_native_block_does_fill_in_are_the_recorded_ones(self) -> None:
        """A neighbouring field appearing is the same class of change and worth seeing."""
        block = _native_block()
        found = set(re.findall(r'native_pack\.setdefault\(\s*["\']([a-z_]+)["\']', block))
        self.assertEqual(
            NATIVE_SETDEFAULTS, found,
            "the native block's own defaults changed. That is where a marker for an empty pack "
            "would go, so the list is pinned rather than left to drift.")

    def test_compaction_does_not_originate_the_flag(self) -> None:
        """The other place it could come from, ruled out by reading both writers.

        Both mentions downstream are conditional on the flag ALREADY being set: one copies it
        onto the compact pack, the other removes it when falsy. Neither invents it, so the native
        pack cannot acquire it on the way out.
        """
        originators = []
        for name in ("matrixark_mcp_context_pack.py", "matrixark_mcp_core_context_pack.py"):
            path = os.path.join(TOOLS, name)
            if not os.path.exists(path):
                continue
            with io.open(path, encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
            for node in ast.walk(tree):
                if not isinstance(node, ast.Assign):
                    continue
                targets = " ".join(ast.unparse(t) for t in node.targets)
                if "insufficient_context" not in targets:
                    continue
                value = ast.unparse(node.value)
                # `compact["insufficient_context"] = True` under `if pack.get(...)` is a copy.
                if value.strip() not in ("True", "pack.get('insufficient_context')",
                                         'pack.get("insufficient_context")'):
                    originators.append("%s: %s = %s" % (name, targets, value))
        self.assertEqual([], originators,
                         "something downstream now computes insufficient_context rather than "
                         "copying it, so an empty native pack may acquire the flag after all: "
                         + "; ".join(originators))


if __name__ == "__main__":
    unittest.main()
