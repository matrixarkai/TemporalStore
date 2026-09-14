#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Three hook controls are documented deployment-wide; only one of them could reach both hooks.

`docs/CLOUD_API_REFERENCE.md` carries them under "Ingestion architecture" as an `Env | Purpose`
table, and `docs/DEPLOY_CLOUD_API.md` puts them in a deployment bash block beside
`MATRIXARK_BULK_INGEST` and `MATRIXARK_RUST_PROXY_ASYNC_STORAGE`. Nothing in either says they apply
to one hook and not the other.

Measured against the two hook modules:

    MATRIXARK_HOOK_AUTO_BATCH_EXTRACT   codex: yes   claude: yes   <- wired here
    MATRIXARK_HOOK_FAST_ASYNC_INGEST    codex: yes   claude: N/A
    MATRIXARK_HOOK_FAIL_OPEN            codex: yes   claude: yes, via its wrapper

AUTO_BATCH_EXTRACT was a missing check, and is fixed. `should_auto_batch_extract_on_ingest` existed
in both hooks; the Codex copy read the flag and the Claude copy did not, so `=0` turned batched
extraction off for Codex and left it on for Claude. Both now read it, through the shared `env_bool`
rather than a comparison written locally -- this tree has already shipped a flag that read "on" as
FALSE by growing its own boolean vocabulary. The default is True in both, so a deployment that has
not set it is unaffected.

THE OTHER TWO ARE NOT MISSING WIRING, and that distinction is the point of this file:

  * FAST_ASYNC_INGEST gates "ingest stores raw and returns, no inline model". The Claude hook has no
    such path -- no async ingest, no raw-and-return, under that or any other name. There is nothing
    for the flag to gate.
  * FAIL_OPEN is supposed to decide whether a failing hook blocks the operation. The Claude hook is
    UNCONDITIONALLY fail-open: the phrase appears in that module only in prose describing what it
    already does. Setting it to 0 cannot make that hook fail closed.

So they are a documentation over-promise, not an implementation oversight, and closing them means
building two features rather than reading two variables. Recorded here rather than guessed at.

This file asserts the state in BOTH directions: a control that stops reaching a hook fails, and a
control that starts reaching one fails too, so the table above cannot rot while the docs keep
promising it.
"""
from __future__ import annotations

import ast
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(TOOLS)

CODEX = "matrixark_codex_hook.py"
CLAUDE = "matrixark_agent_hook.py"

#: flag -> (reaches the codex hook, reaches the claude hook)
RECORDED = {
    "MATRIXARK_HOOK_AUTO_BATCH_EXTRACT": (True, True),
    "MATRIXARK_HOOK_FAST_ASYNC_INGEST": (True, False),
}

#: NOT in the table above, and the reason is a correction worth keeping. MATRIXARK_HOOK_FAIL_OPEN
#: was recorded as a third control the Claude hook ignores. It is not: `matrixark_claude_hook.sh`
#: implements it -- `FAIL_OPEN="${MATRIXARK_HOOK_FAIL_OPEN:-1}"` and `[[ "$FAIL_OPEN" == "1" ]]` --
#: so the control reaches that hook through the WRAPPER rather than through Python. This file asks
#: what the Python modules read, so including it would have compared the wrong surface and called a
#: working control broken. It is also not in either doc this file checks.
#:
#: What IS wrong with it is a different thing entirely, and is not this file's subject: the shell
#: tests for the literal "1" while the Python side uses `env_bool`, so `=true` is fail-open to one
#: and fail-CLOSED to the other.
FAIL_OPEN_REACHES_CLAUDE_THROUGH_THE_WRAPPER = "tools/matrixark_claude_hook.sh"

DOCS = ("docs/CLOUD_API_REFERENCE.md", "docs/DEPLOY_CLOUD_API.md")


def _names_read(module_file):
    """Every environment variable name the module names, read from the syntax."""
    with open(os.path.join(TOOLS, module_file), encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                and node.value.startswith("MATRIXARK_HOOK_"):
            found.add(node.value)
    return found


class ADocumentedHookControlReachesBothHooks(unittest.TestCase):

    def test_both_hook_modules_are_there_to_compare(self) -> None:
        """A floor. An unreadable module would make every row below pass over an empty set."""
        for module_file in (CODEX, CLAUDE):
            with self.subTest(module=module_file):
                self.assertTrue(
                    _names_read(module_file),
                    "%s names no MATRIXARK_HOOK_ variable at all, so this file is comparing "
                    "nothing" % module_file)

    def test_the_docs_still_offer_all_three(self) -> None:
        """The premise. If the docs stop promising them, this file has no subject."""
        for flag in sorted(RECORDED):
            with self.subTest(flag=flag):
                mentions = []
                for doc in DOCS:
                    path = os.path.join(ROOT, doc)
                    if not os.path.exists(path):
                        continue
                    with open(path, encoding="utf-8", errors="replace") as handle:
                        if flag in handle.read():
                            mentions.append(doc)
                self.assertTrue(
                    mentions,
                    "%s is no longer documented, so it is no longer a promise to keep -- strike "
                    "its row" % flag)

    def test_each_control_reaches_exactly_the_hooks_recorded(self) -> None:
        """Asserted in BOTH directions, so the table cannot rot either way."""
        codex, claude = _names_read(CODEX), _names_read(CLAUDE)
        for flag, (want_codex, want_claude) in sorted(RECORDED.items()):
            with self.subTest(flag=flag):
                self.assertEqual(
                    want_codex, flag in codex,
                    "%s reaching the Codex hook changed" % flag)
                self.assertEqual(
                    want_claude, flag in claude,
                    "%s reaching the Claude hook changed. If it now does, that control was wired "
                    "and this row should say so; if it stopped, a documented control went quiet."
                    % flag)

    def test_the_wired_one_gates_the_claude_hook_too(self) -> None:
        """The fix, asserted on the function rather than on the name appearing in the file."""
        with open(os.path.join(TOOLS, CLAUDE), encoding="utf-8", errors="replace") as handle:
            tree = ast.parse(handle.read())
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) \
                    and node.name == "should_auto_batch_extract_on_ingest":
                body = ast.unparse(node)
                self.assertIn(
                    "HOOK_AUTO_BATCH_EXTRACT", body,
                    "the Claude hook's should_auto_batch_extract_on_ingest stopped reading the "
                    "control, so =0 turns batching off for Codex only again")
                return
        self.fail("%s no longer defines should_auto_batch_extract_on_ingest" % CLAUDE)

    def test_the_claude_hook_reads_the_shared_boolean_vocabulary(self) -> None:
        """Not a comparison written here.

        A flag in this tree once read "on" as FALSE because a module grew its own boolean
        vocabulary. The control has to go through the shared reader or it can disagree with the
        Codex hook on a value both accept.
        """
        with open(os.path.join(TOOLS, CLAUDE), encoding="utf-8", errors="replace") as handle:
            body = handle.read()
        self.assertIn("matrixark_mcp_env import env_bool", body)
        self.assertIn('_env_bool("MATRIXARK_HOOK_AUTO_BATCH_EXTRACT", True)', body)

    def test_fail_open_reaches_the_claude_hook_through_its_wrapper(self) -> None:
        """The correction, asserted so it cannot be re-derived wrongly.

        Recorded once as a control the Claude hook ignores. The wrapper implements it, so the
        control works; only its value vocabulary disagrees with Python's, which is a separate
        defect and a separate change.
        """
        with open(os.path.join(ROOT, FAIL_OPEN_REACHES_CLAUDE_THROUGH_THE_WRAPPER),
                  encoding="utf-8", errors="replace") as handle:
            wrapper = handle.read()
        self.assertIn('FAIL_OPEN="${MATRIXARK_HOOK_FAIL_OPEN:-1}"', wrapper,
                      "the Claude hook wrapper stopped reading MATRIXARK_HOOK_FAIL_OPEN, so that "
                      "control no longer reaches that hook at all")
        self.assertNotIn(
            "MATRIXARK_HOOK_FAIL_OPEN", _names_read(CLAUDE),
            "the Claude Python module now reads MATRIXARK_HOOK_FAIL_OPEN as well, so the control "
            "has two implementations and they can disagree")

    def test_the_one_that_cannot_be_wired_has_no_path_to_gate(self) -> None:
        """Why FAST_ASYNC_INGEST is False, checked rather than asserted from memory."""
        with open(os.path.join(TOOLS, CLAUDE), encoding="utf-8", errors="replace") as handle:
            body = handle.read()
        for marker in ("async_ingest", "store_raw", "raw_and_return"):
            with self.subTest(marker=marker):
                self.assertNotIn(
                    marker, body,
                    "the Claude hook grew a %r path, so MATRIXARK_HOOK_FAST_ASYNC_INGEST now has "
                    "something to gate and its row should be revisited" % marker)


if __name__ == "__main__":
    unittest.main()
