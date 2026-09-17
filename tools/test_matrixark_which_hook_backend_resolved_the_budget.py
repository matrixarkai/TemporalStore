#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""One row, two implementations: which hook backend resolved the agent budget.

An agent hook has two backends. ``matrixark_claude_hook.sh`` picks between them with
``MATRIXARK_CLAUDE_HOOK_BACKEND``, whose default is ``auto``: the shared python pipeline when the
rust proxy binary is present, the self-contained offline engine otherwise. **Which one runs turns
on whether a binary exists on the machine the hook runs on**, which the gateway cannot see.

They do not resolve the same budget. The python pipeline reads
``MATRIXARK_HOOK_MAX_CONTEXT_TOKENS``, falls back to ``MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS``, then
to 128000. ``bin/codex_context_hook.rs`` reads the first variable only and falls back to 1024.

Measured by running the python resolver and compiling the rust expression verbatim, over 18
environments: **14 disagreed.** They agree only on a plain decimal integer within u32.

===========================  ==========  ========
environment                  python      rust
===========================  ==========  ========
nothing set                     128000       1024
hook=0                          128000          0
hook=-5 / abc / '' / 0x20       128000       1024
only default=64000               64000       1024
hook=4294967296                  ...          1024
hook=1_000                        1000       1024
hook=<arabic-indic 8000>          8000       1024
hook=10000  /  ' 8000 '  /  +900            AGREE
===========================  ==========  ========

``hook=0`` is the sharp one: python reads 0 as "not set" and returns its default, the offline
engine takes it literally and gives the agent **a budget of zero tokens**.

Which fallback is right for an unconfigured hook is a product decision, recorded rather than
changed -- see ``test_engine_settings_offer_the_engine_default.INLINE_DISAGREES``. What this file
holds is narrower and is the portal's business: the panel shows ONE number for that row, and it
must say when that number speaks for only one of the two backends.

The rust side is pinned by READING its source rather than by trusting this docstring, so a change
there fails here and the recording gets revisited.
"""
from __future__ import annotations

import io
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_runtime_config as runtime  # noqa: E402

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
HOOK_SH = os.path.join(TOOLS, "matrixark_claude_hook.sh")
RUST_HOOK = os.path.join(REPO, "crates", "temporalstore-rust", "src", "bin",
                         "codex_context_hook.rs")
HOOK_VAR = "MATRIXARK_HOOK_MAX_CONTEXT_TOKENS"
DEFAULT_VAR = "MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS"


def read(path: str) -> str:
    with io.open(path, encoding="utf-8", errors="replace") as handle:
        return handle.read()


class TheWrapperAgreesWithItselfAboutItsDefaultTest(unittest.TestCase):
    """The header said one backend was the default and the code applied another.

    Not cosmetic: the two backends resolve different context budgets from the same environment, so
    a reader configuring from the header is wrong about what an agent gets.
    """

    def test_the_documented_default_is_the_one_the_code_applies(self) -> None:
        source = read(HOOK_SH)
        match = re.search(r'BACKEND="\$\{MATRIXARK_CLAUDE_HOOK_BACKEND:-(\w+)\}"', source)
        self.assertIsNotNone(match, "the backend assignment moved; this guard is stale")
        applied = match.group(1)

        documented = set(re.findall(r"^#\s+(\w+)\s+\(default\)", source, re.M))
        self.assertTrue(documented, "no comment names a default backend any more")
        self.assertEqual({applied}, documented,
                         "the comments name %s as the default and the code applies %r"
                         % (sorted(documented), applied))


class TheTwoBackendsResolveDifferentBudgetsTest(unittest.TestCase):
    """The recorded divergence, pinned on both sides."""

    def setUp(self) -> None:
        self.saved = {name: os.environ.get(name) for name in (HOOK_VAR, DEFAULT_VAR)}
        self.addCleanup(self._restore)
        for name in (HOOK_VAR, DEFAULT_VAR):
            os.environ.pop(name, None)

    def _restore(self) -> None:
        for name, value in self.saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def test_the_offline_engine_still_falls_back_to_its_own_number(self) -> None:
        source = read(RUST_HOOK)
        self.assertIn(HOOK_VAR, source, "the offline engine no longer reads the hook budget")
        self.assertRegex(source, r"unwrap_or\(1024\)",
                         "the offline engine's fallback changed; re-measure the table in this "
                         "file's docstring before editing this assertion")

    def test_the_offline_engine_never_reads_the_second_variable(self) -> None:
        """The python resolver consults it; the offline engine does not. That is why configuring
        the general default moves one backend and not the other."""
        self.assertNotIn(DEFAULT_VAR, read(RUST_HOOK))

    def test_with_nothing_set_the_python_side_is_far_larger(self) -> None:
        self.assertEqual(128000, runtime.hook_max_context_tokens())

    def test_zero_means_unset_to_python(self) -> None:
        """And is taken literally by the offline engine, which is the sharp end of this: an
        operator writing 0 gets the python default on one backend and a budget of nothing on the
        other."""
        os.environ[HOOK_VAR] = "0"
        self.assertEqual(128000, runtime.hook_max_context_tokens())

    def test_the_second_variable_moves_the_python_side_only(self) -> None:
        os.environ[DEFAULT_VAR] = "64000"
        self.assertEqual(64000, runtime.hook_max_context_tokens())

    def test_an_ordinary_value_is_read_by_both(self) -> None:
        """The case the panel's note tells an operator to create."""
        os.environ[HOOK_VAR] = "10000"
        self.assertEqual(10000, runtime.hook_max_context_tokens())


class ThePanelSaysWhichBackendItSpeaksForTest(unittest.TestCase):

    def setUp(self) -> None:
        self.saved = os.environ.get(HOOK_VAR)
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        if self.saved is None:
            os.environ.pop(HOOK_VAR, None)
        else:
            os.environ[HOOK_VAR] = self.saved

    def _panel(self):
        import matrixark_v1_gateway as gw
        return gw._skill_budget_panel() if hasattr(gw, "_skill_budget_panel") else None

    def test_the_gateway_reports_whether_the_variable_is_set(self) -> None:
        import matrixark_v1_gateway as gw
        source = read(os.path.join(TOOLS, "matrixark_v1_gateway.py"))
        self.assertIn('"hook_budget_is_set"', source,
                      "the panel no longer says whether the agent-hook row speaks for both "
                      "backends")
        self.assertTrue(hasattr(gw, "ROUTE_DOCS"))

    def test_the_panel_carries_no_field_nothing_renders(self) -> None:
        """A payload field no surface reads is the shape this work keeps finding elsewhere --
        declared, plausible, unreachable. The page says which backend resolved the row in prose,
        which reads better than the implementation word, so the field was dropped rather than
        left to look like an API somebody depends on."""
        source = read(os.path.join(TOOLS, "matrixark_v1_gateway.py"))
        page = read(os.path.join(TOOLS, "portal", "setup_portal.html"))
        self.assertNotIn("hook_budget_resolved_by", source)
        self.assertNotIn("hook_budget_resolved_by", page)

    def test_only_the_variable_both_backends_read_counts_as_set(self) -> None:
        """The resolver falls back through a SECOND variable the offline engine never reads.

        A deployment that has set only MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS still has its two
        backends disagreeing -- 64000 against 1024 -- so treating that as configured would
        suppress the note in exactly the case it is about. This was a live regression for the
        length of one refactor: asking the resolver "did anything answer" instead of "did the hook
        variable answer" reads as configured here.
        """
        import matrixark_mcp_runtime_config as rc
        saved = {name: os.environ.get(name) for name in (HOOK_VAR, DEFAULT_VAR)}
        try:
            for name in (HOOK_VAR, DEFAULT_VAR):
                os.environ.pop(name, None)
            os.environ[DEFAULT_VAR] = "64000"
            value, source = rc.hook_max_context_tokens_with_source()
            self.assertEqual(64000, value)
            self.assertEqual(DEFAULT_VAR, source,
                             "the second variable answered, and the offline engine does not read "
                             "it -- so this must not read as 'the hook budget is configured'")
        finally:
            for name, was in saved.items():
                if was is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = was

    def test_the_gateway_names_the_variable_both_backends_read(self) -> None:
        source = read(os.path.join(TOOLS, "matrixark_v1_gateway.py"))
        self.assertIn('[1] == "MATRIXARK_HOOK_MAX_CONTEXT_TOKENS"', source,
                      "the panel treats any answering variable as configured, which suppresses "
                      "the note for a deployment that set only the general default")

    def test_the_page_warns_only_when_it_is_unset(self) -> None:
        """A warning that always fires is noise -- the same reasoning the panel already applies to
        paths_differ. With the variable set, both backends read it."""
        page = read(os.path.join(TOOLS, "portal", "setup_portal.html"))
        self.assertIn("budgets.hook_budget_is_set === false", page,
                      "the page shows the backend note unconditionally, or not at all")
        self.assertIn("two hook", page)

    def test_the_note_tells_the_reader_what_to_do(self) -> None:
        page = read(os.path.join(TOOLS, "portal", "setup_portal.html"))
        self.assertIn("MATRIXARK_HOOK_MAX_CONTEXT_TOKENS", page)


if __name__ == "__main__":
    unittest.main()
