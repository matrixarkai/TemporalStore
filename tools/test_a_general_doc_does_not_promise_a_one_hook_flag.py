#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A hook flag documented for everyone must be read by every hook, or say which one reads it.

`docs/CLOUD_API_REFERENCE.md` and `docs/DEPLOY_CLOUD_API.md` are the general deployment documents.
Neither is about one agent, and the section that carries these flags is headed "Ingestion
architecture", between the backend and rate-limit sections.

Two `MATRIXARK_HOOK_*` flags appear there, and only one of them behaved the way the table implied:

    MATRIXARK_HOOK_AUTO_BATCH_EXTRACT     read by BOTH hooks
    MATRIXARK_HOOK_FAST_ASYNC_INGEST      read by the CODEX hook only

One row right and one row wrong is what made the table misleading rather than obviously scoped. An
operator who set the fast-ack flag got a Codex hook that stores raw and returns, and a Claude hook
that still calls the model inline on every ingest -- the exact failure the surrounding prose says
these flags exist to avoid, still happening, for half the traffic.

The whole surface, measured: `matrixark_codex_hook` reads 16 `MATRIXARK_HOOK_*` flags and
`matrixark_agent_hook` reads 2. Codex-only flags are not a defect -- most of those 16 are
documented in the Codex installation manual, where being Codex-only is the point. The defect is a
flag in a GENERAL table that only one hook honours, with nothing saying so.

WHAT THIS ASSERTS. Not that the two hooks must support the same flags: that is a product decision
about the Claude hook, and making `matrixark_agent_hook` honour the fast-ack flag would change what
an existing deployment does. What it asserts is that the general documents cannot promise a control
to everyone that only one hook reads, without saying which.
"""
from __future__ import annotations

import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

#: The documents that speak to every deployment, not to one agent's installation.
GENERAL_DOCS = (
    os.path.join(REPO, "docs", "CLOUD_API_REFERENCE.md"),
    os.path.join(REPO, "docs", "DEPLOY_CLOUD_API.md"),
)

#: The hook modules. A flag promised generally has to be honoured by both, or scoped in the text.
HOOKS = {
    "codex": os.path.join(TOOLS, "matrixark_codex_hook.py"),
    "agent": os.path.join(TOOLS, "matrixark_agent_hook.py"),
}

FLAG = re.compile(r"MATRIXARK_HOOK_[A-Z0-9_]+")

#: How far past the flag's own line a scope note may sit. The deploy guide documents these in a
#: bash block where the note is a trailing comment on the following lines.
SCOPE_WINDOW = 3

#: Words that name a hook. A scope note has to say WHICH, not merely that a limit exists.
NAMES_A_HOOK = re.compile(r"codex|claude|agent[_ ]hook|matrixark_agent_hook", re.IGNORECASE)


def _read(path: str) -> str:
    with open(path, encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _flags_read_by_each_hook() -> dict:
    return {name: set(FLAG.findall(_read(path))) for name, path in HOOKS.items()}


def _documented_flags() -> dict:
    """flag -> [(document, line number, the lines a scope note could live on)]"""
    found: dict = {}
    for path in GENERAL_DOCS:
        lines = _read(path).splitlines()
        for number, line in enumerate(lines):
            for flag in set(FLAG.findall(line)):
                window = " ".join(lines[number:number + 1 + SCOPE_WINDOW])
                found.setdefault(flag, []).append((os.path.basename(path), number + 1, window))
    return found


class AGeneralDocDoesNotPromiseAOneHookFlag(unittest.TestCase):

    def test_both_hook_modules_are_readable(self) -> None:
        """A floor. If a module moved, every check below passes over an empty set."""
        by_hook = _flags_read_by_each_hook()
        for name, flags in sorted(by_hook.items()):
            with self.subTest(hook=name):
                self.assertTrue(
                    flags,
                    "%s reads no MATRIXARK_HOOK_ flag at all, so this guard is scanning the wrong "
                    "file and its silence means nothing" % HOOKS[name],
                )

    def test_the_general_docs_still_carry_hook_flags(self) -> None:
        """The other floor: a scan of documents that stopped naming these reports clean."""
        documented = _documented_flags()
        self.assertGreaterEqual(
            len(documented), 2,
            "only %d MATRIXARK_HOOK_ flags found across the general docs; the scan has stopped "
            "seeing them and would report a new unscoped one as clean" % len(documented),
        )

    def test_a_flag_only_one_hook_reads_says_so(self) -> None:
        by_hook = _flags_read_by_each_hook()
        unscoped = []
        for flag, sites in sorted(_documented_flags().items()):
            readers = sorted(name for name, flags in by_hook.items() if flag in flags)
            if len(readers) == len(HOOKS) or not readers:
                # Read by every hook, or by none of them -- neither is this file's subject. A flag
                # no hook reads is a different defect and a different guard.
                continue
            for document, number, window in sites:
                if not NAMES_A_HOOK.search(window):
                    unscoped.append(
                        "%s:%d %s is read only by the %s hook and the text does not say so"
                        % (document, number, flag, "/".join(readers))
                    )
        self.assertEqual(
            [], unscoped,
            "a general deployment document promises a hook control that only one hook honours:\n  "
            + "\n  ".join(unscoped),
        )


if __name__ == "__main__":
    unittest.main()
