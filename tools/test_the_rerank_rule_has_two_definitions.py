#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`cross_session_rerank_adjustment` has two live definitions, and three live callers split.

One ranking rule, two implementations, and which one decides a candidate's boost depends on which
module the caller reached:

    matrixark_mcp_core_candidate_policy   ->  matrixark_mcp_core          (profile-memory aware)
    matrixark_mcp_core_packing            ->  matrixark_mcp_core          (profile-memory aware)
    matrixark_mcp_recall_scoring          ->  matrixark_mcp_access_scope  (NOT profile-memory aware)

Resolved by importing each caller and comparing the bound function's code object, not by reading
the import lines: a `try`/`except` import chain can say one thing and deliver another
(`a-tools-prefixed-import-is-a-different-module-object` is the same trap one level down).

MEASURED on one candidate -- `session_continuity=cross_session`, `ref_type=entity`,
`memory_scope=user_profile`, `profile_memory_kind=durable_profile`:

    question_type      access_scope   core
    profile_memory         0.06       0.16
    current_state          0.10       0.14

The `core` copy carries a whole `question_type == "profile_memory"` branch the other lacks, and a
`profile_boost` of 0.04 for `memory_scope == "user_profile"`. That second one is why the difference
is **not** confined to profile-memory questions: any user-profile candidate scores 0.04 lower
through `recall_scoring` than through the packing path, on an ordinary `current_state` question.

THIS FILE DOES NOT ASSERT THAT THEY AGREE. Which boost is right is a ranking decision -- adopting
the richer rule raises user-profile candidates everywhere `recall_scoring` runs, and adopting the
poorer one drops them everywhere else. Picking either changes what retrieval returns. So the state
is recorded in BOTH directions: a new divergence fails here, and a convergence fails here too and
asks which definition won.

Its siblings all have this guard already -- `test_the_storage_option_normalizers_have_one_definition`,
`test_the_record_materializer_has_one_definition` -- and this pair had none, which is the only
reason it is being written now rather than the divergence being new.

The decision itself, and the two things that would make it cheap to settle, is matrixarkai#1871.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)
REPO = os.path.dirname(TOOLS)

import matrixark_mcp_access_scope as access_scope  # noqa: E402
import matrixark_mcp_core as core  # noqa: E402

HELPER = "cross_session_rerank_adjustment"

#: Caller -> the module whose copy it binds today.
BINDINGS = {
    "matrixark_mcp_core_candidate_policy": "matrixark_mcp_core",
    "matrixark_mcp_core_packing": "matrixark_mcp_core",
    "matrixark_mcp_recall_scoring": "matrixark_mcp_access_scope",
}

#: One candidate, and what each copy scores it. A user-profile entity, cross-session, which is the
#: shape the richer branch exists for.
CANDIDATE = {
    "session_continuity": "cross_session",
    "ref_type": "entity",
    "memory_scope": "user_profile",
    "profile_memory_kind": "durable_profile",
}
SCORES = {
    ("matrixark_mcp_access_scope", "profile_memory"): 0.06,
    ("matrixark_mcp_core", "profile_memory"): 0.16,
    ("matrixark_mcp_access_scope", "current_state"): 0.10,
    ("matrixark_mcp_core", "current_state"): 0.14,
}

COPIES = {
    "matrixark_mcp_access_scope": getattr(access_scope, HELPER),
    "matrixark_mcp_core": getattr(core, HELPER),
}


def where(fn) -> tuple:
    """Which DEFINITION this function is, by where it is written rather than by object identity.

    `fn.__code__ is other.__code__` was the original test, and it is right in one process with one
    copy of each module. The suite is not that: something else imports `tools.matrixark_mcp_core`
    while this file imports `matrixark_mcp_core`, and Python builds two module objects with two
    sets of code objects for the same file. Every `is` then answers False, the caller resolves to
    no known copy at all, and the failure reads "now binds a THIRD copy" -- a ranking change that
    did not happen.

    That is the trap this file's own docstring names one level down, and it caught the file itself.
    Realpath, because the two module objects are loaded from the same path by different names.
    """
    code = fn.__code__
    return (os.path.realpath(code.co_filename), code.co_name, code.co_firstlineno)


class WhereTellsTwoDefinitionsApartTest(unittest.TestCase):
    """`where` is asked about whatever a caller bound, which need not be one of the two copies.

    The path alone happens to separate today's pair, because they are written in different files.
    It would not separate two helpers written in one file, and "the caller binds some other
    function from the right module" is exactly the shape this file exists to notice.
    """

    def test_two_functions_in_one_file_are_not_one_definition(self) -> None:
        def one(_candidate, _question):
            return 0.0

        def other(_candidate, _question):
            return 0.0

        self.assertNotEqual(where(one), where(other))

    def test_the_same_function_is_itself_however_it_is_reached(self) -> None:
        alias = COPIES["matrixark_mcp_core"]
        self.assertEqual(where(alias), where(getattr(core, HELPER)))


class TheRerankRuleHasTwoDefinitions(unittest.TestCase):

    def test_the_two_copies_are_distinct(self) -> None:
        """The floor. If they became one function every comparison below is vacuous."""
        self.assertEqual(
            2, len(set(where(fn) for fn in COPIES.values())),
            "%s is now ONE function. If the copies were consolidated that is the fix -- strike "
            "this file and say which definition won" % HELPER,
        )

    def test_each_caller_still_binds_the_copy_recorded_here(self) -> None:
        """Resolved by where the bound function is WRITTEN, because an import chain can
        deliver what its text does not say -- and because the same file can be loaded twice
        under two names, which is what `where` is for."""
        for caller, expected in sorted(BINDINGS.items()):
            with self.subTest(caller=caller):
                module = __import__(caller)
                bound = getattr(module, HELPER, None)
                self.assertIsNotNone(
                    bound, "%s no longer holds %s at module scope" % (caller, HELPER))
                actual = [
                    name for name, fn in COPIES.items()
                    if where(fn) == where(bound)
                ]
                self.assertEqual(
                    [expected], actual,
                    "%s now binds %s, recorded as %s -- which copy a caller reaches decides the "
                    "boost, so this is a ranking change however it happened"
                    % (caller, actual or "a THIRD copy", expected),
                )

    def test_it_resolves_when_the_suite_has_already_imported_the_prefixed_copy(self) -> None:
        """The condition that reddened main, run on purpose.

        This file passes on its own and failed inside the suite, because the suite imports some of
        these modules as `tools.<name>` before it reaches this file. A child process is the only
        honest way to stage that: importing `tools.matrixark_mcp_recall_scoring` here would leave a
        second copy of it in `sys.modules` for every test that runs after this one.
        """
        script = textwrap.dedent(
            """
            import os, sys
            sys.path.insert(0, %r)
            sys.path.insert(0, %r)
            import tools.matrixark_mcp_recall_scoring   # the second module object
            import tools.matrixark_mcp_core_packing
            import unittest
            loader = unittest.TestLoader()
            suite = loader.loadTestsFromNames(
                ["test_the_rerank_rule_has_two_definitions"
                 ".TheRerankRuleHasTwoDefinitions.test_each_caller_still_binds_the_copy_recorded_here"])
            result = unittest.TextTestRunner(verbosity=0).run(suite)
            sys.exit(0 if result.wasSuccessful() else 1)
            """
        ) % (REPO, TOOLS)
        out = subprocess.run([sys.executable, "-c", script], cwd=TOOLS, capture_output=True,
                             text=True, timeout=600)
        self.assertEqual(
            0, out.returncode,
            "the binding check does not survive a second module object for the same file:\n%s%s"
            % (out.stdout[-1500:], out.stderr[-1500:]))

    def test_the_two_copies_still_score_the_same_candidate_differently(self) -> None:
        """The finding, at the numbers rather than at the source text."""
        for (module_name, question_type), expected in sorted(SCORES.items()):
            with self.subTest(copy=module_name, question_type=question_type):
                self.assertAlmostEqual(
                    expected,
                    COPIES[module_name](dict(CANDIDATE), question_type),
                    places=6,
                    msg="%s scores this candidate differently than recorded for %s. If the two "
                        "copies were reconciled, strike this file and say which won"
                        % (module_name, question_type),
                )

    def test_the_difference_is_not_confined_to_a_profile_memory_question(self) -> None:
        """The part that is easy to miss: the 0.04 user-profile boost applies everywhere.

        Reading only the `question_type == "profile_memory"` branch suggests the copies agree on
        every other question. They do not -- the richer copy adds `profile_boost` to the entity
        branch, so an ordinary `current_state` question already differs.
        """
        poorer = COPIES["matrixark_mcp_access_scope"](dict(CANDIDATE), "current_state")
        richer = COPIES["matrixark_mcp_core"](dict(CANDIDATE), "current_state")
        self.assertNotEqual(
            poorer, richer,
            "the copies now agree on an ordinary question, so the divergence this file records has "
            "narrowed to the profile-memory branch -- say so rather than leaving this assertion",
        )
        self.assertAlmostEqual(0.04, richer - poorer, places=6)


if __name__ == "__main__":
    unittest.main()
