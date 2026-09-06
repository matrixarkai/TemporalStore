#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Three audit modes, three vocabularies, and only one of them was offered.

`MATRIXARK_AUDIT_MODE` governs the access layer and the portal offers it: off, async, full, sync.
Two more exist and neither was offered:

* `MATRIXARK_CONTEXT_AUDIT_MODE` decides whether a retrieve records what it did, and takes
  ``off``, ``telemetry_only`` or ``full`` -- **it refuses the access layer's words outright**.
* `MATRIXARK_DIRECT_AUDIT_MODE` decides how audit records reach the store, and takes ``buffered``,
  ``sync`` or ``drop``.

So a customer reasoning by analogy from the one setting they could see -- "audit mode is async" --
gets `audit_mode must be full, telemetry_only, or off` from a retrieve. `sync` and `full` appear in
more than one of the three meaning different things.

And the retrieval one had a consequence beyond being unreachable: **with nothing set, whether a
retrieve records anything depends on which path served it.** The request path and the direct read
default to ``telemetry_only``; the local adapter passes ``off``. Setting it is how a deployment
makes them agree, which is what the setting is for.

The choices are DERIVED here from the code that validates them, not restated. Two vocabularies that
drift apart silently is how this got confusing in the first place.
"""
from __future__ import annotations

import ast
import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_mcp_retrieve_planning as planning  # noqa: E402
import matrixark_mcp_runtime_config as runtime  # noqa: E402

CONTEXT_KEY = "retrieval.context_audit_mode"
RATE_KEY = "retrieval.context_audit_sample_rate"
DIRECT_KEY = "limits.direct_audit_mode"
ACCESS_KEY = "audit.mode"


def validated_set(filename: str, variable: str) -> set:
    """The set a module checks a value against: `if <variable> not in {...}: raise`."""
    with open(os.path.join(TOOLS, filename), encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=filename)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare) or len(node.ops) != 1:
            continue
        if not isinstance(node.ops[0], ast.NotIn):
            continue
        if not (isinstance(node.left, ast.Name) and node.left.id == variable):
            continue
        target = node.comparators[0]
        if isinstance(target, (ast.Set, ast.Tuple, ast.List)):
            return {e.value for e in target.elts
                    if isinstance(e, ast.Constant) and isinstance(e.value, str)}
    return set()


def compared_literals(filename: str, attribute: str) -> set:
    """Every string a module compares one attribute against: `self.<attribute> == "x"`."""
    path = os.path.join(TOOLS, filename)
    # Named, so it can go missing. mx#1080 deleted the orphaned copy of this branching while it
    # was live in another module, and a bare FileNotFoundError out of here says nothing about
    # which claim broke. If it moves again, the message should name what was being looked for.
    if not os.path.exists(path):
        raise AssertionError(
            "%s is not in tools/ any more; find where `self.%s` is compared and point this at "
            "it, or the choices below are derived from nothing" % (filename, attribute))
    with open(path, encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=filename)
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare) or not node.comparators:
            continue
        left = node.left
        if not (isinstance(left, ast.Attribute) and left.attr == attribute):
            continue
        for element in node.comparators:
            if isinstance(element, ast.Constant) and isinstance(element.value, str):
                found.add(element.value)
    return found


class Case(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = os.environ.get("MATRIXARK_CONTEXT_AUDIT_MODE")
        os.environ.pop("MATRIXARK_CONTEXT_AUDIT_MODE", None)

    def tearDown(self) -> None:
        if self._saved is None:
            os.environ.pop("MATRIXARK_CONTEXT_AUDIT_MODE", None)
        else:
            os.environ["MATRIXARK_CONTEXT_AUDIT_MODE"] = self._saved


class TheChoicesAreWhatTheCodeAcceptsTest(unittest.TestCase):

    def test_the_retrieval_choices_come_from_its_validator(self) -> None:
        accepted = validated_set("matrixark_mcp_retrieve_planning.py", "audit_mode")
        self.assertEqual(accepted, set(cfg.SETTINGS_BY_KEY[CONTEXT_KEY].choices))

    def test_the_validator_was_actually_found(self) -> None:
        """The floor: an empty set would make the rule above compare nothing to nothing."""
        self.assertEqual({"full", "telemetry_only", "off"},
                         validated_set("matrixark_mcp_retrieve_planning.py", "audit_mode"))

    def test_the_store_choices_come_from_what_it_branches_on(self) -> None:
        branched = compared_literals("matrixark_temporal_direct_write.py", "_audit_mode")
        self.assertEqual(branched, set(cfg.SETTINGS_BY_KEY[DIRECT_KEY].choices))

    def test_that_branching_was_actually_found(self) -> None:
        self.assertEqual({"drop", "sync", "buffered"},
                         compared_literals("matrixark_temporal_direct_write.py", "_audit_mode"))


class TheThreeVocabulariesAreDistinctTest(unittest.TestCase):
    """The reason each help text warns about the others."""

    def sets(self):
        return {key: set(cfg.SETTINGS_BY_KEY[key].choices)
                for key in (ACCESS_KEY, CONTEXT_KEY, DIRECT_KEY)}

    def test_no_two_are_the_same(self) -> None:
        values = list(self.sets().values())
        for index, first in enumerate(values):
            for second in values[index + 1:]:
                self.assertNotEqual(first, second)

    def test_they_overlap_which_is_the_hazard(self) -> None:
        """If they were disjoint the confusion would be obvious. They are not: `sync` and `full`
        each appear in two of the three, meaning different things."""
        sets = self.sets()
        self.assertIn("sync", sets[ACCESS_KEY])
        self.assertIn("sync", sets[DIRECT_KEY])
        self.assertIn("full", sets[ACCESS_KEY])
        self.assertIn("full", sets[CONTEXT_KEY])

    def test_each_help_names_a_word_it_refuses(self) -> None:
        """Substance, not a keyword. A reader arriving from the one setting that was already
        offered needs to see the word they were about to type, said to be wrong here -- so each
        help must NAME at least one value that belongs to another of the three and not to it."""
        sets = self.sets()
        for key in (CONTEXT_KEY, DIRECT_KEY):
            others = set().union(*(v for k, v in sets.items() if k != key))
            foreign = others - sets[key]
            help_text = cfg.SETTINGS_BY_KEY[key].help.lower()
            named = [word for word in foreign if word in help_text]
            with self.subTest(setting=key):
                self.assertTrue(named,
                                "%s warns about none of %s" % (key, sorted(foreign)))

    def test_there_are_foreign_words_to_warn_about(self) -> None:
        """The floor: if the three vocabularies stopped overlapping, the rule above would be
        satisfiable by saying nothing."""
        sets = self.sets()
        for key in (CONTEXT_KEY, DIRECT_KEY):
            others = set().union(*(v for k, v in sets.items() if k != key))
            self.assertTrue(others - sets[key])


class AWordFromAnotherVocabularyIsRefusedTest(Case):

    def test_the_access_layers_words_are_not_accepted_here(self) -> None:
        for word in ("async", "sync"):
            with self.subTest(word=word):
                os.environ["MATRIXARK_CONTEXT_AUDIT_MODE"] = word
                with self.assertRaises(Exception):
                    planning.retrieval_audit_policy({})

    def test_its_own_words_are(self) -> None:
        """The floor: a validator that refused everything would satisfy the test above."""
        for word in cfg.SETTINGS_BY_KEY[CONTEXT_KEY].choices:
            with self.subTest(word=word):
                os.environ["MATRIXARK_CONTEXT_AUDIT_MODE"] = word
                mode, _rate = planning.retrieval_audit_policy({})
                self.assertEqual(word, mode)


class SettingItMakesThePathsAgreeTest(Case):
    """The consequence of it being unreachable, and the reason to reach it."""

    def test_with_nothing_set_the_paths_disagree(self) -> None:
        request_path, _ = planning.retrieval_audit_policy({})
        local_adapter, _ = planning.retrieval_audit_policy({}, default="off")
        self.assertNotEqual(request_path, local_adapter)
        self.assertEqual("telemetry_only", request_path)
        self.assertEqual("off", local_adapter)

    def test_setting_it_makes_them_agree(self) -> None:
        for word in ("off", "telemetry_only", "full"):
            with self.subTest(word=word):
                os.environ["MATRIXARK_CONTEXT_AUDIT_MODE"] = word
                self.assertEqual(planning.retrieval_audit_policy({}),
                                 planning.retrieval_audit_policy({}, default="off"))

    def test_the_help_says_so(self) -> None:
        help_text = cfg.SETTINGS_BY_KEY[CONTEXT_KEY].help
        self.assertIn("depends on which path", help_text)


class TheDeclaredDefaultsAreWhatTheBuildRunsTest(unittest.TestCase):

    def test_the_store_audit_default(self) -> None:
        self.assertEqual(runtime.DIRECT_AUDIT_MODE, cfg.SETTINGS_BY_KEY[DIRECT_KEY].default)

    def test_the_sample_rate_default(self) -> None:
        """Read out of the module that reads it, rather than retyped here."""
        with open(os.path.join(TOOLS, "matrixark_mcp_retrieve_planning.py"),
                  encoding="utf-8") as handle:
            source = handle.read()
        match = re.search(r'MATRIXARK_CONTEXT_AUDIT_SAMPLE_RATE",\s*([0-9.]+)', source)
        self.assertIsNotNone(match, "the sample-rate default is not where this looked")
        self.assertEqual(float(match.group(1)),
                         float(cfg.SETTINGS_BY_KEY[RATE_KEY].default))

    def test_two_are_live_and_one_is_not(self) -> None:
        """Not a stylistic split. The two retrieval controls are read inside the call that uses
        them, so a change lands on the next retrieve; the store one is bound when its module is
        imported, so it does not. The labels say which, and `test_matrixark_gateway_config_audit`
        derives that from where each read is -- it caught both of these labelled restart."""
        self.assertEqual("live", cfg.SETTINGS_BY_KEY[CONTEXT_KEY].applies)
        self.assertEqual("live", cfg.SETTINGS_BY_KEY[RATE_KEY].applies)
        self.assertEqual("restart", cfg.SETTINGS_BY_KEY[DIRECT_KEY].applies)

    def test_the_retrieval_default_is_the_one_two_of_three_paths_use(self) -> None:
        """There is no single build default -- that is the defect. The declared one is what the
        request path and the direct read do, and the help names the exception."""
        self.assertEqual("telemetry_only", cfg.SETTINGS_BY_KEY[CONTEXT_KEY].default)
        self.assertEqual("telemetry_only", planning.retrieval_audit_policy({})[0])


if __name__ == "__main__":
    unittest.main()
