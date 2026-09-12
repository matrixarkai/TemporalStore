#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A field the portal calls a bool must accept the words a bool is written with.

`env_bool` is this tree's convention: TRUE_VALUES {"1","true","yes","on"} and FALSE_VALUES
{"0","false","no","off"}, case folded and whitespace stripped. Nineteen settings the portal
declares as type "bool" are read by a comparison written at the site instead, and seven of those
accepted less than the convention does:

    MATRIXARK_DEDUPE_SKILL_CHUNK_TEXT=off        read TRUE  -- and the comment above it says
                                                    "Off restores the second copy"
    MATRIXARK_INDEX_POSTING_LISTS=no             read TRUE
    MATRIXARK_EMBEDDING_VECTOR_INT8=FALSE        read TRUE  (case not folded)
    MATRIXARK_RESOURCE_SLIM_CHUNK_METADATA=" 0 " read TRUE  (whitespace not stripped)
    MATRIXARK_LOCAL_DURABLE_READ_CACHE_ENABLED=off  read TRUE
    MATRIXARK_DIRECT_RAW_INGESTION=on            read FALSE

A false spelling reading as TRUE is the sharp end of this. The operator is not ignored -- they get
the opposite of what they asked for, on a field the portal offered them, with no error anywhere.

What is checked is the VOCABULARY, not the sense and not the default. A default-ON flag written as
`not in {...falsey...}` and a default-OFF flag written as `in {...truthy...}` are both fine; each
simply has to name every spelling of the side it tests.

The empty string is deliberately NOT part of this. Whether `X=` means "off" or "unset" differs
between these sites, it is a real question about what an empty value means, and it is not a
spelling.
"""
from __future__ import annotations

import ast
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}
BOOL_READERS = {"env_bool", "bool_env", "_env_bool", "_bool_env"}


def _unreachable_modules() -> set:
    """Modules only the tests reach, from the guard that maintains that list."""
    import importlib.util

    path = os.path.join(TOOLS, "test_a_module_only_tests_reach_is_not_live.py")
    spec = importlib.util.spec_from_file_location("_reach_for_bool_vocab", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    out: set = set()
    for members in module.UNREACHABLE.values():
        out.update(members)
    return out


def _declared_bool_settings() -> dict:
    """{variable: portal key} for every Setting declared with type "bool"."""
    with open(os.path.join(TOOLS, "matrixark_gateway_config.py"), encoding="utf-8",
              errors="replace") as handle:
        tree = ast.parse(handle.read())
    out = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or getattr(node.func, "id", "") != "Setting":
            continue
        if len(node.args) < 5:
            continue
        try:
            key = ast.literal_eval(node.args[0])
            variable = ast.literal_eval(node.args[2])
            kind = ast.literal_eval(node.args[4])
        except (ValueError, SyntaxError):
            continue
        if isinstance(variable, str) and variable and kind == "bool":
            out[variable] = key
    return out


def _hand_rolled_reads(declared_only: bool = True) -> list:
    """(variable, module, line, accepted, negated, folded) for each hand-written boolean read.

    `folded` is whether the value passes through .strip() and .lower() before the comparison --
    without it "FALSE" and " 0 " are not the spellings they look like.
    """
    unreachable = _unreachable_modules()
    declared = _declared_bool_settings()
    listed = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=REPO,
                            capture_output=True, text=True, check=False).stdout.split()
    rows = []
    for rel in listed:
        stem = os.path.basename(rel)[:-3]
        if stem.startswith("test_") or stem in unreachable:
            continue
        try:
            with open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue
        parents = {child: parent for parent in ast.walk(tree)
                   for child in ast.iter_child_nodes(parent)}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else (
                func.attr if isinstance(func, ast.Attribute) else "")
            if name in BOOL_READERS or name not in ("get", "getenv"):
                continue
            owner = getattr(func, "value", None)
            if not ((isinstance(owner, ast.Attribute) and owner.attr == "environ")
                    or (isinstance(owner, ast.Name) and owner.id in ("os", "environ"))):
                continue
            try:
                variable = ast.literal_eval(node.args[0])
            except (ValueError, SyntaxError):
                continue
            if not isinstance(variable, str):
                continue
            if not variable.startswith(("MATRIXARK_", "TS_")):
                continue
            if declared_only and variable not in declared:
                continue

            cursor, folded, compare = node, set(), None
            for _hop in range(8):
                parent = parents.get(cursor)
                if parent is None:
                    break
                if isinstance(parent, ast.Attribute) and parent.attr in ("strip", "lower"):
                    folded.add(parent.attr)
                    cursor = parent
                    continue
                if isinstance(parent, ast.Call):
                    cursor = parent
                    continue
                if isinstance(parent, ast.Compare) and len(parent.ops) == 1 \
                        and isinstance(parent.ops[0], (ast.In, ast.NotIn)):
                    compare = parent
                    break
                cursor = parent
            if compare is None:
                continue
            comparator = compare.comparators[0]
            if not isinstance(comparator, (ast.Set, ast.Tuple, ast.List)):
                continue
            try:
                members = {str(value).lower() for value in ast.literal_eval(comparator)}
            except (ValueError, SyntaxError, TypeError):
                continue
            if not members & (TRUE_VALUES | FALSE_VALUES):
                continue
            rows.append((variable, stem, node.lineno, members,
                         isinstance(compare.ops[0], ast.NotIn),
                         {"strip", "lower"} <= folded))
    return rows


class APortalBoolAcceptsTheWordsABoolIsWrittenWithTest(unittest.TestCase):

    def test_every_hand_rolled_read_names_every_spelling_of_its_side(self) -> None:
        """The rule. A `not in {...}` test must name every FALSE_VALUES spelling, and an
        `in {...}` test every TRUE_VALUES spelling -- otherwise a word the portal implies is
        accepted reads as the opposite."""
        short = []
        for variable, module, line, members, negated, _folded in _hand_rolled_reads():
            wanted = FALSE_VALUES if negated else TRUE_VALUES
            missing = sorted(wanted - members)
            if missing:
                short.append("%s (%s:%d) misses %s" % (variable, module, line, ",".join(missing)))
        self.assertEqual(
            [], short,
            "these settings are offered as bool and their reader ignores a spelling of the side "
            "it tests, so that value reads as the OPPOSITE of what it says")

    def test_every_hand_rolled_read_folds_case_and_strips_space(self) -> None:
        """Without `.strip().lower()` the vocabulary above is checked against the raw value, so
        "FALSE" and " 0 " are not the words they look like and read as true."""
        raw = []
        for variable, module, line, _members, _negated, folded in _hand_rolled_reads():
            if not folded:
                raw.append("%s (%s:%d)" % (variable, module, line))
        self.assertEqual(
            [], raw,
            "these settings compare the environment value without .strip().lower(), so a capital "
            "or a stray space changes the answer")

    def test_the_scan_finds_the_settings_and_the_reads(self) -> None:
        """A floor. With either side empty both assertions pass over nothing."""
        declared = _declared_bool_settings()
        reads = _hand_rolled_reads()
        # The numbers say what they are FOR, not what the page currently holds. A scan that
        # stopped recognising its shape returns approximately nothing, and that is the only thing
        # these three catch. A floor set from a measurement instead fails the day the count moves
        # for a legitimate reason -- which is what happened here: this sat at 40 when 42 settings
        # were bool, and retiring knobs took it under a number that was never about how many bools
        # the page ought to offer.
        self.assertGreater(len(declared), 15,
                           "the bool-Setting scan came back nearly empty")
        self.assertGreater(len(reads), 10,
                           "the hand-rolled read scan came back nearly empty, so every setting "
                           "would look compliant")
        self.assertGreater(len(_unreachable_modules()), 30,
                           "the reachability list came back nearly empty")


class EveryFlagReadAsABooleanAcceptsTheSameWordsTest(unittest.TestCase):
    """The same rule, for flags the portal never offers.

    Nothing about it depends on a setting being declared. A flag set in a deploy script or by hand
    is written with the same words, and `MATRIXARK_SHADOW_COMPARE=OFF` read TRUE because only the
    case fold was missing -- a false spelling switching a flag on, with no screen involved."""

    def test_every_hand_rolled_read_names_every_spelling_of_its_side(self) -> None:
        short = []
        for variable, module, line, members, negated, _folded in _hand_rolled_reads(False):
            wanted = FALSE_VALUES if negated else TRUE_VALUES
            missing = sorted(wanted - members)
            if missing:
                short.append("%s (%s:%d) misses %s" % (variable, module, line, ",".join(missing)))
        self.assertEqual(
            [], short,
            "these flags are read as booleans and ignore a spelling of the side they test, so "
            "that value means the OPPOSITE of what it says")

    def test_every_hand_rolled_read_folds_case_and_strips_space(self) -> None:
        raw = []
        for variable, module, line, _members, _negated, folded in _hand_rolled_reads(False):
            if not folded:
                raw.append("%s (%s:%d)" % (variable, module, line))
        self.assertEqual(
            [], raw,
            "these flags compare the environment value without .strip().lower(), so a capital or "
            "a stray space changes the answer")

    def test_the_wider_scan_sees_more_than_the_declared_one(self) -> None:
        """A floor with a direction. The two assertions above are only worth having if the wider
        scan actually reaches flags the portal does not declare; if it collapsed to the declared
        set they would be a copy of the tests above."""
        declared = len(_hand_rolled_reads(True))
        every = len(_hand_rolled_reads(False))
        self.assertGreater(
            every, declared,
            "the wider scan found no more reads than the declared-only one, so it is not wider")


if __name__ == "__main__":
    unittest.main()
