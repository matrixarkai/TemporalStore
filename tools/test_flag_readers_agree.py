#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two readers of one variable must agree about it.

A flag read in two places can disagree in three ways, and each produces a setting that half-applies:

  - the DEFAULT differs, so the answer depends on which module was imported
  - the accepted SPELLINGS differ, so the two agree on "1" and part company on "on"
  - the SENSE differs, one asking `in {...}` and the other `not in {...}`

Half-applied is worse than not applied, because each half looks correct where it is written and
nothing reports the disagreement. `MATRIXARK_ALLOW_LOCAL_BACKEND` was in exactly that state: the
hook accepted "on" and the production guard that refuses a local backend did not, so
`MATRIXARK_ALLOW_LOCAL_BACKEND=on` permitted the backend in one place and was refused in the other.

The statement is the unit, not a window of lines. A first version of this scan read three lines
from each match and reported four disagreements, of which three were its own: it took the spellings
of the NEXT line's different variable, and it truncated a set that continued past the window. A
statement carries its own set and nothing else's, which is why the scan below follows brackets.
"""
from __future__ import annotations

import ast
import collections
import os
import re
import subprocess
import unittest
from typing import Dict, List, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

#: The variable names this guard covers. Lifted out of the old read regex, which is gone --
#: it matched per LINE and so could not see a call the formatter split across lines.
_NAME_SHAPE = re.compile(r"[A-Z][A-Z0-9_]{3,}")

_READ = re.compile(
    r'os\.(?:environ\.get|getenv)\(\s*["\']([A-Z][A-Z0-9_]{3,})["\']\s*,\s*["\']([^"\']*)["\']')
_SPELLING = re.compile(r'["\'](1|0|true|false|yes|no|on|off)["\']', re.I)
_NUMERIC = re.compile(r"\b(?:int|float)\s*\(")
_OTHER_FLAG = re.compile(r'["\']([A-Z][A-Z0-9_]{3,})["\']')
# The same read, once it goes through the one parser. Sites are being moved onto `env_bool`, and a
# scan that only knew the hand-rolled shape would watch its own population drain away and then
# report that every remaining reader agrees. Two of the three disagreements cannot occur here --
# `env_bool` fixes the spellings and the sense for every caller -- but the DEFAULT is still written
# at each site, so two readers of one flag can still disagree about what unset means.
_ENV_BOOL = re.compile(
    r'env_bool\(\s*["\']([A-Z][A-Z0-9_]{3,})["\']\s*,\s*(True|False)\s*\)')
ONE_VOCABULARY = ("<env_bool>",)

# Twenty-one when this was written, 15 until mx#1171, 14 now. Asserted so a scan that stops
# matching fails rather than reporting that every reader agrees.
#
# Each step down needs a named cause, or this control quietly becomes a rubber stamp. The last one:
# mx#1171 removed `matrixark_mcp_event_keys.context_event_time_index_entries`, a builder nothing
# called, and it was the SECOND production reader of
# MATRIXARK_CONTEXT_EVENT_TIME_INDEX_FULL_PAYLOAD. That flag now has one reader
# (`matrixark_temporal_direct_backend.py:1568`) and so is no longer shared.
#
# A flag leaving this set is not a loss -- one reader cannot disagree with itself. What the floor
# guards against is the scan matching NOTHING, which looks the same as universal agreement.
# 15 since the scan was rewritten to PARSE. The extra one is
# MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES: its second reader is written with the name on
# the line after `os.environ.get(`, so the old per-line regex never matched it and the flag
# was not in the scan at all -- while its two readers disagreed about "on". A step UP here
# means the scan sees a read it could not see before, which is the only reason this number
# should ever rise without new code.
#
# 14 after MATRIXARK_DIRECT_RAW_INGESTION_QUEUE was retired. It had two readers and they
# agreed; what it did not have was any way to act -- its branch also required
# MATRIXARK_DIRECT_WRITE_QUEUE and a memory queue mode, so an operator turning it on got
# nothing and no error. Raw batches follow the write queue now. A step DOWN is a flag leaving
# the tree, and it should arrive with the commit that removed it, as this one did.
#: 14 until matrixarkai#1575 consolidated matrixark_mcp_segments onto matrixark_mcp_core, which took
#: MATRIXARK_SEGMENT_MODEL_LOCAL_ONLY from two reading sites to one -- the flag did not go anywhere,
#: one of the two copies of the function reading it did. So the number fell because the tree got
#: better, which is the failure mode a floor set from a MEASUREMENT always has.
#:
#: What the floor is FOR is catching a read-shape scan that has stopped matching: that finds
#: approximately nothing, not one fewer. Ten is far below anything consolidation will reach one pair
#: at a time and far above what a broken scan returns.
EXPECTED_SHARED_FLOOR = 10


def _production_sources() -> List[str]:
    listed = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    return [path for path in listed if not os.path.basename(path).startswith("test_")]


#: The words a boolean read tests against. A set holding none of these is not a spelling table.
_SPELLING_WORDS = {"1", "0", "true", "false", "yes", "no", "on", "off"}


def _flag_name(node) -> str:
    """The variable a read names, or "" if the first argument is not a literal."""
    if not isinstance(node, ast.Call) or not node.args:
        return ""
    try:
        value = ast.literal_eval(node.args[0])
    except (ValueError, SyntaxError):
        return ""
    return value if isinstance(value, str) and _NAME_SHAPE.fullmatch(value) else ""


def _is_environ_read(node) -> bool:
    func = node.func
    if not isinstance(func, ast.Attribute) or func.attr not in ("get", "getenv"):
        return False
    owner = func.value
    if isinstance(owner, ast.Attribute):
        return owner.attr == "environ"
    return isinstance(owner, ast.Name) and owner.id in ("os", "environ")


def _readers() -> Dict[str, List[Tuple[str, int, str, Tuple[str, ...], bool]]]:
    """Every boolean read of a flag, PARSED.

    The previous scan matched a regex per line and required the name and the default on the same
    line as `os.environ.get(`. A call the formatter split across lines never matched, so the flag
    did not enter the scan at all -- which is how the one variable read through two different
    spelling tables passed this file.
    """
    found: Dict[str, List[Tuple[str, int, str, Tuple[str, ...], bool]]] = collections.defaultdict(list)
    for path in _production_sources():
        try:
            with open(os.path.join(REPO, path), encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue
        parents = {child: parent for parent in ast.walk(tree)
                   for child in ast.iter_child_nodes(parent)}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else (
                func.attr if isinstance(func, ast.Attribute) else "")

            if name in ("env_bool", "bool_env", "_env_bool", "_bool_env"):
                flag = _flag_name(node)
                if not flag or len(node.args) < 2:
                    continue
                try:
                    default = ast.literal_eval(node.args[1])
                except (ValueError, SyntaxError):
                    continue
                if not isinstance(default, bool):
                    continue
                # Recorded as "1"/"0", not "true"/"false": the same default written the two
                # ways these shapes write it must compare equal, or a flag read through env_bool
                # in one place and os.environ.get(X, "0") in another reports a disagreement about
                # a default the two actually share.
                found[flag].append((path, node.lineno, "1" if default else "0",
                                    ONE_VOCABULARY, False))
                continue

            if not _is_environ_read(node):
                continue
            flag = _flag_name(node)
            if not flag:
                continue
            default = ""
            if len(node.args) > 1:
                try:
                    raw = ast.literal_eval(node.args[1])
                except (ValueError, SyntaxError):
                    raw = ""
                default = str(raw).strip().lower() if isinstance(raw, str) else ""

            # Climb to the comparison this read feeds, through the .strip().lower() chain.
            cursor, compare = node, None
            for _hop in range(8):
                parent = parents.get(cursor)
                if parent is None:
                    break
                if isinstance(parent, ast.Call) and isinstance(parent.func, ast.Attribute):
                    if parent.func.attr in ("int", "float"):
                        break
                    cursor = parent
                    continue
                if isinstance(parent, ast.Call) and isinstance(parent.func, ast.Name) \
                        and parent.func.id in ("int", "float"):
                    break                      # a numeric read, not a boolean one
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
                members = {str(value).strip().lower() for value in ast.literal_eval(comparator)}
            except (ValueError, SyntaxError, TypeError):
                continue
            if not members & _SPELLING_WORDS:
                continue
            # A comparison naming a second flag is not a clean read of either.
            others = {constant.value for constant in ast.walk(compare)
                      if isinstance(constant, ast.Constant) and isinstance(constant.value, str)
                      and _NAME_SHAPE.fullmatch(constant.value)} - {flag}
            if others:
                continue
            accepted = tuple(sorted(members - {default}))
            if not accepted:
                continue
            found[flag].append((path, node.lineno, default, accepted,
                                isinstance(compare.ops[0], ast.NotIn)))
    return found


class TwoReadersOfOneFlagAgreeTest(unittest.TestCase):

    @staticmethod
    def _shared():
        return {name: entries for name, entries in _readers().items()
                if len({(entry[0], entry[1]) for entry in entries}) > 1}

    def test_the_scan_still_finds_shared_flags(self) -> None:
        shared = self._shared()
        self.assertGreaterEqual(
            len(shared), EXPECTED_SHARED_FLOOR,
            "found %d flags read from more than one production site, expected at least %d -- if "
            "the read shape changed, this file is looking for something that no longer exists and "
            "the assertion below passes on an empty set" % (len(shared), EXPECTED_SHARED_FLOOR))

    def test_no_two_readers_disagree(self) -> None:
        disagreeing = []
        for name, entries in sorted(self._shared().items()):
            ways = []
            if len({entry[2] for entry in entries}) > 1:
                ways.append("default")
            if len({entry[3] for entry in entries}) > 1:
                ways.append("spellings")
            if len({entry[4] for entry in entries}) > 1:
                ways.append("sense")
            if not ways:
                continue
            sites = "; ".join(
                "%s:%d default=%r %s{%s}"
                % (os.path.basename(entry[0]), entry[1], entry[2],
                   "not in " if entry[4] else "in ", ",".join(entry[3]))
                for entry in sorted(entries))
            disagreeing.append("%s disagrees on %s -- %s" % (name, "+".join(ways), sites))
        self.assertEqual(
            [], disagreeing,
            "a flag means different things to different readers, so setting it applies in some "
            "places and not others:\n  %s\nGive it one answer. Where the readers guard something, "
            "take the narrower spelling: a permission should not gain accepting spellings."
            % "\n  ".join(disagreeing))


if __name__ == "__main__":
    unittest.main()
