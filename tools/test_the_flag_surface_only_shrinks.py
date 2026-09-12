#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The environment-variable surface only shrinks, and says what holds each part of it.

Production Python under `tools/` reads several hundred distinct `TS_*` / `MATRIXARK_*` /
`TEMPORALSTORE_*` variables. Nothing counted them, so nothing noticed the number growing, and
"there are too many flags" had no denominator to argue with.

This is a CEILING, not a floor: the count may fall freely and a rise fails. That is the opposite
of the vacuity floors next door, which exist to catch a scan that stopped matching and must
therefore be set from what they are FOR -- see the note in
`test_a_portal_bool_accepts_the_words_a_bool_is_written_with`. A ceiling can be pinned to a
measurement precisely because crossing it is the event worth failing on.

## The classification, which is the useful half

A bare count invites the wrong cut. Every flag here falls in one of these, and only the last is a
candidate for removal:

* **selected** -- the portal offers it, `matrixark_load_config.ENV_MAP` maps it, a test sets it,
  or a config file, script, deploy profile, workflow or document names it. Something can choose
  its value, so it is a switch.
* **instructed** -- a sentence in production prose tells a reader to set it: a portal help text, a
  docstring saying what a value does, an error message naming the words it accepts. Nothing in
  the repository SETS `MATRIXARK_RESOURCE_EVENT_TEXT_CHARS`, and its docstring says *"Set ...=0 to
  store the full text"*. Being told to set it is being able to set it.
* **a harness's own CLI** -- a benchmark, report generator or sweep reading its own
  `MATRIXARK_<TOOL>_*` namespace, or an `argparse` default. Nothing sets
  `MATRIXARK_BACKFILL_BENCH_RECORDS` because you set it when you RUN the benchmark.
* **deployment identity** -- an endpoint, credential variable, bucket, model, region, namespace or
  library path. *"Nothing sets it here"* is not the claim *"no deployment needs it"*, and writing
  one down hard-codes where a deployment points.
* **a legacy spelling** -- the second or third link of an alias chain, kept so an older
  configuration keeps working.
* **candidate** -- none of the above. A flag no one can be shown to set or be told to set is not
  a switch, it is a branch, and its live side can be made unconditional. That is the cut
  matrixarkai#1540 took 57 of.

The classifications are computed, not listed, so they cannot go stale -- and each is asserted to
hold a plausible share, because a rule matching nearly everything is measuring the population
rather than the property. Four of these were written too loosely first and did exactly that.
"""
from __future__ import annotations

import ast
import io
import os
import re
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

_NAME = re.compile(r"(?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+")
_FLAG = re.compile(r"^(?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+$")
_READ = re.compile(
    r'os\.(?:environ\.get|getenv)\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']'
    r'|os\.environ\[\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']'
    r'|\b\w*[Ee][Nn][Vv]\w*\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']')

#: Telling a reader to give a flag a value, within reach of the name itself. Matching anywhere in
#: a long string held 478 of 520 -- a rule that holds nearly everything is not a rule.
_INSTRUCTION = re.compile(
    r"\bset\b|\bmust be\b|\bexport\b|\benable[sd]?\b|\bdisable[sd]?\b|\bturn\b|\bto store\b"
    r"|\bapply\b|\bconfigurable\b|\boverride\b|\bdefaults? to\b|=", re.IGNORECASE)
_INSTRUCTION_REACH = 90

#: A module whose env reads ARE its command line rather than its configuration.
_HARNESS = re.compile(r"(benchmark|_report|^run_|^generate_|sweep|probe|soak|harness)")

#: Where a deployment points, who it authenticates as, what it loads.
_IDENTITY = re.compile(
    r"(API_KEY|_KEY_ENV|BASE_URL|_URL$|_URI$|ENDPOINT|PROVIDER|_MODEL$|_MODEL_|BUCKET|PREFIX"
    r"|HOST|_PORT$|_PATH$|_DIR$|_DB$|_CLIENT_ID$|COMMAND|TOKEN|SECRET|CREDENTIAL|REGION"
    r"|ACCOUNT|TENANT|NAMESPACE|_ADDR$|METASERVER|_FILE$|_LOG$|_LIB$)")

#: The ceiling. Lower it when you cut; a rise is the failure this file exists for.
#: 520 on main when this was written. matrixarkai#1540 folds 57 reads of flags nothing sets and
#: takes it to 481 -- lower this in the same commit that merges it, because a ratchet that does
#: not bank a reduction is the reduction nobody can see was made.
MAXIMUM_FLAGS_READ = 520


def _tracked(*globs):
    return subprocess.run(["git", "ls-files", *globs], cwd=REPO,
                          capture_output=True, text=True).stdout.split()


def _text(rel):
    try:
        with io.open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
            return handle.read()
    except OSError:
        return ""


def _flag_of(call):
    if not isinstance(call, ast.Call):
        return None
    func = call.func
    if isinstance(func, ast.Attribute) and func.attr in ("get", "getenv"):
        source = ast.unparse(func.value)
        if "environ" not in source and source != "os":
            return None
    elif not (isinstance(func, ast.Name) and "env" in func.id.lower()):
        return None
    if not call.args or not isinstance(call.args[0], ast.Constant):
        return None
    name = call.args[0].value
    return name if isinstance(name, str) and _FLAG.match(name) else None


def _production_modules():
    return [rel for rel in _tracked("tools/*.py")
            if not os.path.basename(rel).startswith("test_")]


def read_by_production():
    """flag -> {module basenames that read it}."""
    found = {}
    for rel in _production_modules():
        base = os.path.basename(rel)
        for match in _READ.finditer(_text(rel)):
            name = match.group(1) or match.group(2) or match.group(3)
            found.setdefault(name, set()).add(base)
    return found


def _selected():
    names = set()
    for rel in _tracked("tools/test_*.py", "config/*", "scripts/*", "*.sh", "tools/*.sh",
                        "docker/*", ".github/*", "docs/*"):
        names |= set(_NAME.findall(_text(rel)))
    names |= set(_NAME.findall(_text("tools/matrixark_gateway_config.py")))
    names |= set(_NAME.findall(_text("tools/matrixark_load_config.py")))
    return names


def _instructed():
    names = set()
    for rel in _production_modules():
        body = _text(rel)
        try:
            tree = ast.parse(body)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            prose = None
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                prose = node.value
            if not prose or len(prose) < 24:
                continue
            for name in set(_NAME.findall(prose)):
                at = prose.index(name)
                window = prose[max(0, at - _INSTRUCTION_REACH):at + len(name) + _INSTRUCTION_REACH]
                if _INSTRUCTION.search(window.replace(name, " ")):
                    names.add(name)
        # An argparse default reading a variable is that script's command line.
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "add_argument"):
                continue
            for keyword in node.keywords:
                if keyword.arg != "default":
                    continue
                for call in ast.walk(keyword.value):
                    flag = _flag_of(call)
                    if flag:
                        names.add(flag)
    return names


def _harness_only(reads):
    return {name for name, mods in reads.items()
            if mods and all(_HARNESS.search(m) for m in mods)}


def _legacy_spellings():
    """Every flag that appears only as a LATER link of an alias chain."""
    first, later = set(), set()
    for rel in _production_modules():
        try:
            tree = ast.parse(_text(rel))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
                links = []
                for value in node.values:
                    found = [f for f in (_flag_of(c) for c in ast.walk(value)) if f]
                    links.append(found[0] if found else None)
                real = [n for n in links if n]
                if len(real) >= 2:
                    first.add(real[0])
                    later |= set(real[1:])
            name = _flag_of(node)
            if name and len(getattr(node, "args", [])) > 1:
                inner = [f for f in (_flag_of(c) for c in ast.walk(node.args[1])) if f]
                if inner:
                    first.add(name)
                    later |= set(inner)
    return later - first


def classify():
    reads = read_by_production()
    selected = _selected()
    instructed = _instructed()
    harness = _harness_only(reads)
    legacy = _legacy_spellings()
    out = {"selected": set(), "instructed": set(), "harness CLI": set(),
           "deployment identity": set(), "legacy spelling": set(), "candidate": set()}
    for name in reads:
        if name in selected:
            out["selected"].add(name)
        elif name in instructed:
            out["instructed"].add(name)
        elif name in harness:
            out["harness CLI"].add(name)
        elif _IDENTITY.search(name):
            out["deployment identity"].add(name)
        elif name in legacy:
            out["legacy spelling"].add(name)
        else:
            out["candidate"].add(name)
    return reads, out


class TheFlagSurfaceOnlyShrinksTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.reads, cls.groups = classify()

    def test_the_scan_reads_the_tree(self) -> None:
        """A floor on the SCAN, set from what it is for: a reader that stopped matching finds
        approximately nothing, and this whole file would then report a shrinking surface."""
        self.assertGreater(
            len(self.reads), 100,
            "only %d flags found in production python; the read scan has stopped matching, and a "
            "count that falls because the scan broke is the one failure this file cannot see"
            % len(self.reads))

    def test_the_surface_has_not_grown(self) -> None:
        self.assertLessEqual(
            len(self.reads), MAXIMUM_FLAGS_READ,
            "production Python now reads %d distinct environment variables, above the recorded "
            "%d. Either retire one, or raise the ceiling deliberately and say what the new flag "
            "is for -- the point of this number is that nobody could see it moving."
            % (len(self.reads), MAXIMUM_FLAGS_READ))

    def test_the_ceiling_is_not_far_above_the_truth(self) -> None:
        """A ceiling left far above the count stops being a ratchet without saying so."""
        self.assertGreaterEqual(
            len(self.reads), MAXIMUM_FLAGS_READ - 40,
            "the surface is %d and the ceiling is %d. Lower it: a ratchet that banks a reduction "
            "is what makes the next one visible." % (len(self.reads), MAXIMUM_FLAGS_READ))

    def test_every_group_holds_a_plausible_share(self) -> None:
        """The trap that made four earlier versions of these rules useless.

        A rule matching nearly everything is measuring the population, not the property -- and it
        makes the candidate list look empty when it is not. "A test names it" matched the ENV VAR
        and held 100; it means the portal KEY and holds 67. "The reader addresses an operator"
        matched any read and held 121; it means the comment above the read and holds 2.
        """
        total = len(self.reads)
        for name, names in self.groups.items():
            with self.subTest(group=name):
                self.assertLess(
                    len(names), total * 4 // 5,
                    "%r holds %d of %d flags. Check it is asking the question the tree asks "
                    "rather than a looser one." % (name, len(names), total))

    def test_every_flag_lands_in_exactly_one_group(self) -> None:
        counted = sum(len(v) for v in self.groups.values())
        self.assertEqual(
            len(self.reads), counted,
            "the groups hold %d of %d flags, so the classification is not a partition and the "
            "candidate count below cannot be read" % (counted, len(self.reads)))

    def test_the_candidates_are_reported(self) -> None:
        """Not an assertion about how many: a record of what is left, printed where it is read.

        Every candidate is a flag no one can be shown to set and no sentence tells anyone to set.
        Cutting one still needs the suites to be run -- `test_matrixark_knobs_apply_live` refused
        two by name for being wired to what gets stored, which no rule here can see.
        """
        candidates = sorted(self.groups["candidate"])
        self.assertIsInstance(candidates, list)
        if candidates:
            print("\n  %d flags nothing selects and no sentence instructs:" % len(candidates))
            for name in candidates[:40]:
                print("     %-56s %s" % (name, ", ".join(sorted(self.reads[name]))[:60]))


if __name__ == "__main__":
    unittest.main()
