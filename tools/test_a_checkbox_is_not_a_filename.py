#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A setting the portal renders as a checkbox must not be a value the code opens.

`MATRIXARK_SCAN_VISITS_PATH` was declared `kind="bool"`, labelled "Scan visits the node path", and
described as "Off is the shipped behaviour." The retrieve path reads it as a **filename**:

    _visits_path = str(os.environ.get("MATRIXARK_SCAN_VISITS_PATH", "")).strip()
    if _visits_path:
        with open(_visits_path, "a", encoding="utf-8") as _visits_file:

The variable name was read as a verb phrase -- *scan visits the node path* -- when it is a noun:
*the path to write scan-visit counts to*. So ticking the box posted `"1"`, and the serving path
ran `open("1", "a")` and appended two integers to a file named `1` **on every retrieve**, in
whatever directory the process happened to start in. The failure is silent by design: the write
sits in `except OSError: pass`, because "a measurement channel must never break the request it is
measuring".

The setting is gone -- that channel is a developer instrument for an A/B run, not an operator
knob, and it was in no config file and no other guard. This pins the class rather than the
instance: a bool-declared setting whose variable reaches a filesystem or network call is a
checkbox wired to something that needs a string.

The check is on the WHOLE tree, so a new bool setting used as a path fails here the day it lands,
and the floor asserts bool settings are still being found -- with an empty set every assertion
below passes over nothing.
"""
from __future__ import annotations

import ast
import io
import os
import pathlib
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

#: Calls that consume a filename or an address. A bool cannot be any of these.
SINK_CALLS = {
    "open", "io.open", "os.remove", "os.unlink", "os.mkdir", "os.makedirs", "os.rmdir",
    "os.listdir", "os.stat", "os.path.join", "os.path.exists", "os.path.isfile",
    "os.path.isdir", "os.path.abspath", "os.path.dirname", "os.path.basename",
    "pathlib.Path", "Path", "urlopen", "urllib.request.urlopen", "socket.create_connection",
}

#: 79 today. A floor, not a count.
BOOL_SETTING_FLOOR = 40

GETTERS = {"os.environ.get", "os.getenv", "environ.get"}


def bool_settings() -> set:
    return {s.env for s in cfg.SETTINGS
            if getattr(s, "env", "") and getattr(s, "kind", "") == "bool"}


def live_sources():
    listed = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=REPO,
                            capture_output=True, text=True, check=False).stdout.split()
    for rel in listed:
        stem = pathlib.Path(rel).stem
        if stem.startswith("test_"):
            continue
        try:
            with io.open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
                yield stem, handle.read()
        except OSError:  # pragma: no cover
            continue


def env_name_of(node):
    """The variable this node reads, matching on the CALL SHAPE rather than a dotted spelling.

    `matrixark_local_adapter_retrieve` does `import os as _os`, so the read is
    `_os.environ.get(...)`. An exact-name set containing "os.environ.get" does not match it, and
    the general check below then found nothing while the one named instance still failed -- a
    guard that only knows the example it was written for.
    """
    if (isinstance(node, ast.Call) and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)):
        callee = ast.unparse(node.func)
        leaf = callee.rsplit(".", 1)[-1]
        if leaf in ("get", "getenv") and ("environ" in callee or leaf == "getenv"):
            return node.args[0].value
    if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant)             and isinstance(node.slice.value, str):
        if ast.unparse(node.value).endswith("environ"):
            return node.slice.value
    return None


def bool_settings_reaching_a_sink() -> list:
    """(flag, module, line, call) for every bool setting whose value reaches a path/network call.

    A bare name resolves in its own FUNCTION -- `path`, `target` and `name` are assigned in many
    functions of one module, and a module-wide map credits every open() to whichever flag happened
    to share the name.
    """
    wanted = bool_settings()
    found = []
    for stem, source in live_sources():
        if not any(name in source for name in wanted):
            continue
        try:
            tree = ast.parse(source)
        except SyntaxError:  # pragma: no cover
            continue
        scopes = [tree] + [n for n in ast.walk(tree)
                           if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
        enclosing = {}
        for scope in scopes:
            if scope is tree:
                continue
            for node in ast.walk(scope):
                enclosing[id(node)] = scope

        carries = {}
        for scope in scopes:
            local = {}
            for node in ast.walk(scope):
                targets, value = [], None
                if isinstance(node, ast.Assign):
                    targets, value = node.targets, node.value
                elif isinstance(node, ast.AnnAssign) and node.value is not None:
                    targets, value = [node.target], node.value
                if value is None:
                    continue
                names = {n for n in (env_name_of(s) for s in ast.walk(value)) if n in wanted}
                if not names:
                    continue
                for target in targets:
                    if isinstance(target, ast.Name):
                        local.setdefault(target.id, set()).update(names)
            carries[id(scope)] = local

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            callee = ast.unparse(node.func)
            if callee not in SINK_CALLS and callee.split(".")[-1] not in SINK_CALLS:
                continue
            scope = enclosing.get(id(node), tree)
            local = carries.get(id(scope), {})
            module_level = carries.get(id(tree), {})
            for argument in list(node.args) + [k.value for k in node.keywords]:
                names = {n for n in (env_name_of(s) for s in ast.walk(argument)) if n in wanted}
                for sub in ast.walk(argument):
                    if isinstance(sub, ast.Name):
                        names |= local.get(sub.id, set()) or module_level.get(sub.id, set())
                for name in sorted(names):
                    found.append((name, stem, node.lineno, callee))
    return found


class ACheckboxIsNotAFilenameTest(unittest.TestCase):

    def test_no_bool_setting_reaches_a_path_or_network_call(self) -> None:
        offenders = bool_settings_reaching_a_sink()
        # assertEqual on lists, not assertNotIn on a set: unittest prints the whole haystack and
        # buries the one entry that matters.
        self.assertEqual(
            [], offenders,
            "a setting the portal renders as a checkbox is used as a filename or address: "
            + "; ".join("%s at %s:%d in %s()" % (flag, stem, line, call)
                        for flag, stem, line, call in offenders))

    def test_the_removed_setting_stays_removed(self) -> None:
        """The instance. Its variable is still READ -- it is a measurement channel -- but it must
        not be offered as a checkbox again."""
        self.assertTrue(
            "MATRIXARK_SCAN_VISITS_PATH" not in bool_settings(),
            "MATRIXARK_SCAN_VISITS_PATH names a file the retrieve path opens; offering it as a "
            "bool posts \"1\" and writes a file called 1 on every retrieve")


class TheCheckIsLookingAtSomethingTest(unittest.TestCase):
    """Floors. An empty setting list or an unparsed tree passes every assertion above."""

    def test_bool_settings_are_still_found(self) -> None:
        self.assertGreaterEqual(
            len(bool_settings()), BOOL_SETTING_FLOOR,
            "found %d bool settings, expected at least %d -- this check has gone blind"
            % (len(bool_settings()), BOOL_SETTING_FLOOR))

    def test_the_sink_scan_can_find_a_planted_one(self) -> None:
        """The control: the same resolution, run over a planted module, must report it."""
        planted = ast.parse(
            'import os\n'
            'def f():\n'
            '    p = os.environ.get("PLANTED_BOOL_PATH", "")\n'
            '    return open(p, "a")\n')
        wanted = {"PLANTED_BOOL_PATH"}
        local = {}
        for node in ast.walk(planted):
            if isinstance(node, ast.Assign):
                names = {n for n in (env_name_of(s) for s in ast.walk(node.value)) if n in wanted}
                for target in node.targets:
                    if isinstance(target, ast.Name) and names:
                        local.setdefault(target.id, set()).update(names)
        hit = []
        for node in ast.walk(planted):
            if isinstance(node, ast.Call) and ast.unparse(node.func) == "open":
                for argument in node.args:
                    for sub in ast.walk(argument):
                        if isinstance(sub, ast.Name) and sub.id in local:
                            hit.extend(sorted(local[sub.id]))
        self.assertEqual(["PLANTED_BOOL_PATH"], hit,
                         "the scan cannot follow a flag into open(), so a clean tree and a blind "
                         "scan look identical")


if __name__ == "__main__":
    unittest.main()
