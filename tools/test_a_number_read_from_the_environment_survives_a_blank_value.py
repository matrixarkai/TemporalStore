#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A number read from the environment at import must survive the variable being set to nothing.

``os.environ.get("X", "8")`` returns the default only when X is ABSENT. ``export X=`` sets it to the
empty string, the default does not apply, and ``int("")`` raises. At module scope that is not a bad
value -- it is a failed IMPORT, which takes down every module that imports it too.

Demonstrated before this check existed: ``MATRIXARK_TOP_K_PER_LAYER=`` made matrixark_mcp_core fail
to import, and with it everything built on it. An operator who comments out a value in a deploy
template, or a config generator that writes an empty string for an unset field, produces exactly
that.

The shape this requires is the one the rest of the tree already uses:

    int(os.environ.get("X", "").strip() or "8")

``.strip()`` and not a bare ``or`` because ``float("   ")`` raises too and whitespace is truthy.

What this does NOT require is that a malformed value be swallowed. ``X=abc`` still raises, exactly
as before -- "unset" and "set to nothing" should mean the same thing, and a typo should not.

Only module scope is checked. The same read inside a function raises for one caller, which is a
smaller problem and a different fix.
"""
from __future__ import annotations

import ast
import importlib
import importlib.util
import os
import pathlib
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent


def _unreachable():
    spec = importlib.util.spec_from_file_location(
        "_reach_for_env", TOOLS / "test_a_module_only_tests_reach_is_not_live.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    names = set()
    for members in module.UNREACHABLE.values():
        names.update(members)
    return names


def unsafe_module_scope_reads(tree):
    """int()/float() over os.environ.get(...) at module scope with no blank guard."""
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node

    def enclosing_function(node):
        while node in parents:
            node = parents[node]
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return True
        return False

    found = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in {"int", "float"} and node.args):
            continue
        text = ast.unparse(node)
        if "environ" not in text:
            continue
        if enclosing_function(node):
            continue
        guarded = any(isinstance(sub, ast.BoolOp) and isinstance(sub.op, ast.Or)
                      for sub in ast.walk(node)) or ".strip()" in text
        if not guarded:
            found.append((node.lineno, text))
    return found


def _live_modules():
    unreachable = _unreachable()
    for entry in sorted(os.listdir(TOOLS)):
        if not entry.endswith(".py") or entry.startswith("test_"):
            continue
        if pathlib.Path(entry).stem in unreachable:
            continue
        try:
            yield pathlib.Path(entry).stem, ast.parse((TOOLS / entry).read_text(encoding="utf-8"))
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue


CASES = [
    ("bare default, no guard", True,
     'import os\nX = int(os.environ.get("A", "8"))\n'),
    ("float, bare default", True,
     'import os\nX = float(os.environ.get("A", "0.5"))\n'),
    ("guarded with strip and or", False,
     'import os\nX = int(os.environ.get("A", "").strip() or "8")\n'),
    ("guarded with a bare or", False,
     'import os\nX = int(os.environ.get("A", "0") or 0)\n'),
    ("inside a function: not this check's business", False,
     'import os\ndef f():\n    return int(os.environ.get("A", "8"))\n'),
    ("not an environment read at all", False,
     'X = int("8")\n'),
]


_PROBE = "MATRIXARK_PROBE_BLANK_NUMBER"


def _numeric_reader_helpers():
    """(module stem, function) for functions that answer a NUMBER read from their first argument.

    A READER THAT DELEGATES IS STILL A READER, and this derivation was written without that and
    immediately proved why: the moment matrixark_mcp_rust_proxy_config._env_int was fixed to take
    its value through a `_raw` helper, it stopped calling os.environ.get itself and dropped out of
    the very list this test exists to watch. A mutation that removed the fix then passed.

    Keyed by (module, function) rather than by bare name: `_env_int` is defined in five modules,
    and crediting one with what another does is how a helper that never reads the environment ends
    up being probed as though it did.
    """
    modules = {}
    for path in sorted(TOOLS.glob("*.py")):
        if path.name.startswith("test_"):
            continue
        try:
            modules[path.stem] = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:  # pragma: no cover
            continue

    functions, defined, imported = [], {}, {}
    for stem, tree in modules.items():
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.args.args:
                functions.append((stem, node))
                defined.setdefault(stem, set()).add(node.name)
            if isinstance(node, ast.ImportFrom) and node.module:
                source = node.module.rsplit(".", 1)[-1]
                for alias in node.names:
                    imported.setdefault(stem, {})[alias.asname or alias.name] = source

    def resolve(stem, callee):
        if callee in defined.get(stem, ()):
            return (stem, callee)
        source = imported.get(stem, {}).get(callee)
        if source and callee in defined.get(source, ()):
            return (source, callee)
        return None

    reads = set()
    for _round in range(8):
        grew = False
        for stem, node in functions:
            first = node.args.args[0].arg
            for sub in ast.walk(node):
                key = None
                if isinstance(sub, ast.Call):
                    func = sub.func
                    if isinstance(func, ast.Attribute) and func.attr in ("get", "getenv") \
                            and sub.args and ast.unparse(func.value) in ("os.environ", "environ", "os"):
                        key = sub.args[0]
                    if key is None and resolve(stem, getattr(func, "id", "")) in reads and sub.args:
                        key = sub.args[0]
                if isinstance(key, ast.Name) and key.id == first:
                    if (stem, node.name) not in reads:
                        reads.add((stem, node.name))
                        grew = True
                    break
        if not grew:
            break

    out = []
    for stem, node in functions:
        if (stem, node.name) not in reads:
            continue
        for sub in ast.walk(node):
            if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) \
                    and sub.func.id in ("int", "float"):
                out.append((stem, node.name))
                break
    return sorted(set(out))


class ANumberReadFromTheEnvironmentSurvivesABlankValue(unittest.TestCase):

    def test_the_scan_reaches_the_tree(self):
        """The floor: no modules scanned would mean no offenders found, for the wrong reason."""
        modules = list(_live_modules())
        self.assertGreater(len(modules), 120,
                           "found %d live modules, expected the whole tools tree" % len(modules))
        reads = sum(
            1 for _stem, tree in modules for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id in {"int", "float"} and node.args
            and "environ" in ast.unparse(node))
        self.assertGreater(
            reads, 100,
            "found only %d int/float reads of the environment in the whole tree -- the scan is "
            "not matching, so a clean result below would mean nothing" % reads)

    def test_the_check_agrees_with_its_worked_examples(self):
        for label, should_flag, source in CASES:
            found = unsafe_module_scope_reads(ast.parse(source))
            self.assertEqual(
                should_flag, bool(found),
                "the blank-value check %s %r" % ("missed" if should_flag else "wrongly flagged",
                                                 label))

    def test_no_module_scope_number_breaks_on_a_blank_value(self):
        offenders = []
        for stem, tree in _live_modules():
            for line, text in unsafe_module_scope_reads(tree):
                offenders.append("%s:%d  %s" % (stem, line, text[:70]))
        self.assertEqual(
            [], sorted(offenders),
            "os.environ.get returns its default only when the name is ABSENT, so `export NAME=` "
            "gives these the empty string and the conversion raises -- at module scope, which is "
            "a failed import rather than a bad value. Use "
            'int(os.environ.get(NAME, "").strip() or DEFAULT): %s' % "; ".join(offenders))

    def test_a_numeric_reader_helper_treats_blank_as_unset(self) -> None:
        """The case the docstring above defers, for the readers where it is not smaller.

        That docstring scopes this file to MODULE SCOPE, on the grounds that the same read inside a
        function "raises for one caller, which is a smaller problem". That holds for a function
        called on a request. It does not hold for a reader whose callers run at CONFIGURATION time:
        matrixark_mcp_rust_proxy_config._env_int is called fifteen times while the lanes are being
        set up, so `MATRIXARK_RUST_PROXY_WRITE_LANES=` was the same failed startup one frame down.

        Derived rather than listed, and CALLED rather than pattern-matched: a helper takes the
        variable name as an argument, so there is no text at the read to match on, and what matters
        is the answer rather than the spelling. A malformed value is still allowed to raise -- that
        is this file's rule, not an exception to it.
        """
        probed, offenders = 0, []
        for stem, func_name in _numeric_reader_helpers():
            try:
                module = importlib.import_module(stem)
            except Exception:  # pragma: no cover - a module that will not import is not this test
                continue
            func = getattr(module, func_name, None)
            if func is None:  # pragma: no cover
                continue
            answered = False
            for blank in ("", "   "):
                os.environ[_PROBE] = blank
                for arguments in ((_PROBE, "4"), (_PROBE, 4), (_PROBE,)):
                    try:
                        func(*arguments)
                        answered = True
                        break
                    except TypeError:
                        continue
                    except Exception as exc:
                        offenders.append("%s.%s raises %s when the variable is set to %r"
                                         % (stem, func_name, type(exc).__name__, blank))
                        answered = True
                        break
                os.environ.pop(_PROBE, None)
            if answered:
                probed += 1
        derived = _numeric_reader_helpers()
        for named in (("matrixark_mcp_rust_proxy_config", "_env_int"),
                      ("matrixark_mcp_rust_proxy_config", "_env_seconds_from_ms")):
            self.assertIn(
                named, derived,
                "%s.%s is the reader this test was added for -- fifteen calls while the proxy "
                "lanes are configured, and `MATRIXARK_RUST_PROXY_WRITE_LANES=` used to raise "
                "ValueError through it. It reaches os.environ through a helper now, so a "
                "derivation that only follows a DIRECT read stops watching it, which is the one "
                "way this test can quietly stop meaning anything." % named)
        self.assertGreater(
            probed, 5,
            "only %d numeric readers were probed. They are derived by asking which functions read "
            "os.environ with their first parameter and then convert it; near zero means the "
            "derivation stopped matching and this asserts nothing." % probed)
        self.assertEqual(offenders, [], "; ".join(offenders))


if __name__ == "__main__":
    unittest.main()
