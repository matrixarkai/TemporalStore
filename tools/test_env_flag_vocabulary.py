#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""One vocabulary for boolean environment flags, and nothing hand-rolling its own.

Boolean flags were parsed in six different vocabularies. They disagreed on the two words an
operator is most likely to reach for: `{"1","true","yes"}` read `on` as OFF, and the deny-list
`{"0","false","no"}` read `off` as ON. The second is the dangerous direction -- three kill-switches
stayed on when set to `off`, which is the opposite of what a kill-switch is for.

These tests pin the single vocabulary, and then check that the flags which were demonstrably
misread are not being parsed by hand any more.
"""
from __future__ import annotations

import ast
import importlib
import os
import pathlib
import re
import unittest

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool

TOOLS = pathlib.Path(__file__).resolve().parent

# Every flag below was measured reading the OPPOSITE of what the value says, before the sites were
# routed through the one parser. The value column is what an operator would plausibly write.
PREVIOUSLY_MISREAD = [
    ("MATRIXARK_AUDIT_DEBUG_PAYLOAD", "on", True, False),
    ("MATRIXARK_CONTEXT_DEBUG_RECORDS", "on", True, False),
    ("MATRIXARK_DIRECT_WRITE_QUEUE_AUTOSTART", "off", False, True),
    ("MATRIXARK_RUST_PROXY_SHARED_PROCESS", "off", False, True),
    ("MATRIXARK_LOCAL_READ_CACHE_COPY", "off", False, True),
]

# The shape that caused it: a literal env read, lowercased, tested against a set written in place.
INLINE_PARSE = re.compile(
    r"""os\.environ\.get\(\s*["'](?P<name>[A-Z][A-Z0-9_]+)["'][^)]*\)"""
    r"""\s*\.strip\(\)\s*\.lower\(\)\s*(?:not\s+in|in)\s*\{"""
)


def _answers_a_boolean(node):
    """Whether a reader answers a BOOLEAN, as against a number or a string.

    Asked two ways, because either alone is wrong here. The annotation `-> bool` is the declarative
    answer and this tree writes it; the membership test against string words is the structural one,
    and it catches a reader that forgot the annotation. Without this filter the derivation sweeps up
    `_env_int` and `env_text`, which answer the same for `on` and `off` because a count and a piece
    of text SHOULD -- and a test that calls those a vocabulary defect is a test nobody can keep.
    """
    if isinstance(node.returns, ast.Name) and node.returns.id == "bool":
        return True
    for sub in ast.walk(node):
        if isinstance(sub, ast.Compare) and any(
                isinstance(op, (ast.In, ast.NotIn)) for op in sub.ops):
            for operand in sub.comparators:
                if isinstance(operand, (ast.Set, ast.List, ast.Tuple)) and all(
                        isinstance(item, ast.Constant) and isinstance(item.value, str)
                        for item in operand.elts) and operand.elts:
                    return True
                if isinstance(operand, ast.Name) and operand.id in ("TRUE_VALUES", "FALSE_VALUES"):
                    return True
    return False


def _boolean_env_helpers():
    """(module stem, function name) for every function that answers a BOOLEAN read of its first
    argument.

    THE INLINE SCAN ABOVE CANNOT SEE THESE, and that is how a sixth vocabulary survived the cleanup
    that created this file. `INLINE_PARSE` looks for `os.environ.get("NAME").strip().lower() in
    {...}` -- the name written out at the read. matrixark_mcp_rust_proxy_config had

        def _env_bool(name, default="1"):
            return os.environ.get(name, default).strip().lower() not in {"0", "false", "no"}

    which is the same deny list this file was written about, missing `off`, with the name arriving
    as an argument. MATRIXARK_RUST_PROXY_SHARED_PROCESS sits in PREVIOUSLY_MISREAD above with `off`
    recorded as something that must read False, and it read True AT THE SITE for the whole time
    this file has existed -- because the assertion up there calls the canonical parser and asks
    what IT says, which was never in doubt.

    A READER THAT DELEGATES IS STILL A READER. `matrixark_mcp_env.env_bool` reaches os.environ
    through `env_lower`, and the moment the module above was fixed to delegate as well, a
    direct-reads-only derivation stopped watching the one reader this was written for. So the
    relation is propagated to a fixpoint: a function that hands its own first argument to a known
    reader is a reader.
    """
    modules = {}
    for path in sorted(TOOLS.glob("*.py")):
        if path.name.startswith("test_"):
            continue
        try:
            modules[path.stem] = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:  # pragma: no cover
            continue

    functions = []
    defined = {}
    imported = {}
    for stem, tree in modules.items():
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.args.args:
                functions.append((stem, node, [a.arg for a in node.args.args]))
                defined.setdefault(stem, set()).add(node.name)
            if isinstance(node, ast.ImportFrom) and node.module:
                source = node.module.rsplit(".", 1)[-1]
                for alias in node.names:
                    imported.setdefault(stem, {})[alias.asname or alias.name] = source

    def resolve(stem, callee):
        """Which (module, function) a call inside `stem` refers to.

        KEYED BY MODULE, because a bare name is ambiguous here and the ambiguity is not academic:
        `_env_bool` is a name-taking reader in matrixark_codex_hook and a VALUE-taking helper in
        matrixark_v1_gateway, and a name-keyed table credited the second with reading the
        environment it never touches -- then this test fed it a variable name, got False for both
        `on` and `off`, and reported a vocabulary defect in a function that has none.
        """
        if callee in defined.get(stem, ()):
            return (stem, callee)
        source = imported.get(stem, {}).get(callee)
        if source and callee in defined.get(source, ()):
            return (source, callee)
        return None

    reads = {}
    for _round in range(8):
        grew = False
        for stem, node, names in functions:
            first = names[0]
            for sub in ast.walk(node):
                key = None
                if isinstance(sub, ast.Call):
                    func = sub.func
                    if isinstance(func, ast.Attribute) and func.attr in ("get", "getenv") and sub.args:
                        source = ast.unparse(func.value)
                        if "environ" in source or source == "os":
                            key = sub.args[0]
                    if key is None:
                        target = resolve(stem, getattr(func, "id", "") or getattr(func, "attr", ""))
                        if target in reads and len(sub.args) > reads[target]:
                            key = sub.args[reads[target]]
                elif isinstance(sub, ast.Subscript) and ast.unparse(sub.value).endswith("environ"):
                    key = sub.slice
                if isinstance(key, ast.Name) and key.id == first:
                    if (stem, node.name) not in reads:
                        reads[(stem, node.name)] = 0
                        grew = True
                    break
        if not grew:
            break

    out = []
    for stem, node, _names in functions:
        if (stem, node.name) in reads and _answers_a_boolean(node):
            out.append((stem, node.name))
    return sorted(set(out))


class EnvFlagVocabulary(unittest.TestCase):
    def setUp(self):
        self._saved = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._saved)

    def test_both_words_are_honoured_in_both_directions(self):
        """`on` and `off` decide, and they decide the same way whatever the default is."""
        for value in ("1", "true", "yes", "on", "TRUE", " On "):
            for default in (True, False):
                os.environ["MATRIXARK_TEST_VOCAB"] = value
                self.assertTrue(env_bool("MATRIXARK_TEST_VOCAB", default),
                                f"{value!r} must read as true (default={default})")
        for value in ("0", "false", "no", "off", "OFF", " Off "):
            for default in (True, False):
                os.environ["MATRIXARK_TEST_VOCAB"] = value
                self.assertFalse(env_bool("MATRIXARK_TEST_VOCAB", default),
                                 f"{value!r} must read as false (default={default})")

    def test_an_unrecognised_value_falls_back_rather_than_guessing(self):
        """A typo must not silently mean ON. The deny-list spelling made `nope` true."""
        for value in ("nope", "disabled", "2", "enabled"):
            os.environ["MATRIXARK_TEST_VOCAB"] = value
            self.assertFalse(env_bool("MATRIXARK_TEST_VOCAB", False), value)
            self.assertTrue(env_bool("MATRIXARK_TEST_VOCAB", True), value)

    def test_unset_returns_the_default(self):
        os.environ.pop("MATRIXARK_TEST_VOCAB", None)
        self.assertTrue(env_bool("MATRIXARK_TEST_VOCAB", True))
        self.assertFalse(env_bool("MATRIXARK_TEST_VOCAB", False))

    def test_no_reader_makes_on_and_off_mean_the_same_thing(self):
        """Asked of the READER, not of the parser.

        Signature-agnostic on purpose: these helpers take (name), (name, default_str) and
        (name, default_bool), and pinning the default would test the caller's intent rather than
        the vocabulary. What no boolean reader may do, whatever its default, is answer the same for
        `on` as for `off`. A deny list missing `off` fails exactly there, and so does an allow list
        missing `on` -- the two directions the docstring at the top of this file is about.
        """
        probed, offenders = 0, []
        for stem, func_name in _boolean_env_helpers():
            try:
                module = importlib.import_module(stem)
            except Exception:  # pragma: no cover - a module that will not import is not this test's
                continue
            func = getattr(module, func_name, None)
            if func is None:  # pragma: no cover
                continue
            answers = {}
            for word in ("on", "off"):
                os.environ["MATRIXARK_TEST_VOCAB"] = word
                for arguments in (("MATRIXARK_TEST_VOCAB",),
                                  ("MATRIXARK_TEST_VOCAB", "0"),
                                  ("MATRIXARK_TEST_VOCAB", False)):
                    try:
                        answers[word] = bool(func(*arguments))
                        break
                    except Exception:
                        continue
            os.environ.pop("MATRIXARK_TEST_VOCAB", None)
            if len(answers) != 2:
                continue
            probed += 1
            if answers["on"] == answers["off"]:
                offenders.append("%s.%s reads both `on` and `off` as %s"
                                 % (stem, func_name, answers["on"]))
        derived = _boolean_env_helpers()
        self.assertIn(
            ("matrixark_mcp_rust_proxy_config", "_env_bool"), derived,
            "the reader this test was written for is not being probed. It delegates to the "
            "canonical parser now, so a derivation that only follows a DIRECT os.environ read "
            "stops watching the one site that was wrong.")
        self.assertNotIn(
            ("matrixark_v1_gateway", "_env_bool"), derived,
            "matrixark_v1_gateway._env_bool takes a VALUE, not a variable name, and never touches "
            "os.environ. It is here only if the derivation is keyed by bare function name, which "
            "credits it with what the identically-named readers in two other modules do -- and "
            "then this test feeds it a variable name, gets False for both `on` and `off`, and "
            "reports a vocabulary defect in a function that has none.")
        self.assertGreater(
            probed, 8,
            "only %d boolean readers were probed. The derivation finds them by asking which "
            "functions read os.environ with their first parameter; near zero means it stopped "
            "matching and this test is asserting nothing." % probed)
        self.assertEqual(offenders, [], "; ".join(offenders))

    def test_the_flags_that_were_misread_now_do_what_they_say(self):
        for name, value, intended, default in PREVIOUSLY_MISREAD:
            os.environ[name] = value
            self.assertEqual(
                env_bool(name, default), intended,
                f"{name}={value} must read as {intended}; it used to read as {not intended}")

    def test_no_module_hand_rolls_a_vocabulary_for_those_flags(self):
        """The sites themselves must be routed through the parser, not merely agree with it.

        Scans every module and asserts its own extent: a guard that silently scanned nothing would
        pass while the code drifted straight back.
        """
        scanned, offenders = 0, []
        misread = {name for name, _, _, _ in PREVIOUSLY_MISREAD}
        for path in sorted(TOOLS.glob("*.py")):
            if path.name.startswith("test_"):
                continue
            scanned += 1
            for match in INLINE_PARSE.finditer(path.read_text(encoding="utf-8", errors="replace")):
                if match.group("name") in misread:
                    offenders.append(f"{path.name}: {match.group('name')}")
        self.assertGreater(scanned, 100,
                           "the scan covered almost no modules -- it is not proving anything")
        self.assertEqual(offenders, [], "these flags are parsed by hand again")


if __name__ == "__main__":
    unittest.main()
