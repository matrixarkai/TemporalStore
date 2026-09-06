#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A shipped-config key that contradicts the engine's default has to say so.

Most keys in `config/temporalstore.toml` restate the default the engine already has: they document,
and reading one tells you what you would have got anyway. One does not -- it turns a default off --
and from the line alone the two are indistinguishable.

That difference is the whole value of the file to someone deciding what their deployment is doing.
A key that merely agrees can be deleted with no effect; a key that disagrees is the deployment's
decision, and deleting it changes behaviour.

The list below is the set of keys that disagree. It is asserted exactly, so a NEW disagreement
fails here rather than arriving silently, and a listed key that stops disagreeing fails too --
otherwise the list rots into a description of what used to be true.
"""
from __future__ import annotations

import os
import re
import sys
import unittest
from typing import Dict

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
CONFIG = os.path.join(REPO, "config", "temporalstore.toml")
BUILDER = os.path.join(TOOLS, "build_engine_flag_inventory.py")

# Keys whose value is NOT what the engine would have chosen. Each is a deployment decision, and the
# reason belongs beside it here and in the file.
KNOWN_OVERRIDES: Dict[str, str] = {
    "eager_cache_warm_on_load":
        "the engine defaults ON; the shipped config, three binaries and the one-box all turn it "
        "off, because warming every record on load pays the whole cache cost before the first "
        "read is served",
}

# The same, for keys whose value is a number. Empty today: all six comparable numeric keys restate
# the engine default. A new entry here is a deployment decision and its reason belongs beside it in
# the file, exactly as for the booleans above.
KNOWN_NUMERIC_OVERRIDES: Dict[str, str] = {}

_LINE = re.compile(
    r"^\s*([a-z0-9_]+)\s*=\s*([^\s#]+)\s*#\s*((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)")
_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}

EXPECTED_ANNOTATED_FLOOR = 40


def _builder():
    with open(BUILDER, encoding="utf-8") as handle:
        head = handle.read().split("sources = {}")[0]
    namespace: dict = {}
    sys.argv = ["builder", REPO]
    exec(compile(head, BUILDER, "exec"), namespace)  # noqa: S102 - the builder's own prelude
    return namespace


def _engine_defaults() -> Dict[str, str]:
    ns = _builder()
    flag_read, function_end = ns["FLAG_READ"], ns["function_end"]
    default_of, default_of_statement = ns["default_of"], ns["default_of_statement"]
    strip = ns["strip_test_modules"]
    defaults: Dict[str, str] = {}
    source_root = os.path.join(REPO, "crates", "temporalstore-rust", "src")
    for directory, _, names in os.walk(source_root):
        if os.sep + "tests" in directory:
            continue
        for name in sorted(names):
            if not name.endswith(".rs") or name.startswith("test"):
                continue
            with open(os.path.join(directory, name), encoding="utf-8", errors="replace") as handle:
                lines = strip(handle.read()).split("\n")
            for index, line in enumerate(lines):
                found = set(flag_read.findall(line))
                if len(found) != 1:
                    continue
                flag = next(iter(found))
                if flag in defaults:
                    continue
                value = default_of_statement(lines, index)
                if not value:
                    start = index
                    while start > 0 and not re.match(
                            r"\s*(pub(\([a-z():]+\))?\s+)?fn ", lines[start]):
                        start -= 1
                    body = "\n".join(lines[start:function_end(lines, start)])
                    if len(set(flag_read.findall(body))) == 1:
                        value = default_of(body, 1)
                if value:
                    defaults[flag] = value
    return defaults


_NAME_CONST = re.compile(r'pub const (TS_[A-Z0-9_]+)\s*:\s*&str\s*=\s*"([A-Z0-9_]+)"\s*;')
_INTEGER = re.compile(r"-?\d+")

EXPECTED_NUMERIC_COMPARISON_FLOOR = 5


def _engine_numeric_defaults() -> Dict[str, str]:
    """flag -> the number the engine uses when nothing sets it.

    Two routes, both the builder's own. A number stated in the read, and a number named by a
    const beside the const that names the variable -- the storage_config family, whose reads no
    scan of string literals can see. The second pairs on the const IDENTIFIER, because a
    previous-name const such as `TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME` names a variable
    spelled differently from itself.
    """
    ns = _builder()
    flag_read = ns["FLAG_READ"]
    numeric_default = ns["numeric_default_of_statement"]
    literal_consts, strip = ns["literal_consts"], ns["strip_test_modules"]
    sources = {}
    source_root = os.path.join(REPO, "crates", "temporalstore-rust", "src")
    for directory, _, names in os.walk(source_root):
        if os.sep + "tests" in directory:
            continue
        for name in sorted(names):
            if not name.endswith(".rs") or name.startswith("test"):
                continue
            path = os.path.join(directory, name)
            with open(path, encoding="utf-8", errors="replace") as handle:
                sources[path] = strip(handle.read())
    consts = literal_consts(sources)
    defaults: Dict[str, str] = {}
    for path in sorted(sources):
        source = sources[path]
        lines = source.split("\n")
        for match in flag_read.finditer(source):
            flag = match.group(1)
            if flag in defaults:
                continue
            value = numeric_default(lines, source[: match.start()].count("\n"), consts)
            if value:
                defaults[flag] = value
        for ident, env in _NAME_CONST.findall(source):
            paired = consts.get("DEFAULT_" + ident[len("TS_"):])
            if paired and paired not in ("on", "off"):
                defaults.setdefault(env, paired)
    return defaults


def _annotated_numeric_keys():
    """(key, value, flag) for each annotated key whose value is an integer."""
    with open(CONFIG, encoding="utf-8") as handle:
        for raw in handle:
            match = _LINE.match(raw)
            if not match:
                continue
            value = match.group(2).strip().strip('"')
            if _INTEGER.fullmatch(value):
                yield match.group(1), value, match.group(3)


def _numeric_disagreements() -> set:
    defaults = _engine_numeric_defaults()
    return {key for key, value, flag in _annotated_numeric_keys()
            if flag in defaults and defaults[flag] != value}


def _annotated_boolean_keys():
    """(key, value-as-on/off, flag) for each annotated key whose value is a boolean."""
    with open(CONFIG, encoding="utf-8") as handle:
        for raw in handle:
            match = _LINE.match(raw)
            if not match:
                continue
            value = match.group(2).strip().strip('"').lower()
            if value in _TRUE:
                yield match.group(1), "on", match.group(3)
            elif value in _FALSE:
                yield match.group(1), "off", match.group(3)


class TheShippedConfigSaysWhenItOverridesTest(unittest.TestCase):

    def test_the_config_still_annotates_its_keys(self) -> None:
        with open(CONFIG, encoding="utf-8") as handle:
            annotated = sum(1 for line in handle if _LINE.match(line))
        self.assertGreaterEqual(
            annotated, EXPECTED_ANNOTATED_FLOOR,
            "only %d annotated keys found, expected at least %d -- if the file's shape changed, "
            "the assertions below pass on an empty set" % (annotated, EXPECTED_ANNOTATED_FLOOR))

    def test_no_key_disagrees_with_the_engine_without_saying_so(self) -> None:
        defaults = _engine_defaults()
        disagreeing = {key for key, value, flag in _annotated_boolean_keys()
                       if flag in defaults and defaults[flag] != value}
        new = sorted(disagreeing - set(KNOWN_OVERRIDES))
        self.assertEqual(
            [], new,
            "these shipped-config keys set something the engine would not have chosen, and say "
            "nothing about it: %s. A key that agrees with the default documents; a key that "
            "disagrees is a decision, and the file should be readable enough to tell them apart."
            % new)

    def test_a_listed_key_that_stopped_disagreeing_is_struck_off(self) -> None:
        defaults = _engine_defaults()
        disagreeing = {key for key, value, flag in _annotated_boolean_keys()
                       if flag in defaults and defaults[flag] != value}
        stale = sorted(set(KNOWN_OVERRIDES) - disagreeing)
        self.assertEqual(
            [], stale,
            "these are listed as overriding the engine default and no longer do: %s. Strike them "
            "off -- either the default moved to meet them, or the key did." % stale)


class TheShippedConfigSaysWhenItOverridesANumberTest(unittest.TestCase):
    """The same question, of numbers.

    The class above checks booleans, and could only check booleans: the engine's numeric defaults
    were not readable off the source, so a shipped-config number was compared against nothing. They
    are readable now, and half a file checked reads exactly like a whole one.
    """

    def test_the_scan_still_compares_numbers(self) -> None:
        defaults = _engine_numeric_defaults()
        compared = [key for key, _value, flag in _annotated_numeric_keys() if flag in defaults]
        self.assertGreaterEqual(
            len(compared), EXPECTED_NUMERIC_COMPARISON_FLOOR,
            "only %d numeric config keys could be compared against an engine default, expected at "
            "least %d -- below that the assertion below is passing on almost nothing"
            % (len(compared), EXPECTED_NUMERIC_COMPARISON_FLOOR))

    def test_no_number_disagrees_with_the_engine_without_saying_so(self) -> None:
        new = sorted(_numeric_disagreements() - set(KNOWN_NUMERIC_OVERRIDES))
        self.assertEqual(
            [], new,
            "these shipped-config keys set a number the engine would not have chosen, and say "
            "nothing about it: %s. A key that agrees documents; a key that disagrees is a "
            "decision, and the file should be readable enough to tell them apart." % new)

    def test_a_listed_number_that_stopped_disagreeing_is_struck_off(self) -> None:
        stale = sorted(set(KNOWN_NUMERIC_OVERRIDES) - _numeric_disagreements())
        self.assertEqual(
            [], stale,
            "these are listed as overriding a numeric engine default and no longer do: %s. Strike "
            "them off -- either the default moved to meet them, or the key did." % stale)


class AConfigKeyExportsTheNameTheEngineReadsFirstTest(unittest.TestCase):
    """A key in the shipped config must not export a variable the engine has superseded.

    `storage_config.rs` keeps a previous name for a renamed knob so a deployment that sets it goes
    on working, and says exactly what that costs: it is "read only when the current name is
    unset". A config file that exports the OLD name therefore does not lose loudly. It loses to
    anything that sets the current one -- including a write from the portal, which offers exactly
    that variable -- while the operator reads their value in the file and believes it applies.

    That is what `storage.index_dump_oplog_gap_bytes` did: it exported
    TS_INDEX_DUMP_OPLOG_GAP_BYTES, the fallback, rather than TS_INDEX_DUMP_WAL_GAP_BYTES. The key
    keeps its name so deployed files keep working; only the variable moved.

    The superseded names are read from the engine rather than listed here, so retiring another one
    needs no edit to this file -- and if the engine stops marking them, the floor below fails
    rather than the check going quiet.
    """

    STORAGE_CONFIG = os.path.join(
        REPO, "crates", "temporalstore-rust", "src", "storage_config.rs")
    LOADER = os.path.join(TOOLS, "matrixark_load_config.py")

    _SUPERSEDED = re.compile(
        r'pub const [A-Z][A-Z0-9_]*_PREVIOUS_NAME\s*:\s*&str\s*=\s*"([A-Z0-9_]+)"\s*;')
    _MAPPING = re.compile(r'^\s*"([a-z0-9_.]+)"\s*:\s*"([A-Z][A-Z0-9_]+)"\s*,', re.M)

    EXPECTED_MAPPING_FLOOR = 30

    def _superseded(self):
        with open(self.STORAGE_CONFIG, encoding="utf-8") as handle:
            return set(self._SUPERSEDED.findall(handle.read()))

    def _mappings(self):
        with open(self.LOADER, encoding="utf-8") as handle:
            return self._MAPPING.findall(handle.read())

    def test_the_engine_still_marks_a_superseded_name(self) -> None:
        found = self._superseded()
        self.assertTrue(
            found,
            "no const named *_PREVIOUS_NAME in storage_config.rs, so the check below compares "
            "against an empty set and would pass over any deprecated export.")

    def test_the_scan_still_reads_the_mapping_table(self) -> None:
        mappings = self._mappings()
        self.assertGreaterEqual(
            len(mappings), self.EXPECTED_MAPPING_FLOOR,
            "read %d config-key mappings, expected at least %d -- if the table's shape changed, "
            "the assertion below is checking nothing"
            % (len(mappings), self.EXPECTED_MAPPING_FLOOR))

    def test_no_config_key_exports_a_superseded_variable(self) -> None:
        superseded = self._superseded()
        wrong = ["%s -> %s" % (key, env) for key, env in self._mappings() if env in superseded]
        self.assertEqual(
            [], wrong,
            "these config keys export a variable the engine reads only when the current one is "
            "unset, so their value loses silently to anything that sets the current name: %s"
            % wrong)


if __name__ == "__main__":
    unittest.main()
