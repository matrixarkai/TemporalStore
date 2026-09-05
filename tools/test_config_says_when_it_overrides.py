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


if __name__ == "__main__":
    unittest.main()
