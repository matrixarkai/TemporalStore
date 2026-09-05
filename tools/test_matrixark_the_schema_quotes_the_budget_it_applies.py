#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The retrieve schema advertises the budget the server actually applies.

``max_context_tokens`` declared ``"default": 128000`` and said *"Defaults to
MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS, currently 128000."* Every server path that resolves an omitted
budget falls back to ``DEFAULT_MAX_CONTEXT_TOKENS``, which is **500000**::

    matrixark_temporal_direct_read.py:917    int(args.get("max_context_tokens") or DEFAULT_...)
    matrixark_local_adapter_retrieve.py:509  args.get("max_context_tokens", DEFAULT_...)
    matrixark_mcp_core_packing.py:253        args.get("max_context_tokens", DEFAULT_...)

Nothing applies the schema's own ``default``, so the number there is a claim about the server rather
than an instruction to it -- and the claim was wrong by four times. An agent budgeting its prompt
against the schema reserved a quarter of what it would actually have been given.

The two numbers now come from the constant. The test that matters is the last one: it moves the
environment variable and asserts the schema moves with it, because two literals that happen to
agree today would pass everything else here and drift again tomorrow.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_mcp_runtime_config as runtime  # noqa: E402
import matrixark_mcp_schemas as schemas  # noqa: E402

# The modules that decide what an omitted budget becomes.
CONSUMERS = ("matrixark_temporal_direct_read.py", "matrixark_local_adapter_retrieve.py",
             "matrixark_mcp_core_packing.py")


def budget_schema(module=schemas):
    for name in dir(module):
        if not name.isupper():
            continue
        found = _find(getattr(module, name))
        if found is not None:
            return found
    raise AssertionError("no max_context_tokens property in any schema")


def _find(node):
    if isinstance(node, dict):
        properties = node.get("properties")
        if isinstance(properties, dict) and "max_context_tokens" in properties:
            return properties["max_context_tokens"]
        for value in node.values():
            found = _find(value)
            if found is not None:
                return found
    elif isinstance(node, (list, tuple)):
        for value in node:
            found = _find(value)
            if found is not None:
                return found
    return None


class TheSchemaAgreesWithTheServerTest(unittest.TestCase):

    def test_there_is_a_budget_to_check(self) -> None:
        """A floor: everything below reads this one property."""
        self.assertEqual("integer", budget_schema()["type"])

    def test_the_advertised_default_is_the_one_the_server_applies(self) -> None:
        self.assertEqual(runtime.DEFAULT_MAX_CONTEXT_TOKENS, budget_schema()["default"])

    def test_the_description_quotes_the_same_number(self) -> None:
        """The prose is what a person reads; the field is what a client library reads. They were
        both wrong, and both have to be right."""
        described = budget_schema()["description"]
        numbers = [int(n) for n in re.findall(r"\b(\d{4,})\b", described)]
        self.assertIn(runtime.DEFAULT_MAX_CONTEXT_TOKENS, numbers, described)

    def test_it_still_names_the_variable(self) -> None:
        """A number with no name leaves the reader nothing to change."""
        self.assertIn("MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS", budget_schema()["description"])


class TheServerReallyUsesThatConstantTest(unittest.TestCase):
    """A schema agreeing with a constant nobody applies would be tidy and wrong."""

    def test_every_consumer_falls_back_to_it(self) -> None:
        for name in CONSUMERS:
            with self.subTest(module=name):
                with open(os.path.join(TOOLS, name), encoding="utf-8") as handle:
                    source = handle.read()
                self.assertIn("max_context_tokens", source)
                self.assertIn("DEFAULT_MAX_CONTEXT_TOKENS", source,
                              "%s resolves an omitted budget from something else" % name)


class TheNumbersAreDerivedNotCopiedTest(unittest.TestCase):
    """Two literals that happen to agree today pass every test above."""

    PROBE = """
import json, re, sys
sys.path.insert(0, %r)
import matrixark_mcp_runtime_config as runtime
import matrixark_mcp_schemas as schemas
def find(node):
    if isinstance(node, dict):
        properties = node.get("properties")
        if isinstance(properties, dict) and "max_context_tokens" in properties:
            return properties["max_context_tokens"]
        for value in node.values():
            got = find(value)
            if got is not None:
                return got
    elif isinstance(node, (list, tuple)):
        for value in node:
            got = find(value)
            if got is not None:
                return got
    return None
for name in dir(schemas):
    if name.isupper():
        got = find(getattr(schemas, name))
        if got is not None:
            print(json.dumps({"runtime": runtime.DEFAULT_MAX_CONTEXT_TOKENS,
                              "default": got.get("default"),
                              "description": got.get("description")}))
            break
""" % TOOLS

    def _with_budget(self, value):
        env = dict(os.environ)
        env["MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS"] = str(value)
        proc = subprocess.run([sys.executable, "-c", self.PROBE], capture_output=True, text=True,
                              timeout=600, env=env, cwd=TOOLS)
        if proc.returncode != 0:
            raise AssertionError("the probe did not run: %s" % proc.stderr[-400:])
        return json.loads(proc.stdout.strip().splitlines()[-1])

    def test_the_schema_moves_with_the_variable(self) -> None:
        got = self._with_budget(31337)
        self.assertEqual(31337, got["runtime"], "the probe did not take the environment")
        self.assertEqual(31337, got["default"],
                         "the advertised default is a literal, not the applied budget")
        self.assertIn("31337", got["description"],
                      "the description quotes a literal, not the applied budget")

    def test_it_moves_again_for_a_different_value(self) -> None:
        """One value could be a coincidence of a cached module."""
        got = self._with_budget(4096)
        self.assertEqual(4096, got["default"])
        self.assertIn("4096", got["description"])


if __name__ == "__main__":
    unittest.main()
