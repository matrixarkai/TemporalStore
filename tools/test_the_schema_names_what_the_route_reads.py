#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The storage-options schema must name every option the route resolver acts on.

`STORAGE_OPTIONS_SCHEMA` is the contract: it is attached to `storage_options` in four tool
definitions, so it is what a caller reads to learn which options exist. `canonical_storage_route`
is what actually reads them. They had drifted apart, and the drift was invisible because the schema
sets `additionalProperties: true` -- an undeclared option is accepted and acted on, and nothing
anywhere says it exists.

Two were missing when this was written, and both do real work:

  * `durability` -- read at the top of the resolver and used AS `write_mode` when write_mode is
    left at "default", so it decides whether an acknowledgement waits for the durable route.
  * `read_preference` -- returned in the resolved options, defaulting to replica_preferred for
    async ingest so read-heavy paths can use replicas.

A caller could set either and it would work; no document said so. Both are declared now, and this
keeps the pair in step.

Read from the AST rather than by pattern, and scoped to the resolver FUNCTION, because
`storage_options_for_record` further down reads `record_options.get("resource")` from a different
dict entirely -- a per-record-kind mapping, not a storage option. A module-wide scan reports that as
a missing property and is wrong.
"""
from __future__ import annotations

import ast
import os
import unittest
from typing import Dict, Set

TOOLS = os.path.dirname(os.path.abspath(__file__))

RESOLVER = "canonical_storage_route"
OPTIONS_MODULE = os.path.join(TOOLS, "matrixark_mcp_storage_options.py")
SCHEMA_MODULE = os.path.join(TOOLS, "matrixark_mcp_schemas.py")

#: Keys the resolver reads that the schema deliberately does not advertise, and why.
ACCEPTED_ALIASES: Dict[str, str] = {
    "family":
        "the older spelling of storage_family, read as `options.get(\"storage_family\") or "
        "options.get(\"family\")`. Accepting a superseded name costs nothing and refusing it "
        "breaks whoever set it, but ADVERTISING it would invite new callers to the name that is "
        "on its way out",
}

#: Nine when this was written, before the two that were missing.
EXPECTED_OPTION_FLOOR = 8


def _resolver_option_keys() -> Set[str]:
    with open(OPTIONS_MODULE, encoding="utf-8") as handle:
        tree = ast.parse(handle.read())
    found: Set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name != RESOLVER:
            continue
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Call):
                continue
            func = inner.func
            if not isinstance(func, ast.Attribute) or func.attr != "get":
                continue
            if not isinstance(func.value, ast.Name) or func.value.id != "options":
                continue
            if inner.args and isinstance(inner.args[0], ast.Constant) \
                    and isinstance(inner.args[0].value, str):
                found.add(inner.args[0].value)
    return found


def _schema_properties() -> Set[str]:
    with open(SCHEMA_MODULE, encoding="utf-8") as handle:
        tree = ast.parse(handle.read())
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if isinstance(target, ast.Name) and target.id == "STORAGE_OPTIONS_SCHEMA":
                value = node.value
                if value is None:
                    return set()
                return set(ast.literal_eval(value).get("properties", {}))
    return set()


class TheSchemaNamesWhatTheRouteReadsTest(unittest.TestCase):

    def test_both_sides_still_parse(self) -> None:
        """Assert the scan's own extent: two empty sets satisfy any subset check."""
        keys, properties = _resolver_option_keys(), _schema_properties()
        self.assertGreaterEqual(
            len(keys), EXPECTED_OPTION_FLOOR,
            "found %d options read by %s, expected at least %d -- if the resolver was renamed or "
            "rewritten, the assertion below passes on an empty set"
            % (len(keys), RESOLVER, EXPECTED_OPTION_FLOOR))
        self.assertGreaterEqual(
            len(properties), EXPECTED_OPTION_FLOOR,
            "found %d properties on STORAGE_OPTIONS_SCHEMA, expected at least %d"
            % (len(properties), EXPECTED_OPTION_FLOOR))

    def test_the_schema_names_every_option_the_resolver_reads(self) -> None:
        undeclared = sorted(_resolver_option_keys() - _schema_properties()
                            - set(ACCEPTED_ALIASES))
        self.assertEqual(
            [], undeclared,
            "%s acts on these and STORAGE_OPTIONS_SCHEMA does not mention them: %s\nThe schema "
            "sets additionalProperties true, so a caller setting one gets the behaviour and no "
            "document tells them it exists. Declare it, or list it above as an alias with the "
            "reason." % (RESOLVER, undeclared))

    def test_an_alias_is_still_read(self) -> None:
        """A listed alias that the resolver stopped reading is a stale exemption."""
        stale = sorted(set(ACCEPTED_ALIASES) - _resolver_option_keys())
        self.assertEqual(
            [], stale,
            "these are listed as accepted aliases and %s no longer reads them: %s. Strike them "
            "off -- an exemption outliving the code it exempts is how a real gap hides behind a "
            "list." % (RESOLVER, stale))

    def test_every_alias_gives_a_reason(self) -> None:
        thin = sorted(name for name, why in ACCEPTED_ALIASES.items() if len(why.strip()) < 30)
        self.assertEqual([], thin, "listed without a reason worth reading: %s" % thin)


if __name__ == "__main__":
    unittest.main()
