#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The SDK proxy's split tree is not compiled by anything, and that has to be said out loud.

`sdk/rust/temporalstore/src/bin/matrixark_rust_proxy/` holds a full second implementation of the
proxy, split into files. Nothing declares those files as modules and nothing includes them, so no
compiler reads them: `cargo` builds only the two binaries that sit directly in `src/bin/`, and
`build.rs` is empty. The workspace excludes this tree as well, so `cargo check --all-targets` at
the repository root never reaches even the parts that ARE compiled.

That combination is the hazard. An edit here compiles for nobody, is checked by no gate, and looks
exactly like an edit that works -- while a Python test elsewhere reads one of these files as TEXT
and asserts on the identifiers in it, which passes whether or not the code around them is valid.

This was found by retiring a flag. `MATRIXARK_RUST_PROXY_DISABLE_SDK_NATIVE_PACK` was read twice:
once in the binary that runs, and once here. Keeping the two in step is right, but a reader has no
way to know that only one of them is real.

The test fails if any of these files becomes a declared module. That is not a complaint about
fixing it -- it is the signal to come back here and narrow or delete this file, in the same way the
policy-gate census fails when a listed gate is wired.
"""
from __future__ import annotations

import os
import re
import unittest
from typing import List

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
SDK = os.path.join(REPO, "sdk", "rust", "temporalstore")
SPLIT_TREE = os.path.join(SDK, "src", "bin", "matrixark_rust_proxy")

# Forty-seven when this was written. A floor rather than an equality, so deleting one of them is
# not a failure -- shrinking this tree is the direction anyone should be free to go.
EXPECTED_FILE_FLOOR = 40


def _split_tree_modules() -> List[str]:
    if not os.path.isdir(SPLIT_TREE):
        return []
    return sorted(name[:-3] for name in os.listdir(SPLIT_TREE) if name.endswith(".rs"))


def _sdk_rust_sources() -> List[str]:
    found: List[str] = []
    for directory, _, names in os.walk(os.path.join(SDK, "src")):
        for name in sorted(names):
            if name.endswith(".rs"):
                found.append(os.path.join(directory, name))
    return found


class TheSplitProxyTreeIsCompiledByNothingTest(unittest.TestCase):

    def test_the_tree_is_still_there_to_check(self) -> None:
        modules = _split_tree_modules()
        self.assertGreaterEqual(
            len(modules), EXPECTED_FILE_FLOOR,
            "found %d .rs files under %s, expected at least %d -- if the tree moved or was "
            "removed, every assertion below passes on an empty list"
            % (len(modules), os.path.relpath(SPLIT_TREE, REPO), EXPECTED_FILE_FLOOR))

    def test_nothing_declares_them_so_nothing_compiles_them(self) -> None:
        modules = _split_tree_modules()
        declared = []
        for path in _sdk_rust_sources():
            with open(path, encoding="utf-8") as handle:
                text = handle.read()
            for module in modules:
                if re.search(r"^\s*(?:pub\s+)?mod\s+%s\s*;" % re.escape(module), text, re.M):
                    declared.append("%s declares %s" % (os.path.relpath(path, REPO), module))
                elif ('include!("matrixark_rust_proxy/%s.rs")' % module) in text:
                    declared.append("%s includes %s" % (os.path.relpath(path, REPO), module))
        self.assertEqual(
            [], sorted(declared),
            "one of these files is now compiled: %s. That is a good change -- come back here and "
            "narrow or delete this test, which exists only to say that editing them affected "
            "nothing." % sorted(declared))

    def test_the_build_script_generates_nothing(self) -> None:
        build_script = os.path.join(SDK, "build.rs")
        self.assertTrue(os.path.exists(build_script), "the SDK build script has moved")
        with open(build_script, encoding="utf-8") as handle:
            body = handle.read()
        self.assertNotIn(
            "OUT_DIR", body,
            "build.rs now writes generated source, so it may be declaring these modules after "
            "all. The claim that nothing compiles them has to be re-established before this test "
            "can go on making it.")


if __name__ == "__main__":
    unittest.main()
