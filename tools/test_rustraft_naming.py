#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The retired raft name stays out of the Rust surfaces.

This guard was added when those surfaces were renamed off an internal name, and its job is to
keep that name from coming back. A later repository-wide rename of that name to `matrixraft`
rewrote the string this guard *forbids* along with everything else, so the guard came to assert
that `matrixraft` appears nowhere -- while the crate depends on MatrixRaft by exactly that name
(`crates/temporalstore-rust/Cargo.toml` names it, and `src/raft/matrixraft.rs` is a module). It
could not pass, and meanwhile nothing was checking for the retired name at all: the open-source
readiness gate (`tools/validate_open_source_readiness.py`) covers licensing, private paths and
the enterprise object store, not this.

So the subject is restored, spelled in two pieces so that a guard naming what it forbids does not
itself reintroduce it -- and so a repository-wide rename cannot silently retarget it a second
time.
"""
from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

# Assembled at runtime: the literal must not appear in a tracked file, which is the whole point.
RETIRED_NAME = "byte" + "raft"

SCANNED_SUFFIXES = {".rs", ".md", ".py", ".toml", ".json"}

CHECKED_ROOTS = (
    ROOT / "crates" / "temporalstore-rust",
    ROOT / "docs" / "rustraft_production_readiness.md",
    ROOT / "docs" / "distributed_raft_readiness.md",
    ROOT / "tools" / "validate_aws_validation_log.py",
    ROOT / "tools" / "run_temporalstore_unified_tests.py",
)

# A floor on the scan, so a moved or renamed directory cannot leave this quantified over nothing
# and reporting success. 267 files are in scope today.
MIN_SCANNED_FILES = 200


def offending_paths(documents: dict) -> list:
    """Pure over {name: text} so the check can be run against deliberately broken input."""
    return sorted(name for name, text in documents.items() if RETIRED_NAME in text.lower())


def scanned_documents() -> dict:
    documents = {}
    for root in CHECKED_ROOTS:
        if not root.exists():
            continue
        paths = [root] if root.is_file() else [path for path in root.rglob("*") if path.is_file()]
        for path in paths:
            if path.suffix not in SCANNED_SUFFIXES:
                continue
            documents[str(path.relative_to(ROOT))] = path.read_text(encoding="utf-8", errors="ignore")
    return documents


class RustRaftNamingTest(unittest.TestCase):
    def test_rust_surfaces_do_not_expose_the_retired_raft_name(self) -> None:
        documents = scanned_documents()
        missing = [str(root.relative_to(ROOT)) for root in CHECKED_ROOTS if not root.exists()]
        self.assertEqual([], missing, "a checked path has moved, so it is no longer being scanned")
        self.assertGreaterEqual(
            len(documents), MIN_SCANNED_FILES,
            "only %d files were scanned, expected at least %d -- the scan has lost its subject"
            % (len(documents), MIN_SCANNED_FILES))
        self.assertEqual([], offending_paths(documents))

    def test_a_reintroduced_name_would_be_caught(self) -> None:
        """The positive control. A checker that only ever sees clean input passes just as well
        when it has stopped checking."""
        planted = {
            "crates/temporalstore-rust/src/lib.rs": "// migrated from " + RETIRED_NAME.upper(),
            "crates/temporalstore-rust/README.md": "nothing to see here",
        }
        self.assertEqual(["crates/temporalstore-rust/src/lib.rs"], offending_paths(planted))
        # And the current, legitimate dependency name is not what this guard is about.
        self.assertEqual([], offending_paths({"Cargo.toml": 'matrixraft = { package = "matrixraft" }'}))


if __name__ == "__main__":
    unittest.main()
