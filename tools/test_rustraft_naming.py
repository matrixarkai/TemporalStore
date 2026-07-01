#!/usr/bin/env python3
from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class RustRaftNamingTest(unittest.TestCase):
    def test_rust_surfaces_do_not_expose_byteraft_names(self) -> None:
        checked_roots = [
            ROOT / "crates" / "temporalstore-rust",
            ROOT / "docs" / "rustraft_production_readiness.md",
            ROOT / "docs" / "distributed_raft_readiness.md",
            ROOT / "tools" / "validate_aws_validation_log.py",
            ROOT / "tools" / "run_temporalstore_unified_tests.py",
        ]
        offenders: list[str] = []
        for root in checked_roots:
            paths = [root] if root.is_file() else [path for path in root.rglob("*") if path.is_file()]
            for path in paths:
                if path.suffix not in {".rs", ".md", ".py", ".toml", ".json"}:
                    continue
                text = path.read_text(encoding="utf-8", errors="ignore")
                if "byteraft" in text.lower():
                    offenders.append(str(path.relative_to(ROOT)))
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
