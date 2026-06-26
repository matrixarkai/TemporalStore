#!/usr/bin/env python3
from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "run_temporalstore_prebenchmark_gate.sh"


class TemporalStorePrebenchmarkGateTest(unittest.TestCase):
    def test_stage_order_and_remediation_buckets_are_locked(self) -> None:
        text = SCRIPT.read_text(encoding="utf-8")
        expected = [
            "topology_readiness",
            "proxy_client",
            "ingestion_write_path",
            "cache_eviction_invariants",
            "deep_storage_mode_matrix",
            "matrixark_context_parity",
        ]
        positions = [text.index(name) for name in expected]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("fix metaserver reachability, namespace/table creation, placement, slot coverage, primary assignment, or topology readiness retries", text)
        self.assertIn("fix launcher, live proxy port, direct SDK oracle, request timeout, or C++ proxy status warnings", text)
        self.assertIn("fix queue replay, append batching, async oplog, or backend write timeout before MatrixArk context parity", text)
        self.assertIn("fix cache admission, eviction counters, refill-from-persistence, page compaction, GC, and recovery invariants before scale claims", text)
        self.assertIn("stop_on_first_failure", text)
        self.assertIn("timeout", text)


if __name__ == "__main__":
    unittest.main()
