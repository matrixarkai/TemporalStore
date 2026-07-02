#!/usr/bin/env python3
from __future__ import annotations

import json
import unittest

from validate_raft_cpp_rust_parity_contract import REPORT_PAIR_CORPUS, _load_json, validate_report_pair


class RaftCppRustParityContractTest(unittest.TestCase):
    def test_operational_top_level_shape_is_required_and_symmetric(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        rust_extra = json.loads(json.dumps(corpus["rust"]))
        rust_extra["rust_only_debug"] = {"internal": True}
        failures = validate_report_pair(corpus["cpp"], rust_extra)
        self.assertTrue(any("top-level report shape drift" in failure for failure in failures))

        cpp_missing = json.loads(json.dumps(corpus["cpp"]))
        del cpp_missing["replication_metrics"]
        failures = validate_report_pair(cpp_missing, corpus["rust"])
        self.assertIn("cpp report missing top-level `replication_metrics`", failures)
        self.assertIn("cpp report missing operational top-level `replication_metrics`", failures)


if __name__ == "__main__":
    unittest.main()
