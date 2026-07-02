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

    def test_metaserver_raft_phase1_behaviors_are_required(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["metaserver_raft"]["behavior_evidence"]["slot_assignment"]
        failures = validate_report_pair(corpus["cpp"], rust_missing)
        self.assertIn(
            "rust metaserver_raft.behavior_evidence missing `slot_assignment`",
            failures,
        )

        cpp_failed = json.loads(json.dumps(corpus["cpp"]))
        cpp_failed["metaserver_raft"]["behavior_evidence"]["snapshot_restore"]["status"] = "failed"
        failures = validate_report_pair(cpp_failed, corpus["rust"])
        self.assertIn(
            "cpp metaserver_raft.behavior_evidence.snapshot_restore status drift: 'failed'",
            failures,
        )

    def test_required_metrics_are_required_for_both_subsystems(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        cpp_missing_metric = json.loads(json.dumps(corpus["cpp"]))
        del cpp_missing_metric["metaserver_raft"]["metrics"]["leader_election_ms"]
        failures = validate_report_pair(cpp_missing_metric, corpus["rust"])
        self.assertIn(
            "cpp metaserver_raft.metrics missing `leader_election_ms`",
            failures,
        )

        rust_missing_metric = json.loads(json.dumps(corpus["rust"]))
        del rust_missing_metric["data_node_raft"]["metrics"]["stale_leader_observed"]
        failures = validate_report_pair(corpus["cpp"], rust_missing_metric)
        self.assertIn(
            "rust data_node_raft.metrics missing `stale_leader_observed`",
            failures,
        )

    def test_data_node_raft_phase2_behaviors_are_required(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["data_node_raft"]["behavior_evidence"]["quorum_write"]
        failures = validate_report_pair(corpus["cpp"], rust_missing)
        self.assertIn(
            "rust data_node_raft.behavior_evidence missing `quorum_write`",
            failures,
        )

        cpp_failed = json.loads(json.dumps(corpus["cpp"]))
        cpp_failed["data_node_raft"]["behavior_evidence"]["read_after_write_under_leader_change"]["status"] = "failed"
        failures = validate_report_pair(cpp_failed, corpus["rust"])
        self.assertIn(
            "cpp data_node_raft.behavior_evidence.read_after_write_under_leader_change status drift: 'failed'",
            failures,
        )


if __name__ == "__main__":
    unittest.main()
