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

        rust_missing_data_metric = json.loads(json.dumps(corpus["rust"]))
        del rust_missing_data_metric["data_node_raft"]["metrics"]["append_qps"]
        failures = validate_report_pair(corpus["cpp"], rust_missing_data_metric)
        self.assertIn(
            "rust data_node_raft.metrics missing `append_qps`",
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

    def test_unified_matrix_and_fail_closed_gates_are_required(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        cpp_missing_case = json.loads(json.dumps(corpus["cpp"]))
        del cpp_missing_case["test_matrix"]["leader_kill_restart"]
        failures = validate_report_pair(cpp_missing_case, corpus["rust"])
        self.assertIn("cpp test_matrix missing `leader_kill_restart`", failures)

        rust_failed_gate = json.loads(json.dumps(corpus["rust"]))
        rust_failed_gate["fail_closed_gates"]["same_quorum_rule"]["status"] = "failed"
        failures = validate_report_pair(corpus["cpp"], rust_failed_gate)
        self.assertIn("rust fail_closed_gates.same_quorum_rule status drift: 'failed'", failures)

        cpp_missing_evidence = json.loads(json.dumps(corpus["cpp"]))
        del cpp_missing_evidence["fail_closed_gates"]["no_stale_follower_reads_when_ready"]["stale_read_count"]
        failures = validate_report_pair(cpp_missing_evidence, corpus["rust"])
        self.assertIn(
            "cpp fail_closed_gates.no_stale_follower_reads_when_ready missing `stale_read_count`",
            failures,
        )

        rust_quorum_drift = json.loads(json.dumps(corpus["rust"]))
        rust_quorum_drift["fail_closed_gates"]["same_quorum_rule"]["quorum_rule"] = "all(voters)"
        failures = validate_report_pair(corpus["cpp"], rust_quorum_drift)
        self.assertIn(
            "fail_closed_gates.same_quorum_rule.quorum_rule drift: cpp='majority(voters)' rust='all(voters)'",
            failures,
        )

        rust_membership_drift = json.loads(json.dumps(corpus["rust"]))
        rust_membership_drift["fail_closed_gates"]["membership_change_result_match"][
            "membership_change_result"
        ] = "remove_rejected"
        failures = validate_report_pair(corpus["cpp"], rust_membership_drift)
        self.assertIn(
            "fail_closed_gates.membership_change_result_match.membership_change_result drift: "
            "cpp='add_remove_committed' rust='remove_rejected'",
            failures,
        )

    def test_shared_report_summary_is_required(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        cpp_missing_summary = json.loads(json.dumps(corpus["cpp"]))
        del cpp_missing_summary["report_summary"]["storage_mode"]
        failures = validate_report_pair(cpp_missing_summary, corpus["rust"])
        self.assertIn("cpp report_summary missing `storage_mode`", failures)

        rust_bad_backend = json.loads(json.dumps(corpus["rust"]))
        rust_bad_backend["report_summary"]["backend"] = "cpp"
        failures = validate_report_pair(corpus["cpp"], rust_bad_backend)
        self.assertIn("rust report_summary.backend drift: 'cpp'", failures)


if __name__ == "__main__":
    unittest.main()
