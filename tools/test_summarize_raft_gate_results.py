#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import summarize_raft_gate_results as raft_summary


class RaftGateSummaryTest(unittest.TestCase):
    def validate(self, summary, **overrides):
        args = {
            "max_metaserver_failover_ms": 10_000,
            "max_data_failover_write_read_ms": 10_000,
            "max_secondary_visibility_p99_us": 50_000,
            "max_post_failover_apply_lag": 128,
            "max_2node_scale_p99_us": 150_000,
        }
        args.update(overrides)
        return raft_summary.validate_production_assertions(summary, **args)

    def production_summary(self):
        return {
            "passed": 6,
            "failed": 0,
            "case_metrics": {
                "metaserver_failover": {
                    "metaserver_failover_ms": 120,
                    "post_failover_query_ok": 1,
                    "diagnostics_expected_running_count": 2,
                    "diagnostics_alive_count": 2,
                    "diagnostics_unexpected_down_count": 0,
                    "diagnostics_port_up_count": 2,
                    "diagnostics_fatal_log_line_count": 0,
                },
                "metaserver_membership": {
                    "membership_nodes_after_add": 3,
                    "membership_nodes_after_remove": 2,
                    "node3_applied_index_after_add": 10,
                    "leader_after_remove": "127.0.0.1:18010",
                    "removed_node_port_down": "1",
                    "namespace_before_add": "1",
                    "namespace_after_remove": "1",
                    "node3_stale_read_namespace": "1",
                },
                "data_failover": {
                    "post_failover_write_read_ms": 300,
                    "background_failover_enabled": 1,
                    "background_failover_active_at_kill": 1,
                    "background_failover_exit_code": 0,
                    "background_failover_errors": 0,
                    "background_failover_zero_errors": 1,
                    "secondary_visibility_errors": 0,
                    "secondary_visibility_p99_us": 400,
                    "post_failover_after_write_raft_max_apply_lag": 3,
                    "post_failover_after_write_raft_max_fatal_events": 0,
                },
                "data_membership": {
                    "after_scale_up_active_replicas": 3,
                    "after_scale_down_active_replicas": 2,
                    "after_drop_active_replicas": 2,
                    "scale_down_server3_active_partitions": 0,
                    "drop_server3_active_partitions": 0,
                    "server3_after_scale_up_running": 1,
                    "server3_after_scale_up_voter_count": 3,
                    "server3_after_scale_up_fatal_event_count": 0,
                    "baseline_raft_max_apply_lag": 1,
                    "baseline_raft_max_fatal_events": 0,
                    "after_scale_up_raft_max_apply_lag": 0,
                    "after_scale_up_raft_max_fatal_events": 0,
                    "after_scale_down_raft_max_apply_lag": 0,
                    "after_scale_down_raft_max_fatal_events": 0,
                    "after_drop_raft_max_apply_lag": 0,
                    "after_drop_raft_max_fatal_events": 0,
                },
                "data_2node_scale": {
                    "best_set_qps": 1000,
                    "best_get_qps": 1200,
                    "max_errors": 0,
                    "max_exit_code": 0,
                    "max_set_p95_us": 10_000,
                    "max_set_p99_us": 20_000,
                    "max_get_p95_us": 1000,
                    "max_get_p99_us": 2000,
                },
                "data_mixed_rw": {
                    "secondary_phase_count": 2,
                    "background_phase_count": 2,
                    "max_errors": 0,
                    "max_p95_us": 200,
                    "max_p99_us": 400,
                    "max_background_errors": 0,
                    "max_background_exit_code": 0,
                },
                "data_snapshot_restore": {
                    "snapshot_file_count_before_restart": 1,
                    "applied_index_file_count": 1,
                    "wal_file_count": 1,
                },
            },
        }

    def test_production_assertions_pass_for_bounded_raft_lag(self):
        assertions = self.validate(self.production_summary())

        self.assertTrue(assertions["passed"])
        checks = {check["name"]: check for check in assertions["checks"]}
        self.assertTrue(checks["metaserver_failover_no_unexpected_peer_death"]["passed"])
        self.assertTrue(checks["metaserver_failover_no_fatal_logs"]["passed"])
        self.assertTrue(checks["data_membership_apply_lag_bounded"]["passed"])
        self.assertTrue(checks["data_membership_no_raft_fatal_events"]["passed"])
        self.assertTrue(checks["data_2node_scale_has_qps"]["passed"])
        self.assertTrue(checks["data_2node_scale_no_errors"]["passed"])
        self.assertTrue(checks["data_2node_scale_latency_bounded"]["passed"])
        self.assertTrue(checks["data_mixed_rw_no_errors"]["passed"])
        self.assertTrue(checks["data_mixed_rw_visibility_p99_bounded"]["passed"])

    def test_production_assertions_fail_on_membership_lag_regression(self):
        summary = self.production_summary()
        summary["case_metrics"]["data_membership"]["after_scale_up_raft_max_apply_lag"] = 10_000

        assertions = self.validate(
            summary,
            max_post_failover_apply_lag=128,
        )

        checks = {check["name"]: check for check in assertions["checks"]}
        self.assertFalse(assertions["passed"])
        self.assertFalse(checks["data_membership_apply_lag_bounded"]["passed"])

    def test_production_assertions_fail_on_mixed_rw_errors(self):
        summary = self.production_summary()
        summary["case_metrics"]["data_mixed_rw"]["max_background_errors"] = 1

        assertions = self.validate(summary)

        checks = {check["name"]: check for check in assertions["checks"]}
        self.assertFalse(assertions["passed"])
        self.assertFalse(checks["data_mixed_rw_no_errors"]["passed"])

    def test_production_assertions_fail_on_2node_scale_errors(self):
        summary = self.production_summary()
        summary["case_metrics"]["data_2node_scale"]["max_errors"] = 1

        assertions = self.validate(summary)

        checks = {check["name"]: check for check in assertions["checks"]}
        self.assertFalse(assertions["passed"])
        self.assertFalse(checks["data_2node_scale_no_errors"]["passed"])

    def test_production_assertions_fail_on_2node_scale_latency(self):
        summary = self.production_summary()
        summary["case_metrics"]["data_2node_scale"]["max_set_p99_us"] = 200_000

        assertions = self.validate(summary, max_2node_scale_p99_us=150_000)

        checks = {check["name"]: check for check in assertions["checks"]}
        self.assertFalse(assertions["passed"])
        self.assertFalse(checks["data_2node_scale_latency_bounded"]["passed"])

    def test_extract_data_2node_parses_latency_and_exit_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            case_dir = Path(tmp)
            run_dir = case_dir / "run"
            run_dir.mkdir()
            (run_dir / "results.csv").write_text(
                "\n".join(
                    [
                        "threads,set_qps,set_p50_us,set_p95_us,set_p99_us,get_qps,get_p50_us,get_p95_us,get_p99_us,errors,exit_code",
                        "2,100,10,20,30,200,4,5,6,0,0",
                        "4,120,11,21,31,240,5,6,7,0,0",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            metrics = raft_summary.extract_data_2node(case_dir)

        self.assertEqual(metrics["best_set_qps"], 120)
        self.assertEqual(metrics["best_get_qps"], 240)
        self.assertEqual(metrics["max_exit_code"], 0)
        self.assertEqual(metrics["max_set_p99_us"], 31)
        self.assertEqual(metrics["max_get_p99_us"], 7)

    def test_production_assertions_fail_on_metaserver_fatal_diagnostics(self):
        summary = self.production_summary()
        summary["case_metrics"]["metaserver_failover"]["diagnostics_fatal_log_line_count"] = 1

        assertions = self.validate(summary)

        checks = {check["name"]: check for check in assertions["checks"]}
        self.assertFalse(assertions["passed"])
        self.assertFalse(checks["metaserver_failover_no_fatal_logs"]["passed"])

    def test_extract_metaserver_failover_parses_diagnostics(self):
        with tempfile.TemporaryDirectory() as tmp:
            case_dir = Path(tmp)
            run_dir = case_dir / "run"
            run_dir.mkdir()
            (run_dir / "summary.txt").write_text(
                "metaserver_failover_ms=42\n",
                encoding="utf-8",
            )
            (run_dir / "diagnostics_summary.txt").write_text(
                "\n".join(
                    [
                        "diagnostic_reason=success",
                        "expected_running_count=2",
                        "alive_count=2",
                        "unexpected_down_count=0",
                        "port_up_count=2",
                        "fatal_log_line_count=0",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            metrics = raft_summary.extract_metaserver_failover(case_dir)

        self.assertEqual(metrics["metaserver_failover_ms"], 42)
        self.assertEqual(metrics["diagnostics_expected_running_count"], 2)
        self.assertEqual(metrics["diagnostics_unexpected_down_count"], 0)
        self.assertEqual(metrics["diagnostics_fatal_log_line_count"], 0)
        self.assertEqual(metrics["diagnostic_reason"], "success")

    def test_extract_data_mixed_rw_parses_visibility_and_background_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            case_dir = Path(tmp)
            run_dir = case_dir / "run"
            run_dir.mkdir()
            (run_dir / "mixed_visibility.out").write_text(
                "\n".join(
                    [
                        "phase,samples,success,errors,p50_us,p95_us,p99_us",
                        "secondary_visibility_lag_after_primary_set,10,10,0,10,20,30",
                        "secondary_visibility_lag_after_primary_delete,10,10,0,11,21,31",
                        "background,ops,success,errors,exit_code",
                        "writes,100,100,0,0",
                        "reads,100,100,0,0",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            metrics = raft_summary.extract_data_mixed_rw(case_dir)

        self.assertEqual(metrics["secondary_phase_count"], 2)
        self.assertEqual(metrics["background_phase_count"], 2)
        self.assertEqual(metrics["max_errors"], 0)
        self.assertEqual(metrics["max_p99_us"], 31)
        self.assertEqual(metrics["max_background_errors"], 0)
        self.assertEqual(metrics["max_background_exit_code"], 0)

    def test_prometheus_output_contains_production_checks(self):
        summary = self.production_summary()
        summary["production_assertions"] = self.validate(summary)

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "metrics.prom")
            raft_summary.write_prometheus(summary, path)
            payload = path.read_text(encoding="utf-8")

        self.assertIn("temporalstore_raft_gate_production_ready 1", payload)
        self.assertIn(
            'temporalstore_raft_gate_production_check_pass{check="data_membership_apply_lag_bounded"} 1',
            payload,
        )
        self.assertIn(
            "temporalstore_raft_gate_data_membership_after_scale_up_raft_max_apply_lag 0",
            payload,
        )
        self.assertIn("temporalstore_raft_gate_2node_best_set_qps 1000", payload)
        self.assertIn("temporalstore_raft_gate_2node_max_set_p99_us 20000", payload)
        self.assertIn("temporalstore_raft_gate_mixed_rw_max_p99_us 400", payload)
        self.assertIn(
            "temporalstore_raft_gate_metaserver_failover_diagnostics_unexpected_down_count 0",
            payload,
        )


if __name__ == "__main__":
    unittest.main()
