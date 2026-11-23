# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
import json
import unittest
import direct_data_server_e2e_assertions as oracle


def response(value, ok=True):
    return json.dumps({"status": {"ok": ok}, "response": value})


class RecoveryAssertions(unittest.TestCase):
    def test_missing_data_cannot_pass_as_a_recovered_value(self):
        with self.assertRaises(ValueError):
            oracle.assert_response({"kind": "string_get"}, response(oracle.byte_value(None)), oracle.byte_value("v1"))

    def test_expected_deletion_is_valid_and_reappearance_is_refused(self):
        oracle.assert_response({"kind": "string_get"}, response(oracle.byte_value(None)), oracle.byte_value(None))
        with self.assertRaises(ValueError):
            oracle.assert_response({"kind": "string_get"}, response(oracle.byte_value("v3")), oracle.byte_value(None))

    def test_error_status_cannot_pass_with_the_expected_payload(self):
        with self.assertRaises(ValueError):
            oracle.assert_response({"kind": "string_get"}, response(oracle.byte_value("v1"), False), oracle.byte_value("v1"))

    def test_set_order_is_irrelevant_but_list_order_is_checked(self):
        oracle.assert_response({"kind": "set_members"}, response(oracle.members("z", "x")), oracle.members("x", "z"))
        with self.assertRaises(ValueError):
            oracle.assert_response({"kind": "list_range"}, response(oracle.members("e3", "e2")), oracle.members("e2", "e3"))

    def test_sorted_set_scores_and_order_are_checked(self):
        with self.assertRaises(ValueError):
            oracle.assert_response({"kind": "z_set_range"}, response(oracle.members("m1", "9")), oracle.members("m1", "1.5"))

    def test_multi_get_uses_the_json_byte_encoding(self):
        self.assertEqual(oracle.values("a", None), {"kind": "values", "values": ["YQ==", None]})

    def test_a_zero_exit_does_not_hide_partial_harness_failure(self):
        with self.assertRaises(ValueError):
            oracle.check_harness("ops 10\nops_ok 9\narrival_verified true\n", 0)

    def test_unverified_arrival_and_zero_operations_are_refused(self):
        for text in ["ops 10\nops_ok 10\narrival_verified false\n", "ops 0\nops_ok 0\narrival_verified true\n"]:
            with self.assertRaises(ValueError):
                oracle.check_harness(text, 0)

    def test_all_operations_and_arrival_must_succeed(self):
        oracle.check_harness("ops 10\nops_ok 10\narrival_verified true (read-back)\n", 0)
        with self.assertRaises(ValueError):
            oracle.check_harness("ops 10\nops_ok 10\narrival_verified true\n", 1)


if __name__ == "__main__":
    unittest.main()
