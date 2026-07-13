#!/usr/bin/env python3
"""Tests for shared raw-message storage target contract."""

from __future__ import annotations

import unittest

from matrixark_raw_message_storage_contract import (
    RawMessageStorageTarget,
    contract_report,
    generic_object_store_contract,
    normalize_raw_backend,
    raw_message_marker,
    raw_message_payload_sha256,
    raw_message_payload_size_bytes,
    raw_message_should_spill_to_object_store,
    raw_message_time_ms,
    raw_message_timeline_key,
    raw_message_value,
)


class RawMessageStorageContractTest(unittest.TestCase):
    def test_default_target_is_temporalstore(self) -> None:
        message = {
            "event_time_ms": 1781777200000,
            "ingestion_time_ms": 1781777200007,
            "body": "raw body only",
        }
        report = contract_report(message, event_id_hash=42)
        self.assertEqual(report["default_backend"], "temporalstore")
        self.assertEqual(report["target"]["backend"], "temporalstore")
        self.assertEqual(report["timestamp_key_ms"], 1781777200000)
        self.assertEqual(report["stored_value"], "raw body only")
        self.assertEqual(report["stored_value_mode"], "raw_body_utf8")
        self.assertEqual(report["event_key_hash"], 42)
        self.assertEqual(report["timeline_key"], raw_message_timeline_key(1781777200000, 42))
        self.assertTrue(report["uses_timestamp_and_event_key"])
        self.assertTrue(report["cold_storage"])
        self.assertEqual(report["write_policy"], "ColdStoreOnly")
        self.assertEqual(report["cache_policy"], "NoCachePromotion")
        self.assertEqual(report["promotion_policy"], "NoPromotion")
        self.assertEqual(report["metadata_backend"], "temporalstore")
        self.assertTrue(report["metadata_persisted_in_temporalstore"])

    def test_matrixkv_target_resolves_storage_object_key(self) -> None:
        target = RawMessageStorageTarget.matrixkv("user:deeproute", "raw_agent_messages", "codex/raw-1")
        resolved = target.resolve(tenant_hash=99, node_hash=9903, event_time_ms=1781777200000, event_id_hash=7)
        self.assertEqual(resolved.backend, "matrixkv")
        self.assertEqual(resolved.object_key(), "matrixkv:user:deeproute:raw_agent_messages:codex/raw-1")

    def test_message_body_beats_json_envelope(self) -> None:
        message = {
            "event_time_ms": 1781777200000,
            "body": "only this value is persisted as raw payload",
            "metadata": {"debug": "not part of raw value"},
        }
        self.assertEqual(raw_message_time_ms(message), 1781777200000)
        self.assertEqual(raw_message_value(message), "only this value is persisted as raw payload")

    def test_matrixkv_marker_names_body_encoding_and_timestamp_key(self) -> None:
        message = {"event_time_ms": 1781777200000, "body": "matrixkv body"}
        marker = raw_message_marker(
            message,
            target=RawMessageStorageTarget.matrixkv("user", "raw", "k1"),
            event_id_hash=42,
        )
        self.assertEqual(marker["schema"], "matrixark.context.raw_agent_message_ref.v1")
        self.assertEqual(marker["backend"], "matrixkv")
        self.assertEqual(marker["timestamp_key_ms"], 1781777200000)
        self.assertEqual(marker["event_key_hash"], 42)
        self.assertEqual(marker["timeline_key"], raw_message_timeline_key(1781777200000, 42))
        self.assertEqual(marker["value_encoding"], "raw_body_utf8")
        self.assertEqual(marker["object_key"], "matrixkv:user:raw:k1")
        self.assertEqual(marker["metadata_backend"], "matrixkv")
        self.assertEqual(marker["metadata_object_key"], "matrixkv:user:raw:k1")

    def test_same_timestamp_uses_event_key_for_unique_timeline_key(self) -> None:
        timestamp = 1781777200000
        self.assertNotEqual(
            raw_message_timeline_key(timestamp, 41),
            raw_message_timeline_key(timestamp, 42),
        )


    def test_large_payload_spills_from_kv_to_object_store_ref(self) -> None:
        message = {"event_time_ms": 1781777200000, "body": "x" * 128}
        target = RawMessageStorageTarget(backend="temporalstore", options={"max_inline_bytes": "16"})
        report = contract_report(message, target, event_id_hash=7)
        self.assertEqual(raw_message_payload_size_bytes(message), 128)
        self.assertTrue(raw_message_should_spill_to_object_store(message, target))
        self.assertFalse(report["inline_payload"])
        self.assertTrue(report["spilled_to_object_store"])
        self.assertEqual(report["stored_value_mode"], "object_ref_json")
        self.assertEqual(report["object_ref"]["backend"], "objectstore")
        self.assertEqual(report["metadata_backend"], "temporalstore")
        self.assertTrue(report["metadata_persisted_in_temporalstore"])
        self.assertEqual(report["object_ref"]["metadata_backend"], "temporalstore")
        self.assertEqual(report["object_store_name"], "MatrixObject")
        self.assertIn("matrixobject://matrixark/raw-agent-messages/", report["object_ref"]["object_key"])
        self.assertEqual(report["payload_sha256"], raw_message_payload_sha256(message))
        self.assertEqual(report["object_ref"]["payload_sha256"], report["payload_sha256"])
        self.assertEqual(report["payload_size_bytes"], 128)
        self.assertEqual(report["max_inline_bytes"], 16)

    def test_explicit_s3_target_always_uses_object_ref(self) -> None:
        message = {"event_time_ms": 1781777200000, "body": "small body"}
        target = RawMessageStorageTarget.s3(bucket="matrixark-large-resources", prefix="raw")
        marker = raw_message_marker(message, target=target, event_id_hash=9)
        self.assertEqual(marker["backend"], "s3")
        self.assertTrue(marker["spilled_to_object_store"])
        self.assertFalse(marker["inline_payload"])
        self.assertEqual(marker["value_encoding"], "object_ref_json")
        self.assertTrue(marker["object_key"].startswith("s3://matrixark-large-resources/raw/"))
        report = contract_report(message, target, event_id_hash=9)
        self.assertEqual(report["target"]["backend"], "s3")
        self.assertEqual(report["stored_value_mode"], "object_ref_json")
        self.assertEqual(report["metadata_backend"], "temporalstore")
        self.assertTrue(report["metadata_persisted_in_temporalstore"])
        self.assertEqual(report["metadata_target"]["backend"], "temporalstore")
        self.assertEqual(report["metadata_target"]["table"], "context_raw_agent_messages")
        self.assertEqual(report["object_store_contract"]["backend"], "s3")
        self.assertEqual(report["object_store_contract"]["provider_name"], "S3")
        self.assertIn("get_range", report["object_store_contract"]["required_operations"])
        self.assertIn("list_page", report["object_store_contract"]["required_operations"])
        self.assertIn("byte_range_read", report["object_store_contract"]["required_capabilities"])

    def test_object_store_backend_aliases_are_supported(self) -> None:
        self.assertEqual(normalize_raw_backend("object_store"), "objectstore")
        self.assertEqual(normalize_raw_backend("matrixobject"), "objectstore")
        self.assertEqual(normalize_raw_backend("matrixobjectstore"), "objectstore")
        self.assertEqual(normalize_raw_backend("blob"), "objectstore")
        self.assertEqual(normalize_raw_backend("matrix_object_store"), "objectstore")
        self.assertEqual(normalize_raw_backend("aws_s3"), "s3")

    def test_generic_object_store_contract_matches_matrixobject_and_s3_adapter_shape(self) -> None:
        matrix_contract = generic_object_store_contract(RawMessageStorageTarget(backend="matrixobjectstore"))
        self.assertEqual(matrix_contract["backend"], "objectstore")
        self.assertEqual(matrix_contract["provider_name"], "MatrixObject")
        self.assertEqual(matrix_contract["canonical_uri_schemes"], ["matrixobject"])
        self.assertIn("matrixobjectstore", matrix_contract["legacy_uri_schemes"])
        self.assertIn("blob", matrix_contract["legacy_uri_schemes"])
        self.assertEqual(matrix_contract["selection_rule"], "choose_by_uri_scheme_then_capabilities")
        self.assertEqual(matrix_contract["remote_backend_behavior"], "fail_closed_until_linked")
        for operation in (
            "put_if_absent",
            "put_path_unique",
            "get_range",
            "get_to_path",
            "list_page",
            "delete_objects",
            "delete_prefix",
            "copy_object",
            "capabilities",
            "topology",
        ):
            self.assertIn(operation, matrix_contract["required_operations"])
        for capability in (
            "conditional_create",
            "paginated_list",
            "bulk_delete",
            "byte_range_read",
            "opaque_object_validators",
            "split_services",
        ):
            self.assertIn(capability, matrix_contract["required_capabilities"])

        s3_contract = generic_object_store_contract(RawMessageStorageTarget(backend="aws_s3"))
        self.assertEqual(s3_contract["backend"], "s3")
        self.assertEqual(s3_contract["provider_name"], "S3")
        self.assertEqual(s3_contract["canonical_uri_schemes"], ["s3"])
        self.assertEqual(s3_contract["required_operations"], matrix_contract["required_operations"])

    def test_backend_aliases_match_rust_api_names(self) -> None:
        self.assertEqual(normalize_raw_backend("matrix_kv"), "matrixkv")
        self.assertEqual(normalize_raw_backend("temporal_store"), "temporalstore")
        with self.assertRaises(ValueError):
            normalize_raw_backend("unknown")


if __name__ == "__main__":
    unittest.main()
