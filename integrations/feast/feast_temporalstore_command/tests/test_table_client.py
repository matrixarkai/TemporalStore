# SPDX-License-Identifier: Apache-2.0
import asyncio
import unittest
from unittest.mock import patch

try:
    import temporalstore_sdk
except ImportError:
    temporalstore_sdk = None


@unittest.skipUnless(temporalstore_sdk is not None, "the TemporalStore Python SDK is required")
class TableFeastClientTest(unittest.TestCase):
    def setUp(self):
        from feast_temporalstore_command.table_client import TableFeastClient

        self.patch = patch("feast_temporalstore_command.table_client.TableClient")
        constructor = self.patch.start()
        self.addCleanup(self.patch.stop)
        self.table = constructor.return_value
        self.client = TableFeastClient(
            "grpc://example.test", "feast", "online", "legacy", 9999, 40, 10, 1,
            4 * 1024 * 1024, 16, 2,
        )
        self.addCleanup(self.client.close)
        constructor.assert_called_once()
        self.assertEqual(constructor.call_args.args[1:3], ("feast", "online"))

    def test_distributed_table_requires_grpc_metadata(self):
        from feast_temporalstore_command.table_client import TableFeastClient

        with self.assertRaisesRegex(ValueError, "grpc:// or grpcs://"):
            TableFeastClient(
                "http://example.test", "feast", "online", "legacy", 9999, 40, 10, 1,
                4 * 1024 * 1024, 16, 2,
            )

    def test_sync_and_async_reads_preserve_missing_and_empty_binary_fields(self):
        pb = temporalstore_sdk.commands

        def serve(batch):
            command = batch[0].hash_multi_get
            self.assertEqual(list(command.fields), ["empty", "missing"])
            self.assertIn(command.key, ("key-a", "key-b"))
            return [pb.CommandResponse(
                status=pb.Status(ok=True), values=[b"", b""], values_present=bytes((1,)),
            )]

        self.table.batch_execute.side_effect = serve
        keys, fields = ["key-a", "key-b"], ["empty", "missing"]
        self.assertEqual(self.client.batch_hash_multi_get(keys, fields), [[b"", None]] * 2)
        self.assertEqual(asyncio.run(self.client.batch_hash_multi_get_async(keys, fields)),
                         [[b"", None]] * 2)

    def test_conditional_writes_preserve_ttl_and_count_skips(self):
        pb = temporalstore_sdk.commands
        self.table.batch_execute.return_value = [
            pb.CommandResponse(status=pb.Status(ok=True), integer=1),
            pb.CommandResponse(status=pb.Status(ok=True), integer=0),
        ]
        result = self.client.batch_hash_timestamp_set(
            ["value"], "_ts", [("first", 5, 2, [b""]), ("second", 3, 4, [b"abc"])],
            ttl_ms=30000,
        )
        self.assertEqual(result, (1, 1))
        first, second = [command.hash_multi_set_if_timestamp_older
                         for command in self.table.batch_execute.call_args.args[0]]
        self.assertEqual((first.key, first.timestamp_seconds, first.timestamp_nanos, first.ttl_ms),
                         ("first", 5, 2, 30000))
        self.assertEqual((first.entries[0].value, second.entries[0].value), (b"", b"abc"))

    def test_aggregate_preserves_negative_values_and_response_kind(self):
        pb = temporalstore_sdk.commands
        self.table.batch_execute.return_value = [
            pb.CommandResponse(status=pb.Status(ok=True), integer=-4)
        ]
        self.assertEqual(
            self.client.execute({
                "kind": "feature_proto_agg_query", "key": "series",
                "start_ms": 1000, "end_ms": 2000, "aggregator": "sum", "count": None,
            }),
            {"kind": "aggregate", "value": -4},
        )
        command = self.table.batch_execute.call_args.args[0][0].feature_proto_agg_query
        self.assertEqual(command.key, "series")
        self.assertFalse(command.HasField("count"))
