#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Offline tests for the Feast online store backed by TemporalStore.

Two tiers, because the two have different dependencies:

* The **client** tier needs nothing but the standard library. It runs
  everywhere, including the open-source readiness job, and is what keeps these
  files from being a set of sources that nothing ever executes.
* The **online store** tier needs ``feast`` installed. It is skipped, loudly,
  when Feast is absent rather than silently passing.

Neither tier needs a running TemporalStore: the transport is replaced with a
recording stub, so the assertions are about the commands this integration
emits, which is the part that can be wrong in a way a live smoke test would
not notice.
"""
from __future__ import annotations

import base64
import json
import os
import sys
import unittest
from unittest import mock

try:
    import msgpack
except ModuleNotFoundError:
    msgpack = None  # type: ignore[assignment]

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from feast_temporalstore_command.client import (  # noqa: E402
    TemporalStoreClient,
    TemporalStoreCommandError,
    decode_bytes,
    encode_bytes,
)

try:
    import feast  # noqa: F401

    FEAST_AVAILABLE = True
except Exception:  # pragma: no cover - exercised only where feast is absent
    FEAST_AVAILABLE = False


def ok(response):
    return {"status": {"ok": True, "code": "ok", "message": ""}, "response": response}


class RecordingClient(TemporalStoreClient):
    """A client whose transport is a scripted list of answers."""

    def __init__(self, answers=None):
        super().__init__()
        self._binary_batch_supported = False
        self._binary_write_supported = False
        self._shared_field_read_supported = False
        self._compact_shared_field_read_supported = False
        self._shared_field_write_supported = False
        self._conditional_shared_field_write_supported = False
        self.sent = []
        self.shared_reads = []
        self.shared_writes = []
        self.answers = list(answers or [])
        self.tracking_members = None

    def batch_hash_multi_get(self, keys, fields):  # type: ignore[override]
        self.shared_reads.append((list(keys), list(fields)))
        return super().batch_hash_multi_get(keys, fields)

    def batch_hash_multi_set(  # type: ignore[override]
        self, fields, rows, request_upper_bound=None
    ):
        self.shared_writes.append((list(fields), list(rows)))
        return super().batch_hash_multi_set(
            fields, rows, request_upper_bound
        )

    def _answer(self, path, document):
        self.sent.append((path, document))
        if self.tracking_members is not None:
            if path == "/execute" and document["command"]["kind"] == "set_members":
                key = document["command"]["key"]
                return ok(
                    {
                        "kind": "members",
                        "members": [
                            list(member) for member in self.tracking_members.get(key, [])
                        ],
                    }
                )
            if path == "/batch_execute" and all(
                command["kind"] == "set_members" for command in document["commands"]
            ):
                return {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [
                        ok(
                            {
                                "kind": "members",
                                "members": [
                                    list(member)
                                    for member in self.tracking_members.get(
                                        command["key"], []
                                    )
                                ],
                            }
                        )
                        for command in document["commands"]
                    ],
                }
        if self.answers:
            return self.answers.pop(0)
        if path == "/batch_execute":
            return {
                "status": {"ok": True, "code": "ok", "message": ""},
                "responses": [ok({"kind": "empty"})] * len(document["commands"]),
            }
        if path == "/batch_write":
            return {
                "status": {"ok": True, "code": "ok", "message": ""},
                "applied": len(document["commands"]),
                "errors": [],
            }
        return ok({"kind": "empty"})

    def _post(self, path, document):  # type: ignore[override]
        return self._answer(path, document)

    def _post_encoded(self, path, body):  # type: ignore[override]
        return self._answer(path, json.loads(body))


class ByteEncodingTest(unittest.TestCase):
    def test_a_payload_survives_a_round_trip_through_the_request_encoding(self):
        for payload in [b"", b"\x00", b"hello", bytes(range(256)), b"\n\x1b\n"]:
            with self.subTest(payload=payload):
                self.assertEqual(decode_bytes(encode_bytes(payload)), payload)

    def test_the_response_decoder_accepts_older_decimal_arrays(self):
        # Nodes from before compact responses return one number per byte.
        self.assertEqual(decode_bytes([104, 101, 108, 108, 111]), b"hello")

    def test_the_response_decoder_also_accepts_base64(self):
        # The far-side decoder accepts both shapes, so a node that gains the
        # annotation later must not break this client.
        self.assertEqual(decode_bytes(base64.b64encode(b"hello").decode()), b"hello")

    def test_a_missing_field_decodes_to_none_and_not_to_empty_bytes(self):
        # Feast distinguishes "no value stored" from "a zero-length value", and
        # collapsing the two reads downstream as a present-but-null feature.
        self.assertIsNone(decode_bytes(None))
        self.assertEqual(decode_bytes([]), b"")


class ClientProtocolTest(unittest.TestCase):
    def test_a_refusal_is_raised_with_its_code_and_not_swallowed(self):
        client = RecordingClient(
            [{"status": {"ok": False, "code": "shard_not_loaded", "message": "nope"}}]
        )
        with self.assertRaises(TemporalStoreCommandError) as caught:
            client.execute({"kind": "hash_multi_get", "key": "k", "fields": ["f"]})
        self.assertEqual(caught.exception.code, "shard_not_loaded")

    def test_a_short_batch_answer_is_refused_rather_than_misaligned(self):
        # The batch is positional. Two commands answered with one response would
        # otherwise put entity B's value on entity A, which reads as a stale
        # feature rather than as an error.
        client = RecordingClient(
            [
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [ok({"kind": "values", "values": [None]})],
                }
            ]
        )
        with self.assertRaises(TemporalStoreCommandError) as caught:
            client.batch_execute(
                [
                    {"kind": "hash_multi_get", "key": "a", "fields": ["f"]},
                    {"kind": "hash_multi_get", "key": "b", "fields": ["f"]},
                ]
            )
        self.assertEqual(caught.exception.code, "batch_length_mismatch")

    def test_an_empty_batch_costs_no_round_trip(self):
        client = RecordingClient()
        self.assertEqual(client.batch_execute([]), [])
        self.assertEqual(client.sent, [])

    def test_timeout_after_sending_a_write_is_not_retried(self):
        class TimedOutConnection:
            requests = 0
            closed = False

            def request(self, *_args, **_kwargs):
                self.requests += 1

            def getresponse(self):
                raise TimeoutError("response did not arrive")

            def close(self):
                self.closed = True

        client = TemporalStoreClient()
        connection = TimedOutConnection()
        client._local.conn = connection
        with self.assertRaises(TimeoutError):
            client.batch_execute(
                [{"kind": "feature_append", "key": "history", "points": []}]
            )
        self.assertEqual(connection.requests, 1)
        self.assertTrue(connection.closed)

    def test_large_batch_is_split_without_reordering_commands(self):
        client = RecordingClient(
            [
                {"status": {"ok": True}, "responses": [ok({"index": i}) for i in indexes]}
                for indexes in ([0, 1], [2, 3], [4])
            ]
        )
        client.max_batch_commands = 2
        commands = [{"kind": "hash_multi_get", "key": str(i), "fields": ["f"]} for i in range(5)]
        self.assertEqual([answer["index"] for answer in client.batch_execute(commands)], list(range(5)))
        self.assertEqual([len(document["commands"]) for _, document in client.sent], [2, 2, 1])
        self.assertEqual(
            [command["key"] for _, document in client.sent for command in document["commands"]],
            [str(i) for i in range(5)],
        )

    def test_writes_can_use_wider_batches_than_reads(self):
        client = RecordingClient()
        client.max_batch_commands = 2
        client.max_write_batch_commands = 5
        commands = [
            {"kind": "hash_multi_set", "key": str(i), "entries": [["f", b"v"]]}
            for i in range(5)
        ]
        client.batch_write(commands)
        self.assertEqual(
            [len(document["commands"]) for _, document in client.sent], [5]
        )

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_binary_batch_keeps_long_values_as_bytes(self):
        payload = bytes(range(256)) * 16

        class BinaryClient(TemporalStoreClient):
            paths = []

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                self.paths.append((path, content_type))
                return msgpack.packb(
                    {
                        "status": {"ok": True, "code": "ok", "message": ""},
                        "responses": [
                            ok({"kind": "values", "values": [payload, None]})
                        ],
                    },
                    use_bin_type=True,
                )

        client = BinaryClient()
        answer = client.batch_execute(
            [{"kind": "hash_multi_get", "key": "a", "fields": ["f", "missing"]}]
        )[0]
        self.assertEqual(
            client.paths, [("/batch_execute_msgpack", "application/msgpack")]
        )
        self.assertEqual(decode_bytes(answer["values"][0]), payload)
        self.assertIsNone(decode_bytes(answer["values"][1]))

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_binary_batch_request_keeps_long_values_as_bytes(self):
        payload = bytes(range(256)) * 16

        class BinaryClient(TemporalStoreClient):
            documents = []

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                self.documents.append((path, content_type, msgpack.unpackb(body, raw=False)))
                return msgpack.packb(
                    {
                        "status": {"ok": True, "code": "ok", "message": ""},
                        "responses": [ok({"kind": "empty"})],
                    },
                    use_bin_type=True,
                )

        client = BinaryClient()
        client.batch_execute(
            [
                {
                    "kind": "hash_multi_set",
                    "key": "a",
                    "entries": [["sequence", encode_bytes(payload)]],
                }
            ]
        )
        path, content_type, document = client.documents[0]
        self.assertEqual(path, "/batch_execute_msgpack")
        self.assertEqual(content_type, "application/msgpack")
        self.assertEqual(document["commands"][0]["entries"][0][1], payload)

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_shared_field_batch_sends_the_projection_once(self):
        class SharedFieldClient(TemporalStoreClient):
            documents = []

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                document = msgpack.unpackb(body, raw=False)
                self.documents.append((path, content_type, document))
                return msgpack.packb(
                    {
                        "status": {"ok": True, "code": "ok", "message": ""},
                        "rows": [[key.encode()] for key in document["keys"]],
                        "errors": [],
                    },
                    use_bin_type=True,
                )

        client = SharedFieldClient()
        keys = [f"entity-{index}" for index in range(5)]
        fields = [f"feature-{index}" for index in range(64)]
        answers = client.batch_hash_multi_get(keys, fields)

        self.assertEqual(len(client.documents), 1)
        path, content_type, document = client.documents[0]
        self.assertEqual(path, "/batch_hash_multi_get_compact_msgpack")
        self.assertEqual(content_type, "application/msgpack")
        self.assertEqual(document, {"shard_id": 1, "keys": keys, "fields": fields})
        self.assertEqual(
            [decode_bytes(answer[0]).decode() for answer in answers],
            keys,
        )

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_missing_compact_read_endpoint_uses_shared_route_once(self):
        class PreviousSharedReadClient(TemporalStoreClient):
            def __init__(self):
                super().__init__()
                self.compact_attempts = 0
                self.paths = []

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                document = msgpack.unpackb(body, raw=False)
                self.paths.append(path)
                if path == "/batch_hash_multi_get_compact_msgpack":
                    self.compact_attempts += 1
                    raise TemporalStoreCommandError("http_404", "not found")
                return msgpack.packb(
                    {
                        "status": {"ok": True, "code": "ok", "message": ""},
                        "responses": [
                            ok({"kind": "values", "values": [key.encode()]})
                            for key in document["keys"]
                        ],
                    },
                    use_bin_type=True,
                )

        client = PreviousSharedReadClient()
        self.assertEqual(client.batch_hash_multi_get(["a"], ["f"]), [[b"a"]])
        self.assertEqual(client.batch_hash_multi_get(["b"], ["f"]), [[b"b"]])
        self.assertEqual(client.compact_attempts, 1)
        self.assertEqual(
            client.paths,
            [
                "/batch_hash_multi_get_compact_msgpack",
                "/batch_hash_multi_get_msgpack",
                "/batch_hash_multi_get_msgpack",
            ],
        )

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_compact_read_surfaces_an_indexed_error(self):
        class RefusedReadClient(TemporalStoreClient):
            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                return msgpack.packb(
                    {
                        "status": {"ok": True, "code": "ok", "message": ""},
                        "rows": [[b"ok"], [None]],
                        "errors": [
                            {
                                "index": 1,
                                "status": {
                                    "ok": False,
                                    "code": "shard_unavailable",
                                    "message": "not ready",
                                },
                            }
                        ],
                    },
                    use_bin_type=True,
                )

        with self.assertRaises(TemporalStoreCommandError) as caught:
            RefusedReadClient().batch_hash_multi_get(["a", "b"], ["f"])
        self.assertEqual(caught.exception.code, "shard_unavailable")

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_shared_field_batches_keep_order_across_command_boundaries(self):
        client = TemporalStoreClient(max_batch_commands=2)
        keys = [f"entity-{index}" for index in range(5)]
        fields = ["f0", "f1"]

        batches = list(client._shared_field_read_batches(keys, fields))

        self.assertEqual([len(chunk) for chunk, _body in batches], [2, 2, 1])
        documents = [msgpack.unpackb(body, raw=False) for _chunk, body in batches]
        self.assertEqual(
            [key for document in documents for key in document["keys"]], keys
        )
        self.assertTrue(all(document["fields"] == fields for document in documents))

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_bounded_shared_batches_encode_the_whole_request_once(self):
        client = TemporalStoreClient()
        fields = ["timestamp", "feature"]
        rows = [("entity", [b"stamp", b"value"])]
        timestamp_rows = [("entity", 20, 3, [b"stamp", b"value"])]

        with mock.patch(
            "feast_temporalstore_command.client.msgpack.Packer",
            wraps=msgpack.Packer,
        ) as packer:
            read_batches = list(
                client._shared_field_read_batches(["entity"], fields)
            )
            write_batches = list(
                client._shared_field_write_batches(
                    fields,
                    rows,
                    1024,
                )
            )
            timestamp_batches = list(
                client._shared_timestamp_write_batches(
                    fields,
                    "timestamp",
                    timestamp_rows,
                    1024,
                )
            )

        self.assertFalse(
            any(call.kwargs.get("autoreset") is False for call in packer.call_args_list)
        )
        self.assertEqual(len(read_batches), 1)
        self.assertEqual(len(write_batches), 1)
        self.assertEqual(len(timestamp_batches), 1)
        self.assertEqual(
            msgpack.unpackb(read_batches[0][1], raw=False),
            {"shard_id": 1, "keys": ["entity"], "fields": fields},
        )
        self.assertEqual(
            msgpack.unpackb(write_batches[0][1], raw=False)["rows"],
            [["entity", [b"stamp", b"value"]]],
        )
        self.assertEqual(
            msgpack.unpackb(timestamp_batches[0][1], raw=False)["rows"],
            [["entity", 20, 3, [b"stamp", b"value"]]],
        )

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_shared_batch_fast_path_keeps_the_encoded_byte_limit(self):
        client = TemporalStoreClient(
            max_batch_commands=4,
            max_write_batch_commands=4,
            max_batch_request_bytes=120,
        )
        keys = [f"entity-{index}-" + "k" * 36 for index in range(4)]
        rows = [(f"entity-{index}", [b"x" * 48]) for index in range(4)]

        read_batches = list(client._shared_field_read_batches(keys, ["feature"]))
        write_batches = list(
            client._shared_field_write_batches(
                ["feature"],
                rows,
                0,
            )
        )

        self.assertGreater(len(read_batches), 1)
        self.assertGreater(len(write_batches), 1)
        self.assertTrue(all(len(body) <= 120 for _chunk, body in read_batches))
        self.assertTrue(all(len(body) <= 120 for _chunk, body in write_batches))
        self.assertEqual(
            [key for chunk, _body in read_batches for key in chunk],
            keys,
        )
        self.assertEqual(
            [row[0] for chunk, _body in write_batches for row in chunk],
            [row[0] for row in rows],
        )

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_missing_shared_field_endpoint_falls_back_once(self):
        class OlderNodeClient(RecordingClient):
            attempts = 0

            def __init__(self):
                super().__init__()
                self._shared_field_read_supported = None

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                if path == "/batch_hash_multi_get_msgpack":
                    self.attempts += 1
                    raise TemporalStoreCommandError("http_404", "not found")
                return super()._request_encoded(path, body, content_type)

        client = OlderNodeClient()
        client.batch_hash_multi_get(["a"], ["f"])
        client.batch_hash_multi_get(["b"], ["f"])
        self.assertEqual(client.attempts, 1)
        self.assertEqual(
            [path for path, _document in client.sent],
            ["/batch_execute", "/batch_execute"],
        )

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_binary_batch_uses_one_streaming_packer(self):
        client = TemporalStoreClient()
        commands = [
            {"kind": "hash_multi_set", "key": str(i), "entries": [["f", b"v"]]}
            for i in range(16)
        ]

        with mock.patch(
            "feast_temporalstore_command.client.msgpack.Packer",
            wraps=msgpack.Packer,
        ) as packer:
            batches = list(client._binary_batches(commands, 6))

        self.assertEqual([len(chunk) for chunk, _body in batches], [6, 6, 4])
        streaming_packers = [
            call for call in packer.call_args_list if call.kwargs.get("autoreset") is False
        ]
        self.assertEqual(len(streaming_packers), 1)
        self.assertEqual(
            [
                command
                for _chunk, body in batches
                for command in msgpack.unpackb(body, raw=False)["commands"]
            ],
            commands,
        )

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_binary_byte_limit_keeps_greedy_command_packing(self):
        client = TemporalStoreClient(max_batch_request_bytes=180)
        commands = [
            {
                "kind": "hash_multi_set",
                "key": str(i),
                "entries": [["sequence", b"x" * 100]],
            }
            for i in range(3)
        ]

        batches = list(client._binary_batches(commands, len(commands)))

        self.assertEqual([len(chunk) for chunk, _body in batches], [1, 1, 1])
        self.assertTrue(all(len(body) <= 180 for _chunk, body in batches))
        self.assertEqual(
            [
                command["key"]
                for _chunk, body in batches
                for command in msgpack.unpackb(body, raw=False)["commands"]
            ],
            ["0", "1", "2"],
        )

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_missing_binary_batch_endpoint_falls_back_once(self):
        class OlderNodeClient(RecordingClient):
            attempts = 0

            def __init__(self):
                super().__init__()
                self._binary_batch_supported = None

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                if path == "/batch_execute_msgpack":
                    self.attempts += 1
                    raise TemporalStoreCommandError("http_404", "not found")
                return super()._request_encoded(path, body, content_type)

        client = OlderNodeClient()
        command = {"kind": "hash_multi_get", "key": "a", "fields": ["f"]}
        client.batch_execute([command])
        client.batch_execute([command])
        self.assertEqual(client.attempts, 1)
        self.assertEqual([path for path, _ in client.sent], ["/batch_execute", "/batch_execute"])

    def test_encoded_byte_limit_splits_large_feature_values(self):
        client = RecordingClient()
        client.max_batch_request_bytes = 180
        commands = [
            {"kind": "hash_multi_set", "key": str(i), "entries": [["sequence", "x" * 100]]}
            for i in range(3)
        ]
        self.assertEqual(len(client.batch_execute(commands)), 3)
        self.assertEqual([len(document["commands"]) for _, document in client.sent], [1, 1, 1])

    def test_batch_serializes_each_command_once(self):
        from unittest.mock import patch

        client = RecordingClient()
        commands = [
            {"kind": "hash_multi_set", "key": str(i), "entries": [["f", "eA=="]]}
            for i in range(4)
        ]
        real_dumps = json.dumps
        encoded_commands = 0

        def counted_dumps(value, *args, **kwargs):
            nonlocal encoded_commands
            if isinstance(value, dict) and "kind" in value:
                encoded_commands += 1
            return real_dumps(value, *args, **kwargs)

        with patch(
            "feast_temporalstore_command.client.json.dumps",
            side_effect=counted_dumps,
        ):
            client.batch_execute(commands)

        self.assertEqual(encoded_commands, len(commands))
        self.assertEqual(client.sent[0][1]["commands"], commands)

    def test_compact_write_acknowledges_the_whole_batch(self):
        client = RecordingClient()
        commands = [
            {"kind": "hash_multi_set", "key": str(i), "entries": [["f", "eA=="]]}
            for i in range(3)
        ]
        client.batch_write(commands)
        self.assertEqual(client.sent, [("/batch_write", {"shard_id": 1, "commands": commands})])

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_binary_write_request_keeps_values_as_bytes(self):
        payload = bytes(range(256)) * 16

        class BinaryWriteClient(TemporalStoreClient):
            documents = []

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                document = msgpack.unpackb(body, raw=False)
                self.documents.append((path, content_type, document))
                return msgpack.packb(
                    {
                        "status": {"ok": True, "code": "ok", "message": ""},
                        "applied": len(document["commands"]),
                        "errors": [],
                    },
                    use_bin_type=True,
                )

        client = BinaryWriteClient()
        client.batch_write(
            [
                {
                    "kind": "hash_multi_set",
                    "key": "a",
                    "entries": [["sequence", encode_bytes(payload)]],
                }
            ]
        )
        path, content_type, document = client.documents[0]
        self.assertEqual(path, "/batch_write_msgpack")
        self.assertEqual(content_type, "application/msgpack")
        self.assertEqual(document["commands"][0]["entries"][0][1], payload)

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_shared_field_write_keeps_fields_once_and_values_binary(self):
        payload = bytes(range(256)) * 4

        class SharedWriteClient(TemporalStoreClient):
            documents = []

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                document = msgpack.unpackb(body, raw=False)
                self.documents.append((path, content_type, document))
                return msgpack.packb(
                    {
                        "status": {"ok": True, "code": "ok", "message": ""},
                        "applied": len(document["rows"]),
                        "errors": [],
                    },
                    use_bin_type=True,
                )

        client = SharedWriteClient()
        client.batch_hash_multi_set(
            ["feature-a", "feature-b"],
            [("entity-a", [payload, b"small"]), ("entity-b", [b"x", payload])],
        )

        path, content_type, document = client.documents[0]
        self.assertEqual(path, "/batch_hash_multi_set_msgpack")
        self.assertEqual(content_type, "application/msgpack")
        self.assertEqual(document["fields"], ["feature-a", "feature-b"])
        self.assertEqual(document["rows"][0], ["entity-a", [payload, b"small"]])
        self.assertEqual(document["rows"][1], ["entity-b", [b"x", payload]])

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_shared_field_write_batches_keep_row_order(self):
        class SharedWriteClient(TemporalStoreClient):
            documents = []

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                document = msgpack.unpackb(body, raw=False)
                self.documents.append(document)
                return msgpack.packb(
                    {
                        "status": {"ok": True, "code": "ok", "message": ""},
                        "applied": len(document["rows"]),
                        "errors": [],
                    },
                    use_bin_type=True,
                )

        client = SharedWriteClient(max_write_batch_commands=2)
        rows = [(str(index), [bytes([index])]) for index in range(5)]
        client.batch_hash_multi_set(["f"], rows)

        self.assertEqual([len(document["rows"]) for document in client.documents], [2, 2, 1])
        self.assertEqual(
            [row[0] for document in client.documents for row in document["rows"]],
            [str(index) for index in range(5)],
        )

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_conditional_shared_write_carries_timestamp_once_per_row(self):
        class ConditionalWriteClient(TemporalStoreClient):
            documents = []

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                document = msgpack.unpackb(body, raw=False)
                self.documents.append((path, document))
                return msgpack.packb(
                    {
                        "status": {"ok": True, "code": "ok", "message": ""},
                        "applied": 1,
                        "skipped": 1,
                        "errors": [],
                    },
                    use_bin_type=True,
                )

        client = ConditionalWriteClient()
        result = client.batch_hash_timestamp_set(
            ["ts", "feature"],
            "ts",
            [
                ("a", 20, 3, [b"timestamp-a", b"one"]),
                ("b", 10, 2, [b"timestamp-b", b"two"]),
            ],
        )

        self.assertEqual(result, (1, 1))
        path, document = client.documents[0]
        self.assertEqual(path, "/batch_hash_timestamp_set_msgpack")
        self.assertEqual(document["fields"], ["ts", "feature"])
        self.assertEqual(document["timestamp_field"], "ts")
        self.assertEqual(document["rows"][0], ["a", 20, 3, [b"timestamp-a", b"one"]])

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_missing_conditional_write_endpoint_is_remembered(self):
        class OlderNodeClient(TemporalStoreClient):
            attempts = 0

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                self.attempts += 1
                raise TemporalStoreCommandError("http_404", "not found")

        client = OlderNodeClient()
        rows = [("a", 1, 0, [b"timestamp", b"value"])]
        self.assertIsNone(client.batch_hash_timestamp_set(["ts", "f"], "ts", rows))
        self.assertIsNone(client.batch_hash_timestamp_set(["ts", "f"], "ts", rows))
        self.assertEqual(client.attempts, 1)

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_missing_shared_field_write_endpoint_falls_back_once(self):
        class OlderNodeClient(RecordingClient):
            attempts = 0

            def __init__(self):
                super().__init__()
                self._shared_field_write_supported = None

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                if path == "/batch_hash_multi_set_msgpack":
                    self.attempts += 1
                    raise TemporalStoreCommandError("http_404", "not found")
                return super()._request_encoded(path, body, content_type)

        client = OlderNodeClient()
        client.batch_hash_multi_set(["f"], [("a", [b"one"])])
        client.batch_hash_multi_set(["f"], [("b", [b"two"])])

        self.assertEqual(client.attempts, 1)
        self.assertEqual([path for path, _document in client.sent], ["/batch_write", "/batch_write"])
        self.assertEqual(
            [document["commands"][0]["key"] for _path, document in client.sent],
            ["a", "b"],
        )

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_missing_binary_write_endpoint_falls_back_once(self):
        class OlderNodeClient(RecordingClient):
            attempts = 0

            def __init__(self):
                super().__init__()
                self._binary_write_supported = None

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                if path == "/batch_write_msgpack":
                    self.attempts += 1
                    raise TemporalStoreCommandError("http_404", "not found")
                return super()._request_encoded(path, body, content_type)

        client = OlderNodeClient()
        client.batch_write([{"kind": "common_delete", "key": "a"}])
        client.batch_write([{"kind": "common_delete", "key": "b"}])
        self.assertEqual(client.attempts, 1)
        self.assertEqual([path for path, _ in client.sent], ["/batch_write", "/batch_write"])

    def test_compact_write_surfaces_an_indexed_command_error(self):
        client = RecordingClient(
            [
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "applied": 1,
                    "errors": [
                        {
                            "index": 1,
                            "status": {
                                "ok": False,
                                "code": "quota_exhausted",
                                "message": "limit reached",
                            },
                        }
                    ],
                }
            ]
        )
        with self.assertRaises(TemporalStoreCommandError) as caught:
            client.batch_write(
                [
                    {"kind": "common_delete", "key": "a"},
                    {"kind": "common_delete", "key": "b"},
                ]
            )
        self.assertEqual(caught.exception.code, "quota_exhausted")
        self.assertIn("command 1", caught.exception.message)

    def test_missing_compact_write_endpoint_falls_back_once(self):
        class OlderNodeClient(RecordingClient):
            attempts = 0

            def _post_encoded(self, path, body):  # type: ignore[override]
                if path == "/batch_write":
                    self.attempts += 1
                    raise TemporalStoreCommandError("http_404", "not found")
                return super()._post_encoded(path, body)

        client = OlderNodeClient()
        client.max_batch_commands = 2
        client.max_write_batch_commands = 5
        client.batch_write(
            [{"kind": "common_delete", "key": str(i)} for i in range(5)]
        )
        client.batch_write([{"kind": "common_delete", "key": "next"}])
        self.assertEqual(client.attempts, 1)
        self.assertEqual(
            [path for path, _ in client.sent],
            ["/batch_execute"] * 4,
        )
        self.assertEqual(
            [len(document["commands"]) for _, document in client.sent],
            [2, 2, 1, 1],
        )

    def test_the_command_document_is_json_serialisable_as_sent(self):
        client = RecordingClient()
        client.execute(
            {
                "kind": "hash_multi_set",
                "key": "k",
                "entries": [("f", encode_bytes(b"v"))],
            }
        )
        path, document = client.sent[0]
        self.assertEqual(path, "/execute")
        # A tuple is fine in Python and becomes a JSON array, which is the shape
        # `Vec<(String, Vec<u8>)>` deserializes from. Asserted rather than
        # assumed, because a dict here would be accepted by json and refused by
        # the node.
        self.assertIn('[["f","dg=="]]', json.dumps(document, separators=(",", ":")))


class ConnectionLifetimeTest(unittest.TestCase):
    """`close()` must release connections opened on OTHER threads.

    This runs without a server on purpose: `http.client.HTTPConnection` does
    not connect when it is constructed, so the pooling and release logic can be
    exercised offline, which is the half that was wrong.
    """

    def test_close_releases_every_threads_connection_not_just_the_callers(self):
        import threading as _threading

        client = TemporalStoreClient(host="127.0.0.1", port=1)
        opened = []

        def open_one():
            opened.append(client._connection())

        threads = [_threading.Thread(target=open_one) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # DENOMINATOR FIRST: four distinct connections really exist, or the
        # assertion below would pass over an empty population.
        self.assertEqual(len(opened), 4)
        self.assertEqual(len({id(c) for c in opened}), 4, "one connection per thread")
        self.assertEqual(len(client._all_connections), 4)

        # The closing thread is the main thread, which opened NONE of them --
        # exactly the case a per-thread-only close cannot handle.
        client.close()
        self.assertEqual(
            len(client._all_connections),
            0,
            "close() left connections registered; a per-thread close would "
            "have released only the caller's, and under the async path the "
            "caller never owns any",
        )
        for conn in opened:
            self.assertIsNone(conn.sock, "a registered connection was not closed")

    def test_close_is_idempotent_and_safe_from_a_thread_that_never_asked(self):
        client = TemporalStoreClient(host="127.0.0.1", port=1)
        client.close()
        client.close()
        self.assertEqual(len(client._all_connections), 0)

    def test_a_thread_reopens_after_a_close(self):
        # close() clears the per-thread slots, so the next call rebuilds rather
        # than handing back a closed socket.
        client = TemporalStoreClient(host="127.0.0.1", port=1)
        first = client._connection()
        client.close()
        second = client._connection()
        self.assertIsNot(first, second)
        self.assertEqual(len(client._all_connections), 1)
        client.close()


class PackageImportTest(unittest.TestCase):
    def test_importing_the_package_does_not_pull_feast_or_pydantic_in(self):
        """The lazy ``__init__`` is only real if nothing drags Feast in eagerly.

        Asserted in a subprocess on ``sys.modules``, not by checking that an
        import succeeds: the import would also succeed in an environment that
        happens to have Feast installed, which is most of them, so success
        proves nothing about the environments that do not.
        """
        import subprocess

        package_parent = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        code = (
            "import sys;"
            "import feast_temporalstore_command as m;"
            "assert m.TemporalStoreClient is not None;"
            "assert m.decode_bytes([104]) == b'h';"
            "bad = sorted(n for n in sys.modules"
            " if n == 'feast' or n.startswith('feast.') or n == 'pydantic');"
            "print('EAGERLY_LOADED:' + ','.join(bad));"
            "sys.exit(1 if bad else 0)"
        )
        environment = dict(os.environ, PYTHONPATH=package_parent)
        completed = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=environment,
        )
        self.assertEqual(
            completed.returncode,
            0,
            "importing the package loaded Feast eagerly: "
            + completed.stdout
            + completed.stderr,
        )


@unittest.skipUnless(FEAST_AVAILABLE, "feast is not installed")
class OnlineStoreTest(unittest.TestCase):
    """The commands the online store emits for a write and for a read."""

    def setUp(self):
        from datetime import datetime, timezone

        from feast import Entity, FeatureView, Field, FileSource, RepoConfig
        from feast.types import Int64

        from feast_temporalstore_command import TemporalStoreCommandOnlineStore

        self.datetime = datetime
        self.timezone = timezone
        self.config = RepoConfig(
            project="proj",
            provider="local",
            registry="/tmp/feast-temporalstore-test-registry.db",
            online_store={"type": "feast_temporalstore_command.TemporalStoreCommandOnlineStore"},
            entity_key_serialization_version=3,
        )
        source = FileSource(name="src", path="/tmp/none.parquet", timestamp_field="ts")
        self.entity = Entity(name="driver", join_keys=["driver_id"])
        self.view = FeatureView(
            name="stats",
            entities=[self.entity],
            schema=[Field(name="trips", dtype=Int64)],
            source=source,
        )
        self.store = TemporalStoreCommandOnlineStore()

    def _entity_key(self, driver_id: int):
        from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        return EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int64_val=driver_id)]
        )

    def _install(self, answers=None, client=None):
        client = client or RecordingClient(answers)
        self.store._client = client
        self.store._client_signature = (
            self.config.online_store.host,
            self.config.online_store.port,
            self.config.online_store.shard_id,
            self.config.online_store.timeout_seconds,
            self.config.online_store.max_batch_commands,
            self.config.online_store.max_batch_request_bytes,
            self.config.online_store.max_write_batch_commands,
        )
        return client

    def _install_tracking(self, members_by_key=None, legacy_members=None):
        client = self._install()
        client.tracking_members = dict(members_by_key or {})
        if legacy_members:
            legacy_key = self.store._teardown_set_key(self.config, "stats")
            client.tracking_members[legacy_key] = list(legacy_members)
        return client

    def test_a_write_shares_fields_across_rows_in_one_round_trip(self):
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        client = self._install()
        self.config.online_store.skip_dedup = True
        now = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        rows = [
            (self._entity_key(i), {"trips": ValueProto(int64_val=i)}, now, None)
            for i in range(3)
        ]
        self.store.online_write_batch(self.config, self.view, rows, None)

        self.assertEqual(len(client.shared_writes), 1)
        shared_fields, shared_rows = client.shared_writes[0]
        self.assertEqual(shared_fields, ["_ts:stats", "stats:trips"])
        self.assertEqual([key for key, _values in shared_rows], [
            self.store._entity_hash_key(self.config, self._entity_key(i)) for i in range(3)
        ])
        self.assertEqual(len(client.sent), 1, "a write should be ONE round trip")
        path, document = client.sent[0]
        self.assertEqual(path, "/batch_write")
        commands = document["commands"]
        self.assertEqual(len(commands), 3)
        self.assertTrue(all(c["kind"] == "hash_multi_set" for c in commands))
        fields = dict(commands[0]["entries"])
        self.assertIn("_ts:stats", fields)
        self.assertIn("stats:trips", fields)

    def test_mixed_feature_layouts_keep_the_regular_write_batch(self):
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        client = self._install()
        self.config.online_store.skip_dedup = True
        now = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        rows = [
            (self._entity_key(1), {"trips": ValueProto(int64_val=1)}, now, None),
            (self._entity_key(2), {"fares": ValueProto(int64_val=2)}, now, None),
        ]

        self.store.online_write_batch(self.config, self.view, rows, None)

        self.assertEqual(client.shared_writes, [])
        self.assertEqual(len(client.sent), 1)
        self.assertEqual(
            [command["kind"] for command in client.sent[0][1]["commands"]],
            ["hash_multi_set", "hash_multi_set"],
        )

    def test_history_serializes_each_entity_and_value_once(self):
        from unittest.mock import patch

        import feast_temporalstore_command.online_store as online_store_module

        class CountingValue:
            def __init__(self, number):
                self.int64_val = number
                self.serialize_calls = 0

            def SerializeToString(self):
                self.serialize_calls += 1
                return str(self.int64_val).encode("ascii")

            def WhichOneof(self, _name):
                return "int64_val"

        client = self._install()
        self.config.online_store.skip_dedup = True
        self.config.online_store.write_feature_history = True
        values = {"trips": CountingValue(7), "fares": CountingValue(11)}
        original_serialize_entity_key = online_store_module.serialize_entity_key
        entity_serialize_calls = 0

        def count_entity_serialization(*args, **kwargs):
            nonlocal entity_serialize_calls
            entity_serialize_calls += 1
            return original_serialize_entity_key(*args, **kwargs)

        now = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        with patch.object(
            online_store_module,
            "serialize_entity_key",
            side_effect=count_entity_serialization,
        ):
            self.store.online_write_batch(
                self.config,
                self.view,
                [(self._entity_key(1), values, now, None)],
                None,
            )

        self.assertEqual(entity_serialize_calls, 1)
        self.assertTrue(all(value.serialize_calls == 1 for value in values.values()))
        self.assertEqual(client.shared_writes, [])
        self.assertEqual(len(client.sent[0][1]["commands"]), 5)

    def test_a_write_without_skip_dedup_reads_only_the_timestamp_field(self):
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        client = self._install(
            [
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [ok({"kind": "values", "values": [None]})],
                },
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "applied": 1,
                    "errors": [],
                },
            ]
        )
        self.config.online_store.skip_dedup = False
        now = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        self.store.online_write_batch(
            self.config,
            self.view,
            [(self._entity_key(1), {"trips": ValueProto(int64_val=1)}, now, None)],
            None,
        )
        self.assertEqual(len(client.sent), 2, "dedup costs one extra round trip, not N")
        read_commands = client.sent[0][1]["commands"]
        self.assertEqual(read_commands[0]["fields"], ["_ts:stats"])
        self.assertEqual(client.shared_reads[0][1], ["_ts:stats"])

    @unittest.skipIf(msgpack is None, "MessagePack codec is not installed")
    def test_default_write_uses_one_atomic_timestamp_request(self):
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        class ConditionalRecordingClient(RecordingClient):
            def __init__(self):
                super().__init__()
                self._conditional_shared_field_write_supported = None

            def _request_encoded(  # type: ignore[override]
                self, path, body, content_type="application/json"
            ):
                document = msgpack.unpackb(body, raw=False)
                self.sent.append((path, document))
                return msgpack.packb(
                    {
                        "status": {"ok": True, "code": "ok", "message": ""},
                        "applied": len(document["rows"]),
                        "skipped": 0,
                        "errors": [],
                    },
                    use_bin_type=True,
                )

        client = self._install(client=ConditionalRecordingClient())
        self.config.online_store.skip_dedup = False
        now = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        self.store.online_write_batch(
            self.config,
            self.view,
            [(self._entity_key(1), {"trips": ValueProto(int64_val=1)}, now, None)],
            None,
        )

        self.assertEqual(client.shared_reads, [])
        self.assertEqual(len(client.sent), 1)
        path, document = client.sent[0]
        self.assertEqual(path, "/batch_hash_timestamp_set_msgpack")
        self.assertEqual(document["timestamp_field"], "_ts:stats")
        self.assertEqual(len(document["rows"]), 1)

    def test_default_write_prepares_each_feature_field_once_per_batch(self):
        from unittest import mock

        from feast.protos.feast.types.Value_pb2 import Value as ValueProto
        import feast_temporalstore_command.online_store as online_store_module

        class ConditionalRowsClient(RecordingClient):
            def __init__(self):
                super().__init__()
                self.rows = None
                self.fields = None

            def batch_hash_timestamp_set(  # type: ignore[override]
                self, fields, timestamp_field, rows, request_upper_bound=None
            ):
                self.fields = list(fields)
                self.rows = list(rows)
                return len(rows), 0

        client = self._install(client=ConditionalRowsClient())
        self.config.online_store.skip_dedup = False
        now = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        rows = [
            (
                self._entity_key(index),
                {
                    "trips": ValueProto(int64_val=index),
                    "fares": ValueProto(int64_val=index * 2),
                },
                now,
                None,
            )
            for index in range(64)
        ]

        with mock.patch.object(
            online_store_module,
            "_feature_field",
            wraps=online_store_module._feature_field,
        ) as feature_field:
            self.store.online_write_batch(self.config, self.view, rows, None)

        self.assertEqual(feature_field.call_count, 2)
        self.assertEqual(client.fields, ["_ts:stats", "stats:trips", "stats:fares"])
        self.assertEqual(len(client.rows), len(rows))
        self.assertTrue(all(len(row[3]) == 3 for row in client.rows))

    def test_repeated_entity_writes_share_one_timestamp_read(self):
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        client = self._install()
        self.config.online_store.skip_dedup = False
        older = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        newer = self.datetime(2026, 2, 1, tzinfo=self.timezone.utc)
        self.store.online_write_batch(
            self.config,
            self.view,
            [
                (
                    self._entity_key(1),
                    {"trips": ValueProto(int64_val=1)},
                    older,
                    None,
                ),
                (
                    self._entity_key(1),
                    {"trips": ValueProto(int64_val=2)},
                    newer,
                    None,
                ),
            ],
            None,
        )

        read_commands = client.sent[0][1]["commands"]
        self.assertEqual(len(read_commands), 1)
        write_commands = client.sent[1][1]["commands"]
        self.assertEqual(len(write_commands), 1)
        self.assertEqual(
            dict(write_commands[0]["entries"])["stats:trips"],
            encode_bytes(ValueProto(int64_val=2).SerializeToString()),
        )

    def test_a_stale_row_is_not_written_over_a_newer_stored_value(self):
        from google.protobuf.timestamp_pb2 import Timestamp

        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        stored = Timestamp()
        stored.FromDatetime(self.datetime(2026, 6, 1, tzinfo=self.timezone.utc))
        client = self._install(
            [
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [
                        ok(
                            {
                                "kind": "values",
                                "values": [list(stored.SerializeToString())],
                            }
                        )
                    ],
                }
            ]
        )
        self.config.online_store.skip_dedup = False
        older = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        self.store.online_write_batch(
            self.config,
            self.view,
            [(self._entity_key(1), {"trips": ValueProto(int64_val=1)}, older, None)],
            None,
        )
        # The read happened; no write followed it.
        self.assertEqual(len(client.sent), 1)

    def test_two_rows_sharing_an_entity_key_keep_the_newer_one(self):
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        client = self._install()
        self.config.online_store.skip_dedup = True
        older = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        newer = self.datetime(2026, 2, 1, tzinfo=self.timezone.utc)
        key = self._entity_key(7)
        self.store.online_write_batch(
            self.config,
            self.view,
            [
                (key, {"trips": ValueProto(int64_val=1)}, newer, None),
                (key, {"trips": ValueProto(int64_val=2)}, older, None),
            ],
            None,
        )
        commands = client.sent[0][1]["commands"]
        self.assertEqual(
            len(commands), 1, "the older of two rows on one key must not be written"
        )
        self.assertEqual(
            dict(commands[0]["entries"])["stats:trips"],
            encode_bytes(ValueProto(int64_val=1).SerializeToString()),
        )

    def test_latest_only_writes_serialize_only_the_winning_row(self):
        class CountingValue:
            def __init__(self, number):
                self.number = number
                self.serialize_calls = 0

            def SerializeToString(self):
                self.serialize_calls += 1
                return str(self.number).encode("ascii")

        client = self._install()
        self.config.online_store.skip_dedup = True
        self.config.online_store.write_feature_history = False
        values = [CountingValue(number) for number in (1, 2, 3)]
        base = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        self.store.online_write_batch(
            self.config,
            self.view,
            [
                (
                    self._entity_key(1),
                    {"trips": value},
                    base.replace(month=index + 1),
                    None,
                )
                for index, value in enumerate(values)
            ],
            None,
        )

        self.assertEqual([value.serialize_calls for value in values], [0, 0, 1])
        commands = client.sent[0][1]["commands"]
        self.assertEqual(len(commands), 1)
        self.assertEqual(dict(commands[0]["entries"])["stats:trips"], encode_bytes(b"3"))

    def test_a_read_is_one_round_trip_and_answers_positionally(self):
        from google.protobuf.timestamp_pb2 import Timestamp

        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        event = Timestamp()
        event.FromDatetime(self.datetime(2026, 3, 4, tzinfo=self.timezone.utc))
        present = ok(
            {
                "kind": "values",
                "values": [
                    list(ValueProto(int64_val=42).SerializeToString()),
                    list(event.SerializeToString()),
                ],
            }
        )
        absent = ok({"kind": "values", "values": [None, None]})
        client = self._install(
            [
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [present, absent],
                }
            ]
        )
        results = self.store.online_read(
            self.config, self.view, [self._entity_key(1), self._entity_key(2)], ["trips"]
        )
        self.assertEqual(len(client.sent), 1, "a read over N entities is ONE round trip")
        self.assertEqual(client.shared_reads[0][1], ["stats:trips", "_ts:stats"])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0][1]["trips"].int64_val, 42)
        # An entity that was never written must read as (None, None), not as a
        # dict of empty values.
        self.assertEqual(results[1], (None, None))

    def test_repeated_entity_reads_share_one_hash_command(self):
        from google.protobuf.timestamp_pb2 import Timestamp

        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        event = Timestamp()
        event.FromDatetime(self.datetime(2026, 3, 4, tzinfo=self.timezone.utc))
        present = ok(
            {
                "kind": "values",
                "values": [
                    list(ValueProto(int64_val=42).SerializeToString()),
                    list(event.SerializeToString()),
                ],
            }
        )
        client = self._install(
            [
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [present],
                }
            ]
        )
        entity_key = self._entity_key(1)
        with mock.patch.object(
            self.store,
            "_decode_online_response",
            wraps=self.store._decode_online_response,
        ) as decode_online_response:
            results = self.store.online_read(
                self.config,
                self.view,
                [entity_key, self._entity_key(1), entity_key],
                ["trips"],
            )

        commands = client.sent[0][1]["commands"]
        self.assertEqual(len(commands), 1)
        self.assertEqual(
            decode_online_response.call_count,
            1,
            "one unique stored row should be decoded once",
        )
        self.assertEqual([row[1]["trips"].int64_val for row in results], [42, 42, 42])
        self.assertEqual(
            len({id(row[1]["trips"]) for row in results}),
            3,
            "repeated positions must not share mutable protobuf messages",
        )

    def test_a_short_field_answer_does_not_shift_values_onto_other_features(self):
        # A node that answers with fewer slots than fields requested must not
        # silently align `trips` to the timestamp slot.
        client = self._install(
            [
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [ok({"kind": "values", "values": []})],
                }
            ]
        )
        results = self.store.online_read(
            self.config, self.view, [self._entity_key(1)], ["trips"]
        )
        self.assertEqual(results, [(None, None)])

    def test_history_writes_are_off_by_default_and_add_appends_when_on(self):
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        now = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        row = [(self._entity_key(1), {"trips": ValueProto(int64_val=1)}, now, None)]

        self.config.online_store.skip_dedup = True
        client = self._install()
        self.store.online_write_batch(self.config, self.view, row, None)
        kinds = [c["kind"] for c in client.sent[0][1]["commands"]]
        self.assertEqual(kinds, ["hash_multi_set"])

        self.config.online_store.write_feature_history = True
        client = self._install()
        self.store.online_write_batch(self.config, self.view, row, None)
        kinds = [c["kind"] for c in client.sent[0][1]["commands"]]
        self.assertEqual(kinds, ["hash_multi_set", "feature_append", "feature_append"])
        append = client.sent[0][1]["commands"][1]
        # FeaturePoint.value has no base64 annotation; it must go out as a list
        # of integers or the node refuses it.
        self.assertIsInstance(append["points"][0]["value"], list)
        numeric = client.sent[0][1]["commands"][2]
        self.assertTrue(numeric["key"].startswith("fa:"))
        self.assertEqual(bytes(numeric["points"][0]["value"]), b"1")

    def test_repeated_entity_history_is_consolidated_without_losing_points(self):
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        self.config.online_store.skip_dedup = True
        self.config.online_store.write_feature_history = True
        key = self._entity_key(1)
        first = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        second = self.datetime(2026, 1, 1, 0, 0, 1, tzinfo=self.timezone.utc)
        client = self._install()
        self.store.online_write_batch(
            self.config,
            self.view,
            [
                (key, {"trips": ValueProto(int64_val=3)}, first, None),
                (key, {"trips": ValueProto(int64_val=5)}, second, None),
            ],
            None,
        )
        commands = client.sent[0][1]["commands"]
        self.assertEqual(
            [command["kind"] for command in commands],
            ["hash_multi_set", "feature_append", "feature_append"],
        )
        self.assertEqual(
            dict(commands[0]["entries"])["stats:trips"],
            encode_bytes(ValueProto(int64_val=5).SerializeToString()),
        )
        self.assertEqual(len(commands[1]["points"]), 2)
        self.assertEqual(
            [bytes(point["value"]) for point in commands[2]["points"]],
            [b"3", b"5"],
        )

    def test_windowed_aggregate_uses_numeric_series_and_count_uses_raw_history(self):
        self.config.online_store.write_feature_history = True
        client = self._install(
            [
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [ok({"kind": "aggregate", "value": 13})],
                },
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [ok({"kind": "aggregate", "value": 3})],
                },
            ]
        )
        key = self._entity_key(1)
        self.assertEqual(
            self.store.query_feature_aggregate(
                self.config, self.view, key, "trips", 1, 100, "sum"
            ),
            13,
        )
        self.assertEqual(
            self.store.query_feature_aggregate(
                self.config, self.view, key, "trips", 1, 100, "count"
            ),
            3,
        )
        self.assertTrue(client.sent[0][1]["commands"][0]["key"].startswith("fa:"))
        self.assertTrue(client.sent[1][1]["commands"][0]["key"].startswith("fh:"))

    def test_many_windowed_aggregates_share_one_bounded_request(self):
        self.config.online_store.write_feature_history = True
        client = self._install(
            [
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [
                        ok({"kind": "aggregate", "value": 13}),
                        ok({"kind": "aggregate", "value": 3}),
                    ],
                }
            ]
        )
        key = self._entity_key(1)
        values = self.store.query_feature_aggregates(
            self.config,
            self.view,
            [
                (key, "trips", 1, 100, "sum", None),
                (key, "trips", 1, 100, "count", None),
            ],
        )
        self.assertEqual(values, [13, 3])
        self.assertEqual(len(client.sent), 1)
        path, document = client.sent[0]
        self.assertEqual(path, "/batch_execute")
        self.assertEqual(len(document["commands"]), 2)
        self.assertTrue(document["commands"][0]["key"].startswith("fa:"))
        self.assertTrue(document["commands"][1]["key"].startswith("fh:"))

    def test_repeated_windowed_aggregates_share_one_command(self):
        self.config.online_store.write_feature_history = True
        client = self._install(
            [
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [ok({"kind": "aggregate", "value": 13})],
                }
            ]
        )
        key = self._entity_key(1)
        query = (key, "trips", 1, 100, "sum", None)

        values = self.store.query_feature_aggregates(
            self.config,
            self.view,
            [query, query, query],
        )

        self.assertEqual(values, [13, 13, 13])
        self.assertEqual(len(client.sent), 1)
        self.assertEqual(len(client.sent[0][1]["commands"]), 1)

    def test_list_history_keeps_original_value_without_numeric_projection(self):
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        self.config.online_store.skip_dedup = True
        self.config.online_store.write_feature_history = True
        client = self._install()
        sequence = ValueProto()
        sequence.int64_list_val.val.extend(range(4096))
        row = [(self._entity_key(1), {"sequence": sequence}, self.datetime.now(self.timezone.utc), None)]
        self.store.online_write_batch(self.config, self.view, row, None)
        commands = client.sent[0][1]["commands"]
        self.assertEqual([command["kind"] for command in commands], ["hash_multi_set", "feature_append"])
        self.assertEqual(bytes(commands[1]["points"][0]["value"]), sequence.SerializeToString())

    def test_key_tracking_records_each_written_key_and_teardown_deletes_them(self):
        """The opt-in teardown path, which is the only one that deletes anything.

        Covered because a user can switch it on: an untested branch behind a
        config flag is shipped code that nothing has ever executed.
        """
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        self.config.online_store.skip_dedup = True
        self.config.online_store.track_keys_for_teardown = True
        now = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        client = self._install()
        self.store.online_write_batch(
            self.config,
            self.view,
            [
                (self._entity_key(i), {"trips": ValueProto(int64_val=i)}, now, None)
                for i in range(2)
            ],
            None,
        )
        commands = client.sent[0][1]["commands"]
        kinds = [c["kind"] for c in commands]
        self.assertEqual(kinds, ["hash_multi_set", "hash_multi_set", "set_add", "set_add"])
        tracked = {decode_bytes(c["member"]).decode() for c in commands[2:]}
        written = {c["key"] for c in commands[:2]}
        self.assertEqual(
            tracked, written, "the tracked members must be exactly the keys written"
        )
        for command in commands[2:]:
            member = decode_bytes(command["member"]).decode()
            self.assertEqual(
                command["key"],
                self.store._teardown_shard_key(self.config, "stats", member),
            )

        members_by_key = {}
        for command in commands[2:]:
            members_by_key.setdefault(command["key"], []).append(
                decode_bytes(command["member"])
            )
        # Now tear down: one legacy read plus bounded shard reads, then bounded
        # batches of field deletes and the occupied tracking sets dropped last.
        client = self._install_tracking(members_by_key)
        self.store.teardown(self.config, [self.view], [self.entity])
        self.assertEqual(client.sent[0][0], "/execute")
        self.assertEqual(client.sent[0][1]["command"]["kind"], "set_members")
        field_deletes, key_deletes = [], []
        for path, document in client.sent[1:]:
            self.assertEqual(path, "/batch_execute")
            for command in document["commands"]:
                if command["kind"] == "hash_delete":
                    field_deletes.append((command["key"], command["field"]))
                elif command["kind"] == "common_delete":
                    key_deletes.append(command["key"])
                elif command["kind"] != "set_members":
                    self.fail(f"unexpected command {command['kind']}")

        # THE ROWS ARE NOT DELETED, THEIR FIELDS ARE. The row is shared by every
        # view in the project, so dropping it would take the neighbours' features
        # with it.
        self.assertEqual(
            {key for key, _ in field_deletes},
            written,
            "every tracked row must have its fields removed",
        )
        self.assertEqual(
            {field for _, field in field_deletes},
            {"stats:trips", "_ts:stats"},
            "exactly this view's feature fields and its timestamp field",
        )
        # The tracking set is the ONLY thing deleted as a whole key, and it is
        # dropped LAST: while any row it names still holds this view's fields,
        # the set is the only thing that names them, so a run that failed
        # partway can be repeated.
        self.assertEqual(set(key_deletes), set(members_by_key))
        last_path, last_document = client.sent[-1]
        self.assertTrue(
            all(command["kind"] == "common_delete" for command in last_document["commands"])
        )
        self.assertEqual(
            len(field_deletes),
            len(written) * 2,
            "one delete per (row, field), no field twice",
        )

    def test_tearing_down_one_view_leaves_another_views_fields_on_a_shared_row(self):
        """FEAST ISSUE #2150, WHICH THIS LAYOUT IS EXPOSED TO BY CONSTRUCTION.

        There is one hash per (project, entity key) and EVERY feature view in
        the project writes into it -- the view appears only in the field names.
        So a teardown that deleted the row would delete every other view's
        features for that entity, which is the bug Feast's own
        `test_online_store_cleanup` exists to catch. That test is gated
        `universal_online_stores(only=["redis"])`, so it never ran against this
        store; this is the local equivalent.

        Asserted on the FIELD NAMES in the emitted commands, because that is
        where the view lives -- a row-level assertion cannot tell the two views
        apart at all.
        """
        from feast import Field, FeatureView
        from feast.types import Int64

        other = FeatureView(
            name="other_stats",
            entities=[self.entity],
            schema=[Field(name="miles", dtype=Int64)],
            source=self.view.batch_source,
        )

        self.config.online_store.skip_dedup = True
        self.config.online_store.track_keys_for_teardown = True
        written_key = "somerow:proj"
        client = self._install(
            [
                ok({"kind": "members", "members": [list(written_key.encode())]}),
            ]
        )
        self.store.teardown(self.config, [self.view], [self.entity])

        touched = []
        for _path, document in client.sent[1:]:
            touched.extend(document["commands"])

        # DENOMINATOR: it really did emit deletes for the one tracked row.
        self.assertTrue(touched, "teardown emitted nothing")
        self.assertNotIn(
            "common_delete",
            [c["kind"] for c in touched if c.get("key") == written_key],
            "the shared row must NOT be deleted as a whole key -- that is the "
            "defect: it would take every other view's fields with it",
        )
        fields = {c["field"] for c in touched if c["kind"] == "hash_delete"}
        self.assertEqual(fields, {"stats:trips", "_ts:stats"})
        for field in fields:
            self.assertFalse(
                field.startswith(f"{other.name}:") or field == f"_ts:{other.name}",
                f"teardown of 'stats' touched a field belonging to "
                f"'{other.name}': {field}",
            )

    def test_teardown_bounds_its_batches_instead_of_sending_one_per_key_count(self):
        """THE REQUEST SIZE MUST NOT BE A PROPERTY OF THE DATA.

        Found against a live node, not here: with the tracking set holding
        5,000 keys, teardown built a single 5,001-command `/batch_execute` and
        the call timed out. The test that was supposed to cover this path used
        TWO keys, so the unbounded batch and a bounded one were the same
        request and it could not tell them apart -- a guard whose population is
        smaller than the bound it checks is not checking the bound.

        So this one uses a population LARGER than the batch size and asserts on
        the biggest request actually sent.
        """
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        self.config.online_store.skip_dedup = True
        self.config.online_store.track_keys_for_teardown = True
        self.config.online_store.teardown_batch_size = 16
        rows = 100  # > 16, so the bound has to chunk
        now = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)

        client = self._install()
        self.store.online_write_batch(
            self.config,
            self.view,
            [
                (self._entity_key(i), {"trips": ValueProto(int64_val=i)}, now, None)
                for i in range(rows)
            ],
            None,
        )
        written = {
            c["key"] for c in client.sent[0][1]["commands"] if c["kind"] == "hash_multi_set"
        }
        self.assertEqual(len(written), rows, "the fixture must really hold 100 keys")

        tracking_commands = [
            command
            for command in client.sent[0][1]["commands"]
            if command["kind"] == "set_add"
        ]
        members_by_key = {}
        for command in tracking_commands:
            members_by_key.setdefault(command["key"], []).append(
                decode_bytes(command["member"])
            )
        client = self._install_tracking(members_by_key)
        self.store.teardown(self.config, [self.view], [self.entity])

        batches = [
            [command for command in document["commands"] if command["kind"] != "set_members"]
            for _, document in client.sent[1:]
        ]
        batches = [batch for batch in batches if batch]
        self.assertTrue(batches, "teardown sent no deletes at all")
        largest = max(len(b) for b in batches)
        self.assertLessEqual(
            largest,
            16,
            f"teardown sent a {largest}-command batch against a bound of 16; "
            f"the request size is still a property of the data",
        )
        commands = [c for b in batches for c in b]
        field_deletes = [c for c in commands if c["kind"] == "hash_delete"]
        key_deletes = [c for c in commands if c["kind"] == "common_delete"]
        # One hash_delete per (row, field): this view has one feature plus its
        # timestamp field, so two per row. The bound still has to hold over a
        # command count that is now a MULTIPLE of the row count, which is the
        # harder case for it.
        self.assertEqual(len(field_deletes), rows * 2)
        self.assertEqual({c["key"] for c in field_deletes}, written)
        self.assertEqual({c["key"] for c in key_deletes}, set(members_by_key))
        self.assertTrue(all(c["kind"] == "common_delete" for c in batches[-1]))

    def test_the_async_read_answers_and_emits_the_same_one_round_trip(self):
        """Feast's async serving path calls this, and the base class raises.

        So a store with only the four abstract methods fails outright on
        `get_online_features_async`. Asserted on the commands emitted, not just
        on a value coming back, because the async path must not become a second
        request builder that can drift from the sync one.
        """
        import asyncio

        from google.protobuf.timestamp_pb2 import Timestamp

        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        event = Timestamp()
        event.FromDatetime(self.datetime(2026, 3, 4, tzinfo=self.timezone.utc))
        client = self._install(
            [
                {
                    "status": {"ok": True, "code": "ok", "message": ""},
                    "responses": [
                        ok(
                            {
                                "kind": "values",
                                "values": [
                                    list(ValueProto(int64_val=42).SerializeToString()),
                                    list(event.SerializeToString()),
                                ],
                            }
                        )
                    ],
                }
            ]
        )
        results = asyncio.run(
            self.store.online_read_async(
                self.config, self.view, [self._entity_key(1)], ["trips"]
            )
        )
        self.assertEqual(len(client.sent), 1, "an async read is ONE round trip too")
        self.assertEqual(client.sent[0][0], "/batch_execute")
        self.assertEqual(results[0][1]["trips"].int64_val, 42)

    def test_the_async_write_emits_the_same_commands_as_the_sync_write(self):
        import asyncio

        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        now = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        rows = [(self._entity_key(1), {"trips": ValueProto(int64_val=1)}, now, None)]
        self.config.online_store.skip_dedup = True

        sync_client = self._install()
        self.store.online_write_batch(self.config, self.view, rows, None)
        sync_sent = sync_client.sent

        async_client = self._install()
        asyncio.run(
            self.store.online_write_batch_async(self.config, self.view, rows, None)
        )
        # Byte-for-byte the same request, because the async method IS the sync
        # path on a worker thread rather than a second encoder.
        self.assertEqual(
            json.dumps(async_client.sent, sort_keys=True, default=list),
            json.dumps(sync_sent, sort_keys=True, default=list),
        )

    def test_closing_the_store_releases_the_client(self):
        self._install()
        self.assertIsNotNone(self.store._client)
        self.store.close()
        self.assertIsNone(
            self.store._client,
            "close() must drop the client; the inherited close is `pass`, "
            "which leaks a keep-alive connection per thread",
        )

    def test_teardown_without_key_tracking_says_so_instead_of_claiming_success(self):
        client = self._install()
        with self.assertLogs("feast_temporalstore_command.online_store", level="WARNING") as logs:
            self.store.teardown(self.config, [self.view], [self.entity])
        self.assertEqual(client.sent, [])
        self.assertTrue(any("key-space scan" in line for line in logs.output))


@unittest.skipUnless(FEAST_AVAILABLE, "feast is not installed")
class AdvertisedCapabilityTest(unittest.TestCase):
    """What the store TELLS Feast it can do, against what it actually implements.

    Feast does not discover an optional capability by probing for the method. It
    reads `async_supported` and branches: `feature_server` takes the async read
    path only when `.online.read` is true and the async write path only when
    `.online.write` is true. So an implemented-but-unadvertised method is never
    called, and an advertised-but-unimplemented one sends Feast into a base method
    that raises. Both failures are silent in ordinary testing -- the first looks
    like working code, the second surfaces only under async serving -- so the
    agreement between the two is what these tests pin.
    """

    def _store(self):
        from feast_temporalstore_command import TemporalStoreCommandOnlineStore

        return TemporalStoreCommandOnlineStore()

    def test_the_store_advertises_the_async_support_it_implements(self):
        advertised = self._store().async_supported
        self.assertTrue(
            advertised.read,
            "async reads are implemented but not advertised, so Feast's serving "
            "path will never call them",
        )
        self.assertTrue(
            advertised.write,
            "async writes are implemented but not advertised, so Feast's serving "
            "path will never call them",
        )

    def test_the_advertisement_matches_what_the_class_implements(self):
        from feast.infra.online_stores.online_store import OnlineStore
        from feast_temporalstore_command import TemporalStoreCommandOnlineStore

        advertised = self._store().async_supported
        pairs = (("read", "online_read_async"), ("write", "online_write_batch_async"))
        for flag, method in pairs:
            overridden = getattr(
                TemporalStoreCommandOnlineStore, method
            ) is not getattr(OnlineStore, method)
            self.assertEqual(
                getattr(advertised, flag),
                overridden,
                "async_supported.{flag} is {adv} while {method} is {state} on this "
                "class. Advertising what is not implemented sends Feast into a "
                "raising base method; implementing what is not advertised leaves the "
                "work unreachable.".format(
                    flag=flag,
                    adv=getattr(advertised, flag),
                    method=method,
                    state="overridden" if overridden else "inherited",
                ),
            )

    def test_vector_retrieval_is_refused_by_this_store_and_not_by_the_base_class(self):
        from feast.infra.online_stores.online_store import OnlineStore
        from feast_temporalstore_command import TemporalStoreCommandOnlineStore

        store = self._store()
        for name in ("retrieve_online_documents", "retrieve_online_documents_v2"):
            self.assertIsNot(
                getattr(TemporalStoreCommandOnlineStore, name),
                getattr(OnlineStore, name),
                "{} must be refused here, so the message names this store and the "
                "reason, rather than inherited so it names an abstract "
                "method".format(name),
            )
        with self.assertRaises(NotImplementedError) as caught:
            store.retrieve_online_documents(None, None, [], [0.0], 1)
        self.assertIn("no embedding index", str(caught.exception))
        with self.assertRaises(NotImplementedError) as caught:
            store.retrieve_online_documents_v2(None, None, [], [0.0], 1)
        self.assertIn("no embedding index", str(caught.exception))

# The suite is pinned by name in CI, and a filter that matches nothing exits 0.
# These floors are on the number of tests that actually EXECUTED -- collected
# minus skipped -- because `loadTestsFromModule` collects a class decorated with
# `skipUnless` either way, so a count of collected tests cannot tell a run with
# Feast from a run without it, and cannot tell either from a run where the whole
# Feast tier silently stopped executing.
MINIMUM_EXECUTED_WITHOUT_FEAST = 12
MINIMUM_EXECUTED_WITH_FEAST = 29


def main() -> int:
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    expected = (
        MINIMUM_EXECUTED_WITH_FEAST
        if FEAST_AVAILABLE
        else MINIMUM_EXECUTED_WITHOUT_FEAST
    )
    collected = suite.countTestCases()
    if collected == 0:
        print("FAIL: collected 0 tests", file=sys.stderr)
        return 1
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    executed = result.testsRun - len(result.skipped)
    print(
        f"collected {collected}, executed {executed}, skipped {len(result.skipped)} "
        f"(feast_available={FEAST_AVAILABLE}, floor {expected})"
    )
    if executed < expected:
        print(
            f"FAIL: executed {executed} tests, expected at least {expected}",
            file=sys.stderr,
        )
        return 1
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
