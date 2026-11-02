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
        self.sent = []
        self.answers = list(answers or [])

    def _post(self, path, document):  # type: ignore[override]
        self.sent.append((path, document))
        if self.answers:
            return self.answers.pop(0)
        if path == "/batch_execute":
            return {
                "status": {"ok": True, "code": "ok", "message": ""},
                "responses": [ok({"kind": "empty"})] * len(document["commands"]),
            }
        return ok({"kind": "empty"})


class ByteEncodingTest(unittest.TestCase):
    def test_a_payload_survives_a_round_trip_through_the_request_encoding(self):
        for payload in [b"", b"\x00", b"hello", bytes(range(256)), b"\n\x1b\n"]:
            with self.subTest(payload=payload):
                self.assertEqual(decode_bytes(encode_bytes(payload)), payload)

    def test_the_response_decoder_accepts_the_decimal_array_the_node_actually_sends(self):
        # CommandResponse::Values carries no base64 annotation, so a byte comes
        # back as a number. This is the shape in production today.
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

    def _install(self, answers=None):
        client = RecordingClient(answers)
        self.store._client = client
        self.store._client_signature = (
            self.config.online_store.host,
            self.config.online_store.port,
            self.config.online_store.shard_id,
            self.config.online_store.timeout_seconds,
        )
        return client

    def test_a_write_sends_one_hash_multi_set_per_row_in_one_round_trip(self):
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        client = self._install()
        self.config.online_store.skip_dedup = True
        now = self.datetime(2026, 1, 1, tzinfo=self.timezone.utc)
        rows = [
            (self._entity_key(i), {"trips": ValueProto(int64_val=i)}, now, None)
            for i in range(3)
        ]
        self.store.online_write_batch(self.config, self.view, rows, None)

        self.assertEqual(len(client.sent), 1, "a write should be ONE round trip")
        path, document = client.sent[0]
        self.assertEqual(path, "/batch_execute")
        commands = document["commands"]
        self.assertEqual(len(commands), 3)
        self.assertTrue(all(c["kind"] == "hash_multi_set" for c in commands))
        fields = dict(commands[0]["entries"])
        self.assertIn("_ts:stats", fields)
        self.assertIn("stats:trips", fields)

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
                    "responses": [ok({"kind": "empty"})],
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
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0][1]["trips"].int64_val, 42)
        # An entity that was never written must read as (None, None), not as a
        # dict of empty values.
        self.assertEqual(results[1], (None, None))

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
        self.assertEqual(kinds, ["hash_multi_set", "feature_append"])
        append = client.sent[0][1]["commands"][1]
        # FeaturePoint.value has no base64 annotation; it must go out as a list
        # of integers or the node refuses it.
        self.assertIsInstance(append["points"][0]["value"], list)

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
        set_key = commands[2]["key"]
        self.assertEqual(set_key, "fkeys:proj:stats")
        tracked = {decode_bytes(c["member"]).decode() for c in commands[2:]}
        written = {c["key"] for c in commands[:2]}
        self.assertEqual(
            tracked, written, "the tracked members must be exactly the keys written"
        )

        # Now tear down: one set_members read, then bounded batches of deletes,
        # then the tracking set dropped last and on its own.
        client = self._install(
            [
                ok(
                    {
                        "kind": "members",
                        "members": [list(k.encode()) for k in sorted(written)],
                    }
                ),
            ]
        )
        self.store.teardown(self.config, [self.view], [self.entity])
        self.assertEqual(client.sent[0][0], "/execute")
        self.assertEqual(client.sent[0][1]["command"]["kind"], "set_members")
        deleted_keys = []
        for path, document in client.sent[1:]:
            self.assertEqual(path, "/batch_execute")
            for command in document["commands"]:
                self.assertEqual(command["kind"], "common_delete")
                deleted_keys.append(command["key"])
        self.assertEqual(
            set(deleted_keys),
            written | {set_key},
            "teardown must delete every tracked key AND the tracking set",
        )
        # The set is dropped LAST: while any key it names still exists, the set
        # is the only thing that names it, so a run that failed partway can be
        # repeated. Dropping it first would strand the survivors.
        self.assertEqual(deleted_keys[-1], set_key)
        self.assertEqual(
            len(deleted_keys),
            len(written) + 1,
            "no key may be deleted twice",
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

        client = self._install(
            [
                ok(
                    {
                        "kind": "members",
                        "members": [list(k.encode()) for k in sorted(written)],
                    }
                ),
            ]
        )
        self.store.teardown(self.config, [self.view], [self.entity])

        batches = [document["commands"] for _, document in client.sent[1:]]
        self.assertTrue(batches, "teardown sent no deletes at all")
        largest = max(len(b) for b in batches)
        self.assertLessEqual(
            largest,
            16,
            f"teardown sent a {largest}-command batch against a bound of 16; "
            f"the request size is still a property of the data",
        )
        deleted = [c["key"] for b in batches for c in b]
        set_key = self.store._teardown_set_key(self.config, "stats")
        self.assertEqual(set(deleted), written | {set_key})
        self.assertEqual(len(deleted), rows + 1, "no key may be deleted twice")
        self.assertEqual(deleted[-1], set_key, "the tracking set is dropped last")

    def test_teardown_without_key_tracking_says_so_instead_of_claiming_success(self):
        client = self._install()
        with self.assertLogs("feast_temporalstore_command.online_store", level="WARNING") as logs:
            self.store.teardown(self.config, [self.view], [self.entity])
        self.assertEqual(client.sent, [])
        self.assertTrue(any("key-space scan" in line for line in logs.output))


# The suite is pinned by name in CI, and a filter that matches nothing exits 0.
# These floors are on the number of tests that actually EXECUTED -- collected
# minus skipped -- because `loadTestsFromModule` collects a class decorated with
# `skipUnless` either way, so a count of collected tests cannot tell a run with
# Feast from a run without it, and cannot tell either from a run where the whole
# Feast tier silently stopped executing.
MINIMUM_EXECUTED_WITHOUT_FEAST = 9
MINIMUM_EXECUTED_WITH_FEAST = 19


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
