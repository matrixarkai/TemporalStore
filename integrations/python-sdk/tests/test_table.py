# SPDX-License-Identifier: Apache-2.0
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import ExitStack
from threading import Event
import unittest
from unittest.mock import Mock, patch

import grpc
from temporalstore.v1.temporalstore_pb2_grpc import (
    TemporalStoreServiceServicer, add_TemporalStoreServiceServicer_to_server,
)

from temporalstore_sdk import MetadataError, Shard, TableClient, TableTopology, commands


def get(key):
    return commands.Command(string_get=commands.StringGet(key=key))


class TableClientTest(unittest.TestCase):
    def test_single_shard_insert_keeps_batch_error_for_opted_in_caller(self):
        received = []
        command = commands.Command(string_set_conditional=commands.StringSetConditional(
            key="chunk", value=b"\x00\xff",
            condition=commands.STRING_SET_CONDITION_IF_NOT_EXISTS,
        ))

        class Datashard(TemporalStoreServiceServicer):
            def BatchExecuteChecked(self, request, context):
                received.append((request.shard_id, request.load_version, list(request.commands)))
                return commands.BatchExecuteResponse(
                    status=commands.Status(code="raft_error", message="leader changed"),
                )

        with ExitStack() as stack:
            worker = stack.enter_context(ThreadPoolExecutor(max_workers=1))
            server = grpc.server(worker)
            add_TemporalStoreServiceServicer_to_server(Datashard(), server)
            port = server.add_insecure_port("127.0.0.1:0")
            server.start()
            stack.callback(lambda: server.stop(0).wait(timeout=3))
            metadata = Mock()
            metadata.table_topology.return_value = TableTopology("kvcache", "model", 1, 73, 1, (
                Shard(73, None, (), 7, f"127.0.0.1:{port}"),
            ))
            opted_in = stack.enter_context(TableClient(
                metadata, "kvcache", "model", timeout_seconds=3, preserve_insert_outcomes=True,
            ))
            [(index, response)] = opted_in.iter_batch_responses([command])
            self.assertEqual((index, response.status.code), (0, "raft_error"))
            self.assertEqual(received, [(73, 7, [command])])
            default = stack.enter_context(TableClient(metadata, "kvcache", "model", timeout_seconds=3))
            with self.assertRaises(MetadataError) as error:
                default.batch_execute([command])
            self.assertEqual(error.exception.code, "raft_error")
            self.assertEqual(received, [(73, 7, [command]), (73, 7, [command])])

    def test_server_deadline_keeps_the_insert_outcome_without_retrying(self):
        class Cancelled(grpc.RpcError):
            def __init__(self, detail):
                self.detail = detail

            def code(self):
                return grpc.StatusCode.CANCELLED

            def details(self):
                return self.detail

        metadata = Mock()
        command = commands.Command(string_set_conditional=commands.StringSetConditional(
            key="chunk", value=b"payload", condition=commands.STRING_SET_CONDITION_IF_NOT_EXISTS,
        ))
        for uses_raft, expected in (
            (True, "write_outcome_unknown"), (False, "write_durability_unconfirmed"),
        ):
            metadata.table_topology.return_value = TableTopology("kvcache", "model", 1, 73, 1, (
                Shard(73, None, (), 7, "primary-a", uses_raft_route=uses_raft),
            ))
            for detail, recoverable in (
                ("Timeout expired", True), ("Timeout%20expired", True), ("caller cancelled", False),
            ):
                with self.subTest(detail=detail, uses_raft=uses_raft), patch(
                    "temporalstore_sdk.table.CommandClient",
                ) as channel:
                    channel.return_value.batch_execute.side_effect = Cancelled(detail)
                    with TableClient(metadata, "kvcache", "model", preserve_insert_outcomes=True) as table:
                        if recoverable:
                            [reply] = table.batch_execute([command])
                            self.assertEqual(reply.status.code, expected)
                        else:
                            with self.assertRaises(Cancelled):
                                table.batch_execute([command])
                    channel.return_value.batch_execute.assert_called_once()

    def test_lost_insert_reply_preserves_the_other_shards_acknowledgment(self):
        calls = []

        class Datashard(TemporalStoreServiceServicer):
            def BatchExecuteChecked(self, request, context):
                payload = request.commands[0].string_set_conditional.value
                calls.append((request.shard_id, request.load_version, payload))
                if request.shard_id == 74:
                    context.abort(grpc.StatusCode.UNAVAILABLE, "insert reply lost")
                return commands.BatchExecuteResponse(
                    status=commands.Status(ok=True),
                    responses=[commands.CommandResponse(status=commands.Status(ok=True), count=1)],
                )

        first, second = [
            commands.Command(string_set_conditional=commands.StringSetConditional(
                key=key, value=value, condition=commands.STRING_SET_CONDITION_IF_NOT_EXISTS,
            ))
            for key, value in (("alpha", b"added"), ("123456789", b"uncertain"))
        ]

        with ExitStack() as stack:
            worker = stack.enter_context(ThreadPoolExecutor(max_workers=1))
            server = grpc.server(worker)
            add_TemporalStoreServiceServicer_to_server(Datashard(), server)
            addr = f"127.0.0.1:{server.add_insecure_port('127.0.0.1:0')}"
            server.start()
            stack.callback(lambda: server.stop(0).wait(timeout=3))
            metadata = Mock()
            for uses_raft, outcome in (
                (True, "write_outcome_unknown"), (False, "write_durability_unconfirmed"),
            ):
                with self.subTest(uses_raft=uses_raft), TableClient(
                    metadata, "kvcache", "model", preserve_insert_outcomes=True,
                ) as table:
                    metadata.table_topology.return_value = TableTopology("kvcache", "model", 1, 73, 2, (
                        Shard(73, None, (), 7, addr),
                        Shard(74, None, (), 8, addr, uses_raft_route=uses_raft),
                    ))
                    calls.clear()
                    inserted, uncertain = table.batch_execute([first, second], timeout_seconds=2)
                    self.assertTrue(inserted.status.ok)
                    self.assertEqual(inserted.count, 1)
                    self.assertEqual(uncertain.status.code, outcome)
                    self.assertEqual(calls, [(73, 7, b"added"), (74, 8, b"uncertain")])

    def test_batches_by_advertised_primary_and_restores_request_order(self):
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology(
            "kvcache", "model", 1, 73, 2,
            (Shard(73, None, (), 0, "primary-a"), Shard(74, None, (), 0, "primary-b")),
        )
        channels = {}

        def channel(target, **_):
            client = channels.setdefault(target, Mock())
            client.batch_execute.side_effect = lambda shard, batch, **_: commands.BatchExecuteResponse(
                responses=[commands.CommandResponse(
                    value=f"{shard}/{request.string_get.key}".encode(),
                ) for request in batch],
            )
            return client

        keys = ["alpha", "", "123456789", "é"]
        with patch("temporalstore_sdk.table.CommandClient", side_effect=channel) as create:
            with TableClient(metadata, "kvcache", "model") as table:
                expected = [b"73/alpha", b"73/", b"74/123456789", "74/é".encode()]
                self.assertEqual([result.value for result in table.batch_execute(map(get, keys))],
                                 expected)
                table.batch_execute(map(get, keys))
                self.assertEqual(create.call_count, 2)
                metadata.table_topology.assert_called_once_with("kvcache", "model")
        for client in channels.values():
            client.close.assert_called_once_with()

    def test_table_identity_is_stable_across_routes_and_changes_on_recreate(self):
        metadata = Mock()
        shard = Shard(73, None, (), 7, "primary")
        metadata.table_topology.side_effect = [
            TableTopology("kvcache", "model", version, 73, 1, (shard,), table_id=table_id)
            for version, table_id in ((10, 12), (11, 12), (12, 13), (13, None))
        ]
        with patch("temporalstore_sdk.table.monotonic") as clock:
            with TableClient(metadata, "kvcache", "model", topology_ttl_seconds=1) as table:
                for now, identity in [(0, (12, None)), (2, (12, None)),
                                      (4, (13, None)), (6, (None, 13))]:
                    clock.return_value = now
                    self.assertEqual(table.table_identity(), identity)

    def test_checked_batches_use_the_topology_load_version(self):
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology(
            "kvcache", "model", 1, 73, 2,
            (Shard(73, None, (), 0, "primary-a"), Shard(74, None, (), 19, "primary-b")),
        )
        with patch("temporalstore_sdk.table.CommandClient") as channel:
            channel.return_value.batch_execute.return_value = commands.BatchExecuteResponse(
                responses=[commands.CommandResponse()],
            )
            with TableClient(metadata, "kvcache", "model") as table:
                table.batch_execute([get("123456789")])
                channel.assert_called_once_with("primary-b", timeout_seconds=10.0)
                self.assertEqual(channel.return_value.batch_execute.call_args.args[0], 74)
                self.assertEqual(channel.return_value.batch_execute.call_args.kwargs,
                                 {"load_version": 19})

    def test_per_call_deadline_reuses_the_same_channel(self):
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology(
            "kvcache", "model", 1, 73, 1, (Shard(73, None, (), 7, "primary"),),
        )
        with patch("temporalstore_sdk.table.CommandClient") as channel, patch(
            "temporalstore_sdk.table.monotonic", return_value=10,
        ):
            channel.return_value.batch_execute.return_value = commands.BatchExecuteResponse(
                responses=[commands.CommandResponse()],
            )
            with TableClient(metadata, "kvcache", "model", timeout_seconds=5) as table:
                table.batch_execute([get("key")], timeout_seconds=1)
                table.batch_execute([get("key")])
            channel.assert_called_once_with("primary", timeout_seconds=5)
            self.assertEqual(
                [call.kwargs for call in channel.return_value.batch_execute.call_args_list],
                [{"load_version": 7, "timeout_seconds": 1}, {"load_version": 7}],
            )
            metadata.table_topology.assert_called_once_with("kvcache", "model", timeout_seconds=1)

    def test_timed_read_uses_one_budget_across_discovery_and_sequential_shards(self):
        metadata = Mock()
        topology = TableTopology("kvcache", "model", 1, 73, 2, (
            Shard(73, None, (), 7, "first"), Shard(74, None, (), 8, "second"),
        ))
        elapsed = 0.0

        def discover(*_, timeout_seconds):
            nonlocal elapsed
            self.assertEqual(timeout_seconds, 1)
            elapsed += 0.25
            return topology

        def read(*_, **kwargs):
            nonlocal elapsed
            elapsed += 0.25
            return commands.BatchExecuteResponse(responses=[commands.CommandResponse(count=1)])

        metadata.table_topology.side_effect = discover
        with patch("temporalstore_sdk.table.monotonic", side_effect=lambda: elapsed), patch(
            "temporalstore_sdk.table.CommandClient",
        ) as channel, TableClient(metadata, "kvcache", "model") as table:
            channel.return_value.batch_execute.side_effect = read
            results = table.batch_execute([get("alpha"), get("123456789")], timeout_seconds=1)
        self.assertEqual([item.count for item in results], [1, 1])
        self.assertEqual(
            [call.kwargs["timeout_seconds"] for call in channel.return_value.batch_execute.call_args_list],
            [0.75, 0.5],
        )

    def test_timed_cache_insert_window_includes_discovery_and_all_shards(self):
        metadata = Mock()
        topology = TableTopology("kvcache", "model", 1, 73, 2, (
            Shard(73, None, (), 7, "first"), Shard(74, None, (), 8, "second"),
        ))
        first, second = Mock(), Mock()
        inserts = [commands.Command(string_set_conditional=commands.StringSetConditional(
            key=key, value=b"payload", condition=commands.STRING_SET_CONDITION_IF_NOT_EXISTS,
        )) for key in ("alpha", "123456789")]
        elapsed = 0.0

        def discover(*_, **kwargs):
            nonlocal elapsed
            elapsed += 0.25
            return topology

        def insert(*_, **kwargs):
            nonlocal elapsed
            elapsed = 1.0
            return commands.BatchExecuteResponse(
                responses=[commands.CommandResponse(status=commands.Status(ok=True), count=1)],
            )

        metadata.table_topology.side_effect = discover
        first.batch_execute.side_effect = insert
        second.batch_execute.side_effect = insert
        with patch("temporalstore_sdk.table.monotonic", side_effect=lambda: elapsed), patch(
            "temporalstore_sdk.table.CommandClient", side_effect=[first, second],
        ), TableClient(metadata, "kvcache", "model", preserve_insert_outcomes=True) as table:
            accepted, expired = table.batch_execute(inserts, timeout_seconds=1)

        metadata.table_topology.assert_called_once_with("kvcache", "model", timeout_seconds=1)
        self.assertEqual(first.batch_execute.call_args.kwargs["timeout_seconds"], 0.75)
        self.assertEqual(accepted.count, 1)
        self.assertEqual(expired.status.code, "deadline_exceeded")
        second.batch_execute.assert_not_called()

    def test_uncertain_insert_can_finish_while_metadata_refresh_is_in_flight(self):
        metadata = Mock()
        topology = TableTopology("kvcache", "model", 1, 73, 1, (
            Shard(73, None, (), 7, "first", uses_raft_route=True),
        ))
        successor = TableTopology("kvcache", "model", 2, 73, 1, (
            Shard(73, None, (), 8, "second", uses_raft_route=True),
        ))
        started, reply = Event(), Event()
        refreshing, finish_refresh = Event(), Event()

        class Deadline(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.DEADLINE_EXCEEDED

            def details(self):
                return "insert deadline expired"

        def discover(*_, **kwargs):
            if metadata.table_topology.call_count == 1:
                return topology
            refreshing.set()
            if not finish_refresh.wait(4):
                raise AssertionError("test did not release topology refresh")
            return successor

        def write(*_, **kwargs):
            started.set()
            if not reply.wait(4):
                raise AssertionError("test did not release insert response")
            raise Deadline()

        command = commands.Command(string_set_conditional=commands.StringSetConditional(
            key="alpha", value=b"value", condition=commands.STRING_SET_CONDITION_IF_NOT_EXISTS,
        ))
        metadata.table_topology.side_effect = discover
        with ThreadPoolExecutor(max_workers=2) as workers, patch(
            "temporalstore_sdk.table.CommandClient",
        ) as client, TableClient(metadata, "kvcache", "model",
                                preserve_insert_outcomes=True) as table:
            self.assertIs(table._current_topology(), topology)
            client.return_value.batch_execute.side_effect = write
            pending = workers.submit(table.batch_execute, [command], timeout_seconds=2)
            try:
                self.assertTrue(started.wait(2))
                table._invalidate_route(topology)
                refresh = workers.submit(table._current_topology)
                self.assertTrue(refreshing.wait(2))
                reply.set()
                try:
                    [result] = pending.result(timeout=0.5)
                except TimeoutError as error:
                    self.fail(f"insert completion waited for the metadata refresh: {error}")
                self.assertEqual(result.status.code, "write_outcome_unknown")
            finally:
                reply.set()
                finish_refresh.set()
            self.assertIs(refresh.result(timeout=2), successor)
            self.assertIs(table._current_topology(), successor)
            self.assertEqual(metadata.table_topology.call_count, 2)

    def test_timed_partial_read_preserves_a_reply_when_another_shard_cannot_start(self):
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology("kvcache", "model", 1, 73, 2, (
            Shard(73, None, (), 7, "first"), Shard(74, None, (), 8, "second"),
        ))
        first, second = Mock(), Mock()
        with patch("temporalstore_sdk.table.monotonic", return_value=0.0) as clock, patch(
            "temporalstore_sdk.table.CommandClient", side_effect=[first, second],
        ), TableClient(metadata, "kvcache", "model") as table:
            def read(*_, **kwargs):
                clock.return_value = 1
                return commands.BatchExecuteResponse(
                    responses=[commands.CommandResponse(status=commands.Status(ok=True), count=1)],
                )

            first.batch_execute.side_effect = read
            hit, expired = table.batch_execute(
                [get("alpha"), get("123456789")], timeout_seconds=1,
                allow_partial_reads=True,
            )
            self.assertEqual(hit.count, 1)
            self.assertEqual(expired.status.code, "read_unavailable")
            second.batch_execute.assert_not_called()

    def test_discovery_that_spends_the_read_deadline_does_not_dispatch(self):
        metadata = Mock()
        with patch("temporalstore_sdk.table.monotonic", return_value=0.0) as clock, patch(
            "temporalstore_sdk.table.CommandClient",
        ) as channel, TableClient(metadata, "kvcache", "model") as table:
            def discover(*_, timeout_seconds):
                clock.return_value = timeout_seconds
                return TableTopology(
                    "kvcache", "model", 1, 73, 1, (Shard(73, None, (), 7, "primary"),),
                )

            metadata.table_topology.side_effect = discover
            with self.assertRaises(MetadataError) as timed_out:
                table.batch_execute([get("key")], timeout_seconds=0.5)
            self.assertEqual(timed_out.exception.code, "deadline_exceeded")
            channel.assert_not_called()

    def test_timed_read_expires_waiting_for_another_route_refresh(self):
        metadata = Mock()
        refresh_started = Event()
        finish_refresh = Event()
        topology = TableTopology(
            "kvcache", "model", 1, 73, 1, (Shard(73, None, (), 7, "primary"),),
        )

        def discover(*_, **__):
            refresh_started.set()
            self.assertTrue(finish_refresh.wait(3), "route refresh was not released")
            return topology

        metadata.table_topology.side_effect = discover
        with TableClient(metadata, "kvcache", "model") as table, ThreadPoolExecutor(max_workers=1) as pool, patch(
            "temporalstore_sdk.table.CommandClient",
        ) as channel:
            refresh = pool.submit(table.table_identity)
            try:
                self.assertTrue(refresh_started.wait(2), "route refresh did not start")
                with self.assertRaises(MetadataError) as timed_out:
                    table.batch_execute([get("key")], timeout_seconds=0.025)
                self.assertEqual(timed_out.exception.code, "deadline_exceeded")
                self.assertIn("routing wait", str(timed_out.exception))
                metadata.table_topology.assert_called_once()
                channel.assert_not_called()
            finally:
                finish_refresh.set()
            self.assertEqual(refresh.result(timeout=2), table.table_identity())

    def test_stale_fence_expires_route_without_retrying_the_failed_write(self):
        metadata = Mock()
        metadata.table_topology.side_effect = [
            TableTopology("kvcache", "model", 1, 73, 1, (Shard(73, None, (), 7, "primary-a"),)),
            TableTopology("kvcache", "model", 2, 73, 1, (Shard(73, None, (), 8, "primary-a"),)),
        ]
        with patch("temporalstore_sdk.table.CommandClient") as channel:
            channel.return_value.batch_execute.side_effect = [
                commands.BatchExecuteResponse(
                    status=commands.Status(code="load_version_mismatch"),
                ),
                commands.BatchExecuteResponse(
                    responses=[commands.CommandResponse(count=1)],
                ),
            ]
            with TableClient(metadata, "kvcache", "model") as table:
                with self.assertRaises(MetadataError) as stale:
                    table.batch_execute([get("a")])
                self.assertEqual(stale.exception.code, "load_version_mismatch")
                self.assertEqual(channel.return_value.batch_execute.call_count, 1)
                self.assertEqual(table.batch_execute([get("a")])[0].count, 1)
                self.assertEqual(metadata.table_topology.call_count, 2)
                calls = channel.return_value.batch_execute.call_args_list
                self.assertEqual([call.kwargs["load_version"] for call in calls], [7, 8])

    def test_transport_unavailable_refreshes_next_request_without_resending_writes(self):
        class Unavailable(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE

        for rpc in ("execute", "batch_execute"):
            with self.subTest(rpc=rpc):
                metadata = Mock()
                metadata.table_topology.side_effect = [
                    TableTopology("kvcache", "model", 1, 73, 1,
                                  (Shard(73, None, (), 7, "primary-a"),)),
                    TableTopology("kvcache", "model", 2, 73, 1,
                                  (Shard(73, None, (), 8, "primary-b"),)),
                ]
                first, second = Mock(), Mock()
                getattr(first, rpc).side_effect = Unavailable
                getattr(second, rpc).return_value = (
                    commands.ExecuteResponse(status=commands.Status(ok=True))
                    if rpc == "execute" else commands.BatchExecuteResponse(
                        responses=[commands.CommandResponse(count=1)],
                    )
                )
                write = commands.Command(string_set=commands.StringSet(key="a", value=b"value"))
                request = write if rpc == "execute" else [write]
                with patch("temporalstore_sdk.table.CommandClient", side_effect=[first, second]):
                    with TableClient(metadata, "kvcache", "model") as table:
                        call = getattr(table, rpc)
                        with self.assertRaises(Unavailable):
                            call(request)
                        getattr(first, rpc).assert_called_once()
                        getattr(second, rpc).assert_not_called()
                        self.assertEqual(metadata.table_topology.call_count, 1)
                        call(request)
                        self.assertEqual(metadata.table_topology.call_count, 2)
                        self.assertEqual(getattr(second, rpc).call_args.kwargs["load_version"], 8)

    def test_uncertain_conditional_insert_refreshes_route_for_the_next_insert(self):
        class Unavailable(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE

            def details(self):
                return "previous leader is unreachable"

        metadata = Mock()
        metadata.table_topology.side_effect = [
            TableTopology("kvcache", "model", version, 73, 1, (
                Shard(73, None, (), version, address, uses_raft_route=True),
            ))
            for version, address in ((7, "primary-a"), (8, "primary-b"))
        ]
        stale, current = Mock(), Mock()
        stale.batch_execute.side_effect = Unavailable
        current.batch_execute.return_value = commands.BatchExecuteResponse(
            responses=[commands.CommandResponse(status=commands.Status(ok=True), count=1)],
        )
        first, second = (
            commands.Command(string_set_conditional=commands.StringSetConditional(
                key=key, value=key.encode(), condition=commands.STRING_SET_CONDITION_IF_NOT_EXISTS,
            ))
            for key in ("first", "second")
        )
        with patch("temporalstore_sdk.table.CommandClient", side_effect=[stale, current]):
            with TableClient(metadata, "kvcache", "model", preserve_insert_outcomes=True) as table:
                [reply] = table.batch_execute([first])
                self.assertEqual(reply.status.code, "write_outcome_unknown")
                stale.batch_execute.assert_called_once()
                current.batch_execute.assert_not_called()
                self.assertEqual(metadata.table_topology.call_count, 1)
                [reply] = table.batch_execute([second])
                self.assertEqual(reply.count, 1)
                self.assertEqual(metadata.table_topology.call_count, 2)
                current.batch_execute.assert_called_once()
                call = current.batch_execute.call_args
                self.assertEqual(list(call.args[1]), [second])
                self.assertEqual(call.kwargs["load_version"], 8)

    def test_unary_read_refreshes_a_stale_route_within_the_original_deadline(self):
        class Unavailable(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE

        for failure in (Unavailable(), commands.ExecuteResponse(
            status=commands.Status(code="raft_not_leader"),
        )):
            with self.subTest(failure=failure):
                clock = [100.0]
                metadata = Mock()
                metadata.table_topology.side_effect = [
                    TableTopology("kvcache", "model", 1, 73, 1,
                                  (Shard(73, None, (), 7, "primary-a"),)),
                    TableTopology("kvcache", "model", 2, 73, 1,
                                  (Shard(73, None, (), 8, "primary-b"),)),
                ]
                first, second = Mock(), Mock()

                def stale(*args, **kwargs):
                    clock[0] += 0.6
                    if isinstance(failure, grpc.RpcError):
                        raise failure
                    return failure

                first.execute.side_effect = stale
                second.execute.return_value = commands.ExecuteResponse(status=commands.Status(ok=True))
                with patch("temporalstore_sdk.table.monotonic", side_effect=lambda: clock[0]), \
                     patch("temporalstore_sdk.table.CommandClient", side_effect=[first, second]):
                    with TableClient(metadata, "kvcache", "model", timeout_seconds=1) as table:
                        self.assertTrue(table.execute(get("key")).status.ok)

                self.assertEqual(metadata.table_topology.call_count, 2)
                first.execute.assert_called_once()
                second.execute.assert_called_once()
                self.assertEqual(first.execute.call_args.kwargs["load_version"], 7)
                self.assertEqual(second.execute.call_args.kwargs["load_version"], 8)
                self.assertAlmostEqual(first.execute.call_args.kwargs["timeout_seconds"], 1.0)
                self.assertAlmostEqual(second.execute.call_args.kwargs["timeout_seconds"], 0.4)

    def test_unary_read_recovers_exact_bytes_from_the_refreshed_grpc_endpoint(self):
        payload = bytes(range(256)) * 256
        calls = []

        class Datashard(TemporalStoreServiceServicer):
            def __init__(self, role):
                self.role = role

            def ExecuteChecked(self, request, context):
                calls.append((self.role, request.shard_id, request.load_version, request.command.string_get.key))
                if self.role == "old":
                    context.abort(grpc.StatusCode.UNAVAILABLE, "primary changed")
                return commands.ExecuteResponse(
                    status=commands.Status(ok=True), response=commands.CommandResponse(value=payload),
                )

        with ExitStack() as stack:
            pool = stack.enter_context(ThreadPoolExecutor(max_workers=2))
            topologies = []
            for role, version in (("old", 7), ("new", 8)):
                server = grpc.server(pool)
                add_TemporalStoreServiceServicer_to_server(Datashard(role), server)
                port = server.add_insecure_port("127.0.0.1:0")
                server.start()
                stack.callback(lambda server=server: server.stop(0).wait(timeout=3))
                topologies.append(TableTopology("kvcache", "model", version, 73, 1, (
                    Shard(73, None, (), version, f"127.0.0.1:{port}"),
                )))

            metadata = Mock()
            metadata.table_topology.side_effect = topologies
            with TableClient(metadata, "kvcache", "model", timeout_seconds=2) as table:
                reply = table.execute(get("chunk"))

        self.assertTrue(reply.status.ok)
        self.assertEqual(reply.response.value, payload)
        self.assertEqual(calls, [("old", 73, 7, "chunk"), ("new", 73, 8, "chunk")])
        self.assertEqual(metadata.table_topology.call_count, 2)

    def test_missing_elected_leader_refreshes_before_normal_topology_ttl(self):
        metadata = Mock()
        metadata.table_topology.side_effect = [
            TableTopology("kvcache", "model", 1, 73, 1,
                          (Shard(73, None, (), 7, uses_raft_route=True),)),
            TableTopology("kvcache", "model", 2, 73, 1,
                          (Shard(73, "leader-http", (), 7, "leader-grpc", uses_raft_route=True),)),
        ]
        with patch("temporalstore_sdk.table.monotonic") as clock, patch(
            "temporalstore_sdk.table.CommandClient"
        ) as client:
            client.return_value.execute.return_value = commands.ExecuteResponse(
                status=commands.Status(ok=True),
            )
            with TableClient(metadata, "kvcache", "model", topology_ttl_seconds=5) as table:
                for now in (0, 0.1):
                    clock.return_value = now
                    with self.assertRaisesRegex(MetadataError, "no_grpc_primary"):
                        table.execute(get("cached-key"))
                self.assertEqual(metadata.table_topology.call_count, 1)
                clock.return_value = 0.3
                self.assertTrue(table.execute(get("cached-key")).status.ok)
                clock.return_value = 0.6
                self.assertTrue(table.execute(get("cached-key")).status.ok)
                self.assertEqual(metadata.table_topology.call_count, 2)
            client.assert_called_once_with("leader-grpc", timeout_seconds=10)

    def test_reads_reach_independent_shards_before_either_replies(self):
        ready = [Event(), Event()]

        class Datashard(TemporalStoreServiceServicer):
            def __init__(self, index):
                self.index = index

            def BatchExecuteChecked(self, request, context):
                ready[self.index].set()
                if not ready[1 - self.index].wait(2):
                    context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "peer shard received no read")
                if request.load_version != 7 + self.index:
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, "wrong placement generation")
                return commands.BatchExecuteResponse(
                    status=commands.Status(ok=True),
                    responses=[
                        commands.CommandResponse(value=command.string_get.key.encode())
                        for command in request.commands
                    ],
                )

        with ExitStack() as stack:
            workers = stack.enter_context(ThreadPoolExecutor(max_workers=2))
            endpoints = []
            for index in range(2):
                server = grpc.server(workers)
                add_TemporalStoreServiceServicer_to_server(Datashard(index), server)
                port = server.add_insecure_port("127.0.0.1:0")
                server.start()
                stack.callback(lambda node=server: node.stop(0).wait(timeout=3))
                endpoints.append(f"127.0.0.1:{port}")
            meta = Mock()
            meta.table_topology.return_value = TableTopology("kvcache", "model", 1, 73, 2, (
                Shard(73, None, (), 7, endpoints[0]), Shard(74, None, (), 8, endpoints[1]),
            ))
            table = stack.enter_context(TableClient(meta, "kvcache", "model", timeout_seconds=3, parallel_reads=True))
            keys = ["alpha", "123456789", ""]
            self.assertEqual([response.value for response in table.batch_execute(map(get, keys))],
                             [key.encode() for key in keys])

    def test_batch_with_write_retains_sequential_dispatch(self):
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology("kvcache", "model", 1, 73, 2, (
            Shard(73, None, (), 7, "one"), Shard(74, None, (), 8, "two"),
        ))
        first, second = Mock(), Mock()
        first.batch_execute.return_value = commands.BatchExecuteResponse(
            status=commands.Status(code="load_version_mismatch"),
        )
        write = commands.Command(string_set=commands.StringSet(key="alpha", value=b"data"))
        with patch("temporalstore_sdk.table.CommandClient", side_effect=[first, second]):
            with TableClient(metadata, "kvcache", "model", parallel_reads=True) as table:
                with self.assertRaises(MetadataError) as stale:
                    table.batch_execute([write, get("123456789")])
        self.assertEqual(stale.exception.code, "load_version_mismatch")
        first.batch_execute.assert_called_once()
        second.batch_execute.assert_not_called()
        first._start_batch.assert_not_called()
        second._start_batch.assert_not_called()

    def test_partial_lookup_keeps_healthy_results_during_an_election(self):
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology("kvcache", "model", 1, 73, 2, (
            Shard(73, None, (), 7, uses_raft_route=True), Shard(74, None, (), 8, "primary-b"),
        ))
        client = Mock()
        client.batch_execute.return_value = commands.BatchExecuteResponse(
            status=commands.Status(ok=True),
            responses=[commands.CommandResponse(status=commands.Status(ok=True), count=1)],
        )
        with patch("temporalstore_sdk.table.CommandClient", return_value=client):
            with TableClient(metadata, "kvcache", "model", parallel_reads=True) as table:
                keys = [get("123456789"), get("alpha")]
                with self.assertRaises(MetadataError) as missing:
                    table.batch_execute(keys)
                self.assertEqual(missing.exception.code, "no_grpc_primary")
                client.batch_execute.assert_not_called()

                result = table.batch_execute(keys, allow_partial_reads=True)
                self.assertTrue(result[0].status.ok)
                self.assertEqual(result[0].count, 1)
                self.assertEqual(result[1].status.code, "no_grpc_primary")
                client.batch_execute.assert_called_once()
                self.assertEqual(client.batch_execute.call_args.args[0], 74)
                client.reset_mock()

                write = commands.Command(string_set=commands.StringSet(key="123456789", value=b"x"))
                with self.assertRaisesRegex(ValueError, "read-only"):
                    table.batch_execute([write, *keys], allow_partial_reads=True)
                with self.assertRaises(MetadataError):
                    table.batch_execute([write, *keys])
                client.batch_execute.assert_not_called()

    def test_partial_lookup_only_tolerates_transient_transport_errors(self):
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology("kvcache", "model", 1, 73, 2, (
            Shard(73, None, (), 7, "primary-a"), Shard(74, None, (), 8, "primary-b"),
        ))
        first, second = Mock(), Mock()
        second._start_batch.return_value.result.return_value = commands.BatchExecuteResponse(
            status=commands.Status(ok=True),
            responses=[commands.CommandResponse(status=commands.Status(ok=True), count=2)],
        )

        class ReadError(grpc.RpcError):
            def __init__(self, status):
                super().__init__()
                self.status = status

            def code(self):
                return self.status

            def details(self):
                return self.status.name

        with patch("temporalstore_sdk.table.CommandClient", side_effect=[first, second]):
            with TableClient(metadata, "kvcache", "model", parallel_reads=True) as table:
                reads = [get("alpha"), get("123456789")]
                first._start_batch.return_value.result.side_effect = ReadError(grpc.StatusCode.UNAVAILABLE)
                result = table.batch_execute(reads, allow_partial_reads=True)
                self.assertEqual(result[0].status.code, "read_unavailable")
                self.assertEqual(result[1].count, 2)

                first._start_batch.return_value.result.side_effect = ReadError(grpc.StatusCode.PERMISSION_DENIED)
                with self.assertRaises(ReadError):
                    table.batch_execute(reads, allow_partial_reads=True)

    def test_rejected_read_cancels_outstanding_shard_requests(self):
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology("kvcache", "model", 1, 73, 2, (
            Shard(73, None, (), 7, "primary-a"), Shard(74, None, (), 8, "primary-b"),
        ))
        first, second = Mock(), Mock()
        first._start_batch.return_value.result.return_value = commands.BatchExecuteResponse(
            status=commands.Status(code="raft_error", message="leader changed"),
        )
        with patch("temporalstore_sdk.table.CommandClient", side_effect=[first, second]):
            with TableClient(metadata, "kvcache", "model", parallel_reads=True) as table:
                with self.assertRaisesRegex(MetadataError, "leader changed"):
                    table.batch_execute([get("alpha"), get("123456789")])
                self.assertEqual(table._expires_at, 0.0)
        second._start_batch.assert_called_once()
        second._start_batch.return_value.result.assert_not_called()
        second._start_batch.return_value.cancel.assert_called_once()
