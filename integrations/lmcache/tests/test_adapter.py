# SPDX-License-Identifier: Apache-2.0
"""Exercise the adapter task contract with a local transport double."""

import importlib
import sys
import threading
import types
import unittest
from unittest.mock import Mock, patch


class Bitmap:
    def __init__(self, length):
        self.bits = [False] * length

    def set(self, index):
        self.bits[index] = True

    def test(self, index):
        return self.bits[index]


class Notifier:
    next_fd = 10

    def __init__(self):
        self.fd = Notifier.next_fd
        Notifier.next_fd += 1
        self.event = threading.Event()

    def fileno(self):
        return self.fd

    def notify(self):
        self.event.set()

    def close(self):
        pass


class Base:
    def __init__(self, max_capacity_bytes=0):
        self._max_capacity_bytes = max_capacity_bytes

    def _notify_keys_stored(self, keys, sizes):
        pass

    def _notify_keys_deleted(self, keys, sizes):
        pass


class StoreResult:
    def __init__(self, success, transferred):
        self.success = success
        self.transferred = transferred


class Memory:
    def __init__(self, data):
        self.byte_array = bytearray(data)

    def get_size(self):
        return len(self.byte_array)


class Key:
    def __init__(self, name):
        self.name = name
        self.cache_salt = ""


def module(name, **items):
    value = types.ModuleType(name)
    value.__dict__.update(items)
    sys.modules[name] = value


module("lmcache")
module("lmcache.lmcache_native", Bitmap=Bitmap)
module("lmcache.v1")
module("lmcache.v1.distributed")
module(
    "lmcache.v1.distributed.api", ObjectKey=Key,
    MemoryLayoutDesc=object, MemoryResidency=types.SimpleNamespace(LOCAL=1, REMOTE=2),
    MemorySegment=types.SimpleNamespace(STORAGE=1), MemoryTags=lambda *args: args,
)
module("lmcache.v1.distributed.internal_api", L2StoreResult=StoreResult)
module("lmcache.v1.distributed.key_codec", encode_storage_key=lambda key: key.name)
module("lmcache.v1.distributed.l2_adapters")
module("lmcache.v1.distributed.l2_adapters.base", L2AdapterInterface=Base, L2TaskId=int)
module("lmcache.v1.distributed.l2_adapters.config", L2AdapterConfigBase=object)
module("lmcache.v1.memory_management", MemoryObj=Memory)
module("lmcache.v1.platform", create_event_notifier=Notifier)

adapter_module = importlib.import_module("lmcache_temporalstore.adapter")


class FakeBlob:
    def __init__(self, *_args, **_kwargs):
        self.values = {}
        self.fail_put = False
        self.fail_get = False

    def put(self, name, payload):
        if self.fail_put:
            raise OSError("injected write failure")
        self.values[name] = bytes(payload)

    def get_into(self, name, destination):
        if self.fail_get:
            raise OSError("injected read failure")
        payload = self.values.get(name)
        if payload is None or len(payload) != len(destination):
            return False
        destination[:] = payload
        return True

    def get_many_into(self, items):
        found = []
        for key, destination in items:
            try:
                found.append(self.get_into(key, destination))
            except OSError:
                found.append(False)
        return found

    def delete(self, name):
        self.values.pop(name, None)

    def delete_many(self, names):
        result = []
        for name in names:
            try:
                self.delete(name)
            except OSError:
                result.append(False)
            else:
                result.append(True)
        return result

    def close(self):
        pass


class FakeData:
    def __init__(self, **kwargs):
        self.values = {}
        self.timeout_seconds = kwargs.get("timeout_seconds")

    def execute(self, command):
        if command["kind"] == "common_delete":
            self.values.pop(command["key"], None)
        else:
            self.values[command["key"]] = command["value"]
        return {}

    def batch_execute(self, commands):
        answers = []
        for command in commands:
            if command["kind"] == "common_exists":
                answers.append({"kind": "integer", "value": int(command["key"] in self.values)})
            else:
                answers.append(self.execute(command))
        return answers

    def close(self):
        pass


class AdapterTest(unittest.TestCase):
    def test_remote_lookup_timeout_does_not_change_write_timeout(self):
        config = adapter_module.TemporalStoreL2AdapterConfig.from_dict({
            "remote_storage": True, "timeout_seconds": 8,
        })
        adapter = adapter_module.TemporalStoreL2Adapter(config)
        try:
            self.assertEqual(adapter._lookup_data.timeout_seconds, 1)
            self.assertEqual(adapter._data.timeout_seconds, 8)
        finally:
            adapter.close()

    def test_remote_lookup_skips_queued_work_after_transport_failure_then_recovers(self):
        adapter = adapter_module.TemporalStoreL2Adapter({"remote_storage": True})
        self.addCleanup(adapter.close)
        key = Key("remote")
        batch = Mock(side_effect=[
            TimeoutError("replica unavailable"),
            [{"kind": "integer", "value": 1}],
            [{"kind": "integer", "value": 0}],
            [{"kind": "integer", "value": 1}],
        ])
        adapter._lookup_data.batch_execute = batch

        with patch.object(adapter_module, "monotonic", return_value=10.0) as clock:
            tasks = [adapter.submit_lookup_and_lock_task([key], {}) for _ in range(8)]
            adapter._lookup_pool.submit(lambda: None).result(timeout=2)

            self.assertTrue(adapter._lookup_event.event.is_set())
            for task in tasks:
                self.assertEqual(adapter.query_lookup_and_lock_result(task).bits, [False])
            self.assertEqual(batch.call_count, 1)
            self.assertEqual(adapter._locked, {})

            clock.return_value = 11.0
            for expected in (True, False, True):
                adapter._lookup_event.event.clear()
                task = adapter.submit_lookup_and_lock_task([key], {})
                self.assertTrue(adapter._lookup_event.event.wait(2))
                self.assertEqual(adapter.query_lookup_and_lock_result(task).bits, [expected])
                if expected:
                    adapter.submit_unlock([key])
            self.assertEqual(batch.call_count, 4)
            self.assertEqual(adapter._locked, {})

    def test_close_finishes_queued_lookups_without_new_network_calls(self):
        adapter = self.adapter
        entered = threading.Event()
        release = threading.Event()
        probes = []

        def probe(commands):
            probes.append(commands)
            entered.set()
            self.assertTrue(release.wait(2))
            return [{"kind": "integer", "value": 0} for _ in commands]

        adapter._lookup_data.batch_execute = probe
        key = Key("unavailable")
        tasks = [adapter.submit_lookup_and_lock_task([key], {})]
        self.assertTrue(entered.wait(2))
        tasks.extend(adapter.submit_lookup_and_lock_task([key], {}) for _ in range(8))
        draining = threading.Event()
        shutdown = adapter._lookup_pool.shutdown

        def mark_draining(*args, **kwargs):
            draining.set()
            return shutdown(*args, **kwargs)

        with patch.object(adapter._lookup_pool, "shutdown", side_effect=mark_draining):
            closer = threading.Thread(target=adapter.close)
            closer.start()
            try:
                self.assertTrue(draining.wait(2))
            finally:
                release.set()
                closer.join(3)
        self.assertFalse(closer.is_alive())
        self.assertEqual(len(probes), 1)
        self.assertFalse(adapter._locked)
        for task_id in tasks:
            self.assertFalse(adapter.query_lookup_and_lock_result(task_id).test(0))
            self.assertIsNone(adapter.query_lookup_and_lock_result(task_id))

    def test_submissions_after_close_complete_as_cache_misses(self):
        adapter = self.adapter
        key = Key("closed")
        obj = Memory(b"abc")
        adapter.close()

        store = adapter.submit_store_task([key], [obj])
        lookup = adapter.submit_lookup_and_lock_task([key], {})
        load = adapter.submit_load_task([key], [obj])
        adapter.delete([key])

        self.assertFalse(adapter.pop_completed_store_tasks()[store].success)
        self.assertFalse(adapter.query_lookup_and_lock_result(lookup).test(0))
        self.assertFalse(adapter.query_load_result(load).test(0))

    def test_invalid_capacity_is_rejected(self):
        with self.assertRaises(ValueError):
            adapter_module.TemporalStoreL2Adapter(
                adapter_module.TemporalStoreL2AdapterConfig(max_capacity_gb=0)
            )

    def setUp(self):
        self.old_blob = adapter_module.BlobClient
        self.old_data = adapter_module.TemporalStoreClient
        adapter_module.BlobClient = FakeBlob
        adapter_module.TemporalStoreClient = FakeData
        config = adapter_module.TemporalStoreL2AdapterConfig(
            max_capacity_gb=4 / 1024**3
        )
        self.adapter = adapter_module.TemporalStoreL2Adapter(config)

    def tearDown(self):
        self.adapter.close()
        self.adapter.close()
        adapter_module.BlobClient = self.old_blob
        adapter_module.TemporalStoreClient = self.old_data

    def test_table_lookup_retries_only_affected_reads_within_its_original_deadline(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 1, 0)
        self.addCleanup(backend.close)
        keys = [Key("after-election"), Key("healthy"), Key("absent")]
        for key in keys:
            key.model_name, key.kv_rank = "fixture/model", 0
        for code in ("raft_not_leader", "read_unavailable", "load_version_mismatch"):
            with self.subTest(code=code):
                client = Mock()
                client.batch_execute.side_effect = [
                    [
                        pb.CommandResponse(status=pb.Status(code=code)),
                        pb.CommandResponse(status=pb.Status(ok=True), count=7),
                        pb.CommandResponse(status=pb.Status(ok=True), count=0),
                    ],
                    [pb.CommandResponse(status=pb.Status(ok=True), count=4)],
                ]
                with patch.object(backend, "_table", return_value=client), patch(
                    "lmcache_temporalstore.grpc_kv.monotonic", side_effect=[10.0, 10.25]
                ):
                    self.assertEqual(backend.lengths(keys), [4, 7, 0])
                first, second = client.batch_execute.call_args_list
                self.assertEqual(first.kwargs, {"timeout_seconds": 1, "allow_partial_reads": True})
                self.assertEqual(second.kwargs, {"timeout_seconds": 0.75, "allow_partial_reads": True})
                self.assertEqual(second.args[0], first.args[0][:1])

    def test_lookup_waits_for_the_new_route_without_retrying_hits(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        for proxy in ("", "127.0.0.1:1"):
            with self.subTest(proxy=bool(proxy)):
                backend = TableValueBackend(
                    "grpc://example.test", "kvcache", 5, 1, 0,
                    proxy_grpc_target=proxy,
                )
                self.addCleanup(backend.close)
                healthy = pb.CommandResponse(status=pb.Status(ok=True), count=4)
                unavailable = pb.CommandResponse(status=pb.Status(code="read_unavailable"))
                recovered = pb.CommandResponse(status=pb.Status(ok=True), count=8)
                keys = [Key("healthy"), Key("recovering")]
                for key in keys:
                    key.model_name, key.kv_rank = "fixture/model", 0
                client = Mock()
                client.batch_execute.side_effect = [
                    [healthy, unavailable], [unavailable], [recovered],
                ]
                clock = [10.0]

                def now():
                    clock[0] += 0.01
                    return clock[0]

                def wait(delay):
                    clock[0] += delay

                with patch.object(backend, "_table", return_value=client), patch(
                    "lmcache_temporalstore.grpc_kv.monotonic", side_effect=now,
                ), patch("lmcache_temporalstore.grpc_kv.sleep", side_effect=wait) as backoff:
                    self.assertEqual(backend.lengths(keys), [4, 8])

                first, second, third = client.batch_execute.call_args_list
                self.assertEqual(second.args[0], first.args[0][1:])
                self.assertEqual(third.args[0], second.args[0])
                limits = [call.kwargs["timeout_seconds"] for call in (first, second, third)]
                self.assertTrue(0 < limits[2] < limits[1] < limits[0] == 1)
                self.assertEqual(backoff.call_count, 1)

    def test_direct_lookup_refreshes_the_stale_shard_without_repeating_the_healthy_read(self):
        from concurrent.futures import ThreadPoolExecutor
        from contextlib import ExitStack

        import grpc
        from temporalstore.v1.temporalstore_pb2_grpc import (
            MetadataServiceServicer, TemporalStoreServiceServicer,
            add_MetadataServiceServicer_to_server, add_TemporalStoreServiceServicer_to_server,
        )
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        elected = threading.Event()
        calls = {"old": [], "replacement": [], "healthy": []}
        discoveries = []

        class ShardServer(TemporalStoreServiceServicer):
            def __init__(self, name, result):
                self.name, self.result = name, result

            def BatchExecuteChecked(self, request, context):
                calls[self.name].append((
                    request.shard_id, request.load_version,
                    [c.string_len.key for c in request.commands],
                ))
                if self.name == "old":
                    elected.set()
                    return pb.BatchExecuteResponse(status=pb.Status(code="load_version_mismatch"))
                return pb.BatchExecuteResponse(status=pb.Status(ok=True), responses=[
                    pb.CommandResponse(status=pb.Status(ok=True), count=self.result),
                ])

        class MetadataServer(MetadataServiceServicer):
            def GetTableTopology(self, request, context):
                moved = elected.is_set()
                discoveries.append((request.namespace_name, moved))
                result = pb.MetadataTableTopologyResponse(
                    status=pb.Status(ok=True), first_shard_id=73, shard_count=2,
                    table_id=1, topology_version=2 if moved else 1,
                )
                result.shards.add(shard_id=73, uses_raft_route=True,
                                  primary_grpc=replacement if moved else old,
                                  load_version=8 if moved else 7)
                result.shards.add(shard_id=74, uses_raft_route=True,
                                  primary_grpc=healthy, load_version=11)
                return result

        with ExitStack() as stack:
            def start(servicer, register):
                worker = stack.enter_context(ThreadPoolExecutor(max_workers=1))
                server = grpc.server(worker)
                register(servicer, server)
                port = server.add_insecure_port("127.0.0.1:0")
                server.start()
                stack.callback(lambda: server.stop(0).wait(timeout=3))
                return f"127.0.0.1:{port}"

            old = start(ShardServer("old", 0), add_TemporalStoreServiceServicer_to_server)
            replacement = start(ShardServer("replacement", 4),
                                add_TemporalStoreServiceServicer_to_server)
            healthy = start(ShardServer("healthy", 7), add_TemporalStoreServiceServicer_to_server)
            meta = start(MetadataServer(), add_MetadataServiceServicer_to_server)
            backend = TableValueBackend(f"grpc://{meta}", "kvcache", 5, 2, 0)
            stack.callback(backend.close)
            keys = [Key("alpha"), Key("123456789")]
            for key in keys:
                key.model_name, key.kv_rank = "fixture/model", 0
            self.assertEqual(backend.lengths(keys), [4, 7])

        self.assertEqual(discoveries, [("kvcache", False), ("kvcache", True)])
        self.assertEqual(calls, {
            "old": [(73, 7, ["alpha"])],
            "replacement": [(73, 8, ["alpha"])],
            "healthy": [(74, 11, ["123456789"])],
        })

    def test_direct_lookup_cools_only_the_failed_route_and_resumes_after_failover(self):
        from concurrent.futures import ThreadPoolExecutor
        from contextlib import ExitStack

        import grpc
        from temporalstore.v1.temporalstore_pb2_grpc import (
            MetadataServiceServicer, TemporalStoreServiceServicer,
            add_MetadataServiceServicer_to_server, add_TemporalStoreServiceServicer_to_server,
        )
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        elected = threading.Event()
        calls = {"old": [], "replacement": [], "healthy": []}
        discoveries = []

        class ShardServer(TemporalStoreServiceServicer):
            def __init__(self, name, result):
                self.name, self.result = name, result

            def BatchExecuteChecked(self, request, context):
                calls[self.name].append((
                    request.shard_id, request.load_version,
                    [c.string_len.key for c in request.commands],
                ))
                if self.name == "old":
                    context.abort(grpc.StatusCode.UNAVAILABLE, "replica paused")
                return pb.BatchExecuteResponse(status=pb.Status(ok=True), responses=[
                    pb.CommandResponse(status=pb.Status(ok=True), count=self.result),
                ])

        class MetadataServer(MetadataServiceServicer):
            def GetTableTopology(self, request, context):
                moved = elected.is_set()
                discoveries.append((request.namespace_name, moved))
                result = pb.MetadataTableTopologyResponse(
                    status=pb.Status(ok=True), first_shard_id=73, shard_count=2,
                    table_id=1, topology_version=2 if moved else 1,
                )
                result.shards.add(shard_id=73, uses_raft_route=True,
                                  primary_grpc=replacement if moved else old,
                                  load_version=8 if moved else 7)
                result.shards.add(shard_id=74, uses_raft_route=True,
                                  primary_grpc=healthy, load_version=11)
                return result

        with ExitStack() as stack:
            def start(servicer, register):
                worker = stack.enter_context(ThreadPoolExecutor(max_workers=1))
                server = grpc.server(worker)
                register(servicer, server)
                port = server.add_insecure_port("127.0.0.1:0")
                server.start()
                stack.callback(lambda: server.stop(0).wait(timeout=3))
                return f"127.0.0.1:{port}"

            old = start(ShardServer("old", 0), add_TemporalStoreServiceServicer_to_server)
            replacement = start(ShardServer("replacement", 4),
                                add_TemporalStoreServiceServicer_to_server)
            healthy = start(ShardServer("healthy", 7), add_TemporalStoreServiceServicer_to_server)
            meta = start(MetadataServer(), add_MetadataServiceServicer_to_server)
            backend = TableValueBackend(f"grpc://{meta}", "kvcache", 5, 1, 0)
            stack.callback(backend.close)
            keys = [Key("alpha"), Key("123456789")]
            for key in keys:
                key.model_name, key.kv_rank = "fixture/model", 0
            self.assertEqual(backend.lengths(keys), [0, 7])
            self.assertEqual(backend.lengths(keys), [0, 7])
            self.assertEqual(calls["old"], [(73, 7, ["alpha"])])
            elected.set()
            self.assertEqual(backend.lengths(keys), [4, 7])

        self.assertEqual(calls, {
            "old": [(73, 7, ["alpha"])],
            "replacement": [(73, 8, ["alpha"])],
            "healthy": [(74, 11, ["123456789"])] * 3,
        })


    def test_lookup_stops_retrying_when_the_deadline_expires(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 0.04, 0)
        self.addCleanup(backend.close)
        healthy = pb.CommandResponse(status=pb.Status(ok=True), count=4)
        unavailable = pb.CommandResponse(status=pb.Status(code="read_unavailable"))
        keys = [Key("healthy"), Key("unavailable")]
        for key in keys:
            key.model_name, key.kv_rank = "fixture/model", 0
        client = Mock()
        client.batch_execute.side_effect = [[healthy, unavailable], [unavailable]]
        clock = [10.0]

        def now():
            clock[0] += 0.01
            return clock[0]

        def wait(delay):
            clock[0] += delay

        with patch.object(backend, "_table", return_value=client), patch(
            "lmcache_temporalstore.grpc_kv.monotonic", side_effect=now,
        ), patch("lmcache_temporalstore.grpc_kv.sleep", side_effect=wait) as backoff:
            self.assertEqual(backend.lengths(keys), [4, 0])

        first, retry = client.batch_execute.call_args_list
        self.assertEqual(retry.args[0], first.args[0][1:])
        self.assertTrue(0 < retry.kwargs["timeout_seconds"] < 0.04)
        backoff.assert_called_once()
        self.assertLess(backoff.call_args.args[0], 0.025)

    def test_proxy_lookup_keeps_hits_and_retries_only_stale_results(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from temporalstore_sdk import MetadataError

        backend = TableValueBackend(
            "grpc://example.test", "kvcache", 5, 1, 0,
            proxy_grpc_target="127.0.0.1:1",
        )
        self.addCleanup(backend.close)
        client = Mock()

        def partial_results():
            return [
                pb.CommandResponse(status=pb.Status(code="no_grpc_primary")),
                pb.CommandResponse(status=pb.Status(ok=True), count=7),
                pb.CommandResponse(status=pb.Status(code="raft_not_leader")),
            ]

        client.batch_execute.side_effect = [
            partial_results(),
            [pb.CommandResponse(status=pb.Status(ok=True), count=4)],
            [pb.CommandResponse(status=pb.Status(code="permission_denied"))],
        ]
        keys = [Key("unavailable"), Key("healthy"), Key("stale")]
        for key in keys:
            key.model_name, key.kv_rank = "fixture/model", 0
        with patch.object(backend, "_table", return_value=client), patch(
            "lmcache_temporalstore.grpc_kv.monotonic", side_effect=[10.0, 10.25, 11.0],
        ):
            self.assertEqual(backend.lengths(keys), [0, 7, 4])
            with self.assertRaisesRegex(MetadataError, "permission_denied"):
                backend.lengths(keys[:1])
        first, second, _ = client.batch_execute.call_args_list
        self.assertEqual(first.kwargs, {"timeout_seconds": 1})
        self.assertEqual(second.kwargs, {"timeout_seconds": 0.75})
        self.assertEqual(second.args[0], first.args[0][2:])

        client.reset_mock()
        client.batch_execute.side_effect = [
            partial_results(),
            MetadataError("deadline_exceeded", "stale shard did not reply"),
        ]
        with patch.object(backend, "_table", return_value=client), patch(
            "lmcache_temporalstore.grpc_kv.monotonic", side_effect=[20.0, 20.25],
        ):
            self.assertEqual(backend.lengths(keys), [0, 7, 0])
        first, second = client.batch_execute.call_args_list
        self.assertEqual(first.kwargs, {"timeout_seconds": 1})
        self.assertEqual(second.kwargs, {"timeout_seconds": 0.75})
        self.assertEqual(second.args[0], first.args[0][2:])

    def test_table_store_accounts_for_confirmed_chunks_when_a_shard_fails(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from temporalstore_sdk import MetadataError

        adapter = adapter_module.TemporalStoreL2Adapter({"storage_backend": "kv"})
        self.addCleanup(adapter.close)
        adapter._values.close()
        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 1, 0)
        adapter._values = backend
        keys = [Key("created"), Key("unconfirmed")]
        for key in keys:
            key.model_name, key.kv_rank = "fixture/model", 0

        pending = []

        def replies(_):
            pending.append(adapter.report_status())
            yield 0, pb.CommandResponse(status=pb.Status(ok=True), count=1)
            raise MetadataError("raft_error", "leader changed during the batch")

        client = Mock()
        client.iter_batch_responses.side_effect = replies
        with patch.object(backend, "_table", return_value=client), self.assertLogs(level="ERROR"):
            task = adapter.submit_store_task(keys, [Memory(b"abc"), Memory(b"defg")])
            adapter._store_pool.submit(lambda: None).result(timeout=2)
            result = adapter.pop_completed_store_tasks()[task]

        self.assertFalse(result.success)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["store_tasks_submitted"], 1)
        self.assertEqual(pending[0]["store_keys_submitted"], 2)
        self.assertEqual(pending[0]["store_tasks_pending"], 1)
        self.assertEqual(pending[0]["stores"], 0)
        self.assertEqual(pending[0]["store_tasks_failed"], 0)
        status = adapter.report_status()
        self.assertEqual(status["stores"], 1)
        self.assertEqual(status["store_tasks_submitted"], 1)
        self.assertEqual(status["store_keys_submitted"], 2)
        self.assertEqual(status["store_tasks_pending"], 0)
        self.assertEqual(status["store_tasks_failed"], 1)
        self.assertEqual(adapter.report_status()["bytes_used"], 3)
        self.assertEqual(adapter._entries, {keys[0]: 3})
        client.iter_batch_responses.assert_called_once()

    def test_store_readback_requires_exact_bytes_after_an_uncertain_reply(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        for stored, recovered in ((b"tensor", True), (b"wrong!", False), (b"", False)):
            with self.subTest(stored=stored):
                adapter = adapter_module.TemporalStoreL2Adapter({"storage_backend": "kv"})
                self.addCleanup(adapter.close)
                adapter._values.close()
                backend = TableValueBackend("grpc://example.test", "kvcache", 5, 1, 0)
                adapter._values = backend
                key = Key("uncertain")
                key.model_name, key.kv_rank = "fixture/model", 0
                client = Mock()
                reply = pb.CommandResponse(status=pb.Status(code="raft_error", message="leader changed"))
                client.iter_batch_responses.return_value = iter([(0, reply)])
                client.batch_execute.return_value = [
                    pb.CommandResponse(status=pb.Status(ok=True), value=stored),
                ]

                with patch.object(backend, "_table", return_value=client):
                    task = adapter.submit_store_task([key], [Memory(b"tensor")])
                    adapter._store_pool.submit(lambda: None).result(timeout=2)
                    result = adapter.pop_completed_store_tasks()[task]

                self.assertEqual(result.success, recovered)
                status = adapter.report_status()
                self.assertEqual(status["stores_confirmed_by_read"], int(recovered))
                self.assertEqual(status["store_tasks_failed"], int(not recovered))
                self.assertEqual(status["stores"], 0)
                self.assertEqual(status["bytes_used"], 0)
                client.iter_batch_responses.assert_called_once()
                self.assertEqual(client.batch_execute.call_count, 2 if not stored else 1)
                put = client.iter_batch_responses.call_args.args[0][0]
                get = client.batch_execute.call_args.args[0][0]
                self.assertEqual(put.string_set_conditional.key, get.string_get.key)
                self.assertLessEqual(client.batch_execute.call_args.kwargs["timeout_seconds"], 1)

    def test_unclassified_remote_timeout_preserves_the_error_without_readback(self):
        import grpc

        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        class RemoteTimeout(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.CANCELLED

            def details(self):
                return "Timeout%20expired"

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 1, 0)
        self.addCleanup(backend.close)
        key = Key("timeout")
        key.model_name, key.kv_rank = "fixture/model", 0
        client = Mock()
        client.iter_batch_responses.side_effect = RemoteTimeout
        client.batch_execute.return_value = [
            pb.CommandResponse(status=pb.Status(ok=True), value=b"tensor"),
        ]
        with patch.object(backend, "_table", return_value=client):
            with self.assertRaises(RemoteTimeout):
                list(backend.iter_store_results([(key, memoryview(b"tensor"))]))
        self.assertEqual(backend.stores_confirmed_by_read, 0)
        client.iter_batch_responses.assert_called_once()
        client.batch_execute.assert_not_called()

    def test_shared_store_timeout_does_not_confirm_an_insert_by_local_read(self):
        from concurrent.futures import ThreadPoolExecutor

        import grpc
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from temporalstore.v1 import temporalstore_pb2_grpc as rpc
        from temporalstore_sdk import MetadataError
        from temporalstore_sdk.metadata import Shard, TableTopology

        class LocalOwner(rpc.TemporalStoreServiceServicer):
            def __init__(self):
                self.payload = b""
                self.reads = 0

            def BatchExecuteChecked(self, request, context):
                command, = request.commands
                if command.HasField("string_set_conditional"):
                    self.payload = command.string_set_conditional.value
                    context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "shared publication timed out")
                self.reads += 1
                return pb.BatchExecuteResponse(
                    status=pb.Status(ok=True),
                    responses=[pb.CommandResponse(status=pb.Status(ok=True), value=self.payload)],
                )

        owner = LocalOwner()
        with ThreadPoolExecutor(max_workers=1) as pool:
            server = grpc.server(pool)
            rpc.add_TemporalStoreServiceServicer_to_server(owner, server)
            port = server.add_insecure_port("127.0.0.1:0")
            self.assertGreater(port, 0)
            server.start()
            try:
                backend = TableValueBackend("grpc://example.test", "kvcache", 2, 1, 0)
                try:
                    shard = Shard(73, None, (), 7, f"127.0.0.1:{port}")
                    topology = TableTopology("kvcache", "model", 1, 73, 1, (shard,))
                    key = Key("locally-applied")
                    key.model_name, key.kv_rank = "fixture/model", 0
                    with patch.object(backend._metadata, "table_topology", return_value=topology):
                        with self.assertRaisesRegex(MetadataError, "write_durability_unconfirmed"):
                            list(backend.iter_store_results([(key, memoryview(b"tensor"))]))
                    self.assertEqual(backend.stores_confirmed_by_read, 0)
                finally:
                    backend.close()
            finally:
                self.assertTrue(server.stop(0).wait(2))
        self.assertEqual(owner.payload, b"tensor")
        self.assertEqual(owner.reads, 0)

    def test_store_readback_waits_for_a_new_leader_without_writing_again(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from temporalstore_sdk import RaftPrimaryUnavailable

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 2, 0)
        self.addCleanup(backend.close)
        key = Key("leader-change")
        key.model_name, key.kv_rank = "fixture/model", 0
        client = Mock()
        client.iter_batch_responses.return_value = iter([
            (0, pb.CommandResponse(status=pb.Status(code="write_outcome_unknown"))),
        ])
        client.batch_execute.side_effect = [
            RaftPrimaryUnavailable(73),
            [pb.CommandResponse(status=pb.Status(code="raft_not_leader"))],
            [pb.CommandResponse(status=pb.Status(ok=True), value=b"tensor")],
        ]
        with patch.object(backend, "_table", return_value=client), patch(
            "lmcache_temporalstore.grpc_kv.monotonic", side_effect=[0, 0, 0, 0.25, 0.25, 0.5],
        ), patch("lmcache_temporalstore.grpc_kv.sleep") as wait:
            results = list(backend.iter_store_results([(key, memoryview(b"tensor"))]))

        self.assertEqual(results, [(0, False)])
        self.assertEqual(backend.stores_confirmed_by_read, 1)
        client.iter_batch_responses.assert_called_once()
        self.assertEqual(client.batch_execute.call_count, 3)
        self.assertEqual(wait.call_count, 2)
        self.assertEqual(
            [call.kwargs["timeout_seconds"] for call in client.batch_execute.call_args_list],
            [2, 1.75, 1.5],
        )

    def test_unavailable_shard_does_not_use_other_writes_readback_time(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from lmcache.v1.distributed.key_codec import encode_storage_key
        from temporalstore_sdk import MetadataError, RaftPrimaryUnavailable

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 3, 0)
        self.addCleanup(backend.close)
        keys = [Key("unavailable-shard"), Key("available-shard"), Key("later-shard")]
        for key in keys:
            key.model_name, key.kv_rank = "fixture/model", 0
        client = Mock()
        client.iter_batch_responses.return_value = iter([
            (0, pb.CommandResponse(status=pb.Status(code="raft_error", message="old leader"))),
            (1, pb.CommandResponse(status=pb.Status(code="write_outcome_unknown"))),
            (2, pb.CommandResponse(status=pb.Status(code="write_outcome_unknown"))),
        ])
        now = 0.0

        def advance(seconds):
            nonlocal now
            now += seconds

        def read(commands, *, timeout_seconds):
            if commands[0].string_get.key == encode_storage_key(keys[0]):
                raise RaftPrimaryUnavailable(73)
            if commands[0].string_get.key == encode_storage_key(keys[1]):
                self.assertEqual(timeout_seconds, 1)
                advance(0.25)
                return [pb.CommandResponse(status=pb.Status(ok=True), value=b"tensor")]
            self.assertEqual(commands[0].string_get.key, encode_storage_key(keys[2]))
            self.assertEqual(timeout_seconds, 1.75)
            return [pb.CommandResponse(status=pb.Status(ok=True), value=b"later")]

        client.batch_execute.side_effect = read
        with patch.object(backend, "_table", return_value=client), patch(
            "lmcache_temporalstore.grpc_kv.monotonic", side_effect=lambda: now,
        ), patch("lmcache_temporalstore.grpc_kv.sleep", side_effect=advance):
            chunks = [(key, memoryview(payload)) for key, payload in zip(keys, (b"old", b"tensor", b"later"), strict=True)]
            results = backend.iter_store_results(chunks)
            self.assertEqual(next(results), (1, False))
            self.assertEqual(next(results), (2, False))
            with self.assertRaisesRegex(MetadataError, "old leader"):
                next(results)

        self.assertEqual(now, 1.25)
        self.assertEqual(backend.stores_confirmed_by_read, 2)
        client.iter_batch_responses.assert_called_once()

    def test_uncertain_write_can_become_visible_after_the_first_readback(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 2, 0)
        self.addCleanup(backend.close)
        key = Key("pending-apply")
        key.model_name, key.kv_rank = "fixture/model", 0
        client = Mock()
        client.iter_batch_responses.return_value = iter([
            (0, pb.CommandResponse(status=pb.Status(code="raft_error", message="leader changed"))),
        ])
        client.batch_execute.side_effect = [
            [pb.CommandResponse(status=pb.Status(ok=True), value_present=False)],
            [pb.CommandResponse(status=pb.Status(ok=True), value=b"tensor", value_present=True)],
        ]
        with patch.object(backend, "_table", return_value=client), patch(
            "lmcache_temporalstore.grpc_kv.monotonic", side_effect=[0, 0, 0, 0.25],
        ), patch("lmcache_temporalstore.grpc_kv.sleep") as wait:
            results = list(backend.iter_store_results([(key, memoryview(b"tensor"))]))

        self.assertEqual(results, [(0, False)])
        self.assertEqual(backend.stores_confirmed_by_read, 1)
        client.iter_batch_responses.assert_called_once()
        wait.assert_called_once_with(0.25)
        self.assertEqual(
            [call.kwargs["timeout_seconds"] for call in client.batch_execute.call_args_list],
            [2, 1.75],
        )
        for call in client.batch_execute.call_args_list:
            self.assertTrue(call.args[0][0].HasField("string_get"))

    def test_large_raft_store_readback_uses_a_borrowed_reply_and_matches_every_byte(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from temporalstore_sdk import MetadataError
        from temporalstore_sdk.cache_load_wire import LoadReply

        backend = TableValueBackend("grpc://127.0.0.1:1", "kvcache", 3, 1, 0)
        self.addCleanup(backend.close)
        key = Key("uncertain-raft-insert")
        key.model_name, key.kv_rank = "fixture/model", 0
        payload = b"\x00\x80\xff\x19" * (128 * 1024)

        for equal in (True, False):
            with self.subTest(equal=equal):
                client = Mock()
                client.iter_batch_responses.return_value = iter([
                    (0, pb.CommandResponse(status=pb.Status(code="write_outcome_unknown"))),
                ])
                returned = payload if equal else payload[:-1] + b"\x18"
                client.batch_execute.return_value = [
                    LoadReply(pb.Status(ok=True), memoryview(returned), True),
                ]
                with patch.object(backend, "_table", return_value=client):
                    inserts = backend.iter_store_results([(key, memoryview(payload))])
                    if equal:
                        self.assertEqual(list(inserts), [(0, False)])
                    else:
                        with self.assertRaisesRegex(MetadataError, "write_outcome_unknown"):
                            list(inserts)
                client.iter_batch_responses.assert_called_once()
                client.batch_execute.assert_called_once()
                request = client.batch_execute.call_args
                self.assertTrue(request.kwargs["borrowed_load"])
                self.assertEqual(request.args[0][0].string_get.key, key.name)
        self.assertEqual(backend.stores_confirmed_by_read, 1)

    def test_proxy_unknown_durability_never_confirms_through_readback(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from temporalstore_sdk import MetadataError

        backend = TableValueBackend(
            "grpc://example.test", "kvcache", 5, 1, 0, proxy_grpc_target="proxy.test:17200",
        )
        self.addCleanup(backend.close)
        key = Key("proxy-leader-change")
        key.model_name, key.kv_rank = "fixture/model", 0
        client = Mock()
        client.iter_batch_responses.return_value = iter([
            (0, pb.CommandResponse(status=pb.Status(code="write_outcome_unknown"))),
        ])
        with patch.object(backend, "_table", return_value=client):
            with self.assertRaisesRegex(MetadataError, "write_outcome_unknown"):
                list(backend.iter_store_results([(key, memoryview(b"tensor"))]))
        client.iter_batch_responses.assert_called_once()
        client.batch_execute.assert_not_called()
        self.assertEqual(backend.stores_confirmed_by_read, 0)


    def test_proxy_write_unknown_or_cancelled_is_never_confirmed_by_a_read(self):
        from concurrent.futures import ThreadPoolExecutor

        import grpc
        from temporalstore.v1.temporalstore_pb2_grpc import (
            ProxyServiceServicer, add_ProxyServiceServicer_to_server,
        )
        from temporalstore_sdk import MetadataError

        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        payload = bytes(range(256)) * (36 * 1024)

        class Proxy(ProxyServiceServicer):
            def __init__(self, code):
                self.code = code
                self.commands = []
                self.value = None

            def ProxyExecuteStream(self, requests, context):
                for request in requests:
                    replies = []
                    for command in request.batch_execute.commands:
                        kind = command.WhichOneof("kind")
                        self.commands.append(kind)
                        if kind == "string_set_conditional":
                            self.value = command.string_set_conditional.value
                            if self.code == "cancelled":
                                context.abort(grpc.StatusCode.CANCELLED, "grpc_timeout")
                            replies.append(pb.CommandResponse(status=pb.Status(code=self.code)))
                        elif kind == "string_get":
                            replies.append(pb.CommandResponse(
                                status=pb.Status(ok=True), value=self.value,
                            ))
                    yield pb.ProxyStreamEvent(
                        batch_execute=pb.BatchExecuteResponse(responses=replies),
                    )

        for code in ("write_outcome_unknown", "write_durability_unconfirmed", "cancelled"):
            with self.subTest(code=code), ThreadPoolExecutor(max_workers=2) as workers:
                proxy = Proxy(code)
                server = grpc.server(workers, options=[
                    ("grpc.max_receive_message_length", 16 * 1024 * 1024),
                    ("grpc.max_send_message_length", 16 * 1024 * 1024),
                ])
                add_ProxyServiceServicer_to_server(proxy, server)
                port = server.add_insecure_port("127.0.0.1:0")
                server.start()
                backend = TableValueBackend(
                    "grpc://127.0.0.1:1", "kvcache", 5, 1, 0,
                    proxy_grpc_target=f"127.0.0.1:{port}",
                )
                try:
                    key = Key("proxy-write")
                    key.model_name, key.kv_rank = "fixture/model", 0
                    error_type = grpc.RpcError if code == "cancelled" else MetadataError
                    with self.assertRaises(error_type) as outcome:
                        list(backend.iter_store_results([(key, memoryview(payload))]))
                    if code == "cancelled":
                        self.assertEqual(outcome.exception.code(), grpc.StatusCode.CANCELLED)
                    else:
                        self.assertEqual(outcome.exception.code, code)
                    self.assertEqual(proxy.value, payload)
                    self.assertEqual(proxy.commands, ["string_set_conditional"])
                    self.assertEqual(backend.stores_confirmed_by_read, 0)
                finally:
                    backend.close()
                    server.stop(0).wait()

    def test_interrupted_store_reconciles_only_on_direct_route(self):
        from contextlib import closing

        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from temporalstore_sdk import MetadataError

        keys = [Key("responded"), Key("interrupted")]
        for key in keys:
            key.model_name, key.kv_rank = "fixture/model", 0

        def replies():
            yield 0, pb.CommandResponse(status=pb.Status(code="write_outcome_unknown"))
            raise MetadataError("invalid_batch", "incomplete replies")

        chunks = [(keys[0], memoryview(b"tensor")), (keys[1], memoryview(b"other"))]
        for proxy_target in (None, "proxy.test:17200"):
            client = Mock()
            client.iter_batch_responses.return_value = replies()
            client.batch_execute.return_value = [pb.CommandResponse(status=pb.Status(ok=True), value=b"tensor")]
            with self.subTest(proxy_target=proxy_target), closing(TableValueBackend(
                "grpc://example.test", "kvcache", 5, 1, 0, proxy_grpc_target=proxy_target,
            )) as backend, patch.object(backend, "_table", return_value=client):
                results = backend.iter_store_results(chunks)
                if proxy_target is None:
                    self.assertEqual(next(results), (0, False))
                with self.assertRaisesRegex(MetadataError, "incomplete replies"):
                    next(results)
                client.iter_batch_responses.assert_called_once()
                if proxy_target is None:
                    self.assertEqual(backend.stores_confirmed_by_read, 1)
                    client.batch_execute.assert_called_once()
                    write = client.iter_batch_responses.call_args.args[0][0]
                    read = client.batch_execute.call_args.args[0][0]
                    self.assertEqual(read.string_get.key, write.string_set_conditional.key)
                else:
                    self.assertEqual(backend.stores_confirmed_by_read, 0)
                    client.batch_execute.assert_not_called()

    def test_interrupted_store_preserves_acknowledged_results_and_reports_the_error(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, grpc, pb

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 1, 0)
        self.addCleanup(backend.close)
        keys = [Key("acknowledged"), Key("interrupted")]
        for key in keys:
            key.model_name, key.kv_rank = "fixture/model", 0
        error = grpc.RpcError("interrupted")
        chunks = [(key, memoryview(b"tensor")) for key in keys]

        def replies():
            yield 0, pb.CommandResponse(status=pb.Status(ok=True), count=1)
            raise error

        client = Mock()
        client.iter_batch_responses.return_value = replies()
        with patch.object(backend, "_table", return_value=client):
            results = backend.iter_store_results(chunks)
            self.assertEqual(next(results), (0, True))
            with self.assertRaises(grpc.RpcError) as caught:
                next(results)
            self.assertIs(caught.exception, error)
        self.assertEqual(backend.stores_confirmed_by_read, 0)
        client.iter_batch_responses.assert_called_once()
        client.batch_execute.assert_not_called()

    def test_lookup_and_store_share_a_table_client_with_distinct_deadlines(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 1, 0)
        self.addCleanup(backend.close)
        key = Key("shared-channel")
        key.model_name, key.kv_rank = "fixture/model", 0
        result = pb.CommandResponse(status=pb.Status(ok=True), count=1)
        with patch("lmcache_temporalstore.grpc_kv.TableClient") as create:
            client = create.return_value
            client.batch_execute.return_value = [result]
            client.iter_batch_responses.return_value = iter([(0, result)])
            self.assertEqual(backend.lengths([key]), [1])
            self.assertEqual(list(backend.iter_store_results([(key, memoryview(b"v"))])), [(0, True)])

        create.assert_called_once()
        self.assertEqual(create.call_args.kwargs["timeout_seconds"], 5)
        self.assertTrue(create.call_args.kwargs["parallel_reads"])
        self.assertEqual(client.batch_execute.call_args.kwargs, {
            "timeout_seconds": 1, "allow_partial_reads": True,
        })
        self.assertEqual(client.iter_batch_responses.call_args.kwargs, {})

    def test_direct_load_retries_an_unavailable_leader_with_the_remaining_timeout(self):
        import grpc

        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        class Disconnected(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 1, 0)
        self.addCleanup(backend.close)
        key = Key("leader-failover")
        key.model_name, key.kv_rank = "fixture/model", 0
        destination = bytearray(3)
        with patch("lmcache_temporalstore.grpc_kv.TableClient") as create:
            client = create.return_value
            client.batch_execute.side_effect = [
                Disconnected(),
                [pb.CommandResponse(status=pb.Status(ok=True), value=b"new")],
            ]
            self.assertEqual(list(backend.load_into([(key, memoryview(destination))])), [True])

        self.assertEqual(destination, b"new")
        first, retry = client.batch_execute.call_args_list
        self.assertEqual(first.args, retry.args)
        self.assertGreater(retry.kwargs["timeout_seconds"], 0)
        self.assertLessEqual(retry.kwargs["timeout_seconds"], 5)

    def test_borrowed_insert_batch_budget_counts_tensor_bytes(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from temporalstore_sdk.cache_wire import CacheInsert

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 2, 0)
        self.addCleanup(backend.close)
        keys = [Key("first"), Key("second")]
        for key in keys:
            key.model_name, key.kv_rank = "fixture/model", 0
        tensor_bytes = 8 * 1024 * 1024
        tensor = memoryview(bytearray(tensor_bytes)).cast("B", shape=(1, tensor_bytes))
        self.assertEqual((len(tensor), tensor.nbytes), (1, tensor_bytes))
        success = pb.CommandResponse(status=pb.Status(ok=True), count=1)
        client = Mock()
        client.iter_batch_responses.side_effect = [
            iter([(0, success)]), iter([(0, success)]),
        ]
        with patch.object(backend, "_table", return_value=client):
            self.assertEqual(list(backend.iter_store_results([(key, tensor) for key in keys])),
                             [(0, True), (1, True)])
        sent = [call.args[0] for call in client.iter_batch_responses.call_args_list]
        self.assertEqual([len(batch) for batch in sent], [1, 1])
        self.assertTrue(all(isinstance(batch[0], CacheInsert) and batch[0].value.nbytes == tensor_bytes
                            for batch in sent))

    def test_grpc_load_budgets_and_fills_shaped_tensor_views(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from temporalstore_sdk.cache_load_wire import LoadReply

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 1, 0)
        self.addCleanup(backend.close)
        keys = [Key("first"), Key("second")]
        tensor_bytes = 3 * 1024 * 1024
        outputs = [bytearray(tensor_bytes) for _ in keys]
        targets = [memoryview(output).cast("B", shape=(3, tensor_bytes // 3)) for output in outputs]
        self.assertEqual([len(target) for target in targets], [3, 3])
        payloads = [b"a" * tensor_bytes, b"b" * tensor_bytes]
        status = pb.Status(ok=True)
        replies = [LoadReply(status, memoryview(payload), True) for payload in payloads]
        with patch.object(backend, "_iter_responses", side_effect=[
            iter([(0, {}, replies[0])]), iter([(0, {}, replies[1])]),
        ]) as reads:
            result = list(backend.load_into(list(zip(keys, targets, strict=True))))
        self.assertEqual(result, [True, True])
        self.assertEqual(outputs, payloads)
        self.assertEqual([len(call.args[0]) for call in reads.call_args_list], [1, 1])
        self.assertTrue(all(call.kwargs["borrowed_load"] for call in reads.call_args_list))

    def test_metaserver_selects_distributed_kv_by_default(self):
        config = adapter_module.TemporalStoreL2AdapterConfig
        self.assertEqual(config.from_dict({"meta_url": "grpc://example.test"}).storage_backend, "kv")
        self.assertEqual(config(meta_url="grpc://example.test").storage_backend, "kv")
        self.assertEqual(config.from_dict({}).storage_backend, "blob")
        with self.assertRaisesRegex(ValueError, "grpc:// or grpcs://"):
            adapter_module.TemporalStoreL2Adapter({"meta_url": "http://example.test"})
        with self.assertRaisesRegex(ValueError, "requires the kv backend"):
            adapter_module.TemporalStoreL2Adapter({
                "meta_url": "grpc://example.test", "storage_backend": "blob",
            })

    def test_table_layouts_share_one_descriptor_key_and_follow_table_identity(self):
        import json
        from types import SimpleNamespace

        from lmcache_temporalstore.grpc_kv import TABLE_DESCRIPTOR_KEY, TableValueBackend, pb

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 1, 0)
        self.addCleanup(backend.close)
        first, second = Key("first"), Key("second")
        for key in (first, second):
            key.model_name = "fixture/model"
            key.kv_rank = (4 << 24) | (2 << 8)
            key.object_group_id = 0
        metadata = SimpleNamespace(
            shape=(16, 32), dtype="torch.float16", shapes=None, dtypes=None,
            fmt=SimpleNamespace(value=2),
        )
        client = Mock()
        client.table_identity.return_value = (7, None)
        result = pb.CommandResponse(status=pb.Status(ok=True))
        client.iter_batch_responses.side_effect = lambda _: iter([(0, result)])

        with patch.object(backend, "_table", return_value=client):
            backend.register_layouts({first: metadata, second: metadata})
            backend.register_layouts({second: metadata})
            client.iter_batch_responses.assert_called_once()
            command = client.iter_batch_responses.call_args.args[0][0].hash_multi_set
            self.assertEqual(command.key, TABLE_DESCRIPTOR_KEY)
            self.assertEqual(len(command.entries), 1)
            self.assertEqual(json.loads(command.entries[0].value), {
                "model": "fixture/model", "world_size": 4, "local_world_size": 2,
                "rank": first.kv_rank, "group": 0, "shapes": [[16, 32]],
                "dtypes": ["torch.float16"], "memory_format": 2,
            })

            second.object_group_id = 1
            backend.register_layouts({second: metadata})
            second_command = client.iter_batch_responses.call_args.args[0][0].hash_multi_set
            self.assertEqual(second_command.key, TABLE_DESCRIPTOR_KEY)
            self.assertNotEqual(command.entries[0].field, second_command.entries[0].field)

            client.table_identity.return_value = (8, None)
            backend.register_layouts({first: metadata})
            self.assertEqual(client.iter_batch_responses.call_count, 3)
            self.assertEqual(client.iter_batch_responses.call_args.args[0][0].hash_multi_set, command)

    def test_table_store_waits_for_an_elected_primary_before_sending(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from temporalstore_sdk import RaftPrimaryUnavailable

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 1, 0)
        client = Mock()
        client.iter_batch_responses.side_effect = [
            RaftPrimaryUnavailable(73),
            iter([(0, pb.CommandResponse(status=pb.Status(ok=True), count=1))]),
        ]
        key = Key("ready-after-election")
        key.model_name, key.kv_rank = "fixture/model", 0
        with patch.object(backend, "_table", return_value=client), patch(
            "lmcache_temporalstore.grpc_kv.sleep"
        ) as wait:
            result = list(backend.iter_store_results([(key, memoryview(b"payload"))]))
        self.assertEqual(result, [(0, True)])
        self.assertEqual(client.iter_batch_responses.call_count, 2)
        wait.assert_called_once_with(0.25)

    def test_store_retry_uses_the_configured_budget_and_remaining_rpc_timeout(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from temporalstore_sdk import MetadataError

        backend = TableValueBackend(
            "grpc://example.test", "kvcache", 30, 1, 0, store_retry_timeout_seconds=12,
        )
        self.addCleanup(backend.close)
        client = Mock()
        rejected = pb.CommandResponse(status=pb.Status(code="raft_write_not_started"))
        client.iter_batch_responses.side_effect = [iter([(0, rejected)]), iter([(0, rejected)])]
        command = pb.Command(string_set=pb.StringSet(key="key", value=b"payload"))
        with patch(
            "lmcache_temporalstore.grpc_kv.monotonic", side_effect=[10.0, 17.0, 22.0],
        ), patch("lmcache_temporalstore.grpc_kv.sleep") as wait:
            with self.assertRaises(MetadataError) as outcome:
                list(backend._store_responses(client, [command]))

        self.assertEqual(outcome.exception.code, "raft_write_not_started")
        first, retry = client.iter_batch_responses.call_args_list
        self.assertEqual(first.kwargs, {})
        self.assertEqual(retry.kwargs, {"timeout_seconds": 5.0})
        self.assertEqual(first.args, retry.args)
        wait.assert_called_once_with(0.25)

    def test_table_store_retries_only_writes_rejected_before_proposal(self):
        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb

        backend = TableValueBackend("grpc://example.test", "kvcache", 5, 1, 0)
        self.addCleanup(backend.close)
        client = Mock()
        success = pb.CommandResponse(status=pb.Status(ok=True), count=1)
        rejected = pb.CommandResponse(status=pb.Status(code="raft_write_not_started"))
        fenced = pb.CommandResponse(status=pb.Status(code="load_version_mismatch"))
        ambiguous = pb.CommandResponse(status=pb.Status(code="write_outcome_unknown"))
        client.iter_batch_responses.side_effect = [
            iter([(1, rejected), (2, ambiguous), (0, success), (3, fenced)]),
            iter([(0, success), (1, success)]),
        ]
        commands = [pb.Command(string_set=pb.StringSet(key=str(i), value=b"value"))
                    for i in range(4)]
        with patch("lmcache_temporalstore.grpc_kv.sleep") as wait:
            self.assertEqual(list(backend._store_responses(client, commands)), [
                (2, ambiguous), (0, success), (1, success), (3, success),
            ])
        self.assertEqual(client.iter_batch_responses.call_args.args[0], [commands[1], commands[3]])
        wait.assert_called_once_with(0.25)

    def test_cache_load_releases_each_copied_response_before_yielding(self):
        from weakref import ref

        from lmcache_temporalstore.grpc_kv import TableValueBackend, pb
        from lmcache_temporalstore.kv import ValueBackend

        class Response(dict):
            pass

        for mode in ("local", "direct", "proxy"):
            with self.subTest(mode=mode):
                if mode == "local":
                    backend = ValueBackend(Mock(), Mock(), 0)
                else:
                    backend = TableValueBackend(
                        "grpc://127.0.0.1:1", "kvcache", 2, 1, 0,
                        proxy_grpc_target="127.0.0.1:2" if mode == "proxy" else "",
                    )
                keys = [Key("first"), Key("second")]
                for key in keys:
                    key.model_name, key.kv_rank = "fixture/model", 0
                items = [(key, memoryview(bytearray(1024 * 1024))) for key in keys]
                held = []

                def local_replies(_):
                    responses = [Response(value=b"a" * len(items[0][1])),
                                 Response(value=b"b" * len(items[1][1]))]
                    held.extend(ref(x) for x in responses)
                    return responses

                def grpc_replies(entries, **_):
                    for index, (_, document) in enumerate(entries):
                        response = types.SimpleNamespace(
                            status=pb.Status(ok=True),
                            value=memoryview(bytes((97 + index,)) * len(items[index][1])),
                        )
                        held.append(ref(response.value))
                        yield index, document, response

                runner = "_execute" if mode == "local" else "_iter_responses"
                replies = local_replies if mode == "local" else grpc_replies
                try:
                    with patch.object(backend, runner, side_effect=replies):
                        results = backend.load_into(items)
                        self.assertTrue(next(results))
                        self.assertEqual(bytes(items[0][1]), b"a" * len(items[0][1]))
                        self.assertIsNone(held[0]())
                        self.assertIsNotNone(held[1]())
                        self.assertTrue(next(results))
                        self.assertEqual(bytes(items[1][1]), b"b" * len(items[1][1]))
                        self.assertIsNone(held[1]())
                        with self.assertRaises(StopIteration):
                            next(results)
                finally:
                    backend.close()

    def test_kv_backend_keeps_one_binary_value_and_rejects_wrong_destination_size(self):
        adapter = adapter_module.TemporalStoreL2Adapter({"storage_backend": "kv"})
        self.addCleanup(adapter.close)
        key = Key("kv-chunk")
        commands = []

        def batch_execute(batch):
            answers = []
            for command in batch:
                commands.append(command["kind"])
                name = command["key"]
                kind = command["kind"]
                values = adapter._data.values
                if kind == "string_set_conditional":
                    self.assertEqual(command["condition"], "if_not_exists")
                    self.assertIsInstance(command["value"], memoryview)
                    created = name not in values
                    values.setdefault(name, bytes(command["value"]))
                    answers.append({"kind": "integer", "value": int(created)})
                elif kind == "string_len":
                    answers.append({"kind": "integer", "value": len(values.get(name, b""))})
                elif kind == "string_get":
                    answers.append({"kind": "bytes", "value": values.get(name)})
                else:
                    self.assertEqual(kind, "common_delete")
                    values.pop(name)
                    answers.append({"kind": "integer", "value": 1})
            return answers

        adapter._data.batch_execute = batch_execute
        payload = bytes((0, 128, 255))
        for data, expected in ((payload, len(payload)), (b"bad", 0)):
            task = adapter.submit_store_task([key], [Memory(data)])
            adapter._store_pool.submit(lambda: None).result(timeout=2)
            result = adapter.pop_completed_store_tasks()[task]
            self.assertTrue(result.success)
            self.assertEqual(result.transferred, expected)
        self.assertEqual(adapter._data.values, {"kv-chunk": payload})
        task = adapter.submit_lookup_and_lock_task([key, Key("unknown")], {})
        adapter._lookup_pool.submit(lambda: None).result(timeout=2)
        self.assertEqual(adapter.query_lookup_and_lock_result(task).bits, [True, False])

        too_short, exact = Memory(b"ab"), Memory(b"abc")
        task = adapter.submit_load_task([key, key], [too_short, exact])
        adapter._load_pool.submit(lambda: None).result(timeout=2)
        self.assertEqual(adapter.query_load_result(task).bits, [False, True])
        self.assertEqual(too_short.byte_array, b"ab")
        self.assertEqual(exact.byte_array, payload)
        adapter.submit_unlock([key])
        adapter.delete([key])
        adapter._store_pool.submit(lambda: None).result(timeout=2)
        self.assertEqual(adapter._data.values, {})
        self.assertEqual(commands.count("string_set_conditional"), 2)
        self.assertEqual(commands.count("common_delete"), 1)

    def test_store_lookup_load_and_capacity_eviction(self):
        adapter = self.adapter
        self.assertEqual(len({adapter.get_store_event_fd(),
                              adapter.get_lookup_and_lock_event_fd(),
                              adapter.get_load_event_fd()}), 3)
        first = Key("first")
        second = Key("second")
        task = adapter.submit_store_task([first], [Memory(b"\x00\xffA")])
        self.assertTrue(adapter._store_event.event.wait(2))
        self.assertTrue(adapter.pop_completed_store_tasks()[task].success)
        adapter._store_event.event.clear()

        lookup = adapter.submit_lookup_and_lock_task([first], {})
        self.assertTrue(adapter._lookup_event.event.wait(2))
        self.assertEqual(adapter.query_lookup_and_lock_result(lookup).bits, [True])
        self.assertIsNone(adapter.query_lookup_and_lock_result(lookup))
        adapter._lookup_event.event.clear()

        destination = Memory(b"---")
        load = adapter.submit_load_task([first], [destination])
        self.assertTrue(adapter._load_event.event.wait(2))
        self.assertEqual(adapter.query_load_result(load).bits, [True])
        self.assertEqual(destination.byte_array, b"\x00\xffA")
        adapter.submit_unlock([first])

        task = adapter.submit_store_task([second], [Memory(b"\x01\x02\x03")])
        self.assertTrue(adapter._store_event.event.wait(2))
        self.assertTrue(adapter.pop_completed_store_tasks()[task].success)
        self.assertEqual(adapter.report_status()["bytes_used"], 3)
        self.assertEqual(len(adapter._blob.values), 1)
        self.assertEqual(adapter.report_status()["stores"], 2)
        self.assertEqual(adapter.report_status()["loads"], 1)
        adapter.delete([second])
        adapter._store_pool.submit(lambda: None).result(timeout=2)
        self.assertEqual(adapter.report_status()["bytes_used"], 0)
        self.assertEqual(len(adapter._blob.values), 0)

    def test_capacity_eviction_batches_small_blobs_and_preserves_locked_keys(self):
        adapter = self.adapter
        keys = [Key(str(index)) for index in range(4)]
        task = adapter.submit_store_task(keys, [Memory(b"x") for _ in keys])
        self.assertTrue(adapter._store_event.event.wait(2))
        self.assertTrue(adapter.pop_completed_store_tasks()[task].success)
        adapter._store_event.event.clear()

        locked = keys[1]
        task = adapter.submit_lookup_and_lock_task([locked], {})
        self.assertTrue(adapter._lookup_event.event.wait(2))
        self.assertTrue(adapter.query_lookup_and_lock_result(task).test(0))

        removed = []
        delete_many = adapter._blob.delete_many

        def record(names):
            removed.append(list(names))
            return delete_many(names)

        adapter._blob.delete_many = record
        replacement = Key("large")
        task = adapter.submit_store_task([replacement], [Memory(b"abc")])
        self.assertTrue(adapter._store_event.event.wait(2))
        self.assertTrue(adapter.pop_completed_store_tasks()[task].success)

        self.assertEqual(removed, [[adapter._key_names(key)[0] for key in keys if key != locked]])
        self.assertEqual(adapter._blob.values, {
            adapter._key_names(locked)[0]: b"x",
            adapter._key_names(replacement)[0]: b"abc",
        })
        self.assertEqual(adapter.report_status()["bytes_used"], 4)
        self.assertEqual(adapter._evicting, set())
        adapter.submit_unlock([locked])

    def test_lookup_reserves_key_during_metadata_read(self):
        adapter = self.adapter
        first, second = Key("first"), Key("second")
        task = adapter.submit_store_task([first], [Memory(b"abc")])
        self.assertTrue(adapter._store_event.event.wait(2))
        self.assertTrue(adapter.pop_completed_store_tasks()[task].success)
        adapter._store_event.event.clear()

        snapshot_taken = threading.Event()
        release_lookup = threading.Event()
        batch_execute = adapter._data.batch_execute

        def pause_after_read(commands):
            answers = batch_execute(commands)
            if commands[0]["kind"] == "common_exists":
                snapshot_taken.set()
                if not release_lookup.wait(5):
                    raise TimeoutError("lookup was not released")
            return answers

        adapter._data.batch_execute = pause_after_read
        lookup = adapter.submit_lookup_and_lock_task([first], {})
        try:
            self.assertTrue(snapshot_taken.wait(2))
            task = adapter.submit_store_task([second], [Memory(b"def")])
            self.assertTrue(adapter._store_event.event.wait(2))
            self.assertFalse(adapter.pop_completed_store_tasks()[task].success)
        finally:
            release_lookup.set()

        self.assertTrue(adapter._lookup_event.event.wait(2))
        self.assertEqual(adapter.query_lookup_and_lock_result(lookup).bits, [True])
        adapter.submit_unlock([first])
        adapter._store_event.event.clear()
        task = adapter.submit_store_task([second], [Memory(b"def")])
        self.assertTrue(adapter._store_event.event.wait(2))
        self.assertTrue(adapter.pop_completed_store_tasks()[task].success)

    def test_delete_batches_metadata_for_unlocked_keys_with_deleted_blobs(self):
        adapter = self.adapter
        first, second, locked, failing = map(Key, ("first", "second", "locked", "failing"))
        keys = [first, second, locked, failing]
        task = adapter.submit_store_task(keys, [Memory(b"x") for _ in keys])
        adapter._store_pool.submit(lambda: None).result(timeout=2)
        self.assertTrue(adapter.pop_completed_store_tasks()[task].success)
        task = adapter.submit_lookup_and_lock_task([locked], {})
        self.assertTrue(adapter._lookup_event.event.wait(2))
        self.assertTrue(adapter.query_lookup_and_lock_result(task).test(0))

        delete_blob = adapter._blob.delete
        batch_execute = adapter._data.batch_execute
        batches = []

        def delete(name):
            if name == adapter._key_names(failing)[0]:
                raise OSError("blob is unreachable")
            delete_blob(name)

        def record(commands):
            batches.append(commands)
            return batch_execute(commands)

        adapter._blob.delete = delete
        adapter._data.batch_execute = record
        adapter.delete([first, locked, second, first, failing])
        adapter._store_pool.submit(lambda: None).result(timeout=2)

        self.assertEqual(batches, [[
            {"kind": "common_delete", "key": adapter._key_names(key)[1]}
            for key in (first, second)
        ]])
        self.assertEqual(adapter.report_status()["bytes_used"], 2)
        self.assertEqual(set(adapter._data.values), {adapter._key_names(k)[1] for k in (locked, failing)})
        self.assertEqual(adapter._evicting, set())
        adapter.submit_unlock([locked])

    def test_delete_retries_metadata_only_after_a_partial_batch_failure(self):
        adapter = self.adapter
        first, second = Key("first"), Key("second")
        task = adapter.submit_store_task([first, second], [Memory(b"x"), Memory(b"y")])
        adapter._store_pool.submit(lambda: None).result(timeout=2)
        self.assertTrue(adapter.pop_completed_store_tasks()[task].success)
        delete_blob = adapter._blob.delete
        execute = adapter._data.execute
        batch_execute = adapter._data.batch_execute
        blobs = []

        def count_delete(name):
            blobs.append(name)
            delete_blob(name)

        def partial(commands):
            execute(commands[0])
            raise TimeoutError("metadata reply is unavailable")

        def fail_second(command):
            if command["key"] == adapter._key_names(second)[1]:
                raise TimeoutError("metadata shard is unavailable")
            return execute(command)

        adapter._blob.delete = count_delete
        adapter._data.batch_execute = partial
        adapter._data.execute = fail_second
        adapter.delete([first, second])
        adapter._store_pool.submit(lambda: None).result(timeout=2)

        self.assertEqual(blobs, [adapter._key_names(k)[0] for k in (first, second)])
        self.assertEqual(adapter.report_status()["bytes_used"], 1)
        self.assertEqual(set(adapter._data.values), {adapter._key_names(second)[1]})
        self.assertEqual(adapter._evicting, set())

        adapter._data.batch_execute = batch_execute
        adapter._data.execute = execute
        adapter.delete([second])
        adapter._store_pool.submit(lambda: None).result(timeout=2)
        self.assertEqual(adapter.report_status()["bytes_used"], 0)
        self.assertEqual(adapter._data.values, {})

    def test_partial_metadata_batch_is_rolled_back(self):
        adapter = self.adapter
        first, second = Key("first"), Key("second")
        adapter._max_capacity_bytes = 8
        batch_execute = adapter._data.batch_execute

        def apply_one_then_fail(commands):
            self.assertEqual(len(commands), 2)
            batch_execute(commands[:1])
            raise OSError("injected metadata batch failure")

        adapter._data.batch_execute = apply_one_then_fail
        task = adapter.submit_store_task([first, second], [Memory(b"abc"), Memory(b"def")])
        self.assertTrue(adapter._store_event.event.wait(2))
        result = adapter.pop_completed_store_tasks()[task]
        self.assertFalse(result.success)
        self.assertEqual(result.transferred, 0)
        self.assertEqual(adapter._data.values, {})
        self.assertEqual(adapter._blob.values, {})
        self.assertEqual(adapter.report_status()["bytes_used"], 0)

    def test_failures_complete_as_cache_misses(self):
        adapter = self.adapter
        key = Key("failure")
        adapter._blob.fail_put = True
        task = adapter.submit_store_task([key], [Memory(b"abc")])
        self.assertTrue(adapter._store_event.event.wait(2))
        self.assertFalse(adapter.pop_completed_store_tasks()[task].success)
        adapter._blob.fail_put = False
        adapter._store_event.event.clear()
        task = adapter.submit_store_task([key], [Memory(b"abc")])
        self.assertTrue(adapter._store_event.event.wait(2))
        self.assertTrue(adapter.pop_completed_store_tasks()[task].success)
        adapter._blob.fail_get = True
        task = adapter.submit_load_task([key], [Memory(b"---")])
        self.assertTrue(adapter._load_event.event.wait(2))
        self.assertEqual(adapter.query_load_result(task).bits, [False])


if __name__ == "__main__":
    unittest.main()
