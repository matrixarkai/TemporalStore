# SPDX-License-Identifier: Apache-2.0
"""Exercise the adapter task contract with a local transport double."""

import importlib
import sys
import threading
import types
import unittest
from unittest.mock import patch


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

    def mutations_available(self):
        return True

    def defer_mutations(self):
        pass

    def put(self, name, payload):
        if self.fail_put:
            raise OSError("injected write failure")
        self.values[name] = bytes(payload)

    def put_many(self, items):
        written = []
        for name, payload in items:
            try:
                self.put(name, payload)
            except OSError:
                written.append(False)
            else:
                written.append(True)
        return written

    def get_into(self, name, destination):
        if self.fail_get:
            raise OSError("injected read failure")
        payload = self.values.get(name)
        if payload is None or len(payload) != len(destination):
            return False
        destination[:] = payload
        return True

    def get_many_into(self, items, *, staging=False):
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

    def probe_many(self, items):
        return [name in self.values and len(self.values[name]) == size for name, size in items]

    def inventory(self, namespace, _stop=None):
        prefix = f"lmcache/v2/{namespace}/"
        return (
            (bytes.fromhex(name.rsplit("/", 1)[1]), len(payload))
            for name, payload in self.values.items() if name.startswith(prefix)
        )

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

    def test_remote_lookup_pauses_failed_replica_and_resumes(self):
        for payload_metadata in (False, True):
            with self.subTest(payload_metadata=payload_metadata):
                adapter = adapter_module.TemporalStoreL2Adapter(dict(
                    remote_storage=True, lookup_timeout_seconds=0.5,
                    namespace="replica", recover_capacity=True, payload_metadata=payload_metadata,
                ))
                self.addCleanup(adapter.close)
                key = Key("retained")
                store = adapter.submit_store_task([key], [Memory(b"kv")])
                self.assertTrue(adapter._store_event.event.wait(2))
                self.assertTrue(adapter.pop_completed_store_tasks()[store].success)
                if payload_metadata:
                    peer, name = adapter._lookup_blob, "probe_many"
                    peer.values = adapter._blob.values
                else:
                    peer, name = adapter._lookup_data, "batch_execute"
                    peer.values = adapter._data.values
                real_probe = getattr(peer, name)
                attempts = []

                def probe(items):
                    attempts.append(items)
                    if len(attempts) == 1:
                        raise TimeoutError("unreachable replica")
                    return real_probe(items)

                now = 100.0

                def lookup():
                    adapter._lookup_event.event.clear()
                    task = adapter.submit_lookup_and_lock_task([key], {})
                    self.assertTrue(adapter._lookup_event.event.wait(2))
                    return adapter.query_lookup_and_lock_result(task).test(0)

                with patch.object(adapter_module, "monotonic", lambda: now):
                    with patch.object(peer, name, probe):
                        self.assertFalse(lookup())
                        self.assertFalse(lookup())
                        self.assertEqual(len(attempts), 1)
                        self.assertFalse(adapter._locked)
                        now += adapter_module._LOOKUP_FAILURE_COOLDOWN_SECONDS
                        self.assertTrue(lookup())
                        self.assertEqual(len(attempts), 2)
                        adapter.submit_unlock([key])
                        self.assertFalse(adapter._locked)

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
        closer = threading.Thread(target=adapter.close)
        closer.start()
        try:
            self.assertTrue(adapter._inventory_stop.wait(2))
        finally:
            release.set()
            closer.join(3)
        self.assertFalse(closer.is_alive())
        self.assertEqual(len(probes), 1)
        self.assertFalse(adapter._locked)
        for task_id in tasks:
            self.assertFalse(adapter.query_lookup_and_lock_result(task_id).test(0))
            self.assertIsNone(adapter.query_lookup_and_lock_result(task_id))

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

    def test_restart_recovers_usage_and_keeps_a_locked_entry(self):
        blob, data = FakeBlob(), FakeData()
        adapter_module.BlobClient = lambda *_args, **_kwargs: blob
        adapter_module.TemporalStoreClient = lambda *_args, **_kwargs: data
        config = adapter_module.TemporalStoreL2AdapterConfig(
            max_capacity_gb=4 / 1024**3, namespace="persistent", recover_capacity=True,
        )

        def write(adapter, key, payload):
            adapter._store_event.event.clear()
            task = adapter.submit_store_task([key], [Memory(payload)])
            self.assertTrue(adapter._store_event.event.wait(2))
            self.assertTrue(adapter.pop_completed_store_tasks()[task].success)

        first, locked, new = Key("a"), Key("b"), Key("c")
        with_adapter = adapter_module.TemporalStoreL2Adapter(config)
        try:
            write(with_adapter, first, b"ab")
            write(with_adapter, locked, b"cd")
        finally:
            with_adapter.close()

        reopened = adapter_module.TemporalStoreL2Adapter(config)
        try:
            self.assertEqual(reopened.report_status()["bytes_used"], 4)
            task = reopened.submit_lookup_and_lock_task([locked], {})
            self.assertTrue(reopened._lookup_event.event.wait(2))
            self.assertTrue(reopened.query_lookup_and_lock_result(task).test(0))
            write(reopened, new, b"ef")
            self.assertEqual(reopened.report_status()["bytes_used"], 4)
            self.assertEqual(blob.values, {
                reopened._key_names(locked)[0]: b"cd",
                reopened._key_names(new)[0]: b"ef",
            })
            self.assertNotIn(reopened._key_names(first)[1], data.values)
            reopened.submit_unlock([locked])
        finally:
            reopened.close()

    def test_empty_inventory_lookup_completes_without_worker(self):
        blob = FakeBlob()
        adapter_module.BlobClient = lambda *_args, **_kwargs: blob
        adapter = adapter_module.TemporalStoreL2Adapter(
            dict(namespace="empty", recover_capacity=True, payload_metadata=True)
        )
        try:
            def unexpected_submit(*_args):
                self.fail("empty lookup scheduled a worker")

            adapter._lookup_pool.submit = unexpected_submit
            task = adapter.submit_lookup_and_lock_task([Key("absent")], {})
            self.assertTrue(adapter._lookup_event.event.is_set())
            self.assertFalse(adapter.query_lookup_and_lock_result(task).test(0))
            self.assertIsNone(adapter.query_lookup_and_lock_result(task))
            self.assertEqual(adapter._locked, {})
        finally:
            adapter.close()

    def test_payload_metadata_recovers_without_metadata_commands(self):
        blob, data = FakeBlob(), FakeData()
        adapter_module.BlobClient = lambda *_args, **_kwargs: blob
        adapter_module.TemporalStoreClient = lambda *_args, **_kwargs: data
        calls = []
        data.batch_execute = lambda commands: calls.extend(commands)
        options = dict(max_capacity_gb=8 / 1024**3, namespace="persistent", recover_capacity=True)
        key = Key("payload")
        original = adapter_module.TemporalStoreL2Adapter(dict(options, payload_metadata=True))
        try:
            task = original.submit_store_task([key], [Memory(b"value")])
            self.assertTrue(original._store_event.event.wait(2))
            self.assertTrue(original.pop_completed_store_tasks()[task].success)
        finally:
            original.close()

        legacy = adapter_module.TemporalStoreL2Adapter(options)
        try:
            self.assertEqual(legacy.report_status()["bytes_used"], 0)
        finally:
            legacy.close()

        reopened = adapter_module.TemporalStoreL2Adapter(dict(options, payload_metadata=True))
        try:
            self.assertEqual(reopened.report_status()["bytes_used"], 5)
            task = reopened.submit_lookup_and_lock_task([key], {})
            self.assertTrue(reopened._lookup_event.event.wait(2))
            self.assertTrue(reopened.query_lookup_and_lock_result(task).test(0))
            reopened.submit_unlock([key])
            name, _ = reopened._key_names(key)
            blob.values[name] = b"x"
            reopened._lookup_event.event.clear()
            task = reopened.submit_lookup_and_lock_task([key], {})
            self.assertTrue(reopened._lookup_event.event.wait(2))
            self.assertFalse(reopened.query_lookup_and_lock_result(task).test(0))
            reopened.delete([key])
            reopened._store_pool.submit(lambda: None).result(timeout=2)
            self.assertEqual(reopened.report_status()["bytes_used"], 0)
            self.assertNotIn(name, blob.values)
            self.assertEqual(calls, [])
        finally:
            reopened.close()

    def test_payload_metadata_rejects_ttl_and_older_datanodes(self):
        options = dict(payload_metadata=True, namespace="isolated", recover_capacity=True)
        with self.assertRaisesRegex(ValueError, "ttl_seconds=0"):
            adapter_module.TemporalStoreL2Adapter(dict(options, ttl_seconds=1))
        blob = FakeBlob()

        def unsupported(_items):
            raise ValueError("payload_metadata requires a datanode with blob probes")

        blob.probe_many = unsupported
        adapter_module.BlobClient = lambda *_args, **_kwargs: blob
        with self.assertRaisesRegex(ValueError, "blob probes"):
            adapter_module.TemporalStoreL2Adapter(options)

    def test_close_stops_retry_for_an_unavailable_local_inventory(self):
        blobs, inventory = FakeBlob(), FakeBlob()
        clients = iter((blobs, inventory))
        adapter_module.BlobClient = lambda *_args, **_kwargs: next(clients)
        inventory_closed = threading.Event()

        def unavailable(_namespace, _stop):
            raise ConnectionRefusedError("local datanode is unavailable")

        inventory.inventory = unavailable
        inventory.close = inventory_closed.set
        config = adapter_module.TemporalStoreL2AdapterConfig(
            max_capacity_gb=4 / 1024**3, namespace="local",
            recover_capacity=True, remote_storage=False,
        )
        adapter = adapter_module.TemporalStoreL2Adapter(config)
        try:
            self.assertFalse(adapter.report_status()["inventory_ready"])
            self.assertTrue(adapter._inventory_thread.is_alive())
        finally:
            adapter.close()
        self.assertFalse(adapter._inventory_thread.is_alive())
        self.assertTrue(inventory_closed.is_set())

    def test_remote_inventory_retry_gates_operations_and_restores_capacity(self):
        blobs, inventory = FakeBlob(), FakeBlob()
        clients = iter((blobs, inventory))
        adapter_module.BlobClient = lambda *_args, **_kwargs: next(clients)
        available = threading.Event()
        key, replacement = Key("retained"), Key("replacement")
        digest = bytes.fromhex(adapter_module._names(key, "persistent")[0].rsplit("/", 1)[1])

        def restore(_namespace, _stop):
            yield digest, 4
            if not available.is_set():
                raise TimeoutError("replica has not returned")

        inventory.inventory = restore
        config = adapter_module.TemporalStoreL2AdapterConfig(
            max_capacity_gb=4 / 1024**3, namespace="persistent",
            recover_capacity=True, remote_storage=True,
        )
        adapter = adapter_module.TemporalStoreL2Adapter(config)
        name, metadata = adapter._key_names(key)
        blobs.values[name] = b"abcd"
        adapter._data.values[metadata] = b"4"
        adapter._lookup_data.values[metadata] = b"4"
        try:
            status = adapter.report_status()
            self.assertFalse(status["is_healthy"])
            self.assertFalse(status["inventory_ready"])
            self.assertEqual(status["inventory_error"], "TimeoutError")
            self.assertEqual(status["bytes_used"], 0)

            write = adapter.submit_store_task([replacement], [Memory(b"data")])
            self.assertTrue(adapter._store_event.event.wait(1))
            self.assertFalse(adapter.pop_completed_store_tasks()[write].success)
            lookup = adapter.submit_lookup_and_lock_task([key], {})
            self.assertTrue(adapter._lookup_event.event.wait(1))
            self.assertEqual(adapter.query_lookup_and_lock_result(lookup).bits, [False])
            load = adapter.submit_load_task([key], [Memory(b"....")])
            self.assertTrue(adapter._load_event.event.wait(1))
            self.assertEqual(adapter.query_load_result(load).bits, [False])
            adapter.delete([key])
            self.assertEqual(blobs.values, {name: b"abcd"})

            available.set()
            adapter._inventory_thread.join(timeout=3)
            self.assertFalse(adapter._inventory_thread.is_alive())
            status = adapter.report_status()
            self.assertTrue(status["is_healthy"])
            self.assertTrue(status["inventory_ready"])
            self.assertIsNone(status["inventory_error"])
            self.assertEqual(status["bytes_used"], 4)

            adapter._lookup_event.event.clear()
            lookup = adapter.submit_lookup_and_lock_task([key], {})
            self.assertTrue(adapter._lookup_event.event.wait(2))
            self.assertEqual(adapter.query_lookup_and_lock_result(lookup).bits, [True])
            adapter.submit_unlock([key])
            adapter._store_event.event.clear()
            write = adapter.submit_store_task([replacement], [Memory(b"data")])
            self.assertTrue(adapter._store_event.event.wait(2))
            self.assertTrue(adapter.pop_completed_store_tasks()[write].success)
            self.assertEqual(blobs.values, {adapter._key_names(replacement)[0]: b"data"})
            self.assertEqual(adapter.report_status()["bytes_used"], 4)
        finally:
            adapter.close()

    def test_unconfirmed_batch_is_charged_and_retried_as_one_cleanup(self):
        config = adapter_module.TemporalStoreL2AdapterConfig(
            max_capacity_gb=4 / 1024**3, namespace="persistent", recover_capacity=True,
        )
        adapter = adapter_module.TemporalStoreL2Adapter(config)
        keys = [Key("first"), Key("second")]
        names = [adapter._key_names(key)[0] for key in keys]
        put_many, delete_many = adapter._blob.put_many, adapter._blob.delete_many
        calls = []
        unavailable = True

        def lose_receipts(items):
            self.assertEqual(put_many(items), [True, True])
            raise OSError("lost upload receipts")

        def cleanup(targets):
            calls.append(targets)
            if unavailable:
                raise TimeoutError("cleanup datanode unavailable")
            return delete_many(targets)

        def write(success):
            adapter._store_event.event.clear()
            task = adapter.submit_store_task(keys, [Memory(b"ab"), Memory(b"cd")])
            self.assertTrue(adapter._store_event.event.wait(2))
            self.assertEqual(adapter.pop_completed_store_tasks()[task].success, success)

        try:
            adapter._blob.put_many = lose_receipts
            adapter._blob.delete_many = cleanup
            write(False)
            self.assertEqual(calls, [names])
            self.assertEqual(adapter.report_status()["bytes_used"], 4)
            self.assertEqual(len(adapter._uncertain), 2)

            unavailable = False
            adapter._blob.put_many = put_many
            write(True)
            self.assertEqual(calls, [names, names])
            self.assertEqual(adapter.report_status()["bytes_used"], 4)
            self.assertEqual(adapter._uncertain, set())
        finally:
            adapter.close()

    def test_unconfirmed_write_remains_charged_until_cleanup(self):
        config = adapter_module.TemporalStoreL2AdapterConfig(
            max_capacity_gb=4 / 1024**3, namespace="persistent", recover_capacity=True,
        )
        adapter = adapter_module.TemporalStoreL2Adapter(config)
        key, other = Key("first"), Key("other")
        put, delete = adapter._blob.put, adapter._blob.delete

        def lose_receipt(name, payload):
            put(name, payload)
            raise OSError("lost upload receipt")

        def fail_delete(_name):
            raise OSError("datanode is unavailable")

        def write(target, success):
            adapter._store_event.event.clear()
            task = adapter.submit_store_task([target], [Memory(b"abcd")])
            self.assertTrue(adapter._store_event.event.wait(2))
            self.assertEqual(adapter.pop_completed_store_tasks()[task].success, success)

        try:
            adapter._blob.put = lose_receipt
            adapter._blob.delete = fail_delete
            write(key, False)
            self.assertEqual(adapter.report_status()["bytes_used"], 4)
            adapter._blob.put = put
            write(other, False)
            self.assertEqual(adapter._blob.values, {adapter._key_names(key)[0]: b"abcd"})

            adapter._blob.delete = delete
            write(key, True)
            self.assertEqual(adapter.report_status()["bytes_used"], 4)
            self.assertEqual(adapter._uncertain, set())
        finally:
            adapter.close()

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

    def test_only_confirmed_uploads_publish_lookup_metadata(self):
        adapter = self.adapter
        first, second = Key("first"), Key("second")

        def one_write(items):
            name, payload = items[0]
            adapter._blob.put(name, payload)
            return [True, False]

        adapter._blob.put_many = one_write
        task = adapter.submit_store_task([first, second], [Memory(b"a"), Memory(b"bc")])
        self.assertTrue(adapter._store_event.event.wait(2))
        result = adapter.pop_completed_store_tasks()[task]
        self.assertFalse(result.success)
        self.assertEqual(adapter.report_status()["bytes_used"], 1)
        self.assertEqual(adapter.report_status()["stores"], 1)
        lookup = adapter.submit_lookup_and_lock_task([first, second], {})
        self.assertTrue(adapter._lookup_event.event.wait(2))
        self.assertEqual(adapter.query_lookup_and_lock_result(lookup).bits, [True, False])
        adapter.submit_unlock([first])

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
