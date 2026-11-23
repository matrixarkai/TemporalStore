# SPDX-License-Identifier: Apache-2.0
"""Exercise the adapter task contract with a local transport double."""

import importlib
import sys
import threading
import types
import unittest


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
    def __init__(self, *_args):
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

    def delete(self, name):
        self.values.pop(name, None)


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
