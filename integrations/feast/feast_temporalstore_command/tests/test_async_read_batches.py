# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI

import asyncio
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from feast_temporalstore_command.async_transport import AsyncReadHttpError
from feast_temporalstore_command.client import TemporalStoreClient, TemporalStoreCommandError

try:
    import msgpack
except ModuleNotFoundError:
    msgpack = None


@unittest.skipIf(msgpack is None, "msgpack is required")
class AsyncReadBatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = TemporalStoreClient(max_batch_commands=1, max_async_read_batches=3)
        self.client._compact_shared_field_read_supported = True
        self.tasks = []
        self.patches = []

    async def asyncTearDown(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for patch in self.patches:
            patch.stop()
        await self.client.aclose()

    def transport(self, read):
        transport = SimpleNamespace(read=mock.AsyncMock(side_effect=read))
        patch = mock.patch.object(self.client, "_get_async_read_transport", return_value=transport)
        patch.start()
        self.patches.append(patch)
        return transport

    def start(self, keys):
        task = asyncio.create_task(self.client.batch_hash_multi_get_async(keys, ["f"]))
        self.tasks.append(task)
        return task

    @staticmethod
    def key(body):
        return msgpack.unpackb(body, raw=False)["keys"][0]

    @staticmethod
    def payload(key):
        return msgpack.packb({"status": {"ok": True}, "rows": [[key.encode()]]}, use_bin_type=True)

    async def test_warm_batches_overlap_with_bounded_ordered_results(self):
        gates = {str(n): asyncio.Event() for n in range(7)}
        arrived, later_done = asyncio.Event(), asyncio.Event()
        active = peak = 0
        started, finished = [], []

        async def read(path, body):
            nonlocal active, peak
            key = self.key(body)
            started.append(key)
            active += 1
            peak = max(peak, active)
            if len(started) == 3:
                arrived.set()
            try:
                await gates[key].wait()
                finished.append(key)
                if len(finished) == 2:
                    later_done.set()
                return self.payload(key)
            finally:
                active -= 1

        self.transport(read)
        task = self.start(list(gates))
        await asyncio.wait_for(arrived.wait(), 5)
        self.assertEqual(started, ["0", "1", "2"])
        gates["2"].set()
        gates["1"].set()
        await asyncio.wait_for(later_done.wait(), 5)
        self.assertFalse(task.done())
        self.assertEqual(finished, ["2", "1"])
        for gate in gates.values():
            gate.set()
        self.assertEqual(await task, [[str(n).encode()] for n in range(7)])
        self.assertEqual((peak, active), (3, 0))

    async def test_unknown_route_probes_one_batch_before_overlap(self):
        self.client._compact_shared_field_read_supported = None
        first, window = asyncio.Event(), asyncio.Event()
        release_first, release_rest = asyncio.Event(), asyncio.Event()
        started = []

        async def read(path, body):
            key = self.key(body)
            started.append(key)
            if key == "0":
                first.set()
                await release_first.wait()
            else:
                if len(started) == 4:
                    window.set()
                await release_rest.wait()
            return self.payload(key)

        self.transport(read)
        task = self.start(["0", "1", "2", "3"])
        await asyncio.wait_for(first.wait(), 5)
        self.assertEqual(started, ["0"])
        release_first.set()
        await asyncio.wait_for(window.wait(), 5)
        release_rest.set()
        self.assertEqual(await task, [[str(n).encode()] for n in range(4)])

    async def test_encoded_byte_budget_can_keep_the_window_serial(self):
        sizes = [len(body) for _, body in self.client._shared_field_read_batches(["0", "1", "2"], ["f"])]
        self.assertEqual(len(set(sizes)), 1)
        self.client.max_batch_request_bytes = sizes[0] * 2 - 1
        first, release = asyncio.Event(), asyncio.Event()
        active = peak = 0
        started = []

        async def read(path, body):
            nonlocal active, peak
            key = self.key(body)
            started.append(key)
            active += 1
            peak = max(peak, active)
            first.set()
            try:
                await release.wait()
                return self.payload(key)
            finally:
                active -= 1

        self.transport(read)
        task = self.start(["0", "1", "2"])
        await asyncio.wait_for(first.wait(), 5)
        self.assertEqual(started, ["0"])
        release.set()
        self.assertEqual(await task, [[str(n).encode()] for n in range(3)])
        self.assertEqual((peak, active), (1, 0))

    async def test_cancellation_drains_every_pending_batch(self):
        arrived, release = asyncio.Event(), asyncio.Event()
        active = 0
        cancelled = []

        async def read(path, body):
            nonlocal active
            key = self.key(body)
            active += 1
            if active == 3:
                arrived.set()
            try:
                await release.wait()
                return self.payload(key)
            except asyncio.CancelledError:
                cancelled.append(key)
                raise
            finally:
                active -= 1

        transport = self.transport(read)
        task = self.start([str(n) for n in range(10)])
        await asyncio.wait_for(arrived.wait(), 5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(active, 0)
        self.assertEqual(set(cancelled), {"0", "1", "2"})
        self.assertEqual(transport.read.await_count, 3)

    async def test_out_of_order_success_prevents_a_404_fallback(self):
        accepted = asyncio.Event()
        never = asyncio.Event()

        async def read(path, body):
            self.assertEqual(path, "/batch_hash_multi_get_compact_msgpack")
            key = self.key(body)
            if key == "0":
                await accepted.wait()
                raise AsyncReadHttpError(404, "route disappeared")
            if key == "1":
                accepted.set()
                return self.payload(key)
            await never.wait()

        self.transport(read)
        with mock.patch.object(self.client, "batch_hash_multi_get", side_effect=AssertionError):
            with self.assertRaises(TemporalStoreCommandError) as caught:
                await self.client.batch_hash_multi_get_async(["0", "1", "2"], ["f"])
        self.assertEqual(caught.exception.code, "http_404")

    async def test_one_batch_does_not_allocate_a_child_task(self):
        async def read(path, body):
            return self.payload(self.key(body))

        self.transport(read)
        with mock.patch("asyncio.create_task", side_effect=AssertionError), mock.patch.object(
            self.client, "_read_hash_batches_async", side_effect=AssertionError
        ):
            self.assertEqual(await self.client.batch_hash_multi_get_async(["0"], ["f"]), [[b"0"]])

    async def test_invalid_concurrency_is_rejected(self):
        for value in [0, 33, 1.5, True, "4"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                TemporalStoreClient(max_async_read_batches=value)

    async def test_default_keeps_serial_batch_dispatch(self):
        client = TemporalStoreClient()
        self.assertEqual(client.max_async_read_batches, 1)
        await client.aclose()


if __name__ == "__main__":
    unittest.main()
