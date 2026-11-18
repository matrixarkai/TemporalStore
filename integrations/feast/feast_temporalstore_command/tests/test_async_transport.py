# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Focused HTTP, cancellation, and loop-lifetime checks for async reads."""

import asyncio
import errno
import gc
import queue
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import socket
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest import mock
import weakref

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from feast_temporalstore_command.async_transport import AsyncReadClosedError, AsyncReadLimitError, AsyncReadTransport
from feast_temporalstore_command.client import TemporalStoreClient, TemporalStoreCommandError

try:
    import aiohttp
    import msgpack
    AVAILABLE = True
except ModuleNotFoundError:
    AVAILABLE = False


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        document = msgpack.unpackb(body, raw=False)
        with self.server.lock:
            self.server.requests.append((self.path, document, self.client_address))
            action = self.server.actions.pop(0) if self.server.actions else "ok"
        if action == "delay":
            self.server.arrived.set()
            self.server.release.wait(timeout=5)
        if action == "disconnect":
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            return
        status = int(action) if action in {"404", "500"} else 200
        rows = [[None if field == "missing" else key.encode() + b"/" + field.encode()
                 for field in document["fields"]] for key in document["keys"]]
        envelope = {"status": {"ok": True}, "rows": rows}
        if self.path == "/batch_hash_multi_get_msgpack":
            envelope = {"status": {"ok": True}, "responses": [
                {"status": {"ok": True}, "response": {"values": row}} for row in rows]}
        payload = msgpack.packb(envelope, use_bin_type=True)
        if action == "invalid":
            payload = b"\xc1"
        self.send_response(status)
        self.send_header("Content-Type", "application/msgpack")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            if action == "partial":
                self.wfile.write(payload[:3])
                self.wfile.flush()
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
            else:
                self.wfile.write(payload)
        except OSError:
            pass


@unittest.skipUnless(AVAILABLE, "aiohttp and msgpack are required for native async HTTP checks")
class AsyncReadTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.daemon_threads = True
        self.server.actions, self.server.requests = [], []
        self.server.lock = threading.Lock()
        self.server.arrived, self.server.release = threading.Event(), threading.Event()
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
        self.thread.start()
        self.client = TemporalStoreClient(port=self.server.server_port)
        self.transports = []

    async def asyncTearDown(self):
        self.server.release.set()
        await self.client.aclose()
        for transport in self.transports:
            await transport.aclose()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.assertFalse(self.thread.is_alive())

    def transport(self, **kwargs):
        transport = AsyncReadTransport("127.0.0.1", self.server.server_port, 2, **kwargs)
        self.transports.append(transport)
        return transport

    async def test_empty_read_opens_no_pool(self):
        with mock.patch.object(self.client, "_get_async_read_transport", side_effect=AssertionError):
            self.assertEqual(await self.client.batch_hash_multi_get_async([], []), [])

    async def test_binary_rows_keep_order_missing_fields_and_repeated_keys(self):
        rows = await self.client.batch_hash_multi_get_async(["a", "b", "a"], ["f", "missing", "f"])
        self.assertEqual(rows, [[b"a/f", None, b"a/f"], [b"b/f", None, b"b/f"], [b"a/f", None, b"a/f"]])
        self.assertIsNotNone(self.client._async_read_transport)

    async def test_bounded_chunks_reuse_the_loop_connection(self):
        self.client.max_batch_commands = 1
        self.assertEqual(await self.client.batch_hash_multi_get_async(["a", "b", "c"], ["f"]),
                         [[b"a/f"], [b"b/f"], [b"c/f"]])
        await self.client.batch_hash_multi_get_async(["d"], ["f"])
        self.assertEqual(len(self.server.requests), 4)
        self.assertEqual(len({request[2] for request in self.server.requests}), 1)
        transport = self.client._async_read_transport
        self.assertEqual(len(transport._sessions), 1)
        self.assertEqual((transport._requests, transport._bytes), (0, 0))

    async def test_socket_timeout_keeps_progressing_large_reads_eligible(self):
        await self.client.batch_hash_multi_get_async(["a"], ["f"])
        entry = next(iter(self.client._async_read_transport._sessions.values()))
        self.assertIsNone(entry.session.timeout.total)
        self.assertEqual(entry.session.timeout.connect, self.client.timeout_seconds)
        self.assertEqual(entry.session.timeout.sock_connect, self.client.timeout_seconds)
        self.assertEqual(entry.session.timeout.sock_read, self.client.timeout_seconds)

    async def test_closed_connection_retries_one_read(self):
        self.server.actions = ["disconnect", "ok"]
        self.assertEqual(await self.client.batch_hash_multi_get_async(["a"], ["f"]), [[b"a/f"]])
        self.assertEqual(len(self.server.requests), 2)

    async def test_incomplete_body_retries_one_read(self):
        self.server.actions = ["partial", "ok"]
        self.assertEqual(await self.client.batch_hash_multi_get_async(["a"], ["f"]), [[b"a/f"]])
        self.assertEqual(len(self.server.requests), 2)

    async def test_repeated_disconnect_stops_after_two_attempts(self):
        self.server.actions = ["disconnect", "disconnect"]
        with self.assertRaises(aiohttp.ClientConnectionError):
            await self.client.batch_hash_multi_get_async(["a"], ["f"])
        self.assertEqual(len(self.server.requests), 2)

    async def test_http_error_does_not_retry(self):
        self.server.actions = ["500"]
        with self.assertRaises(TemporalStoreCommandError) as caught:
            await self.client.batch_hash_multi_get_async(["a"], ["f"])
        self.assertEqual(caught.exception.code, "http_500")
        self.assertEqual(len(self.server.requests), 1)

    async def test_compact_route_falls_back_to_shared_route_once(self):
        self.server.actions = ["404", "ok", "ok"]
        for _ in range(2):
            self.assertEqual(await self.client.batch_hash_multi_get_async(["a"], ["f"]), [[b"a/f"]])
        self.assertEqual([row[0] for row in self.server.requests], [
            "/batch_hash_multi_get_compact_msgpack", "/batch_hash_multi_get_msgpack", "/batch_hash_multi_get_msgpack"])

    async def test_missing_shared_routes_fall_back_to_the_blocking_client(self):
        self.server.actions = ["404", "404"]
        with mock.patch.object(self.client, "batch_hash_multi_get", return_value=[[b"legacy"]]) as fallback:
            self.assertEqual(await self.client.batch_hash_multi_get_async(["a"], ["f"]), [[b"legacy"]])
        fallback.assert_called_once_with(["a"], ["f"])
        self.assertFalse(self.client._compact_shared_field_read_supported)
        self.assertFalse(self.client._shared_field_read_supported)

    async def test_a_partial_prefix_never_falls_back_or_duplicates_rows(self):
        self.server.actions = ["ok", "404"]
        self.client.max_batch_commands = 1
        with mock.patch.object(self.client, "batch_hash_multi_get", side_effect=AssertionError):
            with self.assertRaises(TemporalStoreCommandError) as caught:
                await self.client.batch_hash_multi_get_async(["a", "b"], ["f"])
        self.assertEqual(caught.exception.code, "http_404")
        self.assertEqual(len(self.server.requests), 2)

    async def test_invalid_binary_does_not_retry(self):
        self.server.actions = ["invalid"]
        with self.assertRaises(TemporalStoreCommandError) as caught:
            await self.client.batch_hash_multi_get_async(["a"], ["f"])
        self.assertEqual(caught.exception.code, "invalid_binary_response")
        self.assertEqual(len(self.server.requests), 1)

    async def test_request_budget_and_cancellation_release_admission(self):
        transport = self.transport(max_requests=1)
        self.server.actions = ["delay", "ok"]
        body = msgpack.packb({"keys": ["a"], "fields": ["f"]})
        pending = asyncio.create_task(transport.read("/batch_hash_multi_get_compact_msgpack", body))
        self.assertTrue(await asyncio.to_thread(self.server.arrived.wait, 2))
        with self.assertRaises(AsyncReadLimitError):
            await transport.read("/batch_hash_multi_get_compact_msgpack", body)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.assertEqual((transport._requests, transport._bytes), (0, 0))
        self.server.release.set()
        await transport.read("/batch_hash_multi_get_compact_msgpack", body)
        self.assertEqual(len(self.server.requests), 2)

    async def test_body_budget_rejects_before_opening_a_session(self):
        transport = self.transport(max_pending_bytes=2)
        with self.assertRaises(AsyncReadLimitError):
            await transport.read("/batch_hash_multi_get_compact_msgpack", b"abc")
        self.assertEqual(len(transport._sessions), 0)
        self.assertEqual((transport._requests, transport._bytes), (0, 0))

    async def test_close_fences_retry_and_awaits_pool_cleanup(self):
        self.server.actions = ["delay"]
        pending = asyncio.create_task(self.client.batch_hash_multi_get_async(["a"], ["f"]))
        self.assertTrue(await asyncio.to_thread(self.server.arrived.wait, 2))
        transport = self.client._async_read_transport
        entry = next(iter(transport._sessions.values()))
        await asyncio.wait_for(self.client.aclose(), 2)
        with self.assertRaises(Exception):
            await pending
        self.assertTrue(entry.session.closed)
        self.assertFalse(transport._sessions)
        self.assertFalse(transport._closing)
        self.assertEqual(len(self.server.requests), 1)

    async def test_asyncio_run_closes_pools_even_without_explicit_close(self):
        def run_loops():
            for _ in range(3):
                self.assertEqual(asyncio.run(self.client.batch_hash_multi_get_async(["a"], ["f"])), [[b"a/f"]])
                self.assertFalse(self.client._async_read_transport._sessions)
        await asyncio.to_thread(run_loops)
        self.assertEqual(len(self.server.requests), 3)

    async def test_sync_close_initiates_cleanup_on_the_owning_loop(self):
        await self.client.batch_hash_multi_get_async(["a"], ["f"])
        transport = self.client._async_read_transport
        entry = next(iter(transport._sessions.values()))
        self.assertIsNone(self.client.close())
        await asyncio.wait_for(asyncio.wrap_future(entry.close_future), 2)
        self.assertTrue(entry.session.closed)
        self.assertFalse(transport._closing)

    async def test_closed_transport_cannot_reopen_an_untracked_pool(self):
        transport = self.transport()
        transport.close()
        with self.assertRaises(AsyncReadClosedError):
            await transport.read("/batch_hash_multi_get_compact_msgpack", b"body")
        self.assertFalse(transport._sessions)
        self.assertFalse(self.server.requests)

    async def test_shutdown_between_admission_and_session_creation_is_fenced(self):
        transport = self.transport()
        original = transport._get_session
        admitted, proceed = asyncio.Event(), asyncio.Event()
        async def delayed():
            admitted.set()
            await proceed.wait()
            return await original()
        with mock.patch.object(transport, "_get_session", new=delayed):
            pending = asyncio.create_task(transport.read("/batch_hash_multi_get_compact_msgpack", b"body"))
            await admitted.wait()
            transport.close()
            proceed.set()
            with self.assertRaises(AsyncReadClosedError):
                await pending
        self.assertFalse(transport._sessions)
        self.assertFalse(self.server.requests)
        self.assertEqual((transport._requests, transport._bytes), (0, 0))

    async def test_client_reuse_after_close_creates_a_new_owned_transport(self):
        await self.client.batch_hash_multi_get_async(["a"], ["f"])
        old = self.client._async_read_transport
        await self.client.aclose()
        self.assertEqual(await self.client.batch_hash_multi_get_async(["b"], ["f"]), [[b"b/f"]])
        self.assertIsNot(self.client._async_read_transport, old)
        self.assertTrue(old._closed)
        self.assertFalse(old._sessions)

    async def test_manual_loop_shutdown_closes_a_stopped_loops_pool(self):
        def run_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.client.batch_hash_multi_get_async(["a"], ["f"]))
                transport = self.client._async_read_transport
                entry = next(iter(transport._sessions.values()))
                self.client.close()
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
                self.assertTrue(entry.session.closed)
                self.assertFalse(transport._closing)
            finally:
                loop.close()
        await asyncio.to_thread(run_loop)

    async def test_async_close_awaits_cleanup_on_another_running_loop(self):
        shared = queue.Queue()
        def run_loop():
            async def read():
                answer = await self.client.batch_hash_multi_get_async(["a"], ["f"])
                stop = asyncio.Event()
                transport = self.client._async_read_transport
                entry = next(iter(transport._sessions.values()))
                shared.put((asyncio.get_running_loop(), stop, entry))
                await stop.wait()
                return answer
            return asyncio.run(read())
        worker = asyncio.create_task(asyncio.to_thread(run_loop))
        loop, stop, entry = await asyncio.to_thread(shared.get, True, 5)
        try:
            await asyncio.wait_for(self.client.aclose(), 2)
            self.assertTrue(entry.session.closed)
        finally:
            loop.call_soon_threadsafe(stop.set)
            self.assertEqual(await worker, [[b"a/f"]])

    async def test_connection_pool_bounds_active_socket_requests(self):
        transport = self.transport(max_connections=1)
        body = msgpack.packb({"keys": ["a"], "fields": ["f"]})
        self.server.actions = ["delay", "ok"]
        first = asyncio.create_task(transport.read("/batch_hash_multi_get_compact_msgpack", body))
        self.assertTrue(await asyncio.to_thread(self.server.arrived.wait, 2))
        second = asyncio.create_task(transport.read("/batch_hash_multi_get_compact_msgpack", body))
        await asyncio.sleep(0)
        self.assertEqual(len(self.server.requests), 1)
        self.assertFalse(second.done())
        self.server.release.set()
        await asyncio.gather(first, second)
        self.assertEqual(len(self.server.requests), 2)

    async def test_one_client_supports_two_simultaneous_event_loops(self):
        ready, checked = threading.Barrier(2, timeout=5), threading.Barrier(2, timeout=5)
        def run_loop(key):
            async def read():
                answer = await self.client.batch_hash_multi_get_async([key], ["f"])
                ready.wait()
                self.assertEqual(len(self.client._async_read_transport._sessions), 2)
                checked.wait()
                return answer
            return asyncio.run(read())
        answers = await asyncio.gather(asyncio.to_thread(run_loop, "a"), asyncio.to_thread(run_loop, "b"))
        self.assertEqual(answers, [[[b"a/f"]], [[b"b/f"]]])
        self.assertFalse(self.client._async_read_transport._sessions)

    async def test_missing_async_dependency_uses_the_worker_fallback(self):
        self.client._async_read_available = False
        with mock.patch.object(self.client, "batch_hash_multi_get", return_value=[[b"legacy"]]) as fallback:
            self.assertEqual(await self.client.batch_hash_multi_get_async(["a"], ["f"]), [[b"legacy"]])
        fallback.assert_called_once_with(["a"], ["f"])

    async def test_timeouts_and_connection_refusals_are_not_retryable(self):
        self.assertFalse(AsyncReadTransport._disconnected(asyncio.TimeoutError()))
        self.assertFalse(AsyncReadTransport._disconnected(aiohttp.ClientOSError(errno.ECONNREFUSED, "refused")))
        self.assertTrue(AsyncReadTransport._disconnected(aiohttp.ClientOSError(errno.ECONNRESET, "reset")))

    async def test_partial_response_is_released_before_retrying(self):
        transport = self.transport()
        references, calls = [], []
        class Response:
            status = 200
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            async def read(self):
                if len(calls) == 1:
                    self.partial = bytearray(1024 * 1024)
                    raise aiohttp.ClientPayloadError("partial")
                return b"ok"
        class Session:
            def post(self, *args, **kwargs):
                if calls:
                    gc.collect()
                    self_test.assertIsNone(references[0]())
                calls.append(1)
                response = Response()
                references.append(weakref.ref(response))
                return response
        self_test = self
        with mock.patch.object(transport, "_get_session", new=mock.AsyncMock(return_value=SimpleNamespace(session=Session()))):
            self.assertEqual(await transport.read("/batch_hash_multi_get_compact_msgpack", b"body"), b"ok")
        self.assertEqual(len(calls), 2)

    async def test_write_routes_and_invalid_limits_are_refused(self):
        transport = self.transport()
        with self.assertRaises(ValueError):
            await transport.read("/execute", b"write")
        for name in ("max_connections", "max_requests", "max_pending_bytes"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                AsyncReadTransport("localhost", 1, 1, **{name: 0})


if __name__ == "__main__":
    if not AVAILABLE:
        raise SystemExit("Native async HTTP checks require aiohttp and msgpack")
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(AsyncReadTransportTests)
    expected = suite.countTestCases()
    assert expected >= 28
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() and result.testsRun == expected and not result.skipped else 1)
