# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Loop-owned HTTP pools for read-only command routes."""

from __future__ import annotations

import asyncio
from concurrent.futures import Future
from dataclasses import dataclass, field
import errno
import importlib.util
import threading
from typing import Any, Dict, Optional


class AsyncReadLimitError(RuntimeError):
    pass


class AsyncReadClosedError(RuntimeError):
    pass


class AsyncReadHttpError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


@dataclass(eq=False)
class _LoopSession:
    loop: asyncio.AbstractEventLoop
    session: Any = None
    lifetime: Any = None
    finished: Future = field(default_factory=Future)
    close_future: Optional[Future] = None


class AsyncReadTransport:
    ROUTES = frozenset({
        "/batch_hash_multi_get_compact_msgpack", "/batch_hash_multi_get_msgpack",
    })

    def __init__(self, host: str, port: int, timeout_seconds: float,
                 max_connections: int = 32, max_requests: int = 256,
                 max_pending_bytes: int = 64 * 1024 * 1024) -> None:
        if min(max_connections, max_requests, max_pending_bytes) < 1 or timeout_seconds <= 0:
            raise ValueError("async transport limits must be positive")
        self.host, self.port, self.timeout_seconds = host, port, timeout_seconds
        self.max_connections, self.max_requests = max_connections, max_requests
        self.max_pending_bytes = max_pending_bytes
        self._lock = threading.Lock()
        self._sessions: Dict[asyncio.AbstractEventLoop, _LoopSession] = {}
        self._closing = set()
        self._generation = 0
        self._closed = False
        self._requests = self._bytes = 0

    @staticmethod
    def available() -> bool:
        return importlib.util.find_spec("aiohttp") is not None

    async def _session_lifetime(self, entry: _LoopSession):
        import aiohttp
        from yarl import URL

        host = self.host[1:-1] if self.host.startswith("[") and self.host.endswith("]") else self.host
        session = aiohttp.ClientSession(
            base_url=URL.build(scheme="http", host=host, port=self.port),
            connector=aiohttp.TCPConnector(limit=self.max_connections),
            timeout=aiohttp.ClientTimeout(total=None, connect=self.timeout_seconds,
                sock_connect=self.timeout_seconds, sock_read=self.timeout_seconds),
            auto_decompress=False, skip_auto_headers={"Accept", "Accept-Encoding", "User-Agent"},
        )
        entry.session = session
        try:
            yield session
        finally:
            failure = None
            try:
                await session.close()
            except BaseException as error:
                failure = error
                raise
            finally:
                with self._lock:
                    if self._sessions.get(entry.loop) is entry:
                        del self._sessions[entry.loop]
                    if entry.close_future is None:
                        self._closing.discard(entry)
                if not entry.finished.done():
                    if failure is None:
                        entry.finished.set_result(None)
                    else:
                        entry.finished.set_exception(failure)

    async def _get_session(self) -> _LoopSession:
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._closed:
                raise AsyncReadClosedError("async read transport is closed")
            entry = self._sessions.get(loop)
            if entry is None:
                entry = _LoopSession(loop)
                entry.lifetime = self._session_lifetime(entry)
                self._sessions[loop] = entry
        if entry.session is None:
            try:
                # Session construction does not suspend before the first yield.
                await entry.lifetime.__anext__()
            except BaseException:
                with self._lock:
                    if self._sessions.get(loop) is entry:
                        del self._sessions[loop]
                raise
        return entry

    @staticmethod
    def _disconnected(error: Exception) -> bool:
        import aiohttp

        reset = getattr(aiohttp, "ClientConnectionResetError", ())
        return (
            isinstance(error, (aiohttp.ServerDisconnectedError, aiohttp.ClientPayloadError))
            or bool(reset and isinstance(error, reset))
            or isinstance(error, aiohttp.ClientOSError)
            and error.errno in {errno.EPIPE, errno.ECONNRESET, errno.ECONNABORTED}
        )

    async def read(self, path: str, body: bytes) -> bytes:
        if path not in self.ROUTES:
            raise ValueError("async transport accepts only shared-field read routes")
        size = len(body)
        with self._lock:
            if self._closed:
                raise AsyncReadClosedError("async read transport is closed")
            if self._requests >= self.max_requests or self._bytes + size > self.max_pending_bytes:
                raise AsyncReadLimitError("async read admission limit exceeded")
            self._requests += 1
            self._bytes += size
            generation = self._generation
        try:
            entry = await self._get_session()
            for attempt in range(2):
                try:
                    async with entry.session.post(path, data=body,
                        headers={"Content-Type": "application/msgpack", "Accept-Encoding": "identity"},
                        allow_redirects=False) as response:
                        payload = await response.read()
                        if response.status != 200:
                            raise AsyncReadHttpError(response.status, payload.decode("utf-8", "replace")[:512])
                        return payload
                except Exception as error:
                    with self._lock:
                        unchanged = generation == self._generation
                    if attempt or not unchanged or not self._disconnected(error):
                        raise
                # Drop an incomplete response and its exception before the bounded retry.
                response = None
            raise AssertionError("unreachable read retry state")
        finally:
            with self._lock:
                self._requests -= 1
                self._bytes -= size

    async def _close_session(self, entry: _LoopSession) -> None:
        if entry.session is not None:
            await entry.session.close()
        if entry.lifetime.ag_running:
            await asyncio.shield(asyncio.wrap_future(entry.finished))
        else:
            await entry.lifetime.aclose()

    def _schedule_close(self, entry: _LoopSession) -> None:
        task = entry.loop.create_task(self._close_session(entry))
        def finished(done):
            try:
                done.result()
            except BaseException as error:
                if not entry.close_future.done():
                    entry.close_future.set_exception(error)
            else:
                if not entry.close_future.done():
                    entry.close_future.set_result(None)
            finally:
                with self._lock:
                    self._closing.discard(entry)
        task.add_done_callback(finished)

    def close(self) -> None:
        """Initiate cleanup on each owning loop; aclose() waits for completion."""
        with self._lock:
            self._closed = True
            self._generation += 1
            entries = list(self._sessions.values())
            self._sessions.clear()
            self._closing.update(entries)
            for entry in self._closing:
                if entry.close_future is None and entry.loop.is_running():
                    entry.close_future = Future()
                    try:
                        entry.loop.call_soon_threadsafe(self._schedule_close, entry)
                    except RuntimeError as error:
                        entry.close_future.set_exception(error)
        # Stopped loops still own their registered async generator. Their normal
        # shutdown_asyncgens() call closes its session before the loop is closed.

    async def aclose(self) -> None:
        self.close()
        with self._lock:
            entries = list(self._closing)
        for entry in entries:
            future = entry.close_future or entry.finished
            if not future.done() and not entry.loop.is_running():
                raise RuntimeError("close async reads before stopping their event loop")
            await asyncio.shield(asyncio.wrap_future(future))
