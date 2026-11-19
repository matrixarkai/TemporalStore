# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""HTTP command client for a TemporalStore data node.

The data node serves binary ``POST /batch_execute_msgpack``, shared-field hash
read/write routes, atomic timestamp-conditioned hash writes, and
``POST /batch_write_msgpack``, plus JSON-compatible ``POST /execute``,
``POST /batch_execute``, and ``POST /batch_write`` routes.
They take a shard id and one or more command documents; ``Command`` is a
serde-tagged enum whose ``kind`` is the snake_case variant name.

Two asymmetries in that wire format are load-bearing for anyone measuring it,
so they are stated here rather than discovered later:

* **Binary request and response payloads stay binary.** MessagePack byte
  strings avoid base64 expansion for long feature values.
* **JSON compatibility payloads use the established byte encodings.**
  :func:`decode_bytes` accepts base64 strings and the older decimal-array
  response shape, so clients can read responses from every node version.
"""

from __future__ import annotations

import base64
import http.client
import json
import threading
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

try:
    import msgpack
except ModuleNotFoundError:  # A source checkout can still use the JSON route.
    msgpack = None  # type: ignore[assignment]

__all__ = [
    "TemporalStoreCommandError",
    "TemporalStoreClient",
    "decode_bytes",
    "encode_bytes",
]

_READ_ONLY_COMMAND_KINDS = frozenset({
    "string_get", "hash_get", "hash_multi_get", "set_members", "set_scan",
    "feature_query", "feature_query_filtered", "feature_agg_query",
    "feature_proto_agg_query", "sequence_query", "window_reduce_rank",
})
_CLOSED_CONNECTION_ERRORS = (
    BrokenPipeError, ConnectionResetError, ConnectionAbortedError,
    http.client.RemoteDisconnected, http.client.CannotSendRequest,
    http.client.ResponseNotReady, http.client.IncompleteRead,
)


class TemporalStoreCommandError(RuntimeError):
    """A command reached the node and the node refused it.

    Carries the refusal's ``code`` so a caller can tell a bad request from a
    shard that is not loaded, instead of matching on message text.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def encode_bytes(value: bytes) -> str:
    """Encode a payload for the JSON-compatible command routes."""
    return base64.b64encode(value).decode("ascii")


def decode_bytes(value: Union[None, str, Sequence[int]]) -> Optional[bytes]:
    """Decode either payload shape the command surface may answer with.

    ``None`` is a real answer -- a field that is absent from a hash -- and is
    passed through rather than turned into empty bytes, because Feast
    distinguishes "no value" from "a value that is zero bytes long".
    """
    if value is None:
        return None
    if isinstance(value, str):
        return base64.b64decode(value)
    return bytes(value)


def _required_binary(value: Any) -> bytes:
    decoded = decode_bytes(value)
    if decoded is None:
        raise TypeError("required byte payload is null")
    return decoded


def _binary_command(command: Dict[str, Any]) -> Dict[str, Any]:
    """Return one command with byte-bearing fields represented as bytes."""
    encoded = dict(command)
    kind = encoded.get("kind")
    if kind in {"hash_multi_set", "hash_multi_set_with_ttl", "hash_multi_set_if_timestamp_older"}:
        encoded["entries"] = [
            (name, _required_binary(value))
            for name, value in encoded.get("entries", [])
        ]
    elif kind in {"string_set", "string_set_ex", "string_set_conditional", "hash_set"}:
        encoded["value"] = _required_binary(encoded["value"])
    elif kind in {"set_add", "set_remove", "zset_add", "zset_score", "zset_remove"}:
        encoded["member"] = _required_binary(encoded["member"])
    elif kind == "set_scan":
        encoded["after"] = _required_binary(encoded["after"])
    elif kind in {"feature_append", "feature_append_with_ttl", "feature_append_with_policy", "feature_replace"}:
        encoded["points"] = [
            {**point, "value": _required_binary(point["value"])}
            for point in encoded.get("points", [])
        ]
    return encoded


def _json_command(command: Dict[str, Any]) -> Dict[str, Any]:
    """Return one command in the established JSON byte encoding."""
    encoded = dict(command)
    kind = encoded.get("kind")
    if kind in {"hash_multi_set", "hash_multi_set_with_ttl", "hash_multi_set_if_timestamp_older"}:
        encoded["entries"] = [
            (name, encode_bytes(_required_binary(value)))
            for name, value in encoded.get("entries", [])
        ]
    elif kind in {"string_set", "string_set_ex", "string_set_conditional", "hash_set"}:
        encoded["value"] = encode_bytes(_required_binary(encoded["value"]))
    elif kind in {"set_add", "set_remove", "zset_add", "zset_score", "zset_remove"}:
        encoded["member"] = encode_bytes(_required_binary(encoded["member"]))
    elif kind == "set_scan":
        encoded["after"] = encode_bytes(_required_binary(encoded["after"]))
    elif kind in {"feature_append", "feature_append_with_ttl", "feature_append_with_policy", "feature_replace"}:
        encoded["points"] = [
            {**point, "value": list(_required_binary(point["value"]))}
            for point in encoded.get("points", [])
        ]
    return encoded


def _msgpack_array_header(length: int) -> bytes:
    if length < 16:
        return bytes((0x90 | length,))
    if length <= 0xFFFF:
        return b"\xdc" + length.to_bytes(2, "big")
    return b"\xdd" + length.to_bytes(4, "big")


def _msgpack_map_header(length: int) -> bytes:
    if length < 16:
        return bytes((0x80 | length,))
    if length <= 0xFFFF:
        return b"\xde" + length.to_bytes(2, "big")
    return b"\xdf" + length.to_bytes(4, "big")


class TemporalStoreClient:
    """A keep-alive HTTP client for one TemporalStore data node.

    One connection is held per thread. Feast's serving path is synchronous and
    usually single-threaded per request, so a per-thread connection avoids both
    a lock on the hot path and a fresh TCP handshake per read -- a handshake
    costs more than the read it would carry.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 17102,
        shard_id: int = 1,
        timeout_seconds: float = 10.0,
        max_batch_commands: int = 512,
        max_batch_request_bytes: int = 4 * 1024 * 1024,
        max_write_batch_commands: Optional[int] = None,
        max_async_read_batches: int = 1,
    ) -> None:
        write_batch_commands = (
            max_batch_commands
            if max_write_batch_commands is None
            else max_write_batch_commands
        )
        if (
            max_batch_commands < 1
            or write_batch_commands < 1
            or max_batch_request_bytes < 1
        ):
            raise ValueError("batch limits must be positive")
        if (isinstance(max_async_read_batches, bool)
                or not isinstance(max_async_read_batches, int)
                or not 1 <= max_async_read_batches <= 32):
            raise ValueError("async read batch concurrency must be between 1 and 32")
        self.host = host
        self.port = port
        self.shard_id = shard_id
        self.timeout_seconds = timeout_seconds
        self.max_batch_commands = max_batch_commands
        self.max_write_batch_commands = write_batch_commands
        self.max_batch_request_bytes = max_batch_request_bytes
        self.max_async_read_batches = max_async_read_batches
        self._local = threading.local()
        # EVERY connection this client ever opened, not just this thread's.
        # `threading.local` is the right place to KEEP a connection and the
        # wrong place to find one from another thread, so `close()` could only
        # ever release the socket belonging to whoever happened to call it.
        # Worker-backed methods may open sockets on executor threads, so the
        # caller closing the store need not be the thread holding the
        # connections. This registry is what
        # makes `close()` mean closed.
        self._all_connections: List[http.client.HTTPConnection] = []
        self._registry_lock = threading.Lock()
        self._async_lock = threading.Lock()
        self._async_read_transport = None
        self._async_read_available: Optional[bool] = None
        self._batch_write_supported: Optional[bool] = None
        self._binary_batch_supported: Optional[bool] = False if msgpack is None else None
        self._binary_write_supported: Optional[bool] = False if msgpack is None else None
        self._shared_field_read_supported: Optional[bool] = (
            False if msgpack is None else None
        )
        self._compact_shared_field_read_supported: Optional[bool] = (
            False if msgpack is None else None
        )
        self._shared_field_write_supported: Optional[bool] = (
            False if msgpack is None else None
        )
        self._shared_field_ttl_write_supported: Optional[bool] = (
            False if msgpack is None else None
        )
        self._conditional_shared_field_write_supported: Optional[bool] = (
            False if msgpack is None else None
        )
        self._conditional_shared_field_ttl_write_supported: Optional[bool] = (
            False if msgpack is None else None
        )
        self._interleaved_conditional_write_supported: Optional[bool] = (
            False if msgpack is None else None
        )

    def timestamp_prefetch_preferred(self) -> bool:
        """Whether this thread recently observed mostly stale bulk writes."""
        return getattr(self._local, "prefer_timestamp_prefetch", False)

    def observe_timestamp_dedup(self, total_rows: int, skipped_rows: int) -> None:
        """Keep a routing hint, never cached timestamps or feature values."""
        if total_rows <= 0 or skipped_rows < 0 or skipped_rows > total_rows:
            return
        if total_rows >= 8 and skipped_rows * 4 >= total_rows * 3:
            self._local.prefer_timestamp_prefetch = True
        elif skipped_rows * 4 <= total_rows:
            self._local.prefer_timestamp_prefetch = False

    # -- connection handling -------------------------------------------------

    def _connection(self) -> http.client.HTTPConnection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = http.client.HTTPConnection(
                self.host, self.port, timeout=self.timeout_seconds
            )
            self._local.conn = conn
            # The lock is taken only when a thread opens its FIRST connection,
            # never on the hot path.
            with self._registry_lock:
                self._all_connections.append(conn)
        return conn

    def _forget(self, conn: http.client.HTTPConnection) -> None:
        with self._registry_lock:
            try:
                self._all_connections.remove(conn)
            except ValueError:
                pass

    def _drop_connection(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._forget(conn)
            self._local.conn = None

    def close(self) -> None:
        """Close blocking connections and initiate async pool shutdown.

        Idempotent, and safe to call from a thread that never issued a request.
        Clearing the thread-local registry makes the next call open a fresh
        connection. Known read-only requests also reconnect once when a pooled
        socket closes; writes are never automatically resent.
        Await aclose() instead when async shutdown must complete before returning.
        """
        with self._registry_lock:
            connections = list(self._all_connections)
            self._all_connections.clear()
        for conn in connections:
            try:
                conn.close()
            except Exception:
                pass
        self._local = threading.local()

        with self._async_lock:
            transport, self._async_read_transport = self._async_read_transport, None
        if transport is not None:
            transport.close()

    async def aclose(self) -> None:
        """Release blocking connections and await all loop-owned read pools."""
        with self._async_lock:
            transport = self._async_read_transport
        self.close()
        if transport is not None:
            await transport.aclose()

    def _get_async_read_transport(self):
        from .async_transport import AsyncReadTransport

        with self._async_lock:
            if self._async_read_available is None:
                self._async_read_available = AsyncReadTransport.available()
            if not self._async_read_available:
                return None
            if self._async_read_transport is None:
                self._async_read_transport = AsyncReadTransport(
                    self.host, self.port, self.timeout_seconds,
                    max_pending_bytes=max(64 * 1024 * 1024, self.max_batch_request_bytes),
                )
            return self._async_read_transport

    def _request_encoded(
        self, path: str, body: bytes, content_type: str = "application/json"
    ) -> bytes:
        headers = {"Content-Type": content_type, "Content-Length": str(len(body))}
        # A timeout can happen after the server applied a write but before its
        # response arrives. Do not resend an ambiguous request: FeatureAppend
        # and other commands are not generally idempotent.
        conn = self._connection()
        try:
            conn.request("POST", path, body=body, headers=headers)
            response = conn.getresponse()
            raw = response.read()
        except (http.client.HTTPException, OSError):
            self._drop_connection()
            raise
        if response.status != 200:
            self._drop_connection()
            raise TemporalStoreCommandError(
                f"http_{response.status}", raw.decode("utf-8", "replace")[:512]
            )
        return raw

    def _request_read_encoded(
        self, path: str, body: bytes, content_type: str = "application/json"
    ) -> bytes:
        try:
            return self._request_encoded(path, body, content_type)
        except _CLOSED_CONNECTION_ERRORS:
            self._drop_connection()
        # Leave the handler before retrying so IncompleteRead releases its partial body.
        return self._request_encoded(path, body, content_type)

    def _post_encoded(self, path: str, body: bytes) -> Dict[str, Any]:
        return json.loads(self._request_encoded(path, body))

    def _post_read_encoded(self, path: str, body: bytes) -> Dict[str, Any]:
        try:
            return self._post_encoded(path, body)
        except _CLOSED_CONNECTION_ERRORS:
            self._drop_connection()
        return self._post_encoded(path, body)

    def _post(self, path: str, document: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(document, separators=(",", ":")).encode("utf-8")
        if path == "/execute" and document.get("command", {}).get("kind") in _READ_ONLY_COMMAND_KINDS:
            return self._post_read_encoded(path, body)
        return self._post_encoded(path, body)

    # -- commands ------------------------------------------------------------

    @staticmethod
    def _unwrap(execute_response: Dict[str, Any]) -> Dict[str, Any]:
        status = execute_response.get("status") or {}
        if not status.get("ok", False):
            raise TemporalStoreCommandError(
                status.get("code", "unknown"), status.get("message", "")
            )
        return execute_response.get("response") or {}

    def execute(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Run one command and return its ``CommandResponse`` document."""
        return self._unwrap(
            self._post("/execute", {"shard_id": self.shard_id, "command": command})
        )

    def batch_execute(self, commands: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run commands in bounded, ordered batches.

        Returns one ``CommandResponse`` per command, positionally aligned with
        ``commands``. An empty list is answered without a round trip: the node
        would accept it, but a caller that batched nothing wants nothing.
        """
        if not commands:
            return []
        if msgpack is not None and self._binary_batch_supported is not False:
            results: List[Dict[str, Any]] = []
            try:
                for chunk, body in self._binary_batches(
                    commands, self.max_batch_commands
                ):
                    request = self._request_read_encoded if all(
                        command.get("kind") in _READ_ONLY_COMMAND_KINDS for command in chunk
                    ) else self._request_encoded
                    raw = request(
                        "/batch_execute_msgpack", body, "application/msgpack"
                    )
                    envelope = msgpack.unpackb(raw, raw=False)
                    results.extend(self._decode_batch_envelope(envelope, chunk))
            except TemporalStoreCommandError as error:
                if error.code != "http_404" or results:
                    raise
                self._binary_batch_supported = False
            except (ValueError, msgpack.UnpackException) as error:
                raise TemporalStoreCommandError(
                    "invalid_binary_response", str(error)
                ) from error
            else:
                self._binary_batch_supported = True
                return results

        results = []
        for chunk, body in self._encoded_batches(commands, self.max_batch_commands):
            results.extend(self._batch_once_json(chunk, body))
        return results

    def batch_hash_multi_get(
        self, keys: List[str], fields: List[str]
    ) -> List[List[Any]]:
        """Read one field projection from many hashes with shared wire metadata.

        Feature serving asks for the same fields from every entity. Sending
        that projection once avoids repeating every field name in every
        command. Nodes without the compact result route fall back once to the
        shared-field response and then to established command batches.
        """
        if not keys:
            return []
        if (
            msgpack is not None
            and self._compact_shared_field_read_supported is not False
        ):
            compact_results: List[List[Any]] = []
            try:
                for chunk, body in self._shared_field_read_batches(keys, fields):
                    raw = self._request_read_encoded(
                        "/batch_hash_multi_get_compact_msgpack",
                        body,
                        "application/msgpack",
                    )
                    envelope = msgpack.unpackb(raw, raw=False)
                    compact_results.extend(
                        self._decode_compact_hash_read_envelope(envelope, len(chunk))
                    )
            except TemporalStoreCommandError as error:
                if error.code != "http_404" or compact_results:
                    raise
                self._compact_shared_field_read_supported = False
            except (ValueError, msgpack.UnpackException) as error:
                raise TemporalStoreCommandError(
                    "invalid_binary_response", str(error)
                ) from error
            else:
                self._compact_shared_field_read_supported = True
                return compact_results

        if msgpack is not None and self._shared_field_read_supported is not False:
            results: List[List[Any]] = []
            try:
                for chunk, body in self._shared_field_read_batches(keys, fields):
                    raw = self._request_read_encoded(
                        "/batch_hash_multi_get_msgpack",
                        body,
                        "application/msgpack",
                    )
                    envelope = msgpack.unpackb(raw, raw=False)
                    results.extend(
                        (response.get("values") or [])
                        for response in self._decode_batch_envelope(envelope, chunk)
                    )
            except TemporalStoreCommandError as error:
                if error.code != "http_404" or results:
                    raise
                self._shared_field_read_supported = False
            except (ValueError, msgpack.UnpackException) as error:
                raise TemporalStoreCommandError(
                    "invalid_binary_response", str(error)
                ) from error
            else:
                self._shared_field_read_supported = True
                return results

        return [
            response.get("values") or []
            for response in self.batch_execute(
                [
                    {"kind": "hash_multi_get", "key": key, "fields": fields}
                    for key in keys
                ]
            )
        ]

    async def batch_hash_multi_get_async(self, keys: List[str], fields: List[str]) -> List[List[Any]]:
        """Read shared projections without occupying one worker per socket."""
        import asyncio
        from .async_transport import AsyncReadClosedError, AsyncReadHttpError, AsyncReadLimitError

        if not keys:
            return []
        transport = self._get_async_read_transport() if msgpack is not None else None
        if transport is None:
            return await asyncio.to_thread(self.batch_hash_multi_get, keys, fields)
        routes = (
            ("/batch_hash_multi_get_compact_msgpack", "_compact_shared_field_read_supported", True),
            ("/batch_hash_multi_get_msgpack", "_shared_field_read_supported", False),
        )
        for path, supported, compact in routes:
            if getattr(self, supported) is False:
                continue
            results: List[List[Any]] = []
            progress = None
            try:
                batches = iter(self._shared_field_read_batches(keys, fields))
                first = next(batches, None)
                if first is None:
                    return []
                second = next(batches, None)
                if second is None:
                    chunk, body = first
                    raw = await transport.read(path, body)
                    if len(raw) > 1024 * 1024:
                        results = await asyncio.to_thread(self._decode_async_hash_payload, raw, chunk, compact)
                    else:
                        results = self._decode_async_hash_payload(raw, chunk, compact)
                else:
                    from itertools import chain
                    progress = [False]
                    batches = chain((first, second), batches)
                    first = second = None
                    async for decoded in self._read_hash_batches_async(
                        transport, path, batches, compact,
                        getattr(self, supported) is True, progress,
                    ):
                        results.extend(decoded)
            except AsyncReadHttpError as error:
                if error.status != 404 or results or progress is not None and progress[0]:
                    raise TemporalStoreCommandError(f"http_{error.status}", str(error)) from error
                setattr(self, supported, False)
            except AsyncReadLimitError as error:
                raise TemporalStoreCommandError("async_read_overloaded", str(error)) from error
            except AsyncReadClosedError as error:
                raise TemporalStoreCommandError("client_closed", str(error)) from error
            except (ValueError, msgpack.UnpackException) as error:
                raise TemporalStoreCommandError("invalid_binary_response", str(error)) from error
            else:
                setattr(self, supported, True)
                return results
        return await asyncio.to_thread(self.batch_hash_multi_get, keys, fields)

    async def _read_hash_batches_async(self, transport, path, batches, compact,
                                      known_supported, progress):
        import asyncio
        from collections import deque
        from itertools import chain

        async def read_batch(chunk, body):
            raw = await transport.read(path, body)
            if len(raw) > 1024 * 1024:
                decoded = await asyncio.to_thread(self._decode_async_hash_payload, raw, chunk, compact)
            else:
                decoded = self._decode_async_hash_payload(raw, chunk, compact)
            # A later batch can finish before the first. Its success also forbids fallback.
            progress[0] = True
            return decoded

        batches = iter(batches)
        if not known_supported:
            first = next(batches, None)
            if first is None:
                return
            yield await read_batch(*first)
        if self.max_async_read_batches == 1:
            for chunk, body in batches:
                yield await read_batch(chunk, body)
            return
        first = next(batches, None)
        if first is None:
            return
        second = next(batches, None)
        if second is None:
            yield await read_batch(*first)
            return
        batches = iter(chain((first, second), batches))
        first = second = None
        pending = deque()
        pending_bytes = 0
        waiting = None
        try:
            while True:
                while len(pending) < self.max_async_read_batches:
                    if waiting is None:
                        waiting = next(batches, None)
                    if waiting is None:
                        break
                    size = len(waiting[1])
                    if pending and pending_bytes + size > self.max_batch_request_bytes:
                        break
                    pending.append((asyncio.create_task(read_batch(*waiting)), size))
                    pending_bytes += size
                    waiting = None
                if not pending:
                    break
                task, size = pending.popleft()
                rows = await task
                pending_bytes -= size
                yield rows
        finally:
            for task, _size in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*(task for task, _size in pending), return_exceptions=True)

    def _decode_async_hash_payload(self, raw: bytes, chunk: List[str], compact: bool) -> List[List[Any]]:
        envelope = msgpack.unpackb(raw, raw=False)
        if compact:
            return self._decode_compact_hash_read_envelope(envelope, len(chunk))
        return [(response.get("values") or [])
                for response in self._decode_batch_envelope(envelope, chunk)]

    @staticmethod
    def _decode_compact_hash_read_envelope(
        envelope: Dict[str, Any], expected_count: int
    ) -> List[List[Any]]:
        status = envelope.get("status") or {}
        if not status.get("ok", False):
            raise TemporalStoreCommandError(
                status.get("code", "unknown"), status.get("message", "")
            )
        rows = envelope.get("rows") or []
        if len(rows) != expected_count:
            raise TemporalStoreCommandError(
                "batch_length_mismatch",
                f"sent {expected_count} reads, got {len(rows)} rows",
            )
        errors = envelope.get("errors") or []
        if errors:
            first = errors[0]
            item_status = first.get("status") or {}
            raise TemporalStoreCommandError(
                item_status.get("code", "unknown"),
                f"read {first.get('index', '?')}: "
                f"{item_status.get('message', '')}",
            )
        return rows

    def _pack_bounded_shared_request(
        self, document: Dict[str, Any], encoded_upper_bound: int
    ) -> Optional[bytes]:
        # The common single-chunk path should not retain both a row-only
        # Packer buffer and a second assembled request of the same size.
        if encoded_upper_bound > self.max_batch_request_bytes:
            return None
        # Small requests do not need a 256 KiB initial encoding arena.
        initial_buffer_bytes = min(256 * 1024, max(256, encoded_upper_bound))
        body = msgpack.packb(document, use_bin_type=True, buf_size=initial_buffer_bytes)
        return body if len(body) <= self.max_batch_request_bytes else None

    def _shared_field_read_batches(
        self, keys: List[str], fields: List[str]
    ) -> Iterator[Tuple[List[str], bytes]]:
        if msgpack is None:
            return
        if len(keys) <= self.max_batch_commands:
            encoded_upper_bound = (
                128
                + 4 * sum(map(len, keys))
                + 5 * len(keys)
                + 4 * sum(map(len, fields))
                + 5 * len(fields)
            )
            body = self._pack_bounded_shared_request(
                {
                    "shard_id": self.shard_id,
                    "keys": keys,
                    "fields": fields,
                },
                encoded_upper_bound,
            )
            if body is not None:
                yield keys, body
                return
        prefix = (
            b"\x83"
            + msgpack.packb("shard_id", use_bin_type=True)
            + msgpack.packb(self.shard_id, use_bin_type=True)
            + msgpack.packb("keys", use_bin_type=True)
        )
        suffix = msgpack.packb("fields", use_bin_type=True) + msgpack.packb(
            fields, use_bin_type=True
        )
        chunk: List[str] = []
        packer = msgpack.Packer(use_bin_type=True, autoreset=False)

        def body_for(key_count: int, payload_end: Optional[int] = None) -> bytes:
            view = packer.getbuffer()
            payload = view if payload_end is None else view[:payload_end]
            body = b"".join(
                (prefix, _msgpack_array_header(key_count), payload, suffix)
            )
            del payload, view
            return body

        for key in keys:
            if chunk and len(chunk) >= self.max_batch_commands:
                completed = chunk
                body = body_for(len(completed))
                packer.reset()
                chunk = []
                yield completed, body

            previous_payload_bytes = len(packer.getbuffer())
            packer.pack(key)
            next_body_bytes = (
                len(prefix)
                + len(_msgpack_array_header(len(chunk) + 1))
                + len(packer.getbuffer())
                + len(suffix)
            )
            if chunk and next_body_bytes > self.max_batch_request_bytes:
                completed = chunk
                body = body_for(len(completed), previous_payload_bytes)
                packer.reset()
                chunk = []
                yield completed, body
                packer.pack(key)
            chunk.append(key)
        if chunk:
            yield chunk, body_for(len(chunk))

    def _binary_batches(
        self, commands: List[Dict[str, Any]], max_commands: int
    ) -> Iterator[Tuple[List[Dict[str, Any]], bytes]]:
        if msgpack is None:
            return
        prefix = (
            b"\x82"
            + msgpack.packb("shard_id", use_bin_type=True)
            + msgpack.packb(self.shard_id, use_bin_type=True)
            + msgpack.packb("commands", use_bin_type=True)
        )
        chunk: List[Dict[str, Any]] = []
        packer = msgpack.Packer(use_bin_type=True, autoreset=False)

        def body_for(command_count: int, payload_end: Optional[int] = None) -> bytes:
            view = packer.getbuffer()
            payload = view if payload_end is None else view[:payload_end]
            body = b"".join((prefix, _msgpack_array_header(command_count), payload))
            del payload, view
            return body

        for command in commands:
            if chunk and len(chunk) >= max_commands:
                completed = chunk
                body = body_for(len(completed))
                packer.reset()
                chunk = []
                yield completed, body

            binary_command = _binary_command(command)
            previous_payload_bytes = len(packer.getbuffer())
            packer.pack(binary_command)
            next_body_bytes = (
                len(prefix)
                + len(_msgpack_array_header(len(chunk) + 1))
                + len(packer.getbuffer())
            )
            if chunk and next_body_bytes > self.max_batch_request_bytes:
                completed = chunk
                body = body_for(len(completed), previous_payload_bytes)
                packer.reset()
                chunk = []
                yield completed, body
                # This command belongs to the next body. Repacking only the
                # boundary command avoids retaining every encoded fragment.
                packer.pack(binary_command)
            chunk.append(command)
        if chunk:
            yield chunk, body_for(len(chunk))

    def batch_hash_multi_set(
        self,
        fields: List[str],
        rows: List[Tuple[str, List[bytes]]],
        request_upper_bound: Optional[int] = None,
        ttl_ms: Optional[int] = None,
    ) -> None:
        """Write one field projection to many hashes with shared wire metadata.

        Nodes without the compact route fall back once to the regular write
        batch. A transport failure after any compact chunk is never retried:
        the server may already have applied that chunk.
        The request size hint lets a row producer reuse size work it already
        performed and encode a bounded single chunk directly.
        """
        if not rows:
            return
        capability = (
            self._shared_field_ttl_write_supported
            if ttl_ms is not None
            else self._shared_field_write_supported
        )
        if msgpack is not None and capability is not False:
            applied_any = False
            try:
                for chunk, body in self._shared_field_write_batches(
                    fields, rows, request_upper_bound, ttl_ms
                ):
                    raw = self._request_encoded(
                        (
                            "/batch_hash_multi_set_ttl_msgpack"
                            if ttl_ms is not None
                            else "/batch_hash_multi_set_msgpack"
                        ),
                        body,
                        "application/msgpack",
                    )
                    envelope = msgpack.unpackb(raw, raw=False)
                    self._check_write_count(envelope, len(chunk))
                    applied_any = True
            except TemporalStoreCommandError as error:
                fallback_codes = {"http_404", "compact_ttl_write_unavailable"}
                if error.code not in fallback_codes or applied_any:
                    raise
                if ttl_ms is not None:
                    self._shared_field_ttl_write_supported = False
                else:
                    self._shared_field_write_supported = False
            except (ValueError, msgpack.UnpackException) as error:
                raise TemporalStoreCommandError(
                    "invalid_binary_response", str(error)
                ) from error
            else:
                if ttl_ms is not None:
                    self._shared_field_ttl_write_supported = True
                else:
                    self._shared_field_write_supported = True
                return

        kind = "hash_multi_set_with_ttl" if ttl_ms is not None else "hash_multi_set"
        commands = [
            {
                "kind": kind,
                "key": key,
                "entries": list(zip(fields, values)),
                **({"ttl_ms": ttl_ms} if ttl_ms is not None else {}),
            }
            for key, values in rows
        ]
        self.batch_write(commands)

    def batch_hash_timestamp_set(
        self,
        fields: List[str],
        timestamp_field: str,
        rows: List[Tuple[str, int, int, List[bytes]]],
        request_upper_bound: Optional[int] = None,
        ttl_ms: Optional[int] = None,
    ) -> Optional[Tuple[int, int]]:
        """Write timestamp-conditioned rows through the atomic batch route."""
        return self._batch_hash_timestamp_set(
            fields, timestamp_field, rows, request_upper_bound, ttl_ms, False
        )

    def batch_hash_timestamp_set_interleaved(
        self,
        fields: List[str],
        timestamp_field: str,
        rows: List[Tuple[str, int, int, List[bytes]]],
        request_upper_bound: Optional[int] = None,
        ttl_ms: Optional[int] = None,
    ) -> Optional[Tuple[int, int]]:
        """Keep rows atomic, allow readers between groups, and await durability.

        A failed call may have applied a prefix. Only an unattempted route-level
        404 permits fallback; an application error or unknown result does not.
        """
        return self._batch_hash_timestamp_set(
            fields, timestamp_field, rows, request_upper_bound, ttl_ms, True
        )

    def _batch_hash_timestamp_set(
        self,
        fields: List[str],
        timestamp_field: str,
        rows: List[Tuple[str, int, int, List[bytes]]],
        request_upper_bound: Optional[int] = None,
        ttl_ms: Optional[int] = None,
        interleaved: bool = False,
    ) -> Optional[Tuple[int, int]]:
        """Atomically write rows whose stored timestamp is older.

        ``None`` means the node does not expose the route and no row was
        attempted. Once any chunk receives a response, later failures are
        surfaced because retrying could overwrite rows twice.
        The request size hint enables direct encoding when the producer has
        already bounded the complete request.
        """
        if not rows:
            return (0, 0)
        if interleaved and self._interleaved_conditional_write_supported is False:
            interleaved = False
        capability = (
            self._interleaved_conditional_write_supported
            if interleaved
            else (
                self._conditional_shared_field_ttl_write_supported
                if ttl_ms is not None
                else self._conditional_shared_field_write_supported
            )
        )
        if msgpack is None or capability is False:
            return None

        processed_any = False
        applied = 0
        skipped = 0
        try:
            for chunk, body in self._shared_timestamp_write_batches(
                fields, timestamp_field, rows, request_upper_bound, ttl_ms
            ):
                path = (
                    "/batch_hash_timestamp_set_interleaved_msgpack"
                    if interleaved
                    else (
                        "/batch_hash_timestamp_set_ttl_msgpack"
                        if ttl_ms is not None
                        else "/batch_hash_timestamp_set_msgpack"
                    )
                )
                raw = self._request_encoded(
                    path,
                    body,
                    "application/msgpack",
                )
                processed_any = True
                envelope = msgpack.unpackb(raw, raw=False)
                status = envelope.get("status") or {}
                if not status.get("ok", False):
                    raise TemporalStoreCommandError(
                        status.get("code", "unknown"), status.get("message", "")
                    )
                errors = envelope.get("errors") or []
                if errors:
                    first = errors[0]
                    item_status = first.get("status") or {}
                    raise TemporalStoreCommandError(
                        item_status.get("code", "unknown"),
                        f"write {first.get('index', '?')}: "
                        f"{item_status.get('message', '')}",
                    )
                chunk_applied = int(envelope.get("applied", 0))
                chunk_skipped = int(envelope.get("skipped", 0))
                if chunk_applied + chunk_skipped != len(chunk):
                    raise TemporalStoreCommandError(
                        "batch_length_mismatch",
                        f"sent {len(chunk)} writes, got "
                        f"{chunk_applied + chunk_skipped} results",
                    )
                applied += chunk_applied
                skipped += chunk_skipped
        except TemporalStoreCommandError as error:
            if error.code != "http_404" or processed_any:
                raise
            if interleaved:
                self._interleaved_conditional_write_supported = False
                return self._batch_hash_timestamp_set(
                    fields, timestamp_field, rows, request_upper_bound, ttl_ms, False
                )
            if ttl_ms is None:
                self._conditional_shared_field_write_supported = False
            else:
                self._conditional_shared_field_ttl_write_supported = False
            return None
        except (ValueError, msgpack.UnpackException) as error:
            raise TemporalStoreCommandError(
                "invalid_binary_response", str(error)
            ) from error

        if interleaved:
            self._interleaved_conditional_write_supported = True
        elif ttl_ms is None:
            self._conditional_shared_field_write_supported = True
        else:
            self._conditional_shared_field_ttl_write_supported = True
        return applied, skipped

    def _shared_timestamp_write_batches(
        self,
        fields: List[str],
        timestamp_field: str,
        rows: List[Tuple[str, int, int, List[bytes]]],
        request_upper_bound: Optional[int] = None,
        ttl_ms: Optional[int] = None,
    ) -> Iterator[Tuple[List[Tuple[str, int, int, List[bytes]]], bytes]]:
        if msgpack is None:
            return
        if (
            request_upper_bound is not None
            and len(rows) <= self.max_write_batch_commands
        ):
            document = {
                "shard_id": self.shard_id,
                "fields": fields,
                "timestamp_field": timestamp_field,
                "rows": rows,
            }
            if ttl_ms is not None:
                document["ttl_ms"] = ttl_ms
            body = self._pack_bounded_shared_request(
                document,
                request_upper_bound,
            )
            if body is not None:
                yield rows, body
                return
        prefix = (
            _msgpack_map_header(5 if ttl_ms is not None else 4)
            + msgpack.packb("shard_id", use_bin_type=True)
            + msgpack.packb(self.shard_id, use_bin_type=True)
            + msgpack.packb("fields", use_bin_type=True)
            + msgpack.packb(fields, use_bin_type=True)
            + msgpack.packb("timestamp_field", use_bin_type=True)
            + msgpack.packb(timestamp_field, use_bin_type=True)
        )
        if ttl_ms is not None:
            prefix += msgpack.packb("ttl_ms", use_bin_type=True) + msgpack.packb(
                ttl_ms, use_bin_type=True
            )
        prefix += msgpack.packb("rows", use_bin_type=True)
        chunk: List[Tuple[str, int, int, List[bytes]]] = []
        packer = msgpack.Packer(use_bin_type=True, autoreset=False)

        def body_for(row_count: int, payload_end: Optional[int] = None) -> bytes:
            view = packer.getbuffer()
            payload = view if payload_end is None else view[:payload_end]
            body = b"".join((prefix, _msgpack_array_header(row_count), payload))
            del payload, view
            return body

        for row in rows:
            if chunk and len(chunk) >= self.max_write_batch_commands:
                completed = chunk
                body = body_for(len(completed))
                packer.reset()
                chunk = []
                yield completed, body

            previous_payload_bytes = len(packer.getbuffer())
            packer.pack(row)
            next_body_bytes = (
                len(prefix)
                + len(_msgpack_array_header(len(chunk) + 1))
                + len(packer.getbuffer())
            )
            if chunk and next_body_bytes > self.max_batch_request_bytes:
                completed = chunk
                body = body_for(len(completed), previous_payload_bytes)
                packer.reset()
                chunk = []
                yield completed, body
                packer.pack(row)
            chunk.append(row)
        if chunk:
            yield chunk, body_for(len(chunk))

    def _shared_field_write_batches(
        self,
        fields: List[str],
        rows: List[Tuple[str, List[bytes]]],
        request_upper_bound: Optional[int] = None,
        ttl_ms: Optional[int] = None,
    ) -> Iterator[Tuple[List[Tuple[str, List[bytes]]], bytes]]:
        if msgpack is None:
            return
        if (
            request_upper_bound is not None
            and len(rows) <= self.max_write_batch_commands
        ):
            body = self._pack_bounded_shared_request(
                {
                    "shard_id": self.shard_id,
                    "fields": fields,
                    "rows": rows,
                    **({"ttl_ms": ttl_ms} if ttl_ms is not None else {}),
                },
                request_upper_bound,
            )
            if body is not None:
                yield rows, body
                return
        prefix = (
            _msgpack_map_header(4 if ttl_ms is not None else 3)
            + msgpack.packb("shard_id", use_bin_type=True)
            + msgpack.packb(self.shard_id, use_bin_type=True)
            + msgpack.packb("fields", use_bin_type=True)
            + msgpack.packb(fields, use_bin_type=True)
            + (
                msgpack.packb("ttl_ms", use_bin_type=True)
                + msgpack.packb(ttl_ms, use_bin_type=True)
                if ttl_ms is not None
                else b""
            )
            + msgpack.packb("rows", use_bin_type=True)
        )
        chunk: List[Tuple[str, List[bytes]]] = []
        packer = msgpack.Packer(use_bin_type=True, autoreset=False)

        def body_for(row_count: int, payload_end: Optional[int] = None) -> bytes:
            view = packer.getbuffer()
            payload = view if payload_end is None else view[:payload_end]
            body = b"".join((prefix, _msgpack_array_header(row_count), payload))
            del payload, view
            return body

        for row in rows:
            if chunk and len(chunk) >= self.max_write_batch_commands:
                completed = chunk
                body = body_for(len(completed))
                packer.reset()
                chunk = []
                yield completed, body

            previous_payload_bytes = len(packer.getbuffer())
            packer.pack(row)
            next_body_bytes = (
                len(prefix)
                + len(_msgpack_array_header(len(chunk) + 1))
                + len(packer.getbuffer())
            )
            if chunk and next_body_bytes > self.max_batch_request_bytes:
                completed = chunk
                body = body_for(len(completed), previous_payload_bytes)
                packer.reset()
                chunk = []
                yield completed, body
                packer.pack(row)
            chunk.append(row)
        if chunk:
            yield chunk, body_for(len(chunk))

    def _encoded_batches(
        self, commands: List[Dict[str, Any]], max_commands: int
    ) -> Iterator[Tuple[List[Dict[str, Any]], bytes]]:
        chunk: List[Dict[str, Any]] = []
        encoded_chunk: List[bytes] = []
        prefix = (
            b'{"shard_id":'
            + str(self.shard_id).encode("ascii")
            + b',"commands":['
        )
        envelope_bytes = len(prefix) + len(b"]}")
        chunk_bytes = envelope_bytes
        for command in commands:
            # Keep the exact encoding used for the request. Long feature values
            # dominate the body; encoding them again while building the envelope
            # doubles client CPU and temporary memory for no change on the wire.
            encoded = json.dumps(
                _json_command(command), separators=(",", ":")
            ).encode("utf-8")
            separator_bytes = 1 if chunk else 0
            if chunk and (
                len(chunk) >= max_commands
                or chunk_bytes + separator_bytes + len(encoded)
                > self.max_batch_request_bytes
            ):
                yield chunk, prefix + b",".join(encoded_chunk) + b"]}"
                chunk = []
                encoded_chunk = []
                chunk_bytes = envelope_bytes
                separator_bytes = 0
            chunk.append(command)
            encoded_chunk.append(encoded)
            chunk_bytes += separator_bytes + len(encoded)
        if chunk:
            yield chunk, prefix + b",".join(encoded_chunk) + b"]}"

    def _batch_once_json(
        self,
        commands: List[Dict[str, Any]],
        body: bytes,
    ) -> List[Dict[str, Any]]:
        post = self._post_read_encoded if commands and all(
            command.get("kind") in _READ_ONLY_COMMAND_KINDS for command in commands
        ) else self._post_encoded
        envelope = post("/batch_execute", body)
        return self._decode_batch_envelope(envelope, commands)

    def _decode_batch_envelope(
        self, envelope: Dict[str, Any], commands: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        status = envelope.get("status") or {}
        if not status.get("ok", False):
            raise TemporalStoreCommandError(
                status.get("code", "unknown"), status.get("message", "")
            )
        responses = envelope.get("responses") or []
        if len(responses) != len(commands):
            # The batch is positional. A short answer silently shifts every
            # value onto the wrong entity, which reads as stale features rather
            # than as an error, so it is refused here instead.
            raise TemporalStoreCommandError(
                "batch_length_mismatch",
                f"sent {len(commands)} commands, got {len(responses)} responses",
            )
        return [self._unwrap(item) for item in responses]

    def batch_write(self, commands: List[Dict[str, Any]]) -> None:
        """Run write commands with a compact success count and sparse failures.

        A node without the write-only endpoint answers 404 before applying anything;
        that response is safe to fall back from to the regular batch endpoint. Other
        transport errors remain ambiguous and are never retried.
        """
        if not commands:
            return
        if msgpack is not None and self._binary_write_supported is not False:
            applied_any = False
            try:
                for chunk, body in self._binary_batches(
                    commands, self.max_write_batch_commands
                ):
                    raw = self._request_encoded(
                        "/batch_write_msgpack", body, "application/msgpack"
                    )
                    envelope = msgpack.unpackb(raw, raw=False)
                    self._check_write_envelope(envelope, chunk)
                    applied_any = True
            except TemporalStoreCommandError as error:
                if error.code != "http_404" or applied_any:
                    raise
                self._binary_write_supported = False
            except (ValueError, msgpack.UnpackException) as error:
                raise TemporalStoreCommandError(
                    "invalid_binary_response", str(error)
                ) from error
            else:
                self._binary_write_supported = True
                return

        for chunk, body in self._encoded_batches(
            commands, self.max_write_batch_commands
        ):
            if self._batch_write_supported is False:
                for fallback_chunk, fallback_body in self._encoded_batches(
                    chunk, self.max_batch_commands
                ):
                    self._batch_once_json(fallback_chunk, fallback_body)
                continue
            try:
                envelope = self._post_encoded("/batch_write", body)
            except TemporalStoreCommandError as error:
                if error.code != "http_404":
                    raise
                self._batch_write_supported = False
                for fallback_chunk, fallback_body in self._encoded_batches(
                    chunk, self.max_batch_commands
                ):
                    self._batch_once_json(fallback_chunk, fallback_body)
                continue

            self._batch_write_supported = True
            self._check_write_envelope(envelope, chunk)

    def _check_write_envelope(
        self, envelope: Dict[str, Any], commands: List[Dict[str, Any]]
    ) -> None:
        self._check_write_count(envelope, len(commands))

    def _check_write_count(
        self, envelope: Dict[str, Any], expected_count: int
    ) -> None:
        status = envelope.get("status") or {}
        if not status.get("ok", False):
            raise TemporalStoreCommandError(
                status.get("code", "unknown"), status.get("message", "")
            )
        errors = envelope.get("errors") or []
        applied = int(envelope.get("applied", 0))
        if applied + len(errors) != expected_count:
            raise TemporalStoreCommandError(
                "batch_length_mismatch",
                f"sent {expected_count} writes, got {applied} successes and "
                f"{len(errors)} errors",
            )
        if errors:
            first = errors[0]
            item_status = first.get("status") or {}
            raise TemporalStoreCommandError(
                item_status.get("code", "unknown"),
                f"command {first.get('index', '?')}: "
                f"{item_status.get('message', '')}",
            )

    def health(self) -> bool:
        try:
            conn = self._connection()
            conn.request("GET", "/health")
            response = conn.getresponse()
            response.read()
            return response.status == 200
        except (http.client.HTTPException, OSError):
            self._drop_connection()
            return False
