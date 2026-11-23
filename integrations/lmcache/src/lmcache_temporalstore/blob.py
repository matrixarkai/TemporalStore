# SPDX-License-Identifier: Apache-2.0
"""Raw, bounded-memory access to the TemporalStore datanode blob route."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import nullcontext
import http.client
from itertools import chain
import json
import logging
from select import select
from threading import Event, Lock
from time import monotonic
from urllib.parse import quote, urlsplit


logger = logging.getLogger(__name__)
_BLOB_BATCH_KEYS = 32
_BLOB_BATCH_BYTES = 512 * 1024 * 1024
_BLOB_SMALL_READ_BYTES = 512 * 1024
_BATCH_RECHECK_SECONDS = 30
_BATCH_UNAVAILABLE = (404, 405, 501)
_MAX_BATCH_CONTROL_BYTES = 64 * 1024


class BlobMutationDeferred(RuntimeError):
    """No mutation was transmitted to the datanode."""


class BlobClient:
    def __init__(
        self, base_url: str, timeout_seconds: float = 10.0, *,
        reuse_read_connection: bool = False, reuse_mutation_connection: bool = False,
        legacy_names: bool = True, mutation_cooldown_seconds: float = 0.0,
    ) -> None:
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("blob_url must be an HTTP URL with a host")
        self._host = parsed.hostname
        self._port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self._connection_type = (
            http.client.HTTPSConnection if parsed.scheme == "https"
            else http.client.HTTPConnection
        )
        self._prefix = parsed.path.rstrip("/")
        self._timeout = timeout_seconds
        self._legacy_names = legacy_names
        self._reuse_read_connection = reuse_read_connection
        self._read_lock = Lock() if reuse_read_connection else nullcontext()
        self._read_connection: http.client.HTTPConnection | None = None
        self._batch_read_retry_at = 0.0
        self._reuse_mutation_connection = reuse_mutation_connection
        self._mutation_lock = Lock() if reuse_mutation_connection else nullcontext()
        self._mutation_connection: http.client.HTTPConnection | None = None
        self._mutation_cooldown_seconds = mutation_cooldown_seconds
        self._mutation_retry_at = 0.0
        self._batch_write_supported = False
        self._batch_write_retry_at = 0.0

    def _request(
        self, method: str, path: str,
        body: bytes | memoryview | Iterable[bytes | memoryview] | None = None, *,
        connection: http.client.HTTPConnection | None = None,
        content_type: str = "application/octet-stream", body_length: int | None = None,
    ):
        if connection is None:
            connection = self._connection_type(
                self._host, self._port, timeout=self._timeout
            )
        try:
            connection.putrequest(method, self._prefix + path)
            if body is not None:
                connection.putheader("Content-Type", content_type)
                if body_length is None:
                    if not isinstance(body, (bytes, memoryview)):
                        raise ValueError("streaming request requires a content length")
                    body_length = len(body)
                connection.putheader("Content-Length", str(body_length))
            connection.endheaders(body)
            response = connection.getresponse()
            return connection, response
        except BaseException:
            connection.close()
            raise

    def mutations_available(self) -> bool:
        return not self._mutation_retry_at or monotonic() >= self._mutation_retry_at

    def defer_mutations(self) -> None:
        if self._mutation_cooldown_seconds:
            self._mutation_retry_at = monotonic() + self._mutation_cooldown_seconds

    def _mutate(
        self, method: str, path: str,
        body: bytes | memoryview | Iterable[bytes | memoryview] | None = None, *,
        content_type: str = "application/octet-stream", body_length: int | None = None,
        allowed_status: tuple[int, ...] = (200,),
    ) -> tuple[int, bytes]:
        with self._mutation_lock:
            if not self.mutations_available():
                raise BlobMutationDeferred("blob mutations are waiting for the datanode")
            connection, self._mutation_connection = self._mutation_connection, None
            if connection is not None and select([connection.sock], [], [], 0)[0]:
                # No response is outstanding. Retire a socket with pending EOF
                # before sending a mutation that cannot be safely replayed.
                connection.close()
                connection = None
            consumed = False
            try:
                if connection is None and self._mutation_cooldown_seconds:
                    connection = self._connection_type(
                        self._host, self._port, timeout=self._timeout,
                    )
                    try:
                        connection.connect()
                    except (OSError, http.client.HTTPException) as error:
                        self.defer_mutations()
                        raise BlobMutationDeferred("could not connect to datanode") from error
                connection, response = self._request(
                    method, path, body, connection=connection, content_type=content_type,
                    body_length=body_length,
                )
                with response:
                    payload = response.read()
                    consumed = response.status in allowed_status
            except (OSError, http.client.HTTPException):
                self.defer_mutations()
                raise
            finally:
                if connection is not None:
                    if consumed and self._reuse_mutation_connection and connection.sock is not None:
                        self._mutation_connection = connection
                    else:
                        connection.close()
            self._mutation_retry_at = 0.0
            if not consumed:
                raise OSError(f"blob {method} returned HTTP {response.status}")
            return response.status, payload

    def put(self, key: str, payload: memoryview) -> None:
        self._mutate("PUT", self._blob_path(key), payload)

    def _put_batch(self, items: list[tuple[str, memoryview]], size: int) -> list[bool | None] | None:
        if not self._batch_write_supported:
            if monotonic() < self._batch_write_retry_at:
                return None
            try:
                status, _ = self._mutate("GET", "/batch_write_blobs", allowed_status=(200, *_BATCH_UNAVAILABLE))
            except (OSError, http.client.HTTPException):
                if not self.mutations_available():
                    return [None] * len(items)
                raise
            self._batch_write_supported = status == 200
            if not self._batch_write_supported:
                self._batch_write_retry_at = monotonic() + _BATCH_RECHECK_SECONDS
                return None
        control = json.dumps([{"key": key, "length": len(payload)} for key, payload in items]).encode()
        if len(control) > _MAX_BATCH_CONTROL_BYTES:
            return None
        prefix = len(control).to_bytes(4, "big")
        body = chain((prefix, control), (payload for _, payload in items))
        status, reply = self._mutate(
            "POST", "/batch_write_blobs", body, body_length=len(prefix) + len(control) + size,
            allowed_status=(200, *_BATCH_UNAVAILABLE),
        )
        if status != 200:
            self._batch_write_supported = False
            self._batch_write_retry_at = monotonic() + _BATCH_RECHECK_SECONDS
            return None
        if len(reply) != len(items) or any(value not in (0, 1) for value in reply):
            raise OSError("invalid blob write receipt")
        return [value == 1 for value in reply]

    def put_many(self, items: list[tuple[str, memoryview]]) -> list[bool | None]:
        # None means no upload was attempted. False may require reconciliation.
        written: list[bool | None] = []
        offset = 0
        while offset < len(items):
            end = offset
            size = 0
            for index in range(offset, min(len(items), offset + _BLOB_BATCH_KEYS)):
                length = len(items[index][1])
                if index > offset and size + length > _BLOB_BATCH_BYTES:
                    break
                end = index + 1
                size += length
            group = items[offset:end]
            offset = end
            if len(group) > 1:
                try:
                    result = self._put_batch(group, size)
                except BlobMutationDeferred:
                    written.extend([None] * len(group))
                    continue
                except (OSError, http.client.HTTPException):
                    logger.exception("TemporalStore L2 blob write batch failed")
                    written.extend([False] * len(group))
                    continue
                if result is not None:
                    written.extend(result)
                    continue
            for key, payload in group:
                try:
                    self.put(key, payload)
                except BlobMutationDeferred:
                    written.append(None)
                except (OSError, http.client.HTTPException):
                    logger.exception("TemporalStore L2 blob write failed")
                    written.append(False)
                else:
                    written.append(True)
        return written

    def _candidate_keys(self, key: str) -> tuple[str, ...]:
        return (key, f"anonymous/{key}") if self._legacy_names else (key,)

    @staticmethod
    def _blob_path(key: str) -> str:
        return f"/blob/{quote(key, safe='/')}"

    def _read_request(
        self, method: str, path: str, body: bytes | None = None,
        content_type: str = "application/octet-stream",
    ):
        connection, self._read_connection = self._read_connection, None
        if connection is not None:
            try:
                return self._request(
                    method, path, body, connection=connection, content_type=content_type,
                )
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
                # Reads can reconnect once when a reused socket is closed.
                pass
        return self._request(method, path, body, content_type=content_type)

    def _finish_read(self, connection: http.client.HTTPConnection, consumed: bool) -> None:
        if consumed and self._reuse_read_connection and connection.sock is not None:
            self._read_connection = connection
        else:
            connection.close()

    @staticmethod
    def _read_into(response: http.client.HTTPResponse, destination: memoryview) -> bool:
        offset = 0
        while offset < len(destination):
            count = response.readinto(destination[offset:])
            if not count:
                return False
            offset += count
        return True

    def get_into(self, key: str, destination: memoryview) -> bool:
        with self._read_lock:
            for candidate in self._candidate_keys(key):
                connection, response = self._read_request("GET", self._blob_path(candidate))
                consumed = False
                try:
                    with response:
                        if response.status == 404:
                            response.read()
                            consumed = True
                            continue
                        if response.status != 200:
                            response.read()
                            raise OSError(f"blob GET returned HTTP {response.status}")
                        length = response.getheader("Content-Length")
                        if length is None or int(length) != len(destination):
                            return False
                        consumed = self._read_into(response, destination)
                        return consumed
                finally:
                    self._finish_read(connection, consumed)
        return False

    def _read_batch(self, items: list[tuple[str, memoryview]]) -> bytes | None:
        with self._read_lock:
            if monotonic() < self._batch_read_retry_at:
                return None
            body = json.dumps([{"key": key, "length": len(out)} for key, out in items]).encode()
            connection, response = self._read_request(
                "POST", "/batch_read_blobs", body, "application/json",
            )
            consumed = False
            try:
                with response:
                    if response.status in _BATCH_UNAVAILABLE:
                        response.read()
                        consumed = True
                        # Recheck periodically so rolling upgrades can enable batching.
                        self._batch_read_retry_at = monotonic() + _BATCH_RECHECK_SECONDS
                        return None
                    if response.status != 200:
                        response.read()
                        raise OSError(f"blob batch read returned HTTP {response.status}")
                    statuses = response.read(len(items))
                    if len(statuses) != len(items) or any(status not in (0, 1, 2) for status in statuses):
                        raise OSError("invalid blob batch read status table")
                    length = len(statuses) + sum(
                        len(out) for status, (_, out) in zip(statuses, items) if status == 1
                    )
                    if int(response.getheader("Content-Length", "-1")) != length:
                        raise OSError("invalid blob batch read length")
                    payload_length = length - len(statuses)
                    if payload_length <= _BLOB_SMALL_READ_BYTES:
                        # Small reads share one bounded buffer to reduce calls into the HTTP reader.
                        payload = memoryview(response.read(payload_length))
                        if len(payload) != payload_length:
                            raise OSError("incomplete blob batch read")
                        offset = 0
                        for status, (_, destination) in zip(statuses, items):
                            if status == 1:
                                end = offset + len(destination)
                                destination[:] = payload[offset:end]
                                offset = end
                    else:
                        for status, (_, destination) in zip(statuses, items):
                            if status == 1 and not self._read_into(response, destination):
                                raise OSError("incomplete blob batch read")
                    consumed = True
                    return statuses
            finally:
                self._finish_read(connection, consumed)

    def get_many_into(self, items: list[tuple[str, memoryview]]) -> list[bool]:
        found = []
        for offset in range(0, len(items), _BLOB_BATCH_KEYS):
            group = items[offset:offset + _BLOB_BATCH_KEYS]
            statuses = None
            if len(group) > 1 and sum(len(out) for _, out in group) <= _BLOB_BATCH_BYTES:
                try:
                    statuses = self._read_batch(group)
                except (OSError, ValueError, http.client.HTTPException):
                    logger.exception("TemporalStore L2 blob read batch failed")
                    found.extend([False] * len(group))
                    continue
            for status, (key, destination) in zip(statuses or bytes(len(group)), group):
                if status == 0:
                    try:
                        found.append(self.get_into(key, destination))
                    except (OSError, ValueError, http.client.HTTPException):
                        logger.exception("TemporalStore L2 blob read failed")
                        found.append(False)
                else:
                    found.append(status == 1)
        return found

    def probe_many(self, items: list[tuple[str, int]]) -> list[bool]:
        """Check named local files and their lengths. An empty batch checks support."""
        found = []
        for offset in range(0, max(1, len(items)), 256):
            batch = items[offset:offset + 256]
            body = json.dumps([{"key": key, "length": length} for key, length in batch]).encode()
            with self._read_lock:
                connection, response = self._read_request(
                    "POST", "/batch_probe_blobs", body, "application/json",
                )
                consumed = False
                try:
                    with response:
                        if response.status in _BATCH_UNAVAILABLE:
                            raise ValueError("payload_metadata requires a datanode with blob probes")
                        if response.status != 200:
                            raise OSError(f"blob probe returned HTTP {response.status}")
                        raw = response.read(4097)
                        if len(raw) > 4096 or not response.isclosed():
                            raise OSError("oversized blob probe response")
                        consumed = True
                    try:
                        replies = json.loads(raw)
                    except ValueError as error:
                        raise OSError("invalid blob probe response") from error
                    if not isinstance(replies, list) or len(replies) != len(batch) or any(
                        not isinstance(reply, bool) for reply in replies
                    ):
                        raise OSError("invalid blob probe results")
                    found.extend(replies)
                finally:
                    self._finish_read(connection, consumed)
        return found

    def inventory(self, namespace: str, stop: Event | None = None) -> Iterator[tuple[bytes, int]]:
        def page(**fields):
            status, body = self._mutate(
                "POST", "/lmcache_blob_inventory",
                json.dumps({"namespace": namespace, **fields}).encode(),
                content_type="application/json", allowed_status=(200, *_BATCH_UNAVAILABLE),
            )
            if status != 200:
                raise RuntimeError(f"datanode does not support LMCache inventory (HTTP {status})")
            return json.loads(body)

        if stop is not None and stop.is_set():
            return
        partitions = page()["partitions"]
        for partition in partitions:
            after = None
            while True:
                if stop is not None and stop.is_set():
                    return
                reply = page(partition=partition, after=after)
                for entry in sorted(reply["entries"], key=lambda item: (item["modified_ms"], item["id"])):
                    yield bytes.fromhex(entry["id"]), entry["size"]
                cursor = reply["next"]
                if cursor is None:
                    break
                if after is not None and cursor <= after:
                    raise ValueError("LMCache inventory cursor did not advance")
                after = cursor

    def close(self) -> None:
        with self._read_lock:
            connection, self._read_connection = self._read_connection, None
            self._batch_read_retry_at = 0.0
            if connection is not None:
                connection.close()
        with self._mutation_lock:
            connection, self._mutation_connection = self._mutation_connection, None
            if connection is not None:
                connection.close()

    def delete(self, key: str) -> None:
        for candidate in self._candidate_keys(key):
            self._mutate("DELETE", self._blob_path(candidate))

    def delete_many(self, keys: list[str]) -> list[bool]:
        if not keys:
            return []
        candidates = [candidate for key in keys for candidate in self._candidate_keys(key)]
        status, reply = self._mutate(
            "POST", "/batch_delete_blobs", json.dumps({"keys": candidates}).encode(),
            content_type="application/json", allowed_status=(200, 404, 405, 501),
        )
        if status != 200:
            # Older datanodes expose individual DELETE requests only.
            deleted = []
            for key in keys:
                try:
                    self.delete(key)
                except (OSError, http.client.HTTPException):
                    logger.exception("TemporalStore L2 blob delete failed")
                    deleted.append(False)
                else:
                    deleted.append(True)
            return deleted

        envelope = json.loads(reply)
        deleted = envelope.get("deleted") if isinstance(envelope, dict) else None
        if not isinstance(deleted, list) or len(deleted) != len(candidates) or any(
            not isinstance(result, bool) for result in deleted
        ):
            raise OSError("invalid blob delete batch response")
        if not self._legacy_names:
            return deleted
        return [primary and legacy for primary, legacy in zip(deleted[::2], deleted[1::2])]
