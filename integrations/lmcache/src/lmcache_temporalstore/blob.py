# SPDX-License-Identifier: Apache-2.0
"""Raw, bounded-memory access to the TemporalStore datanode blob route."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import nullcontext
import http.client
import json
import logging
from select import select
from threading import Lock
from time import monotonic
from urllib.parse import quote, urlsplit


logger = logging.getLogger(__name__)
_READ_BATCH_KEYS = 32
_READ_BATCH_BYTES = 512 * 1024 * 1024
_BATCH_READ_RECHECK_SECONDS = 30


class BlobClient:
    def __init__(
        self, base_url: str, timeout_seconds: float = 10.0, *,
        reuse_read_connection: bool = False, reuse_mutation_connection: bool = False,
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
        self._reuse_read_connection = reuse_read_connection
        self._read_lock = Lock() if reuse_read_connection else nullcontext()
        self._read_connection: http.client.HTTPConnection | None = None
        self._batch_read_retry_at = 0.0
        self._reuse_mutation_connection = reuse_mutation_connection
        self._mutation_lock = Lock() if reuse_mutation_connection else nullcontext()
        self._mutation_connection: http.client.HTTPConnection | None = None

    def _request(
        self, method: str, path: str, body: bytes | memoryview | None = None, *,
        connection: http.client.HTTPConnection | None = None,
        content_type: str = "application/octet-stream",
    ):
        if connection is None:
            connection = self._connection_type(
                self._host, self._port, timeout=self._timeout
            )
        try:
            connection.putrequest(method, self._prefix + path)
            if body is not None:
                connection.putheader("Content-Type", content_type)
                connection.putheader("Content-Length", str(len(body)))
            connection.endheaders()
            if body is not None:
                connection.send(body)
            response = connection.getresponse()
            return connection, response
        except BaseException:
            connection.close()
            raise

    def _mutate(
        self, method: str, path: str, body: bytes | memoryview | None = None, *,
        content_type: str = "application/octet-stream", allowed_status: tuple[int, ...] = (200,),
    ) -> tuple[int, bytes]:
        with self._mutation_lock:
            connection, self._mutation_connection = self._mutation_connection, None
            if connection is not None and select([connection.sock], [], [], 0)[0]:
                # No response is outstanding. Retire a socket with pending EOF
                # before sending a mutation that cannot be safely replayed.
                connection.close()
                connection = None
            connection, response = self._request(
                method, path, body, connection=connection, content_type=content_type,
            )
            consumed = False
            try:
                with response:
                    payload = response.read()
                    if response.status not in allowed_status:
                        raise OSError(f"blob {method} returned HTTP {response.status}")
                    consumed = True
                    return response.status, payload
            finally:
                if consumed and self._reuse_mutation_connection and connection.sock is not None:
                    self._mutation_connection = connection
                else:
                    connection.close()

    def put(self, key: str, payload: memoryview) -> None:
        self._mutate("PUT", self._blob_path(key), payload)

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
            for candidate in (key, f"anonymous/{key}"):
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
                    if response.status in (404, 405, 501):
                        response.read()
                        consumed = True
                        # Recheck periodically so rolling upgrades can enable batching.
                        self._batch_read_retry_at = monotonic() + _BATCH_READ_RECHECK_SECONDS
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
                    for status, (_, destination) in zip(statuses, items):
                        if status == 1 and not self._read_into(response, destination):
                            raise OSError("incomplete blob batch read")
                    consumed = True
                    return statuses
            finally:
                self._finish_read(connection, consumed)

    def get_many_into(self, items: list[tuple[str, memoryview]]) -> list[bool]:
        found = []
        for offset in range(0, len(items), _READ_BATCH_KEYS):
            group = items[offset:offset + _READ_BATCH_KEYS]
            statuses = None
            if len(group) > 1 and sum(len(out) for _, out in group) <= _READ_BATCH_BYTES:
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

    def inventory(self, namespace: str) -> Iterator[tuple[bytes, int]]:
        def page(**fields):
            _, body = self._mutate(
                "POST", "/lmcache_blob_inventory",
                json.dumps({"namespace": namespace, **fields}).encode(),
                content_type="application/json",
            )
            return json.loads(body)

        partitions = page()["partitions"]
        for partition in partitions:
            after = None
            while True:
                reply = page(partition=partition, after=after)
                for entry in sorted(reply["entries"], key=lambda item: (item["modified_ms"], item["id"])):
                    yield bytes.fromhex(entry["id"]), entry["size"]
                cursor = reply["next"]
                if cursor is None:
                    break
                if after is not None and cursor <= after:
                    raise OSError("LMCache inventory cursor did not advance")
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
        for candidate in (key, f"anonymous/{key}"):
            self._mutate("DELETE", self._blob_path(candidate))

    def delete_many(self, keys: list[str]) -> list[bool]:
        if not keys:
            return []
        candidates = [candidate for key in keys for candidate in (key, f"anonymous/{key}")]
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
        return [primary and legacy for primary, legacy in zip(deleted[::2], deleted[1::2])]
