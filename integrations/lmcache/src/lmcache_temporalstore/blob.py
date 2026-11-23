# SPDX-License-Identifier: Apache-2.0
"""Raw, bounded-memory access to the TemporalStore datanode blob route."""

from __future__ import annotations

from contextlib import nullcontext
import http.client
import json
import logging
from select import select
from threading import Lock
from urllib.parse import quote, urlsplit


logger = logging.getLogger(__name__)


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

    def _read_request(self, key: str):
        path = self._blob_path(key)
        connection, self._read_connection = self._read_connection, None
        if connection is not None:
            try:
                return self._request("GET", path, connection=connection)
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
                # The server can close an idle socket before the next GET.
                # A read may retry once before receiving a response.
                pass
        return self._request("GET", path)

    def get_into(self, key: str, destination: memoryview) -> bool:
        with self._read_lock:
            for candidate in (key, f"anonymous/{key}"):
                connection, response = self._read_request(candidate)
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
                        offset = 0
                        while offset < len(destination):
                            count = response.readinto(destination[offset:])
                            if not count:
                                return False
                            offset += count
                        consumed = True
                        return True
                finally:
                    if consumed and self._reuse_read_connection and connection.sock is not None:
                        self._read_connection = connection
                    else:
                        connection.close()
        return False

    def close(self) -> None:
        with self._read_lock:
            connection, self._read_connection = self._read_connection, None
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
