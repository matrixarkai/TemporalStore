# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""HTTP/JSON command client for a TemporalStore data node.

The data node serves the command surface at ``POST /execute`` and
``POST /batch_execute``. Both take a shard id and one or more ``Command``
documents; ``Command`` is a serde-tagged enum, so a command is a JSON object
whose ``kind`` is the snake_case variant name and whose remaining fields are
the variant's fields.

Two asymmetries in that wire format are load-bearing for anyone measuring it,
so they are stated here rather than discovered later:

* **Request payloads are base64.** ``HashMultiSet.entries`` carries a
  ``bytes_serde`` annotation, so values go out as base64 strings -- a flat
  1.33x on the bytes, whatever they are.
* **Response payloads are base64.** ``CommandResponse::Values`` uses the same
  flat 1.33x encoding as requests. :func:`decode_bytes` also accepts the older
  decimal-array shape so clients can read responses from either node version.
"""

from __future__ import annotations

import base64
import http.client
import json
import threading
from typing import Any, Dict, List, Optional, Sequence, Union

__all__ = [
    "TemporalStoreCommandError",
    "TemporalStoreClient",
    "decode_bytes",
    "encode_bytes",
]


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
    """Encode a payload the way the request side of the command surface wants it."""
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
    ) -> None:
        if max_batch_commands < 1 or max_batch_request_bytes < 1:
            raise ValueError("batch limits must be positive")
        self.host = host
        self.port = port
        self.shard_id = shard_id
        self.timeout_seconds = timeout_seconds
        self.max_batch_commands = max_batch_commands
        self.max_batch_request_bytes = max_batch_request_bytes
        self._local = threading.local()
        # EVERY connection this client ever opened, not just this thread's.
        # `threading.local` is the right place to KEEP a connection and the
        # wrong place to find one from another thread, so `close()` could only
        # ever release the socket belonging to whoever happened to call it.
        # Under the async methods the work runs on an executor's threads, so
        # the caller closing the store is never the thread holding the
        # connections, and every one of them would leak. This registry is what
        # makes `close()` mean closed.
        self._all_connections: List[http.client.HTTPConnection] = []
        self._registry_lock = threading.Lock()

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
        """Close every connection this client opened, on any thread.

        Idempotent, and safe to call from a thread that never issued a request.
        A connection closed here that some thread still has in its
        `threading.local` is reopened on that thread's next call, because
        :meth:`_connection` builds one whenever its slot is empty -- and a
        closed `HTTPConnection` raises on reuse, which `_post` already treats
        as a dropped pooled connection and retries once.
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

    def _post(self, path: str, document: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(document, separators=(",", ":")).encode("utf-8")
        headers = {"Content-Type": "application/json", "Content-Length": str(len(body))}
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
        return json.loads(raw)

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
        results: List[Dict[str, Any]] = []
        chunk: List[Dict[str, Any]] = []
        chunk_bytes = 0
        for command in commands:
            # Count encoded bytes, not Python object sizes: large serialized
            # feature values dominate the HTTP body. A single oversized value
            # remains one command and is left for the server to accept or reject.
            command_bytes = len(json.dumps(command, separators=(",", ":")).encode("utf-8"))
            if chunk and (
                len(chunk) >= self.max_batch_commands
                or chunk_bytes + command_bytes + 1 > self.max_batch_request_bytes
            ):
                results.extend(self._batch_once(chunk))
                chunk = []
                chunk_bytes = 0
            chunk.append(command)
            chunk_bytes += command_bytes + 1
        if chunk:
            results.extend(self._batch_once(chunk))
        return results

    def _batch_once(self, commands: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        document = {"shard_id": self.shard_id, "commands": commands}
        envelope = self._post("/batch_execute", document)
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
