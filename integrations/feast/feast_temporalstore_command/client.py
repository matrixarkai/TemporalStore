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
* **Response payloads are decimal arrays.** ``CommandResponse::Values`` and
  ``CommandResponse::HashEntries`` carry no such annotation, so a byte comes
  back as the characters ``104,`` -- two to four characters each, averaging
  about 3.8x for the protobuf-serialized values Feast stores. A read's wire
  cost is therefore roughly 4x its payload, and that is the transport, not the
  engine. :func:`decode_bytes` accepts both shapes because the decoder on the
  far side does, so a server that gains the annotation later needs no change
  here.
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
    ) -> None:
        self.host = host
        self.port = port
        self.shard_id = shard_id
        self.timeout_seconds = timeout_seconds
        self._local = threading.local()

    # -- connection handling -------------------------------------------------

    def _connection(self) -> http.client.HTTPConnection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = http.client.HTTPConnection(
                self.host, self.port, timeout=self.timeout_seconds
            )
            self._local.conn = conn
        return conn

    def _drop_connection(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None

    def close(self) -> None:
        self._drop_connection()

    def _post(self, path: str, document: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(document, separators=(",", ":")).encode("utf-8")
        headers = {"Content-Type": "application/json", "Content-Length": str(len(body))}
        # One retry, and only one: a pooled connection the node closed between
        # calls fails on the write, before anything was applied, so resending is
        # safe. A failure after the request was sent is NOT retried here --
        # these commands are not all idempotent, and a client cannot tell the
        # two cases apart once the bytes are on the wire.
        for attempt in (0, 1):
            conn = self._connection()
            try:
                conn.request("POST", path, body=body, headers=headers)
                response = conn.getresponse()
                raw = response.read()
            except (http.client.HTTPException, OSError):
                self._drop_connection()
                if attempt == 1:
                    raise
                continue
            if response.status != 200:
                self._drop_connection()
                raise TemporalStoreCommandError(
                    f"http_{response.status}", raw.decode("utf-8", "replace")[:512]
                )
            return json.loads(raw)
        raise AssertionError("unreachable")

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
        """Run a list of commands in one round trip, in order.

        Returns one ``CommandResponse`` per command, positionally aligned with
        ``commands``. An empty list is answered without a round trip: the node
        would accept it, but a caller that batched nothing wants nothing.
        """
        if not commands:
            return []
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
