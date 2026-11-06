# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""HTTP command client for a TemporalStore data node.

The data node serves binary ``POST /batch_execute_msgpack`` and
``POST /batch_write_msgpack`` routes, plus JSON-compatible ``POST /execute``,
``POST /batch_execute``, and ``POST /batch_write`` routes. They take a shard id
and one or more ``Command`` documents; ``Command`` is a serde-tagged enum whose
``kind`` is the snake_case variant name.

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
    if kind == "hash_multi_set":
        encoded["entries"] = [
            (name, _required_binary(value))
            for name, value in encoded.get("entries", [])
        ]
    elif kind in {"string_set", "string_set_ex", "string_set_conditional", "hash_set"}:
        encoded["value"] = _required_binary(encoded["value"])
    elif kind in {"set_add", "set_remove", "zset_add", "zset_score", "zset_remove"}:
        encoded["member"] = _required_binary(encoded["member"])
    elif kind in {"feature_append", "feature_append_with_policy", "feature_replace"}:
        encoded["points"] = [
            {**point, "value": _required_binary(point["value"])}
            for point in encoded.get("points", [])
        ]
    return encoded


def _json_command(command: Dict[str, Any]) -> Dict[str, Any]:
    """Return one command in the established JSON byte encoding."""
    encoded = dict(command)
    kind = encoded.get("kind")
    if kind == "hash_multi_set":
        encoded["entries"] = [
            (name, encode_bytes(_required_binary(value)))
            for name, value in encoded.get("entries", [])
        ]
    elif kind in {"string_set", "string_set_ex", "string_set_conditional", "hash_set"}:
        encoded["value"] = encode_bytes(_required_binary(encoded["value"]))
    elif kind in {"set_add", "set_remove", "zset_add", "zset_score", "zset_remove"}:
        encoded["member"] = encode_bytes(_required_binary(encoded["member"]))
    elif kind in {"feature_append", "feature_append_with_policy", "feature_replace"}:
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
        self.host = host
        self.port = port
        self.shard_id = shard_id
        self.timeout_seconds = timeout_seconds
        self.max_batch_commands = max_batch_commands
        self.max_write_batch_commands = write_batch_commands
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
        self._batch_write_supported: Optional[bool] = None
        self._binary_batch_supported: Optional[bool] = False if msgpack is None else None
        self._binary_write_supported: Optional[bool] = False if msgpack is None else None

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

    def _post_encoded(self, path: str, body: bytes) -> Dict[str, Any]:
        return json.loads(self._request_encoded(path, body))

    def _post(self, path: str, document: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(document, separators=(",", ":")).encode("utf-8")
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
                    raw = self._request_encoded(
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
        envelope = self._post_encoded("/batch_execute", body)
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
        status = envelope.get("status") or {}
        if not status.get("ok", False):
            raise TemporalStoreCommandError(
                status.get("code", "unknown"), status.get("message", "")
            )
        errors = envelope.get("errors") or []
        applied = int(envelope.get("applied", 0))
        if applied + len(errors) != len(commands):
            raise TemporalStoreCommandError(
                "batch_length_mismatch",
                f"sent {len(commands)} writes, got {applied} successes and "
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
