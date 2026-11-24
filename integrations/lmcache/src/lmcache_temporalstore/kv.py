# SPDX-License-Identifier: Apache-2.0
"""Store LMCache payloads as ordinary binary TemporalStore string values."""

from collections.abc import Iterator

from feast_temporalstore_command.client import TemporalStoreClient, decode_bytes
from lmcache.v1.distributed.api import ObjectKey
from lmcache.v1.distributed.key_codec import encode_storage_key


class ValueBackend:
    """Ordinary KV storage over an explicitly provisioned data shard."""

    _max_load_payload_bytes = 4 * 1024 * 1024

    def __init__(
        self, client: TemporalStoreClient, lookup_client: TemporalStoreClient,
        ttl_seconds: int,
    ) -> None:
        self._client = client
        self._lookup = lookup_client
        self._ttl_ms = ttl_seconds * 1000 or None

    def _execute(self, entries: list[tuple[ObjectKey, dict]], *, lookup: bool = False) -> list[dict]:
        client = self._lookup if lookup else self._client
        return client.batch_execute([command for _, command in entries])

    def close(self) -> None:
        if self._lookup is not self._client:
            self._lookup.close()
        self._client.close()

    def store_batch(self, chunks: list[tuple[ObjectKey, memoryview]]) -> list[bool]:
        responses = self._execute([
            (key, {
                "kind": "string_set_conditional", "key": encode_storage_key(key), "value": payload,
                "ttl_ms": self._ttl_ms, "condition": "if_not_exists",
                "return_old": False, "keep_ttl": False,
            })
            for key, payload in chunks
        ])
        return [answer["value"] == 1 for answer in responses]

    def lengths(self, keys: list[ObjectKey]) -> list[int]:
        return [
            answer["value"]
            for answer in self._execute([
                (key, {"kind": "string_len", "key": encode_storage_key(key)}) for key in keys
            ], lookup=True)
        ]

    def load_into(self, items: list[tuple[ObjectKey, memoryview]]) -> Iterator[bool]:
        offset = 0
        while offset < len(items):
            end, size = offset, 0
            # The command client limits request bytes. GET keys are small, so
            # apply a second bound based on the expected response payloads.
            while end < min(len(items), offset + 128):
                length = len(items[end][1])
                if end > offset and size + length > self._max_load_payload_bytes:
                    break
                size += length
                end += 1
            batch = items[offset:end]
            responses = self._execute([
                (key, {"kind": "string_get", "key": encode_storage_key(key)}) for key, _ in batch
            ])
            for (_, destination), answer in zip(batch, responses, strict=True):
                payload = decode_bytes(answer.get("value"))
                found = payload is not None and len(payload) == len(destination)
                if found:
                    destination[:] = payload
                yield found
            offset = end

    def delete_batch(self, keys: list[ObjectKey]) -> None:
        self._execute([
            (key, {"kind": "common_delete", "key": encode_storage_key(key)}) for key in keys
        ])
