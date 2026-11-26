# SPDX-License-Identifier: Apache-2.0
"""Store LMCache payloads as ordinary binary TemporalStore string values."""

from collections.abc import Iterator

from feast_temporalstore_command.client import TemporalStoreClient, decode_bytes


class ValueBackend:
    """Phase A transport over an explicitly provisioned data shard."""

    def __init__(
        self, client: TemporalStoreClient, lookup_client: TemporalStoreClient,
        ttl_seconds: int,
    ) -> None:
        self._client = client
        self._lookup = lookup_client
        self._ttl_ms = ttl_seconds * 1000 or None

    def store_batch(self, chunks: list[tuple[str, memoryview]]) -> list[bool]:
        responses = self._client.batch_execute([
            {
                "kind": "string_set_conditional", "key": key, "value": payload,
                "ttl_ms": self._ttl_ms, "condition": "if_not_exists",
                "return_old": False, "keep_ttl": False,
            }
            for key, payload in chunks
        ])
        return [answer["value"] == 1 for answer in responses]

    def lengths(self, keys: list[str]) -> list[int]:
        return [
            answer["value"]
            for answer in self._lookup.batch_execute([
                {"kind": "string_len", "key": key} for key in keys
            ])
        ]

    def load_into(self, items: list[tuple[str, memoryview]]) -> Iterator[bool]:
        offset = 0
        while offset < len(items):
            end, size = offset, 0
            # The command client limits request bytes. GET keys are small, so
            # apply a second bound based on the expected response payloads.
            while end < min(len(items), offset + 128):
                length = len(items[end][1])
                if end > offset and size + length > 4 * 1024 * 1024:
                    break
                size += length
                end += 1
            batch = items[offset:end]
            responses = self._client.batch_execute([
                {"kind": "string_get", "key": key} for key, _ in batch
            ])
            for (_, destination), answer in zip(batch, responses, strict=True):
                payload = decode_bytes(answer.get("value"))
                found = payload is not None and len(payload) == len(destination)
                if found:
                    destination[:] = payload
                yield found
            offset = end

    def delete_batch(self, keys: list[str]) -> None:
        self._client.batch_execute([
            {"kind": "common_delete", "key": key} for key in keys
        ])
