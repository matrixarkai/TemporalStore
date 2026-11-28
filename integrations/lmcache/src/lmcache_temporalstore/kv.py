# SPDX-License-Identifier: Apache-2.0
"""Store LMCache payloads as ordinary binary TemporalStore string values."""

from collections.abc import Iterator

from feast_temporalstore_command.client import TemporalStoreClient, decode_bytes
from lmcache.v1.distributed.api import ObjectKey
from lmcache.v1.distributed.key_codec import encode_storage_key


class ValueBackend:
    """Ordinary KV storage over an explicitly provisioned data shard."""

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

    @property
    def stores_confirmed_by_read(self) -> int:
        return 0

    def _store_entries(self, chunks: list[tuple[ObjectKey, memoryview]]) -> list[tuple[ObjectKey, dict]]:
        return [
            (key, {
                "kind": "string_set_conditional", "key": encode_storage_key(key), "value": payload,
                "ttl_ms": self._ttl_ms, "condition": "if_not_exists",
                "return_old": False, "keep_ttl": False,
            })
            for key, payload in chunks
        ]

    def store_batch(self, chunks: list[tuple[ObjectKey, memoryview]]) -> list[bool]:
        return [answer["value"] == 1 for answer in self._execute(self._store_entries(chunks))]

    def iter_store_results(
        self, chunks: list[tuple[ObjectKey, memoryview]],
    ) -> Iterator[tuple[int, bool]]:
        yield from zip(range(len(chunks)), self.store_batch(chunks), strict=True)

    def lengths(self, keys: list[ObjectKey]) -> list[int]:
        return [
            answer["value"]
            for answer in self._execute([
                (key, {"kind": "string_len", "key": encode_storage_key(key)}) for key in keys
            ], lookup=True)
        ]

    @staticmethod
    def _load_batches(items, *, pair_large=False):
        offset = 0
        while offset < len(items):
            end, size = offset, 0
            # GET keys are small. Bound batches by expected response bytes too.
            byte_limit = 4 * 1024 * 1024
            count_limit = 128
            first_length = items[offset][1].nbytes
            if pair_large and offset + 1 < len(items) and first_length > byte_limit:
                second_length = items[offset + 1][1].nbytes
                if second_length > byte_limit and first_length + second_length <= 18 * 1024 * 1024:
                    byte_limit = 18 * 1024 * 1024
                    count_limit = 2
            while end < min(len(items), offset + count_limit):
                length = items[end][1].nbytes
                if end > offset and size + length > byte_limit:
                    break
                size += length
                end += 1
            batch = items[offset:end]
            for index, (key, destination) in enumerate(batch):
                if destination.ndim != 1 or destination.format != "B":
                    batch[index] = (key, destination.cast("B"))
            entries = [
                (key, {"kind": "string_get", "key": encode_storage_key(key)}) for key, _ in batch
            ]
            yield batch, entries
            offset = end

    def load_into(self, items: list[tuple[ObjectKey, memoryview]]) -> Iterator[bool]:
        for batch, entries in self._load_batches(items):
            responses = self._execute(entries)
            if len(responses) != len(batch):
                raise ValueError("storage returned the wrong number of load responses")
            for index, (_, destination) in enumerate(batch):
                answer = responses[index]
                responses[index] = None
                payload = decode_bytes(answer.get("value"))
                found = payload is not None and len(payload) == len(destination)
                if found:
                    destination[:] = payload
                del payload, answer
                yield found
            del responses

    def delete_batch(self, keys: list[ObjectKey]) -> None:
        self._execute([
            (key, {"kind": "common_delete", "key": encode_storage_key(key)}) for key in keys
        ])
