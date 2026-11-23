# SPDX-License-Identifier: Apache-2.0
"""LMCache multiprocess L2 adapter using TemporalStore's raw blob route."""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from hashlib import sha256
import logging
from math import isfinite
import threading

import torch  # noqa: F401 - load before lmcache_native
from feast_temporalstore_command.client import TemporalStoreClient
from lmcache.lmcache_native import Bitmap
from lmcache.v1.distributed.api import (
    MemoryLayoutDesc, MemoryResidency, MemorySegment, MemoryTags, ObjectKey,
)
from lmcache.v1.distributed.internal_api import L2StoreResult
from lmcache.v1.distributed.key_codec import encode_storage_key
from lmcache.v1.distributed.l2_adapters.base import (
    L2AdapterInterface, L2TaskId,
)
from lmcache.v1.distributed.l2_adapters.config import L2AdapterConfigBase
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.platform import create_event_notifier

from .blob import BlobClient

logger = logging.getLogger(__name__)
_DELETE_BATCH_SIZE = 128


def _names(key: ObjectKey, namespace: str = "") -> tuple[str, str]:
    encoded = encode_storage_key(key).encode("utf-8")
    if namespace:
        scope = sha256(namespace.encode("utf-8")).digest()
        encoded = scope + encoded
    digest = sha256(encoded).hexdigest()
    return f"lmcache/v1/{digest}", f"lmcache:v1:{digest}"


@dataclass
class TemporalStoreL2AdapterConfig(L2AdapterConfigBase):
    blob_url: str = "http://127.0.0.1:17002"
    data_url: str = "http://127.0.0.1:17002"
    shard_id: int = 1
    max_capacity_gb: float = 64.0
    ttl_seconds: int = 0
    timeout_seconds: float = 10.0
    namespace: str = ""
    remote_storage: bool = False
    lookup_timeout_seconds: float | None = None

    @classmethod
    def from_dict(cls, values: dict) -> "TemporalStoreL2AdapterConfig":
        remote_storage = values.get("remote_storage", cls.remote_storage)
        if not isinstance(remote_storage, bool):
            raise ValueError("remote_storage must be a boolean")
        lookup_timeout = values.get("lookup_timeout_seconds")
        config = cls(
            blob_url=str(values.get("blob_url", cls.blob_url)),
            data_url=str(values.get("data_url", cls.data_url)),
            shard_id=int(values.get("shard_id", cls.shard_id)),
            max_capacity_gb=float(values.get("max_capacity_gb", cls.max_capacity_gb)),
            ttl_seconds=int(values.get("ttl_seconds", cls.ttl_seconds)),
            timeout_seconds=float(values.get("timeout_seconds", cls.timeout_seconds)),
            namespace=str(values.get("namespace", cls.namespace)),
            remote_storage=remote_storage,
            lookup_timeout_seconds=float(lookup_timeout) if lookup_timeout is not None else None,
        )
        if config.max_capacity_gb <= 0 or config.ttl_seconds < 0:
            raise ValueError("capacity must be positive and TTL must be nonnegative")
        return config

    @classmethod
    def help(cls) -> str:
        return (
            "blob_url and data_url are datanode HTTP URLs; shard_id selects the "
            "command shard; max_capacity_gb bounds local adapter writes; "
            "ttl_seconds expires metadata; timeout_seconds bounds each request; "
            "namespace isolates one LMCache server's keys and eviction budget; "
            "remote_storage marks a datanode on another host; "
            "lookup_timeout_seconds bounds metadata lookup "
            "(defaults to at most one second for remote storage)"
        )


class TemporalStoreL2Adapter(L2AdapterInterface):
    def __init__(
        self, config: TemporalStoreL2AdapterConfig | dict, **_kwargs: object
    ) -> None:
        if isinstance(config, dict):
            config = TemporalStoreL2AdapterConfig.from_dict(config)
        self._config = config
        capacity = int(config.max_capacity_gb * 1024**3)
        if capacity < 1 or config.shard_id < 1 or not (
            isfinite(config.timeout_seconds) and config.timeout_seconds > 0
        ):
            raise ValueError("capacity, shard_id and timeout_seconds must be positive")
        if config.ttl_seconds < 0:
            raise ValueError("ttl_seconds must be nonnegative")
        lookup_timeout = config.lookup_timeout_seconds
        if lookup_timeout is None:
            lookup_timeout = (
                min(config.timeout_seconds, 1.0) if config.remote_storage
                else config.timeout_seconds
            )
        if not isfinite(lookup_timeout) or lookup_timeout <= 0:
            raise ValueError("lookup_timeout_seconds must be finite and positive")
        super().__init__(capacity)
        self._blob = BlobClient(
            config.blob_url, config.timeout_seconds,
            reuse_read_connection=True, reuse_mutation_connection=True,
        )
        from urllib.parse import urlsplit
        data = urlsplit(config.data_url)
        if data.scheme != "http" or not data.hostname:
            raise ValueError("data_url must be a datanode HTTP URL")
        self._data = TemporalStoreClient(
            host=data.hostname, port=data.port or 80,
            shard_id=config.shard_id, timeout_seconds=config.timeout_seconds,
        )
        self._lookup_data = self._data
        if lookup_timeout != config.timeout_seconds:
            self._lookup_data = TemporalStoreClient(
                host=data.hostname, port=data.port or 80,
                shard_id=config.shard_id, timeout_seconds=lookup_timeout,
            )
        self._store_event = create_event_notifier()
        self._lookup_event = create_event_notifier()
        self._load_event = create_event_notifier()
        self._store_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ts-l2-store")
        self._lookup_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ts-l2-lookup")
        self._load_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ts-l2-load")
        self._lock = threading.Lock()
        self._next_id = 0
        self._closed = False
        self._entries: OrderedDict[ObjectKey, int] = OrderedDict()
        self._locked: dict[ObjectKey, int] = {}
        self._evicting: set[ObjectKey] = set()
        self._publishing: set[ObjectKey] = set()
        self._used = 0
        self._stores = 0
        self._loads = 0
        self._lookups = 0
        self._done_store: dict[L2TaskId, L2StoreResult] = {}
        self._done_lookup: dict[L2TaskId, Bitmap] = {}
        self._done_load: dict[L2TaskId, Bitmap] = {}

    def get_store_event_fd(self) -> int:
        return self._store_event.fileno()

    def get_lookup_and_lock_event_fd(self) -> int:
        return self._lookup_event.fileno()

    def get_load_event_fd(self) -> int:
        return self._load_event.fileno()

    def _allocate_task_id(self) -> int:
        """Allocate a task ID while the caller holds the adapter lock."""
        task_id = self._next_id
        self._next_id += 1
        return task_id

    def submit_store_task(
        self, keys: list[ObjectKey], objects: list[MemoryObj]
    ) -> L2TaskId:
        with self._lock:
            task_id = self._allocate_task_id()
            if self._closed:
                self._done_store[task_id] = L2StoreResult(False, 0)
            else:
                self._store_pool.submit(self._store, task_id, list(keys), list(objects))
        return task_id

    def pop_completed_store_tasks(self) -> dict[L2TaskId, L2StoreResult]:
        with self._lock:
            completed, self._done_store = self._done_store, {}
        return completed

    def submit_lookup_and_lock_task(
        self, keys: list[ObjectKey], group_layout_descs: dict[int, MemoryLayoutDesc]
    ) -> L2TaskId:
        with self._lock:
            task_id = self._allocate_task_id()
            if self._closed:
                self._done_lookup[task_id] = Bitmap(len(keys))
            else:
                self._lookup_pool.submit(self._lookup, task_id, list(keys))
        return task_id

    def query_lookup_and_lock_result(self, task_id: L2TaskId) -> Bitmap | None:
        with self._lock:
            return self._done_lookup.pop(task_id, None)

    def submit_unlock(self, keys: list[ObjectKey]) -> None:
        with self._lock:
            for key in keys:
                count = self._locked.get(key, 0)
                if count <= 1:
                    self._locked.pop(key, None)
                else:
                    self._locked[key] = count - 1

    def submit_load_task(
        self, keys: list[ObjectKey], objects: list[MemoryObj]
    ) -> L2TaskId:
        with self._lock:
            task_id = self._allocate_task_id()
            if self._closed:
                self._done_load[task_id] = Bitmap(len(keys))
            else:
                self._load_pool.submit(self._load, task_id, list(keys), list(objects))
        return task_id

    def query_load_result(self, task_id: L2TaskId) -> Bitmap | None:
        with self._lock:
            return self._done_load.pop(task_id, None)

    def get_memory_tags(self) -> MemoryTags:
        residency = MemoryResidency.REMOTE if self._config.remote_storage else MemoryResidency.LOCAL
        return MemoryTags(MemorySegment.STORAGE, residency)

    def report_status(self) -> dict:
        with self._lock:
            return {
                "is_healthy": not self._closed,
                "adapter": "temporalstore",
                "stores": self._stores,
                "loads": self._loads,
                "lookups": self._lookups,
                "bytes_used": self._used,
            }

    def _key_names(self, key: ObjectKey) -> tuple[str, str]:
        return _names(key, self._config.namespace)

    def _delete_key(self, key: ObjectKey) -> None:
        blob_name, metadata_name = self._key_names(key)
        self._blob.delete(blob_name)
        self._data.execute({"kind": "common_delete", "key": metadata_name})

    def _make_room(self, needed: int) -> bool:
        if needed > self._max_capacity_bytes:
            return False
        while True:
            with self._lock:
                shortage = self._used + needed - self._max_capacity_bytes
                if shortage <= 0:
                    return True
                victims = []
                for key, size in self._entries.items():
                    if key in self._locked or key in self._evicting:
                        continue
                    victims.append(key)
                    shortage -= size
                    if shortage <= 0 or len(victims) == _DELETE_BATCH_SIZE:
                        break
                self._evicting.update(victims)
            if not victims or not self._delete_reserved(victims):
                return False

    def delete(self, keys: list[ObjectKey]) -> None:
        with self._lock:
            if not self._closed:
                self._store_pool.submit(self._delete_many, list(keys))

    def _delete_many(self, keys: list[ObjectKey]) -> None:
        for offset in range(0, len(keys), _DELETE_BATCH_SIZE):
            self._delete_batch(keys[offset:offset + _DELETE_BATCH_SIZE])

    def _delete_batch(self, keys: list[ObjectKey]) -> None:
        reserved = []
        with self._lock:
            for key in keys:
                if key in self._locked or key in self._evicting:
                    continue
                self._evicting.add(key)
                reserved.append(key)
        if reserved:
            self._delete_reserved(reserved)

    def _delete_reserved(self, reserved: list[ObjectKey]) -> int:
        try:
            names = [self._key_names(key) for key in reserved]
            try:
                removed = self._blob.delete_many([blob for blob, _ in names])
            except Exception:
                logger.exception("TemporalStore L2 blob delete batch failed")
                return 0
            commands = {
                key: {"kind": "common_delete", "key": metadata}
                for key, (_, metadata), deleted in zip(reserved, names, removed)
                if deleted
            }
            if not commands:
                return 0

            deleted = list(commands)
            try:
                self._data.batch_execute(list(commands.values()))
            except Exception:
                # A batch error can follow a partial commit. Deleting metadata
                # again is idempotent while this store worker owns the keys.
                logger.exception("TemporalStore L2 metadata delete batch failed")
                deleted = []
                for key, command in commands.items():
                    try:
                        self._data.execute(command)
                    except Exception:
                        logger.exception("TemporalStore L2 metadata delete failed")
                    else:
                        deleted.append(key)

            with self._lock:
                sizes = {key: self._entries.pop(key, 0) for key in deleted}
                self._used -= sum(sizes.values())
            accounted = [(key, size) for key, size in sizes.items() if size]
            if accounted:
                removed, amounts = zip(*accounted)
                self._notify_keys_deleted(list(removed), list(amounts))
            return len(deleted)
        finally:
            with self._lock:
                self._evicting.difference_update(reserved)

    def _publish_stores(self, pending: dict[ObjectKey, tuple[int, bool]]) -> int:
        commands = []
        for key, (size, _) in pending.items():
            command = {
                "kind": "string_set_ex" if self._config.ttl_seconds else "string_set",
                "key": self._key_names(key)[1],
                "value": str(size).encode("ascii"),
            }
            if self._config.ttl_seconds:
                command["ttl_ms"] = self._config.ttl_seconds * 1000
            commands.append(command)
        try:
            self._data.batch_execute(commands)
        except Exception:
            # A failed batch may have committed some commands. Remove any new
            # entries from this batch so a later lookup cannot see a partial write.
            for key, (_, uploaded) in pending.items():
                if uploaded:
                    try:
                        self._delete_key(key)
                    except Exception:
                        logger.exception("TemporalStore L2 failed to remove incomplete entry")
            raise

        keys = [key for key, (_, uploaded) in pending.items() if uploaded]
        sizes = [pending[key][0] for key in keys]
        total = sum(sizes)
        with self._lock:
            self._entries.update(zip(keys, sizes))
            self._used += total
            self._stores += len(keys)
        if keys:
            self._notify_keys_stored(keys, sizes)
        return total

    def _store(
        self, task_id: int, keys: list[ObjectKey], objects: list[MemoryObj]
    ) -> None:
        transferred = 0
        success = len(keys) == len(objects)
        pending: dict[ObjectKey, tuple[int, bool]] = {}
        pending_bytes = 0

        def publish() -> None:
            nonlocal success, transferred, pending_bytes
            if not pending:
                return
            try:
                transferred += self._publish_stores(pending)
            except Exception:
                success = False
                logger.exception("TemporalStore L2 metadata write failed")
            finally:
                with self._lock:
                    self._publishing.difference_update(pending)
                pending.clear()
                pending_bytes = 0

        try:
            for key, obj in zip(keys, objects):
                size = obj.get_size()
                if key in pending:
                    continue
                with self._lock:
                    known_size = self._entries.get(key)
                if known_size is not None:
                    if self._config.ttl_seconds:
                        pending[key] = (known_size, False)
                    continue
                if size > self._max_capacity_bytes:
                    success = False
                    continue
                if pending_bytes + self._used + size > self._max_capacity_bytes:
                    publish()
                    if not self._make_room(size):
                        success = False
                        continue
                with self._lock:
                    if key in self._locked:
                        success = False
                        continue
                    self._publishing.add(key)
                try:
                    payload = memoryview(obj.byte_array).cast("B")[:size]
                    if len(payload) != size:
                        raise ValueError("memory object length changed during store")
                    self._blob.put(self._key_names(key)[0], payload)
                except Exception:
                    success = False
                    logger.exception("TemporalStore L2 payload write failed")
                    with self._lock:
                        self._publishing.discard(key)
                    continue
                pending[key] = (size, True)
                pending_bytes += size
        except Exception:
            success = False
            logger.exception("TemporalStore L2 store task failed")
        finally:
            publish()
            with self._lock:
                self._done_store[task_id] = L2StoreResult(success, transferred)
            self._store_event.notify()

    def _lookup(self, task_id: int, keys: list[ObjectKey]) -> None:
        bitmap = Bitmap(len(keys))
        # Reserve before the metadata read. Otherwise an eviction can finish
        # between the read and the lock acquisition and leave a false hit.
        reserved = []
        with self._lock:
            for index, key in enumerate(keys):
                if key not in self._evicting and key not in self._publishing:
                    self._locked[key] = self._locked.get(key, 0) + 1
                    reserved.append((index, key))
        try:
            commands = [
                {"kind": "common_exists", "key": self._key_names(key)[1]}
                for _, key in reserved
            ]
            answers = self._lookup_data.batch_execute(commands) if commands else []
            for (index, _), answer in zip(reserved, answers):
                if answer.get("kind") == "integer" and answer.get("value") == 1:
                    bitmap.set(index)
        except Exception:
            logger.exception("TemporalStore L2 lookup failed")
        finally:
            self.submit_unlock([key for index, key in reserved if not bitmap.test(index)])
            with self._lock:
                for index, key in reserved:
                    if bitmap.test(index):
                        if key in self._entries:
                            self._entries.move_to_end(key)
                        self._lookups += 1
                self._done_lookup[task_id] = bitmap
            self._lookup_event.notify()

    def _load(
        self, task_id: int, keys: list[ObjectKey], objects: list[MemoryObj]
    ) -> None:
        bitmap = Bitmap(len(keys))
        loaded = 0
        try:
            positions = []
            items = []
            for index, (key, obj) in enumerate(zip(keys, objects)):
                try:
                    destination = memoryview(obj.byte_array).cast("B")[:obj.get_size()]
                    if not destination.readonly:
                        items.append((self._key_names(key)[0], destination))
                        positions.append(index)
                except Exception:
                    logger.exception("TemporalStore L2 load destination failed")
            for index, found in zip(positions, self._blob.get_many_into(items)):
                if found:
                    bitmap.set(index)
                    loaded += 1
        except Exception:
            logger.exception("TemporalStore L2 load task failed")
        finally:
            with self._lock:
                self._loads += loaded
                self._done_load[task_id] = bitmap
            self._load_event.notify()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        for pool in (self._store_pool, self._lookup_pool, self._load_pool):
            pool.shutdown(wait=True)
        if self._lookup_data is not self._data:
            self._lookup_data.close()
        self._data.close()
        self._blob.close()
        self._store_event.close()
        self._lookup_event.close()
        self._load_event.close()
