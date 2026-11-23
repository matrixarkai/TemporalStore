# SPDX-License-Identifier: Apache-2.0
"""LMCache multiprocess L2 adapter using TemporalStore's raw blob route."""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from hashlib import sha256
from http.client import HTTPException
import logging
from math import isfinite
import threading
from time import monotonic

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

from .blob import BlobClient, BlobMutationDeferred

logger = logging.getLogger(__name__)
_DELETE_BATCH_SIZE = 128
_STORE_UPLOAD_BATCH_SIZE = 32
_INVENTORY_RETRY_INITIAL_SECONDS = 0.25
_INVENTORY_RETRY_MAX_SECONDS = 2.0
_MUTATION_COOLDOWN_SECONDS = 1.0
_LOOKUP_FAILURE_COOLDOWN_SECONDS = 1.0


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
    recover_capacity: bool = False
    payload_metadata: bool = False

    @classmethod
    def from_dict(cls, values: dict) -> "TemporalStoreL2AdapterConfig":
        remote_storage = values.get("remote_storage", cls.remote_storage)
        if not isinstance(remote_storage, bool):
            raise ValueError("remote_storage must be a boolean")
        recover_capacity = values.get("recover_capacity", cls.recover_capacity)
        if not isinstance(recover_capacity, bool):
            raise ValueError("recover_capacity must be a boolean")
        payload_metadata = values.get("payload_metadata", cls.payload_metadata)
        if not isinstance(payload_metadata, bool):
            raise ValueError("payload_metadata must be a boolean")
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
            recover_capacity=recover_capacity,
            payload_metadata=payload_metadata,
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
            "recover_capacity enables inventory-backed accounting in a named namespace; "
            "payload_metadata probes files in a separate namespace and requires "
            "recover_capacity and ttl_seconds=0; "
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
        if config.recover_capacity and not config.namespace:
            raise ValueError("recover_capacity requires a named namespace")
        if config.payload_metadata and (not config.recover_capacity or config.ttl_seconds):
            raise ValueError("payload_metadata requires recover_capacity and ttl_seconds=0")
        super().__init__(capacity)
        namespace = config.namespace.encode("utf-8") if config.recover_capacity else b""
        if config.payload_metadata:
            # Legacy namespaces are valid UTF-8. The leading byte separates the disk layouts.
            namespace = b"\xff" + namespace
        self._inventory_scope = sha256(namespace).digest() if config.recover_capacity else b""
        self._inventory_namespace = self._inventory_scope.hex()
        self._lock = threading.Lock()
        self._closed = False
        self._inventory_ready = not config.recover_capacity
        self._inventory_error: str | None = None
        self._inventory_stop = threading.Event()
        self._inventory_thread: threading.Thread | None = None
        self._entries: OrderedDict[ObjectKey | bytes, int] = OrderedDict()
        self._object_keys: dict[bytes, ObjectKey] = {}
        self._uncertain: set[bytes] = set()
        self._used = 0
        self._blob = BlobClient(
            config.blob_url, config.timeout_seconds,
            reuse_read_connection=True, reuse_mutation_connection=True,
            legacy_names=not config.recover_capacity,
            mutation_cooldown_seconds=_MUTATION_COOLDOWN_SECONDS if config.remote_storage else 0.0,
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
        self._lookup_blob = (
            BlobClient(
                config.blob_url, lookup_timeout, reuse_read_connection=True, legacy_names=False,
                coalesce_small_requests=True,
            )
            if config.payload_metadata else None
        )
        retry_inventory = None
        if self._inventory_namespace:
            inventory_blob = BlobClient(
                config.blob_url, min(config.timeout_seconds, lookup_timeout),
                reuse_mutation_connection=True,
            )
            try:
                self._restore_inventory(inventory_blob)
            except (OSError, HTTPException) as error:
                self._inventory_error = type(error).__name__
                logger.warning("TemporalStore L2 inventory unavailable at startup: %s", error)
                retry_inventory = inventory_blob
            except Exception:
                if self._lookup_blob is not None:
                    self._lookup_blob.close()
                self._blob.close()
                self._data.close()
                if self._lookup_data is not self._data:
                    self._lookup_data.close()
                raise
            finally:
                if retry_inventory is None:
                    inventory_blob.close()
        self._store_event = create_event_notifier()
        self._lookup_event = create_event_notifier()
        self._load_event = create_event_notifier()
        self._store_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ts-l2-store")
        self._lookup_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ts-l2-lookup")
        self._lookup_retry_at = 0.0  # Owned by the lookup worker.
        self._load_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ts-l2-load")
        self._next_id = 0
        self._locked: dict[ObjectKey | bytes, int] = {}
        self._evicting: set[ObjectKey | bytes] = set()
        self._publishing: set[ObjectKey | bytes] = set()
        self._stores = 0
        self._loads = 0
        self._lookups = 0
        self._done_store: dict[L2TaskId, L2StoreResult] = {}
        self._done_lookup: dict[L2TaskId, Bitmap] = {}
        self._done_load: dict[L2TaskId, Bitmap] = {}
        if retry_inventory is not None:
            self._inventory_thread = threading.Thread(
                target=self._retry_inventory, args=(retry_inventory,), name="ts-l2-inventory", daemon=True,
            )
            self._inventory_thread.start()

    def _restore_inventory(self, blob: BlobClient) -> None:
        if self._config.payload_metadata:
            blob.probe_many([])
        entries = OrderedDict(blob.inventory(self._inventory_namespace, self._inventory_stop))
        used = sum(entries.values())
        with self._lock:
            if self._closed:
                return
            self._entries = entries
            self._used = used
            self._inventory_error = None
            self._inventory_ready = True

    def _retry_inventory(self, blob: BlobClient) -> None:
        delay = _INVENTORY_RETRY_INITIAL_SECONDS
        try:
            while not self._inventory_stop.wait(delay):
                try:
                    self._restore_inventory(blob)
                except (OSError, HTTPException) as error:
                    with self._lock:
                        self._inventory_error = type(error).__name__
                    delay = min(delay * 2, _INVENTORY_RETRY_MAX_SECONDS)
                except Exception as error:
                    with self._lock:
                        self._inventory_error = type(error).__name__
                    logger.exception("TemporalStore L2 inventory could not be loaded")
                    return
                else:
                    return
        finally:
            blob.close()


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
            if self._closed or not self._inventory_ready:
                self._done_store[task_id] = L2StoreResult(False, 0)
                if not self._closed:
                    self._store_event.notify()
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
            # An empty recovered inventory cannot produce a hit.
            if (
                self._closed
                or not self._inventory_ready
                or not keys
                or (self._inventory_scope and not self._entries)
            ):
                self._done_lookup[task_id] = Bitmap(len(keys))
                if not self._closed:
                    self._lookup_event.notify()
            else:
                self._lookup_pool.submit(self._lookup, task_id, list(keys))
        return task_id

    def query_lookup_and_lock_result(self, task_id: L2TaskId) -> Bitmap | None:
        with self._lock:
            return self._done_lookup.pop(task_id, None)

    def submit_unlock(self, keys: list[ObjectKey]) -> None:
        with self._lock:
            for key in keys:
                key = self._identity(key)
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
            if self._closed or not self._inventory_ready:
                self._done_load[task_id] = Bitmap(len(keys))
                if not self._closed:
                    self._load_event.notify()
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
                "is_healthy": not self._closed and self._inventory_ready,
                "adapter": "temporalstore",
                "inventory_ready": self._inventory_ready,
                "inventory_error": self._inventory_error,
                "mutations_available": (
                    not self._closed and self._inventory_ready and self._blob.mutations_available()
                ),
                "stores": self._stores,
                "loads": self._loads,
                "lookups": self._lookups,
                "bytes_used": self._used,
            }

    def get_usage(self):
        usage = super().get_usage()
        if self._inventory_namespace:
            with self._lock:
                return replace(usage, total_bytes_used=self._used)
        return usage

    def _identity(self, key: ObjectKey) -> ObjectKey | bytes:
        if not self._inventory_scope:
            return key
        encoded = encode_storage_key(key).encode("utf-8")
        return sha256(self._inventory_scope + encoded).digest()

    def _key_names(self, key: ObjectKey | bytes) -> tuple[str, str]:
        if not isinstance(key, bytes):
            key = self._identity(key)
            if not isinstance(key, bytes):
                return _names(key, self._config.namespace)
        namespace = self._inventory_namespace
        digest = key.hex()
        return f"lmcache/v2/{namespace}/{digest[:2]}/{digest}", f"lmcache:v2:{namespace}:{digest}"

    def _remove_unconfirmed_stores(self, items: list[tuple[ObjectKey | bytes, int]]) -> None:
        for offset in range(0, len(items), _DELETE_BATCH_SIZE):
            group = items[offset:offset + _DELETE_BATCH_SIZE]
            targets = [key for key, _ in group]
            cleanup = self._blob.mutations_available()
            with self._lock:
                if cleanup:
                    self._evicting.update(targets)
                for key, size in group:
                    if isinstance(key, bytes):
                        # A lost receipt can leave a complete file on disk.
                        # Hold its capacity until both deletions are confirmed.
                        self._used += size - self._entries.get(key, 0)
                        self._entries[key] = size
                        self._uncertain.add(key)
            if cleanup:
                self._delete_reserved(targets)

    def _make_room(self, needed: int) -> bool:
        if needed > self._max_capacity_bytes:
            return False
        while self._blob.mutations_available():
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
        return False

    def delete(self, keys: list[ObjectKey]) -> None:
        with self._lock:
            if not self._closed and self._inventory_ready:
                self._store_pool.submit(self._delete_many, list(keys))

    def _delete_many(self, keys: list[ObjectKey]) -> None:
        for offset in range(0, len(keys), _DELETE_BATCH_SIZE):
            if not self._blob.mutations_available():
                break
            self._delete_batch(keys[offset:offset + _DELETE_BATCH_SIZE])

    def _delete_batch(self, keys: list[ObjectKey]) -> None:
        reserved = []
        with self._lock:
            for obj_key in keys:
                key = self._identity(obj_key)
                if key in self._locked or key in self._evicting:
                    continue
                self._evicting.add(key)
                reserved.append(key)
        if reserved:
            self._delete_reserved(reserved)

    def _delete_reserved(self, reserved: list[ObjectKey | bytes]) -> int:
        deleted = []
        attempted = False
        try:
            names = [self._key_names(key) for key in reserved]
            try:
                attempted = True
                removed = self._blob.delete_many([blob for blob, _ in names])
            except BlobMutationDeferred:
                attempted = False
                return 0
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
            if not self._config.payload_metadata:
                try:
                    self._data.batch_execute(list(commands.values()))
                except Exception as error:
                    if isinstance(error, (OSError, HTTPException)):
                        self._blob.defer_mutations()
                    # A batch error can follow a partial commit. Deleting metadata
                    # again is idempotent while this store worker owns the keys.
                    logger.exception("TemporalStore L2 metadata delete batch failed")
                    deleted = []
                    for key, command in commands.items():
                        if not self._blob.mutations_available():
                            break
                        try:
                            self._data.execute(command)
                        except Exception as error:
                            logger.exception("TemporalStore L2 metadata delete failed")
                            if self._config.remote_storage and isinstance(error, (OSError, HTTPException)):
                                self._blob.defer_mutations()
                                break
                        else:
                            deleted.append(key)

            with self._lock:
                accounted = []
                for key in deleted:
                    size = self._entries.pop(key, 0)
                    self._used -= size
                    if isinstance(key, bytes):
                        self._uncertain.discard(key)
                        original = self._object_keys.pop(key, None)
                    else:
                        original = key
                    if size and original is not None:
                        accounted.append((original, size))
            if accounted:
                removed, amounts = zip(*accounted)
                self._notify_keys_deleted(list(removed), list(amounts))
            return len(deleted)
        finally:
            with self._lock:
                if self._inventory_namespace and attempted:
                    confirmed = set(deleted)
                    self._uncertain.update(
                        key for key in reserved
                        if key in self._entries and key not in confirmed
                    )
                self._evicting.difference_update(reserved)

    def _publish_metadata(self, pending: dict[ObjectKey | bytes, tuple[int, bool, ObjectKey]]) -> None:
        commands = []
        for key, (size, _, _) in pending.items():
            command = {
                "kind": "string_set_ex" if self._config.ttl_seconds else "string_set",
                "key": self._key_names(key)[1],
                "value": str(size).encode("ascii"),
            }
            if self._config.ttl_seconds:
                command["ttl_ms"] = self._config.ttl_seconds * 1000
            commands.append(command)
        try:
            if not self._blob.mutations_available():
                raise BlobMutationDeferred("metadata publication is waiting for the datanode")
            self._data.batch_execute(commands)
        except Exception as error:
            if isinstance(error, (OSError, HTTPException)):
                self._blob.defer_mutations()
            # A failed batch may have committed some commands. Remove any new
            # entries from this batch so a later lookup cannot see a partial write.
            self._remove_unconfirmed_stores([
                (key, size) for key, (size, uploaded, _) in pending.items() if uploaded
            ])
            raise

    def _publish_stores(self, pending: dict[ObjectKey | bytes, tuple[int, bool, ObjectKey]]) -> int:
        if not self._config.payload_metadata:
            self._publish_metadata(pending)
        uploaded = [(key, size, original) for key, (size, wrote, original) in pending.items() if wrote]
        total = sum(size for _, size, _ in uploaded)
        with self._lock:
            for key, size, original in uploaded:
                self._entries[key] = size
                if isinstance(key, bytes):
                    self._object_keys[key] = original
            self._used += total
            self._stores += len(uploaded)
        if uploaded:
            self._notify_keys_stored(
                [original for _, _, original in uploaded],
                [size for _, size, _ in uploaded],
            )
        return total

    def _store(
        self, task_id: int, keys: list[ObjectKey], objects: list[MemoryObj]
    ) -> None:
        transferred = 0
        success = len(keys) == len(objects)
        pending: dict[ObjectKey | bytes, tuple[int, bool, ObjectKey]] = {}
        pending_bytes = 0
        uploads: list[tuple[ObjectKey | bytes, str, memoryview]] = []

        def upload() -> None:
            nonlocal success, pending_bytes
            if not uploads:
                return
            try:
                written = self._blob.put_many([(name, payload) for _, name, payload in uploads])
                if len(written) != len(uploads):
                    raise OSError("blob receipt count differs from the upload count")
            except Exception:
                logger.exception("TemporalStore L2 payload write failed")
                written = [False] * len(uploads)
            failed = []
            unconfirmed = []
            for (key, _, _), stored in zip(uploads, written, strict=True):
                if stored:
                    continue
                size, _, _ = pending.pop(key)
                pending_bytes -= size
                success = False
                failed.append(key)
                if stored is False and isinstance(key, bytes):
                    unconfirmed.append((key, size))
            if failed:
                self._remove_unconfirmed_stores(unconfirmed)
                with self._lock:
                    self._publishing.difference_update(failed)
            uploads.clear()

        def publish() -> None:
            nonlocal success, transferred, pending_bytes
            upload()
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
            if self._uncertain:
                self._delete_many([key for key in keys if self._identity(key) in self._uncertain])
            for original, obj in zip(keys, objects):
                if not self._blob.mutations_available():
                    success = False
                    break
                key = self._identity(original)
                size = obj.get_size()
                if key in pending:
                    continue
                if key in self._uncertain:
                    success = False
                    continue
                with self._lock:
                    known_size = self._entries.get(key)
                if known_size is not None:
                    recovered_key = isinstance(key, bytes) and key not in self._object_keys
                    if recovered_key and known_size != size:
                        success = False
                        continue
                    if recovered_key or self._config.ttl_seconds:
                        pending[key] = (known_size, False, original)
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
                    name, _ = self._key_names(key)
                except Exception:
                    success = False
                    logger.exception("TemporalStore L2 payload write failed")
                    with self._lock:
                        self._publishing.discard(key)
                    continue
                pending[key] = (size, True, original)
                pending_bytes += size
                uploads.append((key, name, payload))
                if len(uploads) == _STORE_UPLOAD_BATCH_SIZE:
                    upload()
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
            if self._closed or (
                self._lookup_retry_at and monotonic() < self._lookup_retry_at
            ):
                # Close keeps the notifier open until queued tasks finish.
                self._done_lookup[task_id] = bitmap
                self._lookup_event.notify()
                return
            self._lookup_retry_at = 0.0
            for index, original in enumerate(keys):
                key = self._identity(original)
                if key in self._evicting or key in self._publishing:
                    continue
                if isinstance(key, bytes) and (key in self._uncertain or key not in self._entries):
                    continue
                self._locked[key] = self._locked.get(key, 0) + 1
                reserved.append((index, key))
        try:
            if self._lookup_blob is not None:
                found = self._lookup_blob.probe_many([
                    (self._key_names(key)[0], self._entries[key]) for _, key in reserved
                ]) if reserved else []
            else:
                commands = [
                    {"kind": "common_exists", "key": self._key_names(key)[1]}
                    for _, key in reserved
                ]
                answers = self._lookup_data.batch_execute(commands) if commands else []
                found = (answer.get("kind") == "integer" and answer.get("value") == 1 for answer in answers)
            for (index, _), present in zip(reserved, found):
                if present:
                    bitmap.set(index)
        except Exception as error:
            if self._config.remote_storage and isinstance(error, (OSError, HTTPException)):
                self._lookup_retry_at = monotonic() + _LOOKUP_FAILURE_COOLDOWN_SECONDS
            logger.exception("TemporalStore L2 lookup failed")
        finally:
            self.submit_unlock([keys[index] for index, _ in reserved if not bitmap.test(index)])
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
            # LMCache only publishes the reserved buffers reported in this task's bitmap.
            for index, found in zip(positions, self._blob.get_many_into(items, staging=True)):
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
            self._inventory_stop.set()
        if self._inventory_thread is not None:
            self._inventory_thread.join()
        for pool in (self._store_pool, self._lookup_pool, self._load_pool):
            pool.shutdown(wait=True)
        if self._lookup_data is not self._data:
            self._lookup_data.close()
        self._data.close()
        if self._lookup_blob is not None:
            self._lookup_blob.close()
        self._blob.close()
        self._store_event.close()
        self._lookup_event.close()
        self._load_event.close()
