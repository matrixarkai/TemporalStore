# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A Feast **online store** backed by TemporalStore.

This is the online store integration point, not an offline store and not a
materialization engine. The distinction decides what Feast will and will not
call, so it is worth being exact about:

* An **online store** implements :class:`feast.infra.online_stores.online_store.OnlineStore`
  -- ``online_write_batch`` and ``online_read`` -- and is what
  ``store.get_online_features(...)`` reads from at serving time. That is what
  this module is.
* An **offline store** answers historical, point-in-time-correct training
  queries over a warehouse. TemporalStore is not wired up as one here.
* A **materialization engine** is the thing that *moves* rows from the offline
  store into the online store. This integration does not replace it: the stock
  local and Bytewax engines materialize **into** this store by calling
  ``online_write_batch``, which is the normal arrangement.

Layout
------
One TemporalStore hash per ``(project, entity key)``, which is the same
granularity Feast's own Redis store uses, so a row's feature values and its
event timestamp are one key's worth of work:

* **Key** -- ``"{hex(serialized_entity_key)}:{project}"``. TemporalStore keys
  are UTF-8 ``String``, and a serialized Feast entity key is arbitrary binary,
  so it is hex-encoded. That is 2x on the **key**, and nothing on the value.
  Feast's Redis store can use the raw bytes because a Redis key is a byte
  string; this is a real difference in stored key footprint and it is stated
  here so nobody quotes a footprint ratio off it without knowing.
* **Field** -- ``"{feature_view}:{feature_name}"``, plus one
  ``"_ts:{feature_view}"`` holding the row's event timestamp as a serialized
  protobuf ``Timestamp``. Feast's Redis store hashes that same string through
  mmh3 into 8 raw bytes; a TemporalStore hash field is a ``String``, so the
  readable name is kept instead of hex-encoding a digest to the same or
  greater width.
* **Value** -- ``ValueProto.SerializeToString()``, byte for byte what Feast's
  Redis, SQLite and Postgres stores keep. The values are therefore matched
  across stores by construction, which is the only reason a latency comparison
  between them means anything.

Both reads and writes go through ``/batch_execute``, so a
``get_online_features`` call over N entities is one round trip, not N -- the
same shape as the Redis store's pipelining.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import StrictStr

from feast import Entity, FeatureView, RepoConfig, utils
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel

from .client import TemporalStoreClient, decode_bytes, encode_bytes

logger = logging.getLogger(__name__)

__all__ = ["TemporalStoreCommandOnlineStoreConfig", "TemporalStoreCommandOnlineStore"]

_FULL_CLASS_PATH = "feast_temporalstore_command.TemporalStoreCommandOnlineStore"


class TemporalStoreCommandOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for TemporalStore."""

    type: Literal[_FULL_CLASS_PATH] = _FULL_CLASS_PATH
    """Online store type selector."""

    host: StrictStr = "127.0.0.1"
    """Host of the TemporalStore data node."""

    port: int = 17102
    """HTTP port of the TemporalStore data node's command surface."""

    shard_id: int = 1
    """Shard the commands are addressed to."""

    timeout_seconds: float = 10.0
    """Per-request socket timeout."""

    key_ttl_seconds: Optional[int] = None
    """Expiry armed on each entity key after a write.

    TemporalStore's command surface has no key-space scan, so this is the only
    mechanism that reclaims rows without the caller remembering their keys --
    see :meth:`TemporalStoreCommandOnlineStore.teardown`.
    """

    skip_dedup: bool = False
    """Write without first reading each row's stored event timestamp.

    The default (``False``) matches Feast's Redis store: read the stored
    timestamps, then write only the rows that are newer, so an out-of-order
    materialization cannot move a feature backwards. Setting this to ``True``
    halves the round trips and is appropriate for an initial load or an
    append-only pipeline whose rows arrive in order.
    """

    write_feature_history: bool = False
    """Also append every value to a TemporalStore feature series.

    Off by default. When on, each written value is additionally appended to a
    per-``(view, entity, feature)`` feature series keyed by its event
    timestamp, so the history behind the served value stays queryable through
    ``FeatureQuery`` / ``FeatureAggQuery`` -- windowed aggregates over the same
    data the online store is serving, which is the capability a plain key-value
    online store cannot offer.

    Serving reads do **not** change: ``online_read`` always answers from the
    hash. That is deliberate. ``FeatureQuery`` returns a window in ascending
    timestamp order and truncates with ``take(count)``, so asking it for "the
    latest point" costs O(points in the window) rather than O(1); it is a
    history read, not a latest-value read. Turning this on adds one
    ``FeatureAppend`` per feature per row to the **write** path and leaves the
    read path alone.
    """

    track_keys_for_teardown: bool = False
    """Record each written entity key in a per-view set so teardown can find it.

    Off by default because it adds a write per row to a single hot key. With it
    off, :meth:`TemporalStoreCommandOnlineStore.teardown` has nothing to enumerate and
    says so rather than silently leaving data behind.

    **DO NOT TURN THIS ON TOGETHER WITH ASYNC COMMIT. ROWS ARE LOST.**
    Measured on a release build against a freshly created, empty shard, writing
    20,000 entities at a batch width of 500, with ``async_storage=true`` on the
    shard: **17,500 of the 20,000 rows read back empty.** Not late -- empty.
    The shard reported ``total_records=20001``, and ``HashGetAll`` on a lost
    row's key answered with **zero fields**, so the object exists and holds
    nothing; a 20-second pause did not bring the values back. Only the last
    2,500 writes -- the final five batches -- survived, which is the shape of
    older dirty objects being dropped before they were persisted.

    It is an INTERACTION, and that is why it went unseen: neither half does it
    alone. Same node, same payload, same entity count:

    ===============  ==========  =====================
    async_storage    this flag   rows lost of 20,000
    ===============  ==========  =====================
    false            true        0
    true             false       0
    true             **true**    **17,500**
    ===============  ==========  =====================

    The default path is unaffected, because this flag is off by default and
    ``async_storage=true`` with it off loses nothing. If you need exact teardown
    AND async commit, do the teardown pass against a shard configured for
    durable commit.

    **AND THE WRITE PATH IS QUADRATIC EVEN WHEN IT IS SAFE.**
    Every key goes into ONE set -- the shard reports it as a single
    `set_records` entry -- so each row's `SetAdd` is a write to a container
    holding every row before it. Measured on a release build, writing N rows at
    a batch width of 500, against the same node and payload:

    ========  ==============  ==============
    rows      elapsed         vs 5,000 rows
    ========  ==============  ==============
    5,000       3.05 s          1.0x
    20,000     39.94 s         13.1x
    50,000    463.67 s        152.0x
    ========  ==============  ==============

    Ten times the rows for about a hundred and fifty times the work. Two other
    online stores driven through the same harness stayed linear over the same
    range, so this is this write path and not the host.

    Use it for a small view, or for a test fixture that has to clean up after
    itself. For a production view of any size, leave it off and reclaim rows
    with :attr:`key_ttl_seconds` instead -- an expiry costs one command per row
    once, not one container rewrite per row.
    """

    teardown_batch_size: int = 512
    """How many deletes `teardown` sends per round trip.

    THIS BOUND IS NOT COSMETIC. `teardown` deletes one key per row the view
    ever wrote, and that count is a property of the data, not of the caller --
    so without a bound the request size is unbounded. Measured against a live
    node: a view holding 5,000 entities built a single 5,001-command
    `/batch_execute` and the call timed out, taking the rest of the run with
    it. A view with a million entities would have built a million-command
    request. Chunking keeps each round trip's size a function of this setting
    and makes teardown's cost linear in a bounded number of calls instead of
    one unbounded one.
    """


def _ts_field(feature_view_name: str) -> str:
    return f"_ts:{feature_view_name}"


def _feature_field(feature_view_name: str, feature_name: str) -> str:
    return f"{feature_view_name}:{feature_name}"


class TemporalStoreCommandOnlineStore(OnlineStore):
    """TemporalStore implementation of Feast's online store interface."""

    _client: Optional[TemporalStoreClient] = None
    _client_signature: Optional[Tuple[Any, ...]] = None

    # -- plumbing ------------------------------------------------------------

    def _get_client(
        self, config: RepoConfig
    ) -> TemporalStoreClient:  # pragma: no cover - trivial
        online_config = config.online_store
        assert isinstance(online_config, TemporalStoreCommandOnlineStoreConfig)
        signature = (
            online_config.host,
            online_config.port,
            online_config.shard_id,
            online_config.timeout_seconds,
        )
        # Rebuild on a config change rather than handing back a client pointed
        # at the previous node: a cached client that outlives its config reads
        # the wrong store and still answers.
        if self._client is None or self._client_signature != signature:
            if self._client is not None:
                self._client.close()
            self._client = TemporalStoreClient(*signature)
            self._client_signature = signature
        return self._client

    @staticmethod
    def _view_name(table: FeatureView, config: RepoConfig) -> str:
        # Mirror the Redis store's handling of online feature-view versioning so
        # a repo that enables it gets the same key space from either backend.
        try:
            from feast.infra.online_stores.helpers import compute_versioned_name

            return compute_versioned_name(
                table, config.registry.enable_online_feature_view_versioning
            )
        except Exception:
            return table.name

    @staticmethod
    def _entity_hash_key(config: RepoConfig, entity_key: EntityKeyProto) -> str:
        serialized = serialize_entity_key(
            entity_key,
            entity_key_serialization_version=config.entity_key_serialization_version,
        )
        return f"{serialized.hex()}:{config.project}"

    # -- writes --------------------------------------------------------------

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        if not data:
            return
        online_config = config.online_store
        assert isinstance(online_config, TemporalStoreCommandOnlineStoreConfig)
        client = self._get_client(config)
        view_name = self._view_name(table, config)
        ts_field = _ts_field(view_name)

        hash_keys = [self._entity_hash_key(config, row[0]) for row in data]

        stored_nanos: Dict[str, int] = {}
        if not online_config.skip_dedup:
            # One batched read of just the timestamp field, then one batched
            # write: two round trips for the whole call, not two per row.
            reads = client.batch_execute(
                [
                    {"kind": "hash_multi_get", "key": key, "fields": [ts_field]}
                    for key in hash_keys
                ]
            )
            for key, response in zip(hash_keys, reads):
                values = response.get("values") or []
                raw = decode_bytes(values[0]) if values else None
                if raw:
                    stored = Timestamp()
                    stored.ParseFromString(raw)
                    stored_nanos[key] = stored.seconds * 1_000_000_000 + stored.nanos

        commands: List[Dict[str, Any]] = []
        written = 0
        skipped = 0
        # The newest event time queued so far in this call, per key. Without it,
        # two rows sharing an entity key would both compare against the same
        # pre-call value, both pass, and whichever was queued last would win
        # regardless of its event time.
        queued_nanos: Dict[str, int] = {}
        tracked_keys: List[str] = []

        for (entity_key, values, timestamp, _created), hash_key in zip(data, hash_keys):
            aware_ts = utils.make_tzaware(timestamp)
            proto_ts = Timestamp()
            proto_ts.FromDatetime(aware_ts)
            event_nanos = proto_ts.seconds * 1_000_000_000 + proto_ts.nanos

            latest_seen = max(
                stored_nanos.get(hash_key, 0), queued_nanos.get(hash_key, 0)
            )
            if latest_seen and event_nanos <= latest_seen:
                skipped += 1
                if progress:
                    progress(1)
                continue
            queued_nanos[hash_key] = event_nanos

            entries = [(ts_field, encode_bytes(proto_ts.SerializeToString()))]
            for feature_name, value in values.items():
                entries.append(
                    (
                        _feature_field(view_name, feature_name),
                        encode_bytes(value.SerializeToString()),
                    )
                )
            commands.append(
                {"kind": "hash_multi_set", "key": hash_key, "entries": entries}
            )

            if online_config.write_feature_history:
                timestamp_ms = proto_ts.seconds * 1000 + proto_ts.nanos // 1_000_000
                for feature_name, value in values.items():
                    commands.append(
                        {
                            "kind": "feature_append",
                            "key": self._history_key(
                                config, view_name, entity_key, feature_name
                            ),
                            # FeaturePoint.value carries no base64 annotation, so
                            # it goes out as a decimal array -- the one payload on
                            # this path that is not base64.
                            "points": [
                                {
                                    "timestamp_ms": timestamp_ms,
                                    "value": list(value.SerializeToString()),
                                }
                            ],
                        }
                    )

            if online_config.key_ttl_seconds:
                commands.append(
                    {
                        "kind": "common_expire",
                        "key": hash_key,
                        "ttl_ms": int(online_config.key_ttl_seconds) * 1000,
                    }
                )
            if online_config.track_keys_for_teardown:
                tracked_keys.append(hash_key)
            written += 1

        for key in tracked_keys:
            commands.append(
                {
                    "kind": "set_add",
                    "key": self._teardown_set_key(config, view_name),
                    "member": encode_bytes(key.encode("utf-8")),
                }
            )

        if commands:
            client.batch_execute(commands)
        if progress and written:
            progress(written)
        logger.debug(
            "temporalstore online_write_batch: view=%s rows=%d written=%d skipped_stale=%d",
            view_name,
            len(data),
            written,
            skipped,
        )

    @staticmethod
    def _history_key(
        config: RepoConfig,
        view_name: str,
        entity_key: EntityKeyProto,
        feature_name: str,
    ) -> str:
        serialized = serialize_entity_key(
            entity_key,
            entity_key_serialization_version=config.entity_key_serialization_version,
        )
        return f"fh:{config.project}:{view_name}:{serialized.hex()}:{feature_name}"

    @staticmethod
    def _teardown_set_key(config: RepoConfig, view_name: str) -> str:
        return f"fkeys:{config.project}:{view_name}"

    # -- reads ---------------------------------------------------------------

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        if not entity_keys:
            return []
        client = self._get_client(config)
        view_name = self._view_name(table, config)

        feature_names = list(
            requested_features
            if requested_features
            else [feature.name for feature in table.features]
        )
        ts_field = _ts_field(view_name)
        fields = [_feature_field(view_name, name) for name in feature_names] + [ts_field]

        responses = client.batch_execute(
            [
                {
                    "kind": "hash_multi_get",
                    "key": self._entity_hash_key(config, entity_key),
                    "fields": fields,
                }
                for entity_key in entity_keys
            ]
        )

        results: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for response in responses:
            raw_values = response.get("values") or []
            # HashMultiGet answers positionally, one slot per requested field,
            # with null for a field the hash does not hold. A short answer would
            # shift every value onto the wrong feature, so pad rather than zip
            # a ragged pair.
            if len(raw_values) < len(fields):
                raw_values = list(raw_values) + [None] * (
                    len(fields) - len(raw_values)
                )

            decoded = [decode_bytes(value) for value in raw_values]
            ts_bytes = decoded[-1]
            event_timestamp: Optional[datetime] = None
            if ts_bytes:
                proto_ts = Timestamp()
                proto_ts.ParseFromString(ts_bytes)
                event_timestamp = proto_ts.ToDatetime()

            row: Dict[str, ValueProto] = {}
            for feature_name, value_bytes in zip(feature_names, decoded[:-1]):
                value = ValueProto()
                if value_bytes:
                    value.ParseFromString(value_bytes)
                row[feature_name] = value

            # A row with no stored timestamp and no stored value was never
            # written; Feast expects (None, None) for that, not a dict of empty
            # values, which would read downstream as "present but null".
            if event_timestamp is None and not any(decoded[:-1]):
                results.append((None, None))
            else:
                results.append((event_timestamp, row))
        return results

    # -- lifecycle -----------------------------------------------------------

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ) -> None:
        # Nothing to create: TemporalStore is schemaless at the command surface,
        # so a new feature view needs no table and no migration. Views being
        # removed are swept through the same path as `teardown`.
        if tables_to_delete:
            self._delete_views(config, tables_to_delete)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        self._delete_views(config, tables)

    def _delete_views(
        self, config: RepoConfig, tables: Sequence[FeatureView]
    ) -> None:
        online_config = config.online_store
        assert isinstance(online_config, TemporalStoreCommandOnlineStoreConfig)
        if not tables:
            return
        if not online_config.track_keys_for_teardown:
            # Said out loud rather than returning quietly. TemporalStore's
            # command surface has no key-space scan, so without the tracking set
            # there is no way to enumerate what was written, and a caller who
            # believes teardown emptied the store would be wrong.
            logger.warning(
                "temporalstore: cannot enumerate keys for %d feature view(s) because "
                "track_keys_for_teardown is off and the command surface has no "
                "key-space scan; rows remain until their key_ttl_seconds expiry. "
                "Set track_keys_for_teardown=true to make teardown exact.",
                len(tables),
            )
            return

        client = self._get_client(config)
        batch_size = max(1, int(online_config.teardown_batch_size))
        for table in tables:
            view_name = self._view_name(table, config)
            set_key = self._teardown_set_key(config, view_name)
            members = client.execute({"kind": "set_members", "key": set_key}).get(
                "members"
            ) or []
            commands: List[Dict[str, Any]] = []
            for member in members:
                raw = decode_bytes(member)
                if raw is None:
                    continue
                commands.append(
                    {"kind": "common_delete", "key": raw.decode("utf-8")}
                )
            # The tracking set is dropped LAST, and only after every key it
            # named is gone. If a chunk fails partway, the set still lists the
            # survivors, so a re-run finishes the job; dropping the set first
            # would strand whatever remained with nothing left to name it.
            deleted = 0
            for start in range(0, len(commands), batch_size):
                chunk = commands[start : start + batch_size]
                client.batch_execute(chunk)
                deleted += len(chunk)
            client.batch_execute([{"kind": "common_delete", "key": set_key}])
            logger.info(
                "temporalstore: tore down view=%s keys=%d in %d round trip(s) "
                "of at most %d",
                view_name,
                deleted,
                (len(commands) + batch_size - 1) // batch_size + 1,
                batch_size,
            )

    # -- the async serving path -----------------------------------------------
    #
    # WITHOUT THESE TWO, FEAST'S ASYNC SERVING DOES NOT WORK AT ALL. The base
    # class's `online_read_async` and `online_write_batch_async` raise
    # `NotImplementedError`, and `FeatureStore.get_online_features_async` calls
    # straight into them, so a store that implements only the four abstract
    # methods serves synchronously and fails outright on the async endpoint --
    # which is the one a serving deployment under concurrency reaches for.
    #
    # These offload the synchronous call to a worker thread rather than running
    # a native async HTTP client, and that is a deliberate choice, not a stub:
    #
    #  * The work is one blocking socket round trip. CPython releases the GIL
    #    around socket reads and writes, so N coroutines on N worker threads
    #    genuinely overlap their waiting -- the concurrency is real, and it is
    #    measured in `docs/feast_online_store_benchmark.md` rather than assumed.
    #  * `asyncio.to_thread` uses the loop's default executor, whose threads are
    #    REUSED. The client keeps one keep-alive connection per thread, so a
    #    bounded pool means a bounded number of connections and no handshake per
    #    read. A native per-request async client would have to pool connections
    #    itself to match that.
    #  * It cannot drift from the sync path, because it IS the sync path. Two
    #    separate request builders would be two places for the wire format to be
    #    wrong, and only one of them covered by the command-shape tests.
    #
    # The honest cost: a worker thread per in-flight request, so this scales
    # with the executor's size, not with the socket count a native async client
    # could reach. Raise the loop's default executor if you need more.

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        import asyncio

        return await asyncio.to_thread(
            self.online_read, config, table, entity_keys, requested_features
        )

    async def online_write_batch_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        import asyncio

        return await asyncio.to_thread(
            self.online_write_batch, config, table, data, progress
        )

    # -- shutdown --------------------------------------------------------------

    def close(self) -> None:
        """Release every pooled connection this store opened.

        The base class's `close` is `pass`, which is correct for a store that
        holds nothing and wrong for this one: the client keeps a keep-alive
        connection per thread, and under the async methods those threads belong
        to an executor the caller never sees. A serving process that builds and
        discards stores would leak a file descriptor per thread per store with
        the inherited no-op, and the leak is invisible because every call still
        succeeds.
        """
        if self._client is not None:
            self._client.close()
            self._client = None
            self._client_signature = None
