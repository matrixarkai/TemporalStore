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

Reads use bounded binary ``/batch_hash_multi_get_compact_msgpack`` requests
with one shared field projection and one top-level status per batch.
The common latest-value write uses ``/batch_hash_multi_set_msgpack`` with the
field projection encoded once; augmented or mixed-layout writes use bounded
``/batch_write_msgpack`` command batches. Both paths fall back once to
compatible command routes when needed, so serving groups entities instead of
issuing one request per entity.
"""

from __future__ import annotations

import logging
import zlib
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import StrictStr

from feast import Entity, FeatureView, RepoConfig, utils
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.supported_async_methods import SupportedAsyncMethods
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.types import Int32, Int64

from .client import TemporalStoreClient, decode_bytes

logger = logging.getLogger(__name__)

__all__ = ["TemporalStoreCommandOnlineStoreConfig", "TemporalStoreCommandOnlineStore"]

_FULL_CLASS_PATH = "feast_temporalstore_command.TemporalStoreCommandOnlineStore"
_TEARDOWN_TRACKING_SHARDS = 1024

_NO_VECTOR_SEARCH = (
    "TemporalStore's online store does not serve vector or document retrieval. It "
    "stores a feature row as a hash of serialized values addressed by entity key, "
    "and keeps no embedding index, so there is nothing to search by distance and no "
    "cheap way to synthesise one at query time. "
    "`retrieve_online_documents` and `retrieve_online_documents_v2` are therefore "
    "refused HERE, deliberately, rather than inherited from Feast's base class: an "
    "inherited NotImplementedError names an abstract method and leaves a reader "
    "wondering whether the integration is unfinished. This one is finished, and this "
    "query is out of scope for it. Use an online store with a vector index for "
    "document retrieval, and this one for key-addressed feature serving."
)


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

    max_batch_commands: int = 512
    """Maximum commands per HTTP batch during reads."""

    max_write_batch_commands: int = 4096
    """Maximum commands per HTTP batch during materialization writes."""

    max_batch_request_bytes: int = 4 * 1024 * 1024
    """Approximate encoded command bytes per HTTP batch."""

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
    timestamp. Scalar integer features also get a numeric series for native
    windowed aggregates; the raw history keeps its original serialized value.

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

    Off by default because it adds one write per row. With it
    off, :meth:`TemporalStoreCommandOnlineStore.teardown` has nothing to enumerate and
    says so rather than silently leaving data behind.

    Async commit is supported. A release-build regression run on a fresh shard
    retained all 20,000 feature-shaped rows while this option wrote the shared
    tracking set. Container blocks can be recovered from their WAL records
    after cache pressure, so the tracking write cannot make an acknowledged
    hash row disappear.

    Tracking is spread over 1,024 deterministic sets. Each set therefore holds
    roughly one key in 1,024 instead of becoming a single ever-growing hot
    container. The count is fixed rather than configurable so a deployment
    cannot strand keys by changing it before teardown. Teardown also reads the
    former unsharded set, so views written by an older provider remain
    cleanable after an upgrade.
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
            online_config.max_batch_commands,
            online_config.max_batch_request_bytes,
            online_config.max_write_batch_commands,
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
    def _entity_key_hex(config: RepoConfig, entity_key: EntityKeyProto) -> str:
        return serialize_entity_key(
            entity_key,
            entity_key_serialization_version=config.entity_key_serialization_version,
        ).hex()

    @classmethod
    def _entity_hash_key(cls, config: RepoConfig, entity_key: EntityKeyProto) -> str:
        return f"{cls._entity_key_hex(config, entity_key)}:{config.project}"

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

        entity_key_hexes = [self._entity_key_hex(config, row[0]) for row in data]
        hash_keys = [
            f"{entity_key_hex}:{config.project}" for entity_key_hex in entity_key_hexes
        ]

        stored_nanos: Dict[str, int] = {}
        if not online_config.skip_dedup:
            # Read just the timestamp field before the bounded write batches.
            unique_hash_keys = list(dict.fromkeys(hash_keys))
            reads = client.batch_hash_multi_get(unique_hash_keys, [ts_field])
            for key, values in zip(unique_hash_keys, reads):
                raw = decode_bytes(values[0]) if values else None
                if raw:
                    stored = Timestamp()
                    stored.ParseFromString(raw)
                    stored_nanos[key] = stored.seconds * 1_000_000_000 + stored.nanos

        latest_timestamps: Dict[str, Timestamp] = {}
        latest_values: Dict[str, Dict[str, ValueProto]] = {}
        latest_serialized_values: Dict[
            str, List[Tuple[str, ValueProto, bytes]]
        ] = {}
        history_points: Dict[str, List[Dict[str, Any]]] = {}
        numeric_points: Dict[str, List[Dict[str, Any]]] = {}
        expiry_keys: Dict[str, None] = {}
        written = 0
        skipped = 0
        # The newest event time queued so far in this call, per key. Without it,
        # two rows sharing an entity key would both compare against the same
        # pre-call value, both pass, and whichever was queued last would win
        # regardless of its event time.
        queued_nanos: Dict[str, int] = {}
        tracked_keys: List[str] = []

        for (_entity_key, values, timestamp, _created), hash_key, entity_key_hex in zip(
            data, hash_keys, entity_key_hexes
        ):
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

            # A Feast batch can contain many events for one entity. Only its
            # newest accepted row belongs in the latest-value hash; history
            # below retains every accepted timestamp.
            latest_timestamps[hash_key] = proto_ts

            if online_config.write_feature_history:
                serialized_values = [
                    (feature_name, value, value.SerializeToString())
                    for feature_name, value in values.items()
                ]
                latest_serialized_values[hash_key] = serialized_values
                timestamp_ms = proto_ts.seconds * 1000 + proto_ts.nanos // 1_000_000
                for feature_name, value, serialized_value in serialized_values:
                    history_key = self._history_key_from_hex(
                        config, view_name, entity_key_hex, feature_name
                    )
                    history_points.setdefault(history_key, []).append(
                        {
                            "timestamp_ms": timestamp_ms,
                            "value": serialized_value,
                        }
                    )
                    numeric_field = value.WhichOneof("val")
                    if numeric_field in ("int32_val", "int64_val"):
                        aggregate_key = self._aggregate_key_from_hex(
                            config, view_name, entity_key_hex, feature_name
                        )
                        numeric_points.setdefault(aggregate_key, []).append(
                            {
                                "timestamp_ms": timestamp_ms,
                                "value": str(getattr(value, numeric_field)).encode("ascii"),
                            }
                        )
            else:
                latest_values[hash_key] = values

            if online_config.key_ttl_seconds:
                expiry_keys[hash_key] = None
            if online_config.track_keys_for_teardown:
                tracked_keys.append(hash_key)
            written += 1

        hash_rows: List[Tuple[str, List[Tuple[str, bytes]]]] = []
        shared_fields: Optional[Tuple[str, ...]] = None
        shared_layout = True
        for hash_key, proto_ts in latest_timestamps.items():
            serialized_values = latest_serialized_values.get(hash_key)
            if serialized_values is None:
                serialized_values = [
                    (feature_name, value, value.SerializeToString())
                    for feature_name, value in latest_values[hash_key].items()
                ]
            entries = [(ts_field, proto_ts.SerializeToString())]
            entries.extend(
                (_feature_field(view_name, feature_name), serialized_value)
                for feature_name, _value, serialized_value in serialized_values
            )
            hash_rows.append((hash_key, entries))
            row_fields = tuple(field for field, _value in entries)
            if shared_fields is None:
                shared_fields = row_fields
            elif row_fields != shared_fields:
                shared_layout = False
        commands: List[Dict[str, Any]] = []
        commands.extend(
            {"kind": "feature_append", "key": key, "points": points}
            for key, points in history_points.items()
        )
        commands.extend(
            {"kind": "feature_append", "key": key, "points": points}
            for key, points in numeric_points.items()
        )
        commands.extend(
            {
                "kind": "common_expire",
                "key": key,
                "ttl_ms": int(online_config.key_ttl_seconds) * 1000,
            }
            for key in expiry_keys
        )
        for key in dict.fromkeys(tracked_keys):
            commands.append(
                {
                    "kind": "set_add",
                    "key": self._teardown_shard_key(config, view_name, key),
                    "member": key.encode("utf-8"),
                }
            )

        if shared_fields is not None and shared_layout and not commands:
            client.batch_hash_multi_set(
                list(shared_fields),
                [
                    (hash_key, [value for _field, value in entries])
                    for hash_key, entries in hash_rows
                ],
            )
        elif hash_rows or commands:
            client.batch_write(
                [
                    {"kind": "hash_multi_set", "key": key, "entries": entries}
                    for key, entries in hash_rows
                ]
                + commands
            )
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
    def _history_key_from_hex(
        config: RepoConfig,
        view_name: str,
        entity_key_hex: str,
        feature_name: str,
    ) -> str:
        return f"fh:{config.project}:{view_name}:{entity_key_hex}:{feature_name}"

    @classmethod
    def _history_key(
        cls,
        config: RepoConfig,
        view_name: str,
        entity_key: EntityKeyProto,
        feature_name: str,
    ) -> str:
        return cls._history_key_from_hex(
            config, view_name, cls._entity_key_hex(config, entity_key), feature_name
        )

    @staticmethod
    def _aggregate_key_from_hex(
        config: RepoConfig,
        view_name: str,
        entity_key_hex: str,
        feature_name: str,
    ) -> str:
        return f"fa:{config.project}:{view_name}:{entity_key_hex}:{feature_name}"

    @classmethod
    def _aggregate_key(
        cls,
        config: RepoConfig,
        view_name: str,
        entity_key: EntityKeyProto,
        feature_name: str,
    ) -> str:
        return cls._aggregate_key_from_hex(
            config, view_name, cls._entity_key_hex(config, entity_key), feature_name
        )

    def query_feature_aggregate(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_key: EntityKeyProto,
        feature_name: str,
        start_ms: int,
        end_ms: int,
        aggregator: str,
        count: Optional[int] = None,
    ) -> int:
        """Query a history window; numeric aggregates require scalar integer values."""
        return self.query_feature_aggregates(
            config,
            table,
            [(entity_key, feature_name, start_ms, end_ms, aggregator, count)],
        )[0]

    def query_feature_aggregates(
        self,
        config: RepoConfig,
        table: FeatureView,
        queries: Sequence[
            Tuple[EntityKeyProto, str, int, int, str, Optional[int]]
        ],
    ) -> List[int]:
        """Query many history windows through bounded ordered batches."""
        commands = []
        command_indexes = {}
        response_indexes = []
        for query in queries:
            command = self._feature_aggregate_command(config, table, *query)
            identity = (
                command["key"],
                command["start_ms"],
                command["end_ms"],
                command["aggregator"],
                command["count"],
            )
            command_index = command_indexes.get(identity)
            if command_index is None:
                command_index = len(commands)
                command_indexes[identity] = command_index
                commands.append(command)
            response_indexes.append(command_index)

        answers = self._get_client(config).batch_execute(commands)
        unique_values = []
        for answer in answers:
            if answer.get("kind") != "aggregate":
                raise ValueError("unexpected feature aggregate response")
            unique_values.append(int(answer["value"]))
        return [unique_values[index] for index in response_indexes]

    def _feature_aggregate_command(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_key: EntityKeyProto,
        feature_name: str,
        start_ms: int,
        end_ms: int,
        aggregator: str,
        count: Optional[int],
    ) -> Dict[str, Any]:
        online_config = config.online_store
        assert isinstance(online_config, TemporalStoreCommandOnlineStoreConfig)
        if not online_config.write_feature_history:
            raise ValueError("write_feature_history must be enabled")
        if start_ms < 0 or end_ms < start_ms or (count is not None and count < 0):
            raise ValueError("invalid aggregate window or count")
        feature = next(
            (item for item in table.features if item.name == feature_name), None
        )
        if feature is None:
            raise ValueError(f"unknown feature: {feature_name}")
        name = aggregator.lower()
        if name not in (
            "sum", "avg", "average", "min", "max", "first", "last", "count", "events"
        ):
            raise ValueError(f"unsupported aggregator: {aggregator}")
        if name not in ("count", "events") and feature.dtype not in (Int32, Int64):
            raise ValueError("numeric aggregates require an Int32 or Int64 feature")
        view_name = self._view_name(table, config)
        key = (
            self._history_key(config, view_name, entity_key, feature_name)
            if name in ("count", "events")
            else self._aggregate_key(config, view_name, entity_key, feature_name)
        )
        return {
            "kind": "feature_agg_query",
            "key": key,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "aggregator": name,
            "count": count,
        }

    @staticmethod
    def _teardown_set_key(config: RepoConfig, view_name: str) -> str:
        return f"fkeys:{config.project}:{view_name}"

    @classmethod
    def _teardown_shard_key(
        cls, config: RepoConfig, view_name: str, row_key: str
    ) -> str:
        shard = zlib.crc32(row_key.encode("utf-8")) & (_TEARDOWN_TRACKING_SHARDS - 1)
        return f"{cls._teardown_set_key(config, view_name)}:{shard:03x}"

    @classmethod
    def _teardown_shard_keys(
        cls, config: RepoConfig, view_name: str
    ) -> List[str]:
        prefix = cls._teardown_set_key(config, view_name)
        return [f"{prefix}:{shard:03x}" for shard in range(_TEARDOWN_TRACKING_SHARDS)]

    # -- reads ---------------------------------------------------------------

    @staticmethod
    def _decode_online_response(
        raw_values: Sequence[Any],
        fields: Sequence[str],
        feature_names: Sequence[str],
    ) -> Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]:
        # HashMultiGet answers positionally, one slot per requested field,
        # with null for a field the hash does not hold. A short answer would
        # shift every value onto the wrong feature, so pad rather than zip
        # a ragged pair.
        if len(raw_values) < len(fields):
            raw_values = list(raw_values) + [None] * (len(fields) - len(raw_values))

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

        if event_timestamp is None and not any(decoded[:-1]):
            return None, None
        return event_timestamp, row

    @staticmethod
    def _copy_online_row(row: Dict[str, ValueProto]) -> Dict[str, ValueProto]:
        copied: Dict[str, ValueProto] = {}
        for feature_name, value in row.items():
            copied_value = ValueProto()
            copied_value.CopyFrom(value)
            copied[feature_name] = copied_value
        return copied

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

        unique_hash_keys: List[str] = []
        response_index_by_hash_key: Dict[str, int] = {}
        response_indices: List[int] = []
        for entity_key in entity_keys:
            hash_key = self._entity_hash_key(config, entity_key)
            response_index = response_index_by_hash_key.get(hash_key)
            if response_index is None:
                response_index = len(unique_hash_keys)
                response_index_by_hash_key[hash_key] = response_index
                unique_hash_keys.append(hash_key)
            response_indices.append(response_index)

        responses = client.batch_hash_multi_get(unique_hash_keys, fields)

        # Storage commands are already coalesced by entity key. Decode on that same
        # unique cardinality instead of reparsing every protobuf for every repeated
        # requested position.
        decoded_rows = [
            self._decode_online_response(response, fields, feature_names)
            for response in responses
        ]
        returned = [False] * len(decoded_rows)
        results: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for response_index in response_indices:
            event_timestamp, row = decoded_rows[response_index]
            if row is None:
                results.append((None, None))
            elif not returned[response_index]:
                returned[response_index] = True
                results.append((event_timestamp, row))
            else:
                # Feast callers may mutate returned protobufs. Repeated positions
                # therefore share parsing work, not mutable message instances.
                results.append((event_timestamp, self._copy_online_row(row)))
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
            legacy_set_key = self._teardown_set_key(config, view_name)
            shard_keys = self._teardown_shard_keys(config, view_name)
            # DELETE THIS VIEW'S FIELDS, NOT THE WHOLE ROW. The layout is one
            # hash per (project, entity key), SHARED BY EVERY FEATURE VIEW in
            # the project -- the view appears only in the FIELD names. So a
            # `CommonDelete` on the row removed every other view's features for
            # that entity too, and the caller asked for one view to be torn
            # down.
            #
            # This is Feast's own issue #2150, and its universal suite has a
            # test for exactly it -- `test_online_store_cleanup`, which
            # registers two views over a shared `driver` entity, deletes one,
            # and requires the other's features to survive. That test is gated
            # `@pytest.mark.universal_online_stores(only=["redis"])`, so it
            # never ran against this store and the defect was not caught by it.
            #
            # The fields are derivable rather than discovered: a write puts
            # `"{view}:{feature}"` for each feature plus one `"_ts:{view}"`, so
            # the view's own schema names them. It costs F+1 `HashDelete`
            # commands per row instead of one `CommonDelete`, which is the
            # price of not destroying a neighbour's data.
            fields = [
                _feature_field(view_name, feature.name) for feature in table.features
            ] + [_ts_field(view_name)]
            row_keys = set()
            active_set_keys = []
            pending_deletes: List[Dict[str, Any]] = []
            deleted = 0
            field_delete_round_trips = 0

            def delete_members(members: Sequence[Any]) -> None:
                nonlocal deleted, field_delete_round_trips
                for member in members:
                    raw = decode_bytes(member)
                    if raw is None:
                        continue
                    row_key = raw.decode("utf-8")
                    if row_key in row_keys:
                        continue
                    row_keys.add(row_key)
                    for field in fields:
                        pending_deletes.append(
                            {"kind": "hash_delete", "key": row_key, "field": field}
                        )
                        if len(pending_deletes) == batch_size:
                            client.batch_execute(pending_deletes)
                            deleted += len(pending_deletes)
                            field_delete_round_trips += 1
                            pending_deletes.clear()

            legacy_members = client.execute(
                {"kind": "set_members", "key": legacy_set_key}
            ).get("members") or []
            if legacy_members:
                active_set_keys.append(legacy_set_key)
                delete_members(legacy_members)

            tracking_read_round_trips = 1
            read_batch_size = client.max_batch_commands
            for start in range(0, len(shard_keys), read_batch_size):
                key_batch = shard_keys[start : start + read_batch_size]
                shard_answers = client.batch_execute(
                    [{"kind": "set_members", "key": key} for key in key_batch]
                )
                tracking_read_round_trips += 1
                for set_key, answer in zip(key_batch, shard_answers):
                    shard_members = answer.get("members") or []
                    if shard_members:
                        active_set_keys.append(set_key)
                        delete_members(shard_members)

            if pending_deletes:
                client.batch_execute(pending_deletes)
                deleted += len(pending_deletes)
                field_delete_round_trips += 1
                pending_deletes.clear()

            # Tracking sets are dropped LAST, only after every row they named
            # has lost this view's fields. If a chunk fails partway, remaining
            # sets still name their rows and a re-run finishes the cleanup.
            tracking_deletes = [
                {"kind": "common_delete", "key": set_key}
                for set_key in active_set_keys
            ]
            tracking_delete_round_trips = 0
            for start in range(0, len(tracking_deletes), batch_size):
                client.batch_execute(tracking_deletes[start : start + batch_size])
                tracking_delete_round_trips += 1
            logger.info(
                "temporalstore: tore down view=%s rows=%d fields=%d in %d round "
                "trip(s) of at most %d",
                view_name,
                len(row_keys),
                deleted,
                field_delete_round_trips
                + tracking_read_round_trips
                + tracking_delete_round_trips,
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

    # -- what this store TELLS Feast it can do ---------------------------------
    #
    # THIS PROPERTY IS LOAD-BEARING, AND ITS ABSENCE MADE THE TWO METHODS BELOW
    # DEAD CODE.
    #
    # `OnlineStore.async_supported` returns a bare `SupportedAsyncMethods()`,
    # whose `read` and `write` both default to False, and Feast GATES on it:
    # `feature_server` tests `store._get_provider().async_supported.online.read`
    # before taking the async read path and `.online.write` before the async
    # write path, reaching this store through
    # `PassthroughProvider.async_supported`. So a store that implements the async
    # methods without advertising them is never asked for them -- the two
    # implementations below, and the two tests covering them, were complete,
    # correct and unreachable. Declaring this is the whole fix.
    #
    # `test_the_advertisement_matches_what_the_class_implements` keeps the two
    # from drifting apart again IN EITHER DIRECTION: advertising support that is
    # not implemented sends Feast into a raising base method, and implementing
    # support that is not advertised is the inert state this property ends.
    @property
    def async_supported(self) -> SupportedAsyncMethods:
        return SupportedAsyncMethods(read=True, write=True)

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

    # -- vector retrieval, refused by name -------------------------------------

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: List[float],
        top_k: int,
        distance_metric: Optional[str] = None,
    ) -> List[Any]:
        raise NotImplementedError(_NO_VECTOR_SEARCH)

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
        filters: Optional[Any] = None,
        include_feature_view_version_metadata: bool = False,
    ) -> List[Any]:
        raise NotImplementedError(_NO_VECTOR_SEARCH)

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
