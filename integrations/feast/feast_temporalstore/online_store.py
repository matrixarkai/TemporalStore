# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
r"""A Feast online store backed by TemporalStore over its RESP surface.

Configure it by naming the class in ``feature_store.yaml``::

    online_store:
        type: feast_temporalstore.online_store.TemporalStoreOnlineStore
        connection_string: localhost:6399
        key_ttl_seconds: 86400

Feast resolves a custom store by requiring the class name to end in ``OnlineStore`` and then
importing ``<ClassName>Config`` from the same module, so both names here are load-bearing.

TWO DESIGN DECISIONS ARE FORCED BY MEASURED SERVER BEHAVIOUR, not by taste.

1. **Keys and hash fields are encoded.** The RESP dispatch decodes both with
   ``String::from_utf8_lossy``, and Feast addresses rows by binary keys and 4-byte binary field
   names. Unencoded, two distinct field names collide once in 1,042 instead of once in
   4.29 billion, which returns the WRONG feature's value. See :mod:`feast_temporalstore.wire`
   for the figures and for why keys use hex and fields use base64url.

2. **Deletion walks a registry, never ``SCAN``.** The server builds its scannable keyspace in
   per-connection state (a fresh ``RedisCommandState`` per accepted socket), so ``SCAN`` returns
   only the keys written on that same socket. Feast's own cleanup scans from a fresh client in a
   separate CLI process, where that set is empty -- so a scan-based ``teardown`` would report
   success and delete nothing. This store records each view's row keys in a set as it writes
   them and enumerates that set to clean up. It is also cheaper than a full keyspace scan, which
   is why Feast's Redis store gates its own behind ``full_scan_for_deletion``.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from pydantic import StrictStr

from feast import Entity, FeatureView, RepoConfig
from feast.infra.online_stores.helpers import _mmh3, _redis_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from google.protobuf.timestamp_pb2 import Timestamp

from .wire import encode_field, encode_key

logger = logging.getLogger(__name__)

# The registry of row keys per feature view. Plain ASCII because we choose it, so it needs no
# encoding -- and it carries characters outside the hex alphabet, so it can never collide with
# an encoded row key.
_REGISTRY_PREFIX = "ts:feast:rows:"


class TemporalStoreOnlineStoreConfig(FeastConfigBaseModel):
    """Configuration for the TemporalStore online store."""

    type: StrictStr = "feast_temporalstore.online_store.TemporalStoreOnlineStore"
    connection_string: StrictStr = "localhost:6399"
    key_ttl_seconds: Optional[int] = None
    skip_dedup: bool = False
    socket_timeout_seconds: Optional[float] = 10.0


class TemporalStoreOnlineStore(OnlineStore):
    """Feast's ``OnlineStore`` contract over TemporalStore's RESP surface."""

    _client: Optional[Any] = None

    # ---- connection -------------------------------------------------------------------

    def _connect(self, config: RepoConfig):
        if self._client is not None:
            return self._client
        import redis  # a RESP client; TemporalStore speaks the protocol

        store_config = config.online_store
        host, _, port = store_config.connection_string.partition(":")
        self._client = redis.Redis(
            host=host or "localhost",
            port=int(port or 6399),
            socket_timeout=store_config.socket_timeout_seconds,
            # Bytes in, bytes out. Decoding responses to str here would hand Python the same
            # lossy problem the encoding exists to keep off the wire.
            decode_responses=False,
        )
        return self._client

    @staticmethod
    def _view_name(table: FeatureView, config: RepoConfig) -> str:
        """The versioned view name Feast uses to namespace fields, if this Feast has one."""
        try:
            from feast.infra.online_stores.helpers import _versioned_fv_name

            return _versioned_fv_name(table, config)
        except Exception:
            return table.name

    @staticmethod
    def _registry_key(project: str, view: str) -> bytes:
        return "{}{}:{}".format(_REGISTRY_PREFIX, project, view).encode("utf-8")

    # ---- writes -----------------------------------------------------------------------

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        client = self._connect(config)
        store_config = config.online_store
        project = config.project
        view = self._view_name(table, config)
        ts_field = encode_field("_ts:{}".format(view).encode("utf-8"))
        registry = self._registry_key(project, view)
        ttl = getattr(store_config, "key_ttl_seconds", None)

        rows = []
        for entity_key, values, timestamp, _created in data:
            row_key = encode_key(_redis_key(project, entity_key))
            stamp = Timestamp()
            stamp.FromDatetime(timestamp)
            mapping = {ts_field: stamp.SerializeToString()}
            for feature_name, value in values.items():
                field = encode_field(_mmh3("{}:{}".format(view, feature_name)))
                mapping[field] = value.SerializeToString()
            rows.append((row_key, mapping, stamp))

        # Dedup: keep the newest write per row, the rule Feast's own store applies. One pipelined
        # read of the stored stamp, then a pipelined write of only the rows that move forward.
        # `skip_dedup` writes unconditionally, as Feast's flag does.
        keep = rows
        if not getattr(store_config, "skip_dedup", False):
            pipe = client.pipeline(transaction=False)
            for row_key, _mapping, _stamp in rows:
                pipe.hmget(row_key, [ts_field])
            stored = [item[0] for item in pipe.execute()]
            keep = []
            for (row_key, mapping, stamp), previous in zip(rows, stored):
                if previous:
                    before = Timestamp()
                    before.ParseFromString(previous)
                    if _nanos(before) >= _nanos(stamp):
                        if progress:
                            progress(1)
                        continue
                keep.append((row_key, mapping, stamp))

        pipe = client.pipeline(transaction=False)
        for row_key, mapping, _stamp in keep:
            pipe.hset(row_key, mapping=mapping)
            # The registry is what teardown enumerates, because SCAN cannot see another
            # connection's writes. Recorded in the same pipeline as the row, so the two cannot
            # drift apart by more than one failed flush.
            pipe.sadd(registry, row_key)
            if ttl:
                pipe.expire(row_key, ttl)
        pipe.execute()
        if progress:
            progress(len(keep))

    # ---- reads ------------------------------------------------------------------------

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        client = self._connect(config)
        project = config.project
        view = self._view_name(table, config)
        if requested_features:
            names = list(requested_features)
        else:
            names = [feature.name for feature in table.features]
        fields = [encode_field(_mmh3("{}:{}".format(view, name))) for name in names]
        ts_field = encode_field("_ts:{}".format(view).encode("utf-8"))

        pipe = client.pipeline(transaction=False)
        for entity_key in entity_keys:
            pipe.hmget(encode_key(_redis_key(project, entity_key)), fields + [ts_field])
        rows = pipe.execute()

        results: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for row in rows:
            feature_bytes, stamp_bytes = row[:-1], row[-1]
            if stamp_bytes is None and all(value is None for value in feature_bytes):
                results.append((None, None))
                continue
            event_time = None
            if stamp_bytes is not None:
                stamp = Timestamp()
                stamp.ParseFromString(stamp_bytes)
                event_time = stamp.ToDatetime()
            values: Dict[str, ValueProto] = {}
            for name, raw in zip(names, feature_bytes):
                value = ValueProto()
                if raw is not None:
                    value.ParseFromString(raw)
                values[name] = value
            results.append((event_time, values))
        return results

    # ---- lifecycle --------------------------------------------------------------------

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ) -> None:
        for table in tables_to_delete:
            self._drop_view(config, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        for table in tables:
            self._drop_view(config, table)

    def _drop_view(self, config: RepoConfig, table: FeatureView) -> None:
        """Remove one view's fields from every row it wrote, and the row when nothing is left.

        Enumerated from the registry rather than from SCAN -- see the module note.
        """
        client = self._connect(config)
        project = config.project
        view = self._view_name(table, config)
        registry = self._registry_key(project, view)
        row_keys = list(client.smembers(registry) or [])
        if not row_keys:
            return

        mine = {encode_field(_mmh3("{}:{}".format(view, feature.name))) for feature in table.features}
        mine.add(encode_field("_ts:{}".format(view).encode("utf-8")))

        pipe = client.pipeline(transaction=False)
        for row_key in row_keys:
            pipe.hkeys(row_key)
        present = pipe.execute()

        pipe = client.pipeline(transaction=False)
        for row_key, fields in zip(row_keys, present):
            held = set(fields or [])
            # A field this view does not own belongs to another view on the same entity row, so
            # the row has to survive and only our own fields come off it.
            ours = held & mine
            if held and not (held - mine):
                pipe.delete(row_key)
            elif ours:
                pipe.hdel(row_key, *sorted(ours))
        pipe.delete(registry)
        pipe.execute()


def _nanos(stamp: Timestamp) -> int:
    return stamp.seconds * 1_000_000_000 + stamp.nanos
