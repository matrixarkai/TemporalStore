#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Direct TemporalStore cache adapter mixin."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json
    from tools import matrixark_mcp_direct_cache as direct_cache_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json
    import matrixark_mcp_direct_cache as direct_cache_helpers


class TemporalDirectCacheAdapterMixin:
    """Adapter methods that bridge direct TemporalStore cache helpers."""

    def _ensure_direct_context_pack_response_cache(self) -> None:
        direct_cache_helpers.ensure_direct_context_pack_response_cache(self)

    def _direct_context_pack_response_cache_key(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Json,
    ) -> str:
        return direct_cache_helpers.direct_context_pack_response_cache_key(
            self,
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            request=request,
        )

    def _direct_context_pack_response_cache_get(self, cache_key: str) -> Json | None:
        return direct_cache_helpers.direct_context_pack_response_cache_get(self, cache_key)

    def _direct_context_pack_response_cache_put(self, cache_key: str, response: Json) -> None:
        direct_cache_helpers.direct_context_pack_response_cache_put(self, cache_key, response)

    def _direct_record_load_lock(self) -> Any:
        return direct_cache_helpers.direct_record_load_lock(self)

    def _get_direct_record_cache(self, count: int) -> list[Json] | None:
        return direct_cache_helpers.get_direct_record_cache(self, count)

    def _put_direct_record_cache(self, count: int, records: list[Json]) -> None:
        direct_cache_helpers.put_direct_record_cache(self, count, records)

    def _drop_direct_record_cache(self) -> None:
        direct_cache_helpers.drop_direct_record_cache(self)

    def _retrieval_candidate_cache_key(
        self,
        *,
        count: int,
        scope: Json,
        record_types: set[str] | None,
        secondary_index_groups: list[set[str]] | None,
        selected_node_hashes: set[int] | None,
    ) -> str:
        return direct_cache_helpers.retrieval_candidate_cache_key(
            self,
            count=count,
            scope=scope,
            record_types=record_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
        )

    def _prune_retrieval_candidate_cache(self, current_count: int) -> None:
        direct_cache_helpers.prune_retrieval_candidate_cache(self, current_count)

    def _placement_candidate_table_cache_key(
        self,
        *,
        count: int,
        scope_key: str,
        node_hash: int,
        record_type: str,
        resource_version_watermark: str = "",
    ) -> str:
        return direct_cache_helpers.placement_candidate_table_cache_key(
            self,
            count=count,
            scope_key=scope_key,
            node_hash=node_hash,
            record_type=record_type,
            resource_version_watermark=resource_version_watermark,
        )

    def _record_primary_hash(self, record: Json) -> int:
        return direct_cache_helpers.record_primary_hash(record)

    def _placement_candidate_records_from_cache_or_load(
        self,
        *,
        count: int,
        scope: Json,
        allowed_types: set[str],
        selected_nodes: set[int],
        locations: list[Json],
        resource_version_watermark: str = "",
    ) -> Json:
        return direct_cache_helpers.placement_candidate_records_from_cache_or_load(
            self,
            count=count,
            scope=scope,
            allowed_types=allowed_types,
            selected_nodes=selected_nodes,
            locations=locations,
            resource_version_watermark=resource_version_watermark,
        )
