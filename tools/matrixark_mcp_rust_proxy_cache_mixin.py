#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Cache adapter mixin for the MatrixArk Rust proxy client."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json
    from tools.matrixark_mcp_rust_proxy_cache import (
        context_pack_response_cache_clear,
        context_pack_response_cache_get,
        context_pack_response_cache_key,
        context_pack_response_cache_put,
        context_pack_response_singleflight_enter,
        context_pack_response_singleflight_finish,
        context_pack_response_singleflight_wait,
        mark_context_pack_response_cache_hit,
        scan_hash_cache_get,
        scan_hash_cache_invalidate_keys,
        scan_hash_cache_put,
        string_cache_get,
        string_cache_key_allowed,
        string_cache_put,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json
    from matrixark_mcp_rust_proxy_cache import (
        context_pack_response_cache_clear,
        context_pack_response_cache_get,
        context_pack_response_cache_key,
        context_pack_response_cache_put,
        context_pack_response_singleflight_enter,
        context_pack_response_singleflight_finish,
        context_pack_response_singleflight_wait,
        mark_context_pack_response_cache_hit,
        scan_hash_cache_get,
        scan_hash_cache_invalidate_keys,
        scan_hash_cache_put,
        string_cache_get,
        string_cache_key_allowed,
        string_cache_put,
    )


class MatrixArkRustProxyCacheMixin:
    """Thin instance-method wrappers over Rust proxy cache helpers."""

    def _string_cache_key_allowed(self, key: str) -> bool:
        return string_cache_key_allowed(self, key)

    def _string_cache_get(self, key: str) -> str | None:
        return string_cache_get(self, key)

    def _string_cache_put(self, key: str, value: str) -> None:
        string_cache_put(self, key, value)

    def _scan_hash_cache_get(self, key: str) -> Json | None:
        return scan_hash_cache_get(self, key)

    def _scan_hash_cache_put(self, key: str, response: Json) -> None:
        scan_hash_cache_put(self, key, response)

    def _scan_hash_cache_invalidate_keys(self, keys: Any) -> None:
        scan_hash_cache_invalidate_keys(self, keys)

    def _context_pack_response_cache_key(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Json,
    ) -> str:
        return context_pack_response_cache_key(
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            request=request,
        )

    def _mark_context_pack_response_cache_hit(self, response: Json) -> Json:
        return mark_context_pack_response_cache_hit(response)

    def _context_pack_response_cache_get(self, cache_key: str) -> Json | None:
        return context_pack_response_cache_get(self, cache_key)

    def _context_pack_response_cache_put(self, cache_key: str, response: Json) -> None:
        context_pack_response_cache_put(self, cache_key, response)

    def _context_pack_response_cache_clear(self) -> None:
        context_pack_response_cache_clear(self)

    def _context_pack_response_singleflight_enter(self, cache_key: str) -> tuple[Json, bool]:
        return context_pack_response_singleflight_enter(self, cache_key)

    def _context_pack_response_singleflight_finish(
        self,
        cache_key: str,
        inflight: Json,
        error: BaseException | None,
    ) -> None:
        context_pack_response_singleflight_finish(self, cache_key, inflight, error)

    def _context_pack_response_singleflight_wait(self, cache_key: str, inflight: Json) -> Json:
        return context_pack_response_singleflight_wait(self, cache_key, inflight)
