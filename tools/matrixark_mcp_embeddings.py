#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk embedding helpers and local embedding cache."""

from __future__ import annotations

import collections
import hashlib
import math
import os
import threading
from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_scoring import tokens
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_scoring import tokens


EMBEDDING_DIM = 32
_OSS_EMBEDDING_MODEL_CACHE: dict[str, Any] = {}
# Bounded LRU rather than a plain dict. The previous policy CLEARED the whole cache on overflow,
# which throws away every warm entry at the exact moment the cache starts paying off -- on a corpus
# with any repeated text that turns a steady hit rate into a sawtooth. Eviction is now
# least-recently-used, and the bound is configurable because the cache is real memory: an entry is a
# 384-float vector, so the 8192 default is roughly 25 MB per worker.
_EMBEDDING_VECTOR_CACHE: "collections.OrderedDict[tuple[str, str], list[float]]" = collections.OrderedDict()
_EMBEDDING_VECTOR_CACHE_LOCK = threading.RLock()
_EMBEDDING_CACHE_STATS = {"hits": 0, "misses": 0, "evictions": 0}


def embedding_cache_capacity() -> int:
    try:
        return max(0, int(os.environ.get("MATRIXARK_EMBEDDING_CACHE_ENTRIES", "8192")))
    except ValueError:
        return 8192


def _cache_get(key: "tuple[str, str]") -> "list[float] | None":
    """Read through the LRU, refreshing recency. Caller must hold the lock."""
    vector = _EMBEDDING_VECTOR_CACHE.get(key)
    if vector is None:
        _EMBEDDING_CACHE_STATS["misses"] += 1
        return None
    _EMBEDDING_VECTOR_CACHE.move_to_end(key)
    _EMBEDDING_CACHE_STATS["hits"] += 1
    return vector


def _cache_put(key: "tuple[str, str]", vector: "list[float]") -> None:
    """Insert and evict the least recently used entries. Caller must hold the lock."""
    capacity = embedding_cache_capacity()
    if capacity <= 0:
        return
    _EMBEDDING_VECTOR_CACHE[key] = list(vector)
    _EMBEDDING_VECTOR_CACHE.move_to_end(key)
    while len(_EMBEDDING_VECTOR_CACHE) > capacity:
        _EMBEDDING_VECTOR_CACHE.popitem(last=False)
        _EMBEDDING_CACHE_STATS["evictions"] += 1


def embedding_cache_stats() -> dict:
    """Hits, misses, evictions and current size -- so cache behaviour is observable."""
    with _EMBEDDING_VECTOR_CACHE_LOCK:
        stats = dict(_EMBEDDING_CACHE_STATS)
        stats["entries"] = len(_EMBEDDING_VECTOR_CACHE)
        stats["capacity"] = embedding_cache_capacity()
        looked_up = stats["hits"] + stats["misses"]
        stats["hit_rate"] = round(stats["hits"] / looked_up, 4) if looked_up else 0.0
        return stats
_EMBEDDING_FALLBACK_USED = False


def embedding_for_text(text: str) -> list[float]:
    model = embedding_model_name()
    cache_key = (model, text)
    with _EMBEDDING_VECTOR_CACHE_LOCK:
        cached = _cache_get(cache_key)
        if cached is not None:
            return list(cached)
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip().lower()
    if provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        vector = oss_embedding_for_text(text)
        with _EMBEDDING_VECTOR_CACHE_LOCK:
            _cache_put(cache_key, vector)
        return vector
    if provider in _API_EMBEDDING_PROVIDERS:
        vector = api_embedding_for_text(text, provider)
        with _EMBEDDING_VECTOR_CACHE_LOCK:
            _cache_put(cache_key, vector)
        return vector
    vector = [0.0] * EMBEDDING_DIM
    for token in tokens(text):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = digest[0] % EMBEDDING_DIM
        sign = 1.0 if digest[1] % 2 == 0 else -1.0
        vector[index] += sign
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        result = vector
    else:
        result = [round(value / norm, 6) for value in vector]
    with _EMBEDDING_VECTOR_CACHE_LOCK:
        _cache_put(cache_key, result)
    return result


def embeddings_for_texts(texts: list[str]) -> list[list[float]]:
    """Batch-friendly embedding helper with the same cache as embedding_for_text."""
    if not texts:
        return []
    model = embedding_model_name()
    results: list[list[float] | None] = []
    missing: list[tuple[int, str]] = []
    with _EMBEDDING_VECTOR_CACHE_LOCK:
        for index, text in enumerate(texts):
            cached = _cache_get((model, text))
            if cached is None:
                results.append(None)
                missing.append((index, text))
            else:
                results.append(list(cached))
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip().lower()
    if missing and provider in _API_EMBEDDING_PROVIDERS:
        vectors = api_embedding_for_texts([text for _index, text in missing], provider)
        with _EMBEDDING_VECTOR_CACHE_LOCK:
            for (index, text), vector in zip(missing, vectors):
                materialized = [round(float(value), 6) for value in vector]
                _cache_put((model, text), materialized)
                results[index] = materialized
    elif missing and provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        model_ref = os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get(
            "MATRIXARK_EMBEDDING_MODEL",
            "sentence-transformers/all-MiniLM-L6-v2",
        )
        try:
            encoder = _OSS_EMBEDDING_MODEL_CACHE.get(model_ref)
            if encoder is None:
                from sentence_transformers import SentenceTransformer  # type: ignore

                encoder = SentenceTransformer(model_ref)
                _OSS_EMBEDDING_MODEL_CACHE[model_ref] = encoder
            vectors = encoder.encode([text for _index, text in missing], normalize_embeddings=True, show_progress_bar=False)
            with _EMBEDDING_VECTOR_CACHE_LOCK:
                for (index, text), vector in zip(missing, vectors):
                    materialized = [round(float(value), 6) for value in vector]
                    _cache_put((model, text), materialized)
                    results[index] = materialized
        except Exception:
            for index, text in missing:
                results[index] = embedding_for_text(text)
    else:
        for index, text in missing:
            results[index] = embedding_for_text(text)
    return [list(item or []) for item in results]


def embedding_model_name() -> str:
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip().lower()
    if provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        return os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get(
            "MATRIXARK_EMBEDDING_MODEL",
            "sentence-transformers/all-MiniLM-L6-v2",
        )
    if provider in _API_EMBEDDING_PROVIDERS:
        _endpoint, _api_key, model, _key_env = _api_embedding_config(provider)
        return model
    return "matrixark-local-token-hash-v1"


def embedding_execution_mode_name() -> str:
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip().lower()
    if embedding_fallback_used():
        return "local_hash_embedding_fallback"
    if provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        return "oss_embedding_model"
    if provider in _API_EMBEDDING_PROVIDERS:
        return "voyage_embedding_api" if provider == "voyage" else "openai_embedding_api"
    if provider == "hash":
        return "hashing-local"
    return "deterministic-token-hash"


def embedding_fallback_used() -> bool:
    return _EMBEDDING_FALLBACK_USED


# --- API embedding providers (production: OpenAI / Voyage / any OpenAI-compatible endpoint) -------
# OSS models stay the zero-config default (provider "deterministic"/"oss"); these activate ONLY when
# MATRIXARK_EMBEDDING_PROVIDER is set to an API provider AND the API key env is present. Anthropic has
# no embeddings API, so API embeddings are OpenAI/Voyage (or any OpenAI-compatible server, incl. a
# local vLLM/Ollama endpoint via MATRIXARK_EMBEDDING_API_BASE). No new dependency: stdlib urllib only.
_API_EMBEDDING_PROVIDERS = {"openai", "openai_compatible", "openai-compatible", "azure_openai", "voyage", "api"}


def _deterministic_embedding_for_text(text: str) -> list[float]:
    """The zero-config token-hash embedding, also used as the graceful fallback for oss/api providers."""
    vector = [0.0] * EMBEDDING_DIM
    for token in tokens(text):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = digest[0] % EMBEDDING_DIM
        sign = 1.0 if digest[1] % 2 == 0 else -1.0
        vector[index] += sign
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector
    return [round(value / norm, 6) for value in vector]


def _api_embedding_config(provider: str) -> tuple[str, str, str, str]:
    """(endpoint, api_key, model, key_env) for the selected API provider. Base URL + key env are
    overridable so the same path serves OpenAI, Azure, Voyage, and self-hosted OpenAI-compatible servers."""
    if provider == "voyage":
        base = os.environ.get("MATRIXARK_EMBEDDING_API_BASE", "https://api.voyageai.com/v1")
        key_env = os.environ.get("MATRIXARK_EMBEDDING_API_KEY_ENV", "VOYAGE_API_KEY")
        default_model = "voyage-3"
    else:  # openai / openai_compatible / azure_openai / api
        base = os.environ.get("MATRIXARK_EMBEDDING_API_BASE", "https://api.openai.com/v1")
        key_env = os.environ.get("MATRIXARK_EMBEDDING_API_KEY_ENV", "OPENAI_API_KEY")
        default_model = "text-embedding-3-large"
    model = os.environ.get("MATRIXARK_EMBEDDING_MODEL", default_model)
    endpoint = base.rstrip("/") + "/embeddings"
    return endpoint, os.environ.get(key_env, "").strip(), model, key_env


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def require_model_embeddings(path: str) -> bool:
    """True when a failed real-encoder call must raise instead of falling back to hash vectors.

    Three knobs name this idea and they are easy to confuse:
    ``MATRIXARK_REQUIRE_API_EMBEDDINGS`` (hosted/OpenAI-compatible endpoints) and
    ``MATRIXARK_REQUIRE_OSS_EMBEDDINGS`` (a locally loaded sentence-transformers model) each guard one
    path, while ``MATRIXARK_REQUIRE_MODEL_EMBEDDINGS`` reads like the provider-agnostic form of both --
    it is what an operator naturally reaches for, and it appears in the deployment config map. It
    previously enforced NOTHING, so a deployment that set it still degraded silently to 32-dimension
    hash vectors while reporting success. It is now honoured on both paths.

    Silent degradation is the failure worth preventing here: hash vectors answer 200, retrieve
    plausibly, and poison the store with mismatched-dimension data that only shows up as bad recall
    much later.
    """
    return _truthy_env("MATRIXARK_REQUIRE_MODEL_EMBEDDINGS") or _truthy_env(path)


def api_embedding_for_texts(texts: list[str], provider: str) -> list[list[float]]:
    """Embed via an OpenAI-compatible or Voyage embeddings API. Falls back to the deterministic
    encoder on missing key / network error unless MATRIXARK_REQUIRE_API_EMBEDDINGS is set (then it
    raises, so production fails fast instead of silently poisoning the store with mismatched-dim vectors)."""
    global _EMBEDDING_FALLBACK_USED
    endpoint, api_key, model, key_env = _api_embedding_config(provider)
    require = require_model_embeddings("MATRIXARK_REQUIRE_API_EMBEDDINGS")
    if not api_key:
        if require:
            raise MatrixArkError(f"API embeddings require {key_env} for provider '{provider}'")
        _EMBEDDING_FALLBACK_USED = True
        return [_deterministic_embedding_for_text(text) for text in texts]
    try:
        import json as _json
        import urllib.request as _urlreq

        body = _json.dumps({"model": model, "input": list(texts)}).encode("utf-8")
        request = _urlreq.Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        )
        timeout = float(os.environ.get("MATRIXARK_EMBEDDING_API_TIMEOUT_S", "30"))
        with _urlreq.urlopen(request, timeout=timeout) as response:  # noqa: S310 - fixed provider endpoint
            payload = _json.loads(response.read().decode("utf-8"))
        rows = sorted(payload.get("data", []), key=lambda row: row.get("index", 0))
        if not rows:
            raise MatrixArkError("API embedding response had no data")
        return [[round(float(value), 6) for value in row.get("embedding", [])] for row in rows]
    except MatrixArkError:
        raise
    except Exception as exc:  # network / auth / shape errors
        if require:
            raise MatrixArkError(f"API embedding call failed for provider '{provider}' ({model}): {exc}") from exc
        _EMBEDDING_FALLBACK_USED = True
        return [_deterministic_embedding_for_text(text) for text in texts]


def api_embedding_for_text(text: str, provider: str) -> list[float]:
    result = api_embedding_for_texts([text], provider)
    return result[0] if result else _deterministic_embedding_for_text(text)


def oss_embedding_for_text(text: str) -> list[float]:
    global _EMBEDDING_FALLBACK_USED
    model_ref = os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get(
        "MATRIXARK_EMBEDDING_MODEL",
        "sentence-transformers/all-MiniLM-L6-v2",
    )
    try:
        encoder = _OSS_EMBEDDING_MODEL_CACHE.get(model_ref)
        if encoder is None:
            from sentence_transformers import SentenceTransformer  # type: ignore

            encoder = SentenceTransformer(model_ref)
            _OSS_EMBEDDING_MODEL_CACHE[model_ref] = encoder
        vector = encoder.encode([text], normalize_embeddings=True, show_progress_bar=False)[0]
        return [round(float(value), 6) for value in vector]
    except Exception as exc:  # pragma: no cover - depends on optional local model packages.
        if require_model_embeddings("MATRIXARK_REQUIRE_OSS_EMBEDDINGS"):
            raise MatrixArkError(f"OSS embedding model is required but unavailable: {model_ref}: {exc}") from exc
        _EMBEDDING_FALLBACK_USED = True
        previous = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER")
        try:
            os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = "deterministic"
            return embedding_for_text(text)
        finally:
            if previous is None:
                os.environ.pop("MATRIXARK_EMBEDDING_PROVIDER", None)
            else:
                os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = previous
