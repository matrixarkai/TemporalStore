#!/usr/bin/env python3
"""MatrixArk embedding helpers and local embedding cache."""

from __future__ import annotations

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
_EMBEDDING_VECTOR_CACHE: dict[tuple[str, str], list[float]] = {}
_EMBEDDING_VECTOR_CACHE_LOCK = threading.RLock()
_EMBEDDING_FALLBACK_USED = False


def embedding_for_text(text: str) -> list[float]:
    model = embedding_model_name()
    cache_key = (model, text)
    with _EMBEDDING_VECTOR_CACHE_LOCK:
        cached = _EMBEDDING_VECTOR_CACHE.get(cache_key)
        if cached is not None:
            return list(cached)
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip().lower()
    if provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        vector = oss_embedding_for_text(text)
        with _EMBEDDING_VECTOR_CACHE_LOCK:
            if len(_EMBEDDING_VECTOR_CACHE) >= 8192:
                _EMBEDDING_VECTOR_CACHE.clear()
            _EMBEDDING_VECTOR_CACHE[cache_key] = list(vector)
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
        if len(_EMBEDDING_VECTOR_CACHE) >= 8192:
            _EMBEDDING_VECTOR_CACHE.clear()
        _EMBEDDING_VECTOR_CACHE[cache_key] = list(result)
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
            cached = _EMBEDDING_VECTOR_CACHE.get((model, text))
            if cached is None:
                results.append(None)
                missing.append((index, text))
            else:
                results.append(list(cached))
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip().lower()
    if missing and provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
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
                if len(_EMBEDDING_VECTOR_CACHE) + len(missing) >= 8192:
                    _EMBEDDING_VECTOR_CACHE.clear()
                for (index, text), vector in zip(missing, vectors):
                    materialized = [round(float(value), 6) for value in vector]
                    _EMBEDDING_VECTOR_CACHE[(model, text)] = list(materialized)
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
    return "matrixark-local-token-hash-v1"


def embedding_execution_mode_name() -> str:
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip().lower()
    if embedding_fallback_used():
        return "local_hash_embedding_fallback"
    if provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        return "oss_embedding_model"
    if provider == "hash":
        return "hashing-local"
    return "deterministic-token-hash"


def embedding_fallback_used() -> bool:
    return _EMBEDDING_FALLBACK_USED


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
        if os.environ.get("MATRIXARK_REQUIRE_OSS_EMBEDDINGS", "").strip().lower() in {"1", "true", "yes"}:
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
