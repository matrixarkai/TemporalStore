#!/usr/bin/env python3
"""Centralized-config loader for TemporalStore / MatrixArk.

Reads ``config/temporalstore.toml`` and, for every ``SECTION.key`` it knows,
exports the mapped environment variable **only if that variable is not already
set**. This gives the precedence:

    built-in code default  <  config file  <  explicit environment variable

Nothing here edits the scattered readers. The Python gateway and the Rust
engine keep reading their existing ``os.environ.get(...)`` / ``std::env::var``
names; this loader just seeds those names from one documented file.

Usage (as a launcher, the normal path):

    # via the shell entrypoint (recommended)
    scripts/with_config.sh python3 tools/matrixark_v1_gateway.py
    scripts/with_config.sh matrixark_rust_datanode

    # or directly: print the exports for eval, or a dry-run for inspection
    eval "$(python3 tools/matrixark_load_config.py --print-exports)"
    python3 tools/matrixark_load_config.py --dry-run          # show what WOULD be exported
    python3 tools/matrixark_load_config.py --print-mapping    # SECTION.key -> ENV_VAR table

Config file resolution order:
    1. --config PATH
    2. $MATRIXARK_CONFIG_FILE
    3. <repo>/config/temporalstore.toml
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

try:  # Python 3.11+
    import tomllib  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - older interpreters
    try:
        import tomli as tomllib  # type: ignore
    except ModuleNotFoundError:
        tomllib = None  # type: ignore


# =============================================================================
# Authoritative SECTION.key -> ENV_VAR mapping.
#
# This is the single source of truth linking config keys to the environment
# variable names the gateway (Python) and engine (Rust) already read. Keys that
# are commented out in the shipped TOML are still listed here so an operator who
# uncomments them gets the right export.
# =============================================================================
ENV_MAP: Dict[str, str] = {
    # ---- [server] -----------------------------------------------------------
    "server.bind_addr": "TS_SERVER_BIND_ADDR",
    "server.advertise_addr": "TS_SERVER_ADVERTISE_ADDR",
    "server.node_id": "TS_SERVER_NODE_ID",
    "server.shard_id": "TS_SHARD_ID",
    "server.worker_threads": "TS_SERVER_WORKER_THREADS",
    "server.max_queue_depth": "TS_SERVER_MAX_QUEUE_DEPTH",
    "server.max_background_queue_depth": "TS_SERVER_MAX_BACKGROUND_QUEUE_DEPTH",
    "server.heartbeat_interval_ms": "TS_SERVER_HEARTBEAT_INTERVAL_MS",
    "server.location": "TS_SERVER_LOCATION",
    "server.cache_dir": "TS_CACHE_DIR",
    "server.page_store_dir": "TS_PAGE_STORE_DIR",
    "server.blob_store_dir": "TS_BLOB_STORE_DIR",
    "server.index_dir": "TS_INDEX_DIR",
    "server.cache_memory_bytes": "TS_CACHE_MEMORY_BYTES",
    "server.blob_chunk_bytes": "TS_BLOB_CHUNK_BYTES",
    "server.standalone": "TS_STANDALONE",
    "server.distributed": "TS_DISTRIBUTED",
    # ---- [storage] ----------------------------------------------------------
    "storage.backend": "TS_STORAGE_BACKEND",
    # neutral config keys -> the engine's actual (enterprise) env names:
    "storage.object_store_endpoint": "TS_MATRIXOBJECT_ENDPOINT",
    "storage.object_store_bucket": "TS_MATRIXOBJECT_BUCKET",
    "storage.object_store_dir": "TS_MATRIXOBJECT_STORE_DIR",
    "storage.shared_store_dir": "TS_SHARED_STORE_DIR",
    "storage.zone_size_bytes": "TS_STORAGE_ZONE_SIZE",
    "storage.context_page_target_bytes": "TS_CONTEXT_PAGE_TARGET_BYTES",
    "storage.block_slab_target_bytes": "TS_BLOCK_SEGMENT_TARGET_BYTES",
    "storage.stream_max_blob_size": "TS_STREAM_MAX_BLOB_SIZE",
    "storage.compaction_watermark_bytes": "TS_COMPACTION_WATERMARK_BYTES",
    "storage.page_index_cache_bytes": "TS_PAGE_INDEX_CACHE_BYTES",
    "storage.block_index_cache_bytes": "TS_BLOCK_INDEX_CACHE_BYTES",
    "storage.cold_scan_no_cache_fill": "TS_COLD_SCAN_NO_CACHE_FILL",
    "storage.page_store_compression_enabled": "TS_PAGE_STORE_COMPRESSION_ENABLED",
    "storage.page_store_compression_level": "TS_PAGE_STORE_COMPRESSION_LEVEL",
    "storage.page_store_compression_min_bytes": "TS_PAGE_STORE_COMPRESSION_MIN_BYTES",
    "storage.cross_shard_reclaim_guard": "TS_CROSS_SHARD_RECLAIM_GUARD",
    # The engine reads TS_INDEX_DUMP_WAL_GAP_BYTES first and falls back to
    # TS_INDEX_DUMP_OPLOG_GAP_BYTES only when it is unset, so exporting the older name put
    # every value from this file behind anything that set the current one -- including a
    # portal write, which offers exactly that variable. The key keeps its name so deployed
    # files keep working; only the variable it exports moves to the one the engine reads
    # first. (Key and variable already differ elsewhere: block_slab_target_bytes exports
    # TS_BLOCK_SEGMENT_TARGET_BYTES.)
    "storage.index_dump_oplog_gap_bytes": "TS_INDEX_DUMP_WAL_GAP_BYTES",
    # ---- [wal] --------------------------------------------------------------
    "wal.wal_legacy_recovery": "TS_WAL_LEGACY_RECOVERY",
    "wal.wal_compress_records": "TS_WAL_COMPRESS_RECORDS",
    "wal.raft_wal_dir": "TS_RAFT_WAL_DIR",
    # ---- [replication] ------------------------------------------------------
    "replication.meta_addr": "TS_META_ADDR",
    "replication.meta_bind_addr": "TS_META_BIND_ADDR",
    "replication.proxy_bind_addr": "TS_PROXY_BIND_ADDR",
    "replication.proxy_advertised_addr": "TS_PROXY_ADVERTISED_ADDR",
    "replication.redis_bind_addr": "TS_REDIS_BIND_ADDR",
    "replication.proxy_connect_timeout_ms": "TS_PROXY_CONNECT_TIMEOUT_MS",
    "replication.proxy_io_timeout_ms": "TS_PROXY_IO_TIMEOUT_MS",
    "replication.proxy_max_retries": "TS_PROXY_MAX_RETRIES",
    "replication.meta_raft": "TS_META_RAFT",
    "replication.meta_raft_nodes": "TS_META_RAFT_NODES",
    "replication.raft_auto_failover": "TS_RAFT_AUTO_FAILOVER",
    "replication.meta_auto_rebalance": "TS_META_AUTO_REBALANCE",
    "replication.data_raft_read_mode": "TS_DATA_RAFT_READ_MODE",
    # ---- [recovery] ---------------------------------------------------------
    "recovery.bulk_ingest": "MATRIXARK_BULK_INGEST",
    "recovery.eager_cache_warm_on_load": "MATRIXARK_EAGER_CACHE_WARM_ON_LOAD",
    "recovery.bulk_ingest_replay_from_sequence": "MATRIXARK_BULK_INGEST_REPLAY_FROM_SEQUENCE",
    # ---- [retrieval] --------------------------------------------------------
    "retrieval.default_max_context_tokens": "MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS",
    "retrieval.gateway_default_max_context_tokens": "MATRIXARK_GATEWAY_DEFAULT_MAX_CONTEXT_TOKENS",
    "retrieval.context_source_mode": "MATRIXARK_CONTEXT_SOURCE_MODE",
    "retrieval.retrieval_min_score": "MATRIXARK_RETRIEVAL_MIN_SCORE",
    "retrieval.top_k_per_layer": "MATRIXARK_TOP_K_PER_LAYER",
    "retrieval.max_candidates_per_node": "MATRIXARK_MAX_CANDIDATES_PER_NODE",
    "retrieval.max_global_candidates": "MATRIXARK_MAX_GLOBAL_CANDIDATES",
    "retrieval.max_selected_refs": "MATRIXARK_MAX_SELECTED_REFS",
    "retrieval.budget_fill_policy": "MATRIXARK_BUDGET_FILL_POLICY",
    "retrieval.near_duplicate_overlap_threshold": "MATRIXARK_NEAR_DUPLICATE_OVERLAP_THRESHOLD",
    "retrieval.max_children_scored_per_parent": "MATRIXARK_MAX_CHILDREN_SCORED_PER_PARENT",
    "retrieval.cross_session_budget_ratio": "MATRIXARK_CROSS_SESSION_BUDGET_RATIO",
    "retrieval.cross_session_max_candidates": "MATRIXARK_CROSS_SESSION_MAX_CANDIDATES",
    "retrieval.cross_session_max_sessions": "MATRIXARK_CROSS_SESSION_MAX_SESSIONS",
    "retrieval.cross_session_min_score": "MATRIXARK_CROSS_SESSION_MIN_SCORE",
    "retrieval.cross_session_max_budget_tokens": "MATRIXARK_CROSS_SESSION_MAX_BUDGET_TOKENS",
    "retrieval.cross_session_max_budget_ratio": "MATRIXARK_CROSS_SESSION_MAX_BUDGET_RATIO",
    "retrieval.cross_session_parallelism": "MATRIXARK_CROSS_SESSION_PARALLELISM",
    "retrieval.cross_session_profile_max_candidates": "MATRIXARK_CROSS_SESSION_PROFILE_MAX_CANDIDATES",
    "retrieval.cross_session_profile_max_sessions": "MATRIXARK_CROSS_SESSION_PROFILE_MAX_SESSIONS",
    "retrieval.cross_session_profile_budget_ratio": "MATRIXARK_CROSS_SESSION_PROFILE_BUDGET_RATIO",
    "retrieval.shared_resource_budget_ratio": "MATRIXARK_SHARED_RESOURCE_BUDGET_RATIO",
    "retrieval.shared_skill_budget_ratio": "MATRIXARK_SHARED_SKILL_BUDGET_RATIO",
    "retrieval.shared_context_min_score": "MATRIXARK_SHARED_CONTEXT_MIN_SCORE",
    "retrieval.mode_dependent_quota": "MATRIXARK_MODE_DEPENDENT_QUOTA",
    "retrieval.query_rewrite": "MATRIXARK_QUERY_REWRITE",
    "retrieval.pack_precision_expand": "MATRIXARK_PACK_PRECISION_EXPAND",
    "retrieval.skill_discovery": "MATRIXARK_SKILL_DISCOVERY",
    # ---- [extraction] -------------------------------------------------------
    "extraction.summary_refresh_interval_ms": "MATRIXARK_SUMMARY_REFRESH_INTERVAL_MS",
    "extraction.summary_refresh_limit": "MATRIXARK_SUMMARY_REFRESH_LIMIT",
    "extraction.time_compression_window_events": "MATRIXARK_TIME_COMPRESSION_WINDOW_EVENTS",
    "extraction.time_compression_min_events": "MATRIXARK_TIME_COMPRESSION_MIN_EVENTS",
    "extraction.time_compression_max_raw_events_per_node": "MATRIXARK_TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE",
    "extraction.entity_merge_operator": "MATRIXARK_ENTITY_MERGE_OPERATOR",
    "extraction.enable_llm_merge_operator": "MATRIXARK_ENABLE_LLM_MERGE_OPERATOR",
    "extraction.embed_drainer": "MATRIXARK_EMBED_DRAINER",
    "extraction.embed_drainer_batch": "MATRIXARK_EMBED_DRAINER_BATCH",
    "extraction.embed_drainer_interval_ms": "MATRIXARK_EMBED_DRAINER_INTERVAL_MS",
    "extraction.embed_base_url": "MATRIXARK_EMBED_BASE_URL",
    "extraction.embedding_model": "MATRIXARK_EMBEDDING_MODEL",
    "extraction.require_model_embeddings": "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS",
    "extraction.require_model_summaries": "MATRIXARK_REQUIRE_MODEL_SUMMARIES",
    # ---- [auth] -------------------------------------------------------------
    "auth.require_auth": "MATRIXARK_REQUIRE_AUTH",
    "auth.api_keys_file": "MATRIXARK_API_KEYS_FILE",
    "auth.api_keys_hashed_file": "MATRIXARK_API_KEYS_HASHED_FILE",
    "auth.gateway_forward_api_key": "MATRIXARK_GATEWAY_FORWARD_API_KEY",
    "auth.http_allowed_origin": "MATRIXARK_HTTP_ALLOWED_ORIGIN",
    # ---- [gateway] ----------------------------------------------------------
    "gateway.http_host": "MATRIXARK_HTTP_HOST",
    "gateway.http_port": "MATRIXARK_HTTP_PORT",
    "gateway.http_mode": "MATRIXARK_HTTP_MODE",
    "gateway.gateway_threads": "MATRIXARK_GATEWAY_THREADS",
    "gateway.gateway_backend_timeout_ms": "MATRIXARK_GATEWAY_BACKEND_TIMEOUT_MS",
    "gateway.blob_timeout_s": "MATRIXARK_BLOB_TIMEOUT_S",
    "gateway.datanode_url": "MATRIXARK_DATANODE_URL",
    "gateway.datanode_blob_url": "MATRIXARK_DATANODE_BLOB_URL",
    "gateway.gateway_direct_backend": "MATRIXARK_GATEWAY_DIRECT_BACKEND",
    "gateway.gateway_direct_url": "MATRIXARK_GATEWAY_DIRECT_URL",
    "gateway.gateway_direct_pool": "MATRIXARK_GATEWAY_DIRECT_POOL",
    "gateway.metaserver": "MATRIXARK_TEMPORALSTORE_METASERVER",
    "gateway.namespace": "MATRIXARK_NAMESPACE",
    "gateway.table": "MATRIXARK_TABLE",
    # ---- [limits] -----------------------------------------------------------
    "limits.rl_ingest_rps": "MATRIXARK_RL_INGEST_RPS",
    "limits.rl_ingest_burst": "MATRIXARK_RL_INGEST_BURST",
    "limits.rl_retrieve_rps": "MATRIXARK_RL_RETRIEVE_RPS",
    "limits.rl_retrieve_burst": "MATRIXARK_RL_RETRIEVE_BURST",
    "limits.rl_blob_streams": "MATRIXARK_RL_BLOB_STREAMS",
    "limits.quota_max_body_bytes": "MATRIXARK_QUOTA_MAX_BODY_BYTES",
    "limits.quota_max_batch": "MATRIXARK_QUOTA_MAX_BATCH",
    "limits.quota_max_blob_bytes": "MATRIXARK_QUOTA_MAX_BLOB_BYTES",
    "limits.direct_record_bundle_max_bytes": "MATRIXARK_DIRECT_RECORD_BUNDLE_MAX_BYTES",
    "limits.direct_record_hot_cache_max_records": "MATRIXARK_DIRECT_RECORD_HOT_CACHE_MAX_RECORDS",
    "limits.backend_readiness_timeout_ms": "MATRIXARK_BACKEND_READINESS_TIMEOUT_MS",
    "limits.backend_readiness_backoff_ms": "MATRIXARK_BACKEND_READINESS_BACKOFF_MS",
    "limits.resource_async_default_bytes": "MATRIXARK_RESOURCE_ASYNC_DEFAULT_BYTES",
}


def repo_root() -> Path:
    # tools/matrixark_load_config.py -> repo root is the parent of tools/.
    return Path(__file__).resolve().parent.parent


def resolve_config_path(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).expanduser()
    env_path = os.environ.get("MATRIXARK_CONFIG_FILE", "").strip()
    if env_path:
        return Path(env_path).expanduser()
    return repo_root() / "config" / "temporalstore.toml"


def _fmt_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def _parse_scalar(raw: str) -> Any:
    raw = raw.strip()
    if (raw.startswith('"') and raw.endswith('"')) or (
        raw.startswith("'") and raw.endswith("'")
    ):
        return raw[1:-1]
    low = raw.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


def _minimal_toml(text: str) -> Dict[str, Any]:
    """Tiny TOML reader for the flat ``[section]`` + ``key = value`` grammar this
    config file uses. Used only when neither tomllib (3.11+) nor tomli is
    available (e.g. Python 3.10 without an installed TOML library). Handles
    section headers, scalar values (string / int / float / bool), and both
    full-line and trailing ``#`` comments (outside quoted strings)."""
    doc: Dict[str, Any] = {}
    section: Dict[str, Any] | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            name = stripped[1:-1].strip()
            section = {}
            doc[name] = section
            continue
        if "=" not in stripped or section is None:
            continue
        key, _, rest = stripped.partition("=")
        # strip a trailing comment that is outside any quoted string.
        in_str = False
        quote = ""
        cut = len(rest)
        for i, ch in enumerate(rest):
            if in_str:
                if ch == quote:
                    in_str = False
            elif ch in ('"', "'"):
                in_str = True
                quote = ch
            elif ch == "#":
                cut = i
                break
        section[key.strip()] = _parse_scalar(rest[:cut])
    return doc


def load_toml(path: Path) -> Dict[str, Any]:
    if tomllib is not None:
        with open(path, "rb") as handle:
            return tomllib.load(handle)
    with open(path, "r", encoding="utf-8") as handle:
        return _minimal_toml(handle.read())


def flatten(doc: Dict[str, Any]) -> Iterable[Tuple[str, Any]]:
    """Yield (``section.key``, value) for every scalar leaf under a section."""
    for section, body in doc.items():
        if not isinstance(body, dict):
            # top-level scalar (not part of the section grammar) - skip.
            continue
        for key, value in body.items():
            if isinstance(value, (dict, list)):
                continue
            yield f"{section}.{key}", value


def compute_exports(
    doc: Dict[str, Any], environ: Dict[str, str] | None = None
) -> Tuple[Dict[str, str], list[str], list[str]]:
    """Return (to_export, skipped_env_wins, unmapped_keys).

    ``to_export``   : ENV_VAR -> value  (mapped keys whose env var is NOT set)
    ``skipped``     : ENV_VARs skipped because they were already set (env wins)
    ``unmapped``    : config keys with no ENV_MAP entry (config-file typos etc.)
    """
    environ = os.environ if environ is None else environ
    to_export: Dict[str, str] = {}
    skipped: list[str] = []
    unmapped: list[str] = []
    for dotted, value in flatten(doc):
        env_name = ENV_MAP.get(dotted)
        if env_name is None:
            unmapped.append(dotted)
            continue
        if env_name in environ and environ[env_name] != "":
            skipped.append(env_name)
            continue
        to_export[env_name] = _fmt_scalar(value)
    return to_export, skipped, unmapped


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="path to temporalstore.toml")
    parser.add_argument(
        "--print-exports",
        action="store_true",
        help="print 'export ENV=value' lines for eval in a shell",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the resolved ENV=value exports (and env-wins skips) without applying",
    )
    parser.add_argument(
        "--print-mapping",
        action="store_true",
        help="print the SECTION.key -> ENV_VAR mapping table and exit",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="mutate os.environ in-process (used when imported as a module)",
    )
    args = parser.parse_args(argv)

    if args.print_mapping:
        for dotted in sorted(ENV_MAP):
            print(f"{dotted:60s} -> {ENV_MAP[dotted]}")
        return 0

    path = resolve_config_path(args.config)
    if not path.exists():
        sys.stderr.write(f"[matrixark-config] no config file at {path}; nothing to export\n")
        return 0

    doc = load_toml(path)
    to_export, skipped, unmapped = compute_exports(doc)

    if unmapped:
        sys.stderr.write(
            "[matrixark-config] WARNING: unmapped config keys (ignored): "
            + ", ".join(sorted(unmapped))
            + "\n"
        )

    if args.print_exports:
        for env_name, value in sorted(to_export.items()):
            print(f"export {env_name}={_shell_quote(value)}")
        return 0

    if args.dry_run:
        sys.stderr.write(f"[matrixark-config] config: {path}\n")
        sys.stderr.write(
            f"[matrixark-config] would export {len(to_export)} vars; "
            f"{len(skipped)} skipped (env already set, env wins)\n"
        )
        for env_name, value in sorted(to_export.items()):
            print(f"{env_name}={value}")
        if skipped:
            sys.stderr.write(
                "[matrixark-config] env-wins (not overridden): "
                + ", ".join(sorted(skipped))
                + "\n"
            )
        return 0

    # Default / --apply: mutate the environment in-process.
    for env_name, value in to_export.items():
        os.environ[env_name] = value
    sys.stderr.write(
        f"[matrixark-config] applied {len(to_export)} vars from {path} "
        f"({len(skipped)} kept from environment)\n"
    )
    return 0


def apply_from_file(path: str | os.PathLike[str] | None = None) -> Dict[str, str]:
    """Importable helper: seed os.environ from the config file and return the
    dict of vars that were exported (env-set vars are left untouched)."""
    resolved = resolve_config_path(str(path) if path is not None else None)
    if not resolved.exists():
        return {}
    doc = load_toml(resolved)
    to_export, _skipped, _unmapped = compute_exports(doc)
    for env_name, value in to_export.items():
        os.environ[env_name] = value
    return to_export


if __name__ == "__main__":
    raise SystemExit(main())
