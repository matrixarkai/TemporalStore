#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Local MatrixArk adapter and in-memory serving backend."""

from __future__ import annotations

from contextlib import contextmanager
import base64 as _base64
import copy as _copy
import hashlib as _hashlib
import struct as _struct
import queue as thread_queue
import zlib
from typing import Any

try:
    from tools.matrixark_mcp_core import *
    from tools.matrixark_mcp_core import _mcp_debug_log  # import * skips underscore names
    from tools.matrixark_mcp_core import compact_context_pack_for_serving_flat as compact_context_pack_for_serving
    from tools.matrixark_mcp_serving_records import (
        latest_context_state_key,
        compact_latest_context_state_records,
        context_debug_records_enabled,
        materialize_serving_record_batch,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *
    from matrixark_mcp_core import _mcp_debug_log  # import * skips underscore names
    from matrixark_mcp_core import compact_context_pack_for_serving_flat as compact_context_pack_for_serving
    from matrixark_mcp_serving_records import (
        latest_context_state_key,
        compact_latest_context_state_records,
        context_debug_records_enabled,
        materialize_serving_record_batch,
    )

try:
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_metrics import MatrixArkServiceMetrics

try:
    from tools.matrixark_mcp_session_policy import auto_batch_extract_enabled, session_boundary_commit_requested
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_session_policy import auto_batch_extract_enabled, session_boundary_commit_requested

try:
    from tools.matrixark_mcp_retrieve_pack_builder import (
        dropped_ref_layer_budget,
        memory_layer_pressure_summary,
        selected_ref_layer_budget,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_pack_builder import (
        dropped_ref_layer_budget,
        memory_layer_pressure_summary,
        selected_ref_layer_budget,
    )

try:
    from tools.matrixark_mcp_summary_runtime import async_summary_progress_records
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_summary_runtime import async_summary_progress_records

try:
    from tools.matrixark_mcp_summary_dirty import pending_dirty_node_records
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_summary_dirty import pending_dirty_node_records

try:
    from tools.matrixark_mcp_async_readiness import async_pipeline_retrieval_readiness, latest_async_pipeline_rows
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_async_readiness import async_pipeline_retrieval_readiness, latest_async_pipeline_rows

try:
    from tools.matrixark_mcp_retrieve_request import pre_retrieval_idle_commit_flush
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_request import pre_retrieval_idle_commit_flush

try:
    from tools.matrixark_mcp_retrieve_pre_refresh import (
        auto_extraction_phase_budget_tokens as shared_auto_extraction_phase_budget_tokens,
        auto_memory_layer_budget_tokens as shared_auto_memory_layer_budget_tokens,
        auto_memory_selection_policy_budget_tokens as shared_auto_memory_selection_policy_budget_tokens,
        pre_retrieval_summary_refresh_memory_layer_budget_tokens as shared_pre_retrieval_summary_refresh_memory_layer_budget_tokens,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_pre_refresh import (
        auto_extraction_phase_budget_tokens as shared_auto_extraction_phase_budget_tokens,
        auto_memory_layer_budget_tokens as shared_auto_memory_layer_budget_tokens,
        auto_memory_selection_policy_budget_tokens as shared_auto_memory_selection_policy_budget_tokens,
        pre_retrieval_summary_refresh_memory_layer_budget_tokens as shared_pre_retrieval_summary_refresh_memory_layer_budget_tokens,
    )

try:
    from tools.matrixark_mcp_local_idempotency import build_idempotency_record as _build_idempotency_record
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_local_idempotency import build_idempotency_record as _build_idempotency_record

RETRIEVAL_HOT_RECORD_TYPES = {
    "context_compression_event",
    "context_embedding",
    "context_entity",
    "context_event",
    "context_index",
    "context_segment",
    "context_summary",
    "matrixark_async_pipeline_task",
    "resource_chunk",
    "resource_manifest",
    "skill_registry_update",
    "skill_section",
}

RESOURCE_IMPORT_IGNORE_DIRS = {".git", "node_modules", "target", "build", "dist", ".venv", "__pycache__"}
LOCAL_DURABLE_READ_CACHE_ENABLED = os.environ.get("MATRIXARK_LOCAL_DURABLE_READ_CACHE_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
# Records the tail file may hold before the base is folded back in. Bounds both the
# delta file and the work a load does stitching it onto the base.
# 250 rather than 2000, because the tail is now on the COLD path: a load stitches the delta onto the
# base and compacts the result, and the delta is parsed a line at a time where the base is a single
# bulk decode. Medians over four interleaved rounds of 60 ingest-then-read cycles:
#
#     cap      cold      read     ingest     delta
#     main    77 ms     87 ms    319 ms         0
#      250   119 ms     28 ms     97 ms       152
#     2000   228 ms     26 ms     82 ms     1,386
#
# Past a couple of hundred the cap buys nothing a read or an ingest can feel and charges the cold
# start for it. Raising it trades cold start for base rewrites; lowering it does the reverse.
#
# The cold start IS slower than it was -- about 42 ms here, and it does not go away by shrinking the
# cap further, because most of it is the load-time compaction rather than the delta parse (at a cap
# of 50, where the delta empties, cold still measured ~86 ms against main's ~64). It is paid once
# per process, and one read plus one ingest already returns ~281 ms of it.
#: Store the snapshot as a compressed binary container instead of plain JSON.
#:
#: The snapshot is the largest artifact this module writes and the most repetitive: interning
#: leaves it full of repeated bundle tokens and structure. Measured on 3 x 1.00 MB skills, 12.57 MB
#: of snapshot, zlib level 6 gives 21.08x for 32 ms of decode -- against 4.51 GB projected at 2.35M
#: records, that is the difference between 4.5 GB and 0.21 GB.
#:
#: zlib rather than zstd because `dependencies = []` in pyproject is deliberate: zstd measured
#: 27.53x, better but not enough to earn the project its first runtime dependency.
#: ON. Measured on three 1.00 MB skills: the snapshot goes 12.97 MB -> 0.74 MB (17.55x), 1,909 B
#: a record -> 109 B, for about 32 ms of decode. An existing JSON snapshot still loads, so a store
#: written before this needs no migration -- it is simply rewritten in the container on the next
#: full write.
#:
#: `MATRIXARK_LOCAL_DURABLE_READ_CACHE_COMPRESS=0` returns the JSON form, and reading never depends
#: on the flag: the loader decides by what the bytes say they are, so a store written across a flip
#: reads either way and turning it off again is not a one-way door.
LOCAL_DURABLE_READ_CACHE_COMPRESS = os.environ.get(
    "MATRIXARK_LOCAL_DURABLE_READ_CACHE_COMPRESS", "1"
).strip().lower() not in ("0", "false", "no", "off")
LOCAL_DURABLE_READ_CACHE_COMPRESS_LEVEL = max(
    1, min(9, int(os.environ.get("MATRIXARK_LOCAL_DURABLE_READ_CACHE_COMPRESS_LEVEL", "6")))
)
#: Container prefix. A JSON snapshot always starts with `{`, so this can never be mistaken for one,
#: and the codec byte after it leaves room for another encoding without a second format.
_SNAPSHOT_CONTAINER_MAGIC = b"MASNAP\x01"
_SNAPSHOT_CODEC_ZLIB = b"\x01"
#: A delta block: one codec byte, a four-byte big-endian payload length, then the payload. No
#: delimiter and no newline -- a compressed payload can contain either, and a reader that scans for
#: one would have to unstuff every record to be sure it had not.
_DELTA_BLOCK_CODEC_ZLIB = b"\x01"
#: A block whose payload is a JSON ARRAY rather than one document per line. Newline-delimited
#: records cost one json.loads call PER RECORD, and that is the whole of the difference: on 60,000
#: records the line form decodes in 621 ms, the array form in 318, and one whole document in 442.
#: So the array is faster than either -- at the same size, since the compressor sees the same text.
#:
#: 0x01 still reads. A tail written by the build that first shipped blocks carries it, and dropping
#: it would make those tails undecodable rather than merely slower.
_DELTA_BLOCK_CODEC_ZLIB_ARRAY = b"\x02"
_DELTA_BLOCK_HEADER_BYTES = 5
#: A SEALED shard: the magic, a codec byte, then the whole shard compressed. A JSONL shard always
#: starts with `{`, so a reader can tell the two apart without being told which it has.
#:
#: Only ROTATED shards are sealed, and that is the whole safety argument: a rotated shard is
#: finished -- nothing appends to it again -- while the ACTIVE shard stays plain text, so appending,
#: recovering and reading the log by hand are untouched by this.
#:
#: Measured on the corpus this module is tuned for: once the read snapshot became a container the
#: event log was 94.3% of everything written here, and at the default retention the rotated shards
#: are 74.9% of the log and compress 11.91x.
_SHARD_CONTAINER_MAGIC = b"MASHRD\x01"
_SHARD_CODEC_ZLIB = b"\x01"
#: A shard written as a STREAM of blocks rather than one compressed blob. This is the form the
#: ACTIVE log can take: a sealed shard is finished and compresses whole, but a log is appended to,
#: and a stream of self-describing blocks is appendable where a single deflate stream is not.
_SHARD_CODEC_BLOCKS = b"\x02"
#: Off by default. This is the only change in this family that alters the DURABLE SOURCE OF TRUTH
#: rather than something derived from it, and the failure it guards against is a real one: a process
#: appending plain JSON to a block-framed log corrupts it. The form is taken from the file on disk
#: rather than from this flag (see _log_append_form), so turning it on affects only logs this build
#: creates, and turning it off again leaves every existing log readable.
LOCAL_JSONL_BLOCK_LOG = os.environ.get(
    "MATRIXARK_LOCAL_JSONL_BLOCK_LOG", "0"
).strip().lower() not in ("0", "false", "no", "off")
#: On, and reversible: the reader takes either form, so a store written with this on still reads
#: with it off, and a shard sealed once never needs unsealing.
LOCAL_JSONL_COMPRESS_SEALED = os.environ.get(
    "MATRIXARK_LOCAL_JSONL_COMPRESS_SEALED", "1"
).strip().lower() not in ("0", "false", "no", "off")
#: The BASE in the same blocks as the tail. A new codec byte rather than a new magic: a build from
#: before this raises on an unknown codec, and the loader answers that by re-deriving from the log.
_SNAPSHOT_CODEC_BLOCKS = b"\x02"
#: 256 records to a block. Measured on this corpus, per-block compression reaches 97% of its ceiling
#: by 256 and the decoded transient is one block, so a larger block buys ratio the store will not
#: notice and costs memory the cold read will.
LOCAL_DURABLE_READ_CACHE_BLOCK_RECORDS = max(
    1, int(os.environ.get("MATRIXARK_LOCAL_DURABLE_READ_CACHE_BLOCK_RECORDS", "256"))
)

LOCAL_DURABLE_READ_CACHE_MAX_DELTA = max(
    1, int(os.environ.get("MATRIXARK_LOCAL_DURABLE_READ_CACHE_MAX_DELTA", "250"))
)
# No floor by default. One was added because the fallback rewrote the WHOLE record set as JSON
# whenever the append-only path could not apply, which was almost every append -- so a delay
# between writes was the only thing keeping it bounded.
#
# A write no longer takes that fallback at all: it continues the tail or leaves the snapshot alone.
# Measured over 40 ingests, with the writes interleaved to cancel machine drift:
#
#   no structural fix, no floor      ingest 2.714 s   time inside the writer 5.869 s
#   no structural fix, 250 ms floor  ingest ~2.0 s    time inside the writer ~2.9 s
#   this, no floor                   ingest 0.768 s   time inside the writer 0.146 s
#
# So the delay is no longer buying anything on that path, and it cost correctness: four tests assert
# that a snapshot is refreshed promptly enough for a restart to use it, and a floor made that false
# for a quarter of a second. They pass again at 0.
#
# The reason it cost correctness was WHERE it was checked, not the idea. The floor sat at the top of
# _write_durable_read_cache, so it gated the cheap tail append -- which is what keeps the head's
# signature current after a write -- as well as the O(corpus) rewrite it was aimed at. It now guards
# the rewrite alone, and those four tests pass with a floor set.
#
# The default stays 0, because turning it on is a real trade rather than a free win. Measured over
# 12 queries interleaved with ingest, 128 documents, both orderings:
#
#   floor 0      12 of 12 base rewrites   median 2,352 / 2,713 ms   cold load 1,391 / 1,368 ms
#   floor 5000    0 of 12 base rewrites   median 1,922 / 2,037 ms   cold load 1,507 / 1,694 ms
#
# -21.8% on every query against roughly +16% on a cold start. Worth it for most deployments, since
# queries are frequent and restarts are not -- but that is an operator's call, not a default.
LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS = max(0.0, float(os.environ.get("MATRIXARK_LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS", "0")))


def _encode_delta_block(records: list[Json]) -> bytes:
    """One appended batch as one self-describing block, its payload a JSON array."""
    payload = zlib.compress(
        json.dumps(records, separators=(",", ":")).encode("utf-8"),
        LOCAL_DURABLE_READ_CACHE_COMPRESS_LEVEL,
    )
    return _DELTA_BLOCK_CODEC_ZLIB_ARRAY + len(payload).to_bytes(4, "big") + payload


def _encode_log_block(records: list[Json]) -> bytes:
    """One append batch as one block, its payload one document per line.

    Lines rather than the array the snapshot blocks use, and the reason is this file rather than
    that one. The log is parsed line-by-line today, so a line payload is neutral where an array
    would be a second change measured separately; and two of the five shard readers scan for a
    single record type with a substring test, which a line payload keeps.
    """
    payload = zlib.compress(
        b"\n".join(json.dumps(record, separators=(",", ":")).encode("utf-8")
                     for record in records),
        LOCAL_DURABLE_READ_CACHE_COMPRESS_LEVEL,
    )
    return _DELTA_BLOCK_CODEC_ZLIB + len(payload).to_bytes(4, "big") + payload


def _iter_block_stream_lines(handle):
    """Every line of a block-stream shard, one block decompressed at a time."""
    while True:
        header = handle.read(_DELTA_BLOCK_HEADER_BYTES)
        if len(header) < _DELTA_BLOCK_HEADER_BYTES:
            return
        codec = header[:1]
        length = int.from_bytes(header[1:], "big")
        body = handle.read(length)
        if len(body) < length or codec not in (_DELTA_BLOCK_CODEC_ZLIB,
                                               _DELTA_BLOCK_CODEC_ZLIB_ARRAY):
            # A torn final block, or one this build does not know. Both mean the same thing here:
            # stop, and let the caller work with what came before it -- a half-written append is
            # exactly what a crash mid-write leaves, and dropping it is what the plain form does
            # with a half-written line.
            return
        decoded = zlib.decompress(body)
        if codec == _DELTA_BLOCK_CODEC_ZLIB_ARRAY:
            for record in json.loads(decoded):
                yield json.dumps(record, separators=(",", ":"))
            continue
        for line in decoded.split(b"\n"):
            if line.strip():
                yield line.decode("utf-8")


def _iter_delta_blocks(raw: bytes):
    """One block's records at a time, in the order they were written.

    The flat decoder below is a wrapper over this. The base needs the streaming form -- holding
    every record AND the bytes they came from is the transient this format exists to bound -- and
    two decoders for one format is how a format decision gets taught to only one of its readers.

    A truncated final block is DROPPED rather than raising: a snapshot is derived state, the head
    records how many records it should hold, and a short read is caught by the count check that
    already guards it. Raising would turn a torn write into an unreadable store when re-deriving
    from the log is the answer.
    """
    at = 0
    while at + _DELTA_BLOCK_HEADER_BYTES <= len(raw):
        codec = raw[at:at + 1]
        length = int.from_bytes(raw[at + 1:at + _DELTA_BLOCK_HEADER_BYTES], "big")
        start = at + _DELTA_BLOCK_HEADER_BYTES
        if (codec not in (_DELTA_BLOCK_CODEC_ZLIB, _DELTA_BLOCK_CODEC_ZLIB_ARRAY)
                or start + length > len(raw)):
            return
        body = zlib.decompress(raw[start:start + length])
        if codec == _DELTA_BLOCK_CODEC_ZLIB_ARRAY:
            yield loads_with_interned_keys(body)
        else:
            # The line form, kept readable for tails written before the array one.
            yield [loads_with_interned_keys(line)
                   for line in body.split(b"\n") if line.strip()]
        at = start + length


def _iter_snapshot_blocks(payload: Json):
    """The snapshot as a header block followed by blocks of records, one piece at a time.

    The header is everything the payload carries EXCEPT the records -- schema version, counts, the
    cache key -- and it goes in a block of its own so the reader can have it before it has spent
    memory on anything else.

    A generator rather than a list, so the writer below can drain it to the file handle. Joining
    first meant holding every compressed block at once: small on this corpus, 0.26 GB at a thousand
    1 MB skills, which is the same shape of transient this format exists to remove.
    """
    records = payload.get("records") or []
    header = {key: value for key, value in payload.items() if key != "records"}
    yield _SNAPSHOT_CONTAINER_MAGIC + _SNAPSHOT_CODEC_BLOCKS
    yield _encode_delta_block([header])
    size = LOCAL_DURABLE_READ_CACHE_BLOCK_RECORDS
    for start in range(0, len(records), size):
        yield _encode_delta_block(records[start:start + size])


def _write_blocked_snapshot(path: Path, payload: Json) -> None:
    """Drain the blocks to `path`. The caller renames it into place, so this need not be atomic."""
    with path.open("wb") as handle:
        for chunk in _iter_snapshot_blocks(payload):
            handle.write(chunk)


def _encode_blocked_snapshot(payload: Json) -> bytes:
    """The same bytes in one object. For callers that want the whole thing -- tests, mostly."""
    return b"".join(_iter_snapshot_blocks(payload))


def _decode_blocked_snapshot(body: bytes) -> Json:
    """The inverse, holding one block at a time rather than the whole document."""
    blocks = _iter_delta_blocks(body)
    first = next(blocks, None)
    if not first:
        raise ValueError("a blocked snapshot with no header block")
    payload = dict(first[0])
    records: list[Json] = []
    for block in blocks:
        records.extend(block)
    payload["records"] = records
    return payload


def _decode_delta_blocks(raw: bytes) -> list[Json]:
    """Every record in a block-framed tail, in the order it was appended.

    A truncated final block is DROPPED rather than raising: a tail is derived state, and the head
    records how many records it should hold, so a short read is caught by the count check that
    already guards the plain form. Raising here would turn a torn append into an unreadable
    snapshot when re-deriving from the log is the answer.
    """
    return [record for block in _iter_delta_blocks(raw) for record in block]


def _iter_shard_lines(path: Path):
    """Every line of a shard, whichever form it is stored in.

    The PLAIN shard keeps the buffered text iterator it always had. Reading it as bytes and decoding
    each line in Python instead cost +67% on a cold read (72.8 -> 121.7 ms median) -- a cost that
    would have been charged to compression by anyone measuring the two together. The plain shard is
    the ACTIVE one, so that is the common case, and the seven-byte sniff is one extra syscall.

    A sealed shard is decompressed in chunks rather than whole. A shard runs to
    MATRIXARK_LOCAL_JSONL_MAX_BYTES -- 64 MB by default -- and four of this module's five readers
    scan it for one record type, so materialising the whole thing would trade the bytes this saves
    on disk for the same bytes in memory.
    """
    with path.open("rb") as probe:
        head = probe.read(len(_SHARD_CONTAINER_MAGIC))
    if head != _SHARD_CONTAINER_MAGIC:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                yield line
        return
    with path.open("rb") as handle:
        handle.seek(len(_SHARD_CONTAINER_MAGIC))
        codec = handle.read(1)
        if codec == _SHARD_CODEC_BLOCKS:
            for line in _iter_block_stream_lines(handle):
                yield line
            return
        if codec != _SHARD_CODEC_ZLIB:
            raise ValueError("unknown sealed-shard codec %r" % codec)
        decompressor = zlib.decompressobj()
        pending = b""
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                break
            pending += decompressor.decompress(chunk)
            parts = pending.split(b"\n")
            pending = parts.pop()
            for part in parts:
                yield part.decode("utf-8")
        pending += decompressor.flush()
        for part in pending.split(b"\n"):
            if part:
                yield part.decode("utf-8")


def _decode_snapshot_bytes(raw: bytes) -> Json:
    """Turn snapshot bytes into the payload, whatever wrote them.

    Sniffs the container prefix, so a plain-JSON snapshot written by any earlier build keeps
    loading unchanged and a container written by a newer one is refused with a clear error rather
    than mis-parsed. Every decode goes through here.

    The interning hook is applied either way: it is worth 24.5% of the loaded cache, and losing it
    on the compressed path would trade durable bytes for resident ones.
    """
    if raw.startswith(_SNAPSHOT_CONTAINER_MAGIC):
        body = raw[len(_SNAPSHOT_CONTAINER_MAGIC):]
        codec, payload = body[:1], body[1:]
        if codec == _SNAPSHOT_CODEC_BLOCKS:
            return _decode_blocked_snapshot(payload)
        if codec != _SNAPSHOT_CODEC_ZLIB:
            raise ValueError(f"unknown snapshot codec {codec!r}")
        raw = zlib.decompress(payload)
    return json.loads(raw.decode("utf-8"), object_pairs_hook=_interned_pairs)


def _snapshot_prefix_fingerprint(record: "Json") -> str:
    """Fingerprint one record, so a snapshot can prove on disk which prefix it already holds.

    The head names the last record it persisted. A caller holding a longer list can then check
    that its own record at that position is the same one, which establishes that the file is a
    prefix of the list in hand -- something per-instance bookkeeping cannot establish for a list
    the instance did not build.
    """
    try:
        return _hashlib.sha256(
            json.dumps(record, separators=(",", ":"), sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:32]
    except (TypeError, ValueError):
        return ""
# Version 2 stores the base snapshot in the same interned form the durable log uses, so the two
# files no longer disagree about how the same records are written. The bump is what protects an
# older reader: it would otherwise serve tokenised records with the interned fields missing, so a
# version it does not recognise has to send it back to the log, which it already does.
LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION = 2
PRE_RETRIEVAL_SUMMARY_REFRESH = os.environ.get("MATRIXARK_PRE_RETRIEVAL_SUMMARY_REFRESH", "0").strip().lower() in {"1", "true", "yes"}

QUALITY_FIRST_UNDERFILL_DROP_KEYS = {
    "cross_session_budget",
    "cross_session_session_cap",
    "cross_session_candidate_cap",
    "low_score",
    "memory_layer_budget",
    "memory_layer_floor",
    "memory_selection_policy_budget",
    "shared_resource_budget",
    "shared_skill_budget",
    "source_role_budget",
    "stale",
}


def quality_first_underfill_summary(
    *,
    budget_fill_policy: str,
    selected_ref_count: int,
    used_context_tokens: int,
    remote_context_budget_tokens: int,
    dropped_over_budget: Json,
) -> Json:
    if str(budget_fill_policy or "").strip().lower() != "quality_first":
        return {"enabled": False}
    if selected_ref_count <= 0:
        return {"enabled": False}
    unused_tokens = max(0, int(remote_context_budget_tokens or 0) - int(used_context_tokens or 0))
    if unused_tokens <= 0:
        return {"enabled": False}
    dropped_reason_counts: Json = {}
    for key in sorted(QUALITY_FIRST_UNDERFILL_DROP_KEYS):
        try:
            count = int(dropped_over_budget.get(key) or 0)
        except (AttributeError, TypeError, ValueError):
            count = 0
        if count > 0:
            dropped_reason_counts[key] = count
    dropped_ref_count = sum(int(count or 0) for count in dropped_reason_counts.values())
    if dropped_ref_count <= 0:
        return {"enabled": False}
    return {
        "enabled": True,
        "policy": "quality_first",
        "unused_remote_context_tokens": unused_tokens,
        "dropped_ref_count": dropped_ref_count,
        "dropped_reason_counts": dropped_reason_counts,
        "warning": f"quality_first_budget_underfill:unused_tokens={unused_tokens},dropped_refs={dropped_ref_count}",
    }


def retrieval_memory_inventory(records: list[Json], retrieval_scope: Json) -> Json:
    """Summarize memory models available after retrieval scope filtering.

    This is serving-facing, not debug lineage: it helps a client distinguish
    "no profile memory exists" from "profile memory exists but was not selected
    under the current query/budget."
    """

    inventory: Json = {
        "session": {
            "context_events": 0,
            "context_segments": 0,
            "context_entities": 0,
            "context_embeddings": 0,
            "context_indexes": 0,
            "context_summaries": 0,
            "summary_dirty_markers": 0,
        },
        "profile": {
            "context_entities": 0,
            "context_embeddings": 0,
            "context_indexes": 0,
            "context_summaries": 0,
            "summary_dirty_markers": 0,
        },
        "shared": {
            "resource_chunks": 0,
            "resource_manifests": 0,
            "skill_sections": 0,
            "skill_manifests": 0,
            "context_entities": 0,
            "context_embeddings": 0,
            "context_indexes": 0,
        },
        "available_layers": [],
        "query_scope": {
            "session_scope": session_scope_mode(retrieval_scope),
            "has_session_id": bool(str(retrieval_scope.get("session_id") or "").strip()),
            "has_user_id": bool(str(retrieval_scope.get("user_id") or "").strip()),
            "has_tenant_id": bool(str(retrieval_scope.get("tenant_id") or "").strip()),
        },
    }

    def count(layer: str, field: str, amount: int = 1) -> None:
        bucket = inventory.setdefault(layer, {})
        bucket[field] = int(bucket.get(field) or 0) + amount

    for record in records:
        if not isinstance(record, dict):
            continue
        record_type = str(record.get("record_type") or "")
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        memory_scope = str(record.get("memory_scope") or metadata.get("memory_scope") or "").strip().lower()
        session_continuity = str(
            record.get("session_continuity") or metadata.get("session_continuity") or ""
        ).strip().lower()
        data_model = str(record.get("data_model") or metadata.get("data_model") or "").strip().lower()
        access_scope = candidate_access_scope(record)
        sharing_scope = str(access_scope.get("sharing_scope") or record.get("sharing_scope") or "").strip().lower()
        is_shared = (
            sharing_scope in {"tenant_shared", "global_shared"}
            or record_type in {"resource_chunk", "resource_manifest", "skill_section", "skill_manifest", "skill_registry_update"}
            or data_model in {"resource_chunk", "skill_section"}
        )
        is_profile = (
            memory_scope in {"user_profile", "profile", "cross_session_profile"}
            or data_model == "context_profile_entity"
            or (
                record_type in {"context_entity", "context_embedding", "context_summary", "context_summary_dirty"}
                and session_continuity == "cross_session"
            )
        )
        # An index posting carries neither a memory scope nor a session continuity -- it is a
        # derived row pointing at one -- so on those two tests alone it belonged to no layer and
        # was skipped: twenty postings on a log, context_indexes 0 in all three layers. The
        # profile clause above already reads data_model for exactly this reason; the session
        # layer needs the same, for the models the session's own postings are attributed to.
        is_session = (
            memory_scope in {"session", "session_memory"}
            or session_continuity == "same_session"
            or data_model in {"context_event", "context_batch_commit", "context_segment"}
        )

        if is_shared:
            # A folded owner counts as the embedding it carries -- the separate record it
            # replaced would have landed in this same layer.
            if record_type != "context_embedding" and (
                record.get("vector") or record.get("embedding_meta")
            ):
                count("shared", "context_embeddings")
            if record_type == "resource_chunk":
                count("shared", "resource_chunks")
            elif record_type == "resource_manifest":
                count("shared", "resource_manifests")
            elif record_type == "skill_section":
                count("shared", "skill_sections")
            elif record_type in {"skill_manifest", "skill_registry_update"}:
                count("shared", "skill_manifests")
            elif record_type == "context_entity":
                count("shared", "context_entities")
            elif record_type == "context_embedding":
                count("shared", "context_embeddings")
            elif record_type == "context_index":
                count("shared", "context_indexes")
            continue

        if is_profile:
            if record_type != "context_embedding" and (
                record.get("vector") or record.get("embedding_meta")
            ):
                count("profile", "context_embeddings")
            if record_type == "context_entity":
                count("profile", "context_entities")
            elif record_type == "context_embedding":
                count("profile", "context_embeddings")
            elif record_type == "context_index":
                count("profile", "context_indexes")
            elif record_type == "context_summary":
                count("profile", "context_summaries")
            elif record_type == "context_summary_dirty":
                count("profile", "summary_dirty_markers")
            continue

        if is_session or record_type in {"context_event", "context_segment"}:
            if record_type != "context_embedding" and (
                record.get("vector") or record.get("embedding_meta")
            ):
                count("session", "context_embeddings")
            if record_type == "context_event":
                count("session", "context_events")
            elif record_type == "context_segment":
                count("session", "context_segments")
            elif record_type == "context_entity":
                count("session", "context_entities")
            elif record_type == "context_embedding":
                count("session", "context_embeddings")
            elif record_type == "context_index":
                count("session", "context_indexes")
            elif record_type == "context_summary":
                count("session", "context_summaries")
            elif record_type == "context_summary_dirty":
                count("session", "summary_dirty_markers")

    availability = {
        "session": any(int(value or 0) > 0 for value in inventory["session"].values()),
        "profile": any(int(value or 0) > 0 for value in inventory["profile"].values()),
        "shared": any(int(value or 0) > 0 for value in inventory["shared"].values()),
    }
    inventory["available_layers"] = [layer for layer, available in availability.items() if available]
    inventory["has_session_memory"] = availability["session"]
    inventory["has_profile_memory"] = availability["profile"]
    inventory["has_shared_memory"] = availability["shared"]
    inventory["profile_records_available_but_not_selected"] = False
    return inventory


def positive_int_value(value: Any, default: int) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return max(1, int(default))


def positive_int_env(name: str, default: int) -> int:
    return positive_int_value(os.environ.get(name, str(default)), default)


def bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT = positive_int_env("MATRIXARK_PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT", 2)
LOCAL_JSONL_ENABLED = bool_env("MATRIXARK_LOCAL_JSONL_ENABLED", True)
LOCAL_JSONL_INCLUDE_BULKY_FIELDS = bool_env("MATRIXARK_LOCAL_JSONL_INCLUDE_BULKY_FIELDS", False)
LOCAL_JSONL_MAX_BYTES = positive_int_env("MATRIXARK_LOCAL_JSONL_MAX_BYTES", 64 * 1024 * 1024)
LOCAL_JSONL_RETENTION_COUNT = positive_int_env("MATRIXARK_LOCAL_JSONL_RETENTION_COUNT", 4)
LOCAL_JSONL_RETENTION_AGE_MS = positive_int_env("MATRIXARK_LOCAL_JSONL_RETENTION_AGE_MS", 7 * 24 * 60 * 60 * 1000)


def _memory_purge_threshold() -> int:
    """Tombstone count that auto-triggers a physical purge after delete/forget. 0 (default) = off."""
    try:
        return max(0, int(os.environ.get("MATRIXARK_MEMORY_PURGE_THRESHOLD", "0")))
    except (TypeError, ValueError):
        return 0


MEMORY_PURGE_THRESHOLD = _memory_purge_threshold()
LOCAL_JSONL_BULKY_FIELDS = {
    "agent_debug",
    "debug",
    "debug_payload",
    "full_tool_output",
    "internal_extraction",
    "raw",
    "raw_hook_payload",
    "raw_payload",
    "raw_request",
    "raw_response",
    "replay_payload",
    "tool_payload",
    "tool_result",
    "tool_stdout",
    "tool_stderr",
    "transcript",
}
PROFILE_PROMOTION_POLICY_ALWAYS = "always_when_profile_scope_available"
PROFILE_PROMOTION_SCOPE_MISSING_BLOCKER = "profile_scope_missing"

# ------------------------------------------------------------------------------------------------
# Record-metadata interning codec (write-side compress / read-side expand).
#
# The backend routing/placement config (``storage_route``, ``storage_options``, ``placement_key``,
# ``placement_hash``, ``posting_policy``) is near-constant per store yet re-stamped on every record;
# measured at ~61% of on-disk memory across a 50-turn workload with only a handful of DISTINCT values
# each. The codec replaces each per-record value with a short content-hash token and writes the full
# value ONCE as a durable ``matrixark_intern_dict`` sidecar record in the same log. Every read choke
# point re-expands the tokens so downstream consumers see byte-identical, fully-expanded records --
# this is purely a storage-representation change.
#
# Design properties:
#   * Crash-safe: a value's dict record is emitted in the SAME append batch, on lines that PRECEDE the
#     first data record referencing it (sequential writes under the event-log lock), so a persisted
#     data record's token table is always already persisted. Duplicate dict records (across process
#     restarts) are harmless -- they map the same token to the same value (content-addressed).
#   * Reload-safe: a fresh adapter rebuilds the token->value map purely from the durable log.
#   * Multi-writer-safe: tokens are content hashes, so two writers never assign the same token to
#     different values (no sequential-counter collision).
#   * Backward-compatible: an old log (inline fields, no dict records / no token key) expands as a
#     no-op. Expansion ALWAYS runs regardless of the flag; only WRITE-side interning is gated, so a
#     log written while the flag was ON still reads correctly if the flag is later turned OFF.
#   * Flag OFF => byte-identical to today (no dict records, no token key emitted).
INTERN_RECORD_METADATA = bool_env("MATRIXARK_INTERN_RECORD_METADATA", True)
INTERN_METADATA_FIELDS = (
    "storage_route",
    "storage_options",
    "placement_key",
    "placement_hash",
    "posting_policy",
    # NB: scope_key is deliberately NOT interned. It is load-bearing for the RAW-log rewrite paths --
    # scope-level forget / reset apply their tombstones to unexpanded records in purge_tombstones(),
    # matching by scope_key. Eliding it there makes scope-level tombstones miss, so tombstoned records
    # survive a purge. Interning it transparently would require expanding at every raw rewrite path;
    # not worth the ~4% for the correctness risk. The routing/placement fields above are never
    # tombstone-matched, so interning them (incl. via the bundle format) is safe.
    #
    # Everything below repeats a value the record's own type already implies. A census of the log
    # found 30.4% of its bytes in fields holding one value across every record of their type, on a
    # corpus deliberately varied across 4 tenants, 7 users and 11 sessions so uniformity could not
    # manufacture the result. Interning was already ON and not reaching them: it reads this list,
    # and this list held five routing fields.
    #
    # Measured over this repo's own markdown, with read-back asserted identical each time:
    #   uniform corpus              231.3 KB -> 173.2 KB   -25.1%   5 -> 8 sidecars
    #   varied roles and tenants    263.9 KB -> 206.1 KB   -21.9%   5 -> 14 sidecars
    #
    # The sidecar count is the thing to watch rather than the byte count: fields go into ONE shared
    # bundle, so a field that varies makes every record's token vary and multiplies the sidecars
    # instead of removing bytes. It degrades rather than breaks -- 14 sidecars against 208 records
    # -- but a field known to vary per record does not belong here.
    #
    # Excluded, beyond scope_key: record_type (every raw filter matches it), the model registry's
    # identity fields (model_kind, model_ref, model_name, model_hash, provider, execution_mode --
    # _seed_model_registry_seen_locked reads them off the unexpanded log), and the bundle's own
    # token key, which cannot be part of what it names.
    # Measured on a corpus of 1 MB documents: `access_scope` is 9.6% of the durable log and
    # `deployment_scope` a further 1.2%, and BOTH carry exactly one distinct value across the
    # whole store -- 5,940 rows, one value each. They are the two largest constants on the wire.
    #
    # They are safe here on the criterion this list already uses. The exclusions above are fields
    # some path reads off the UNEXPANDED log, where an interned value is invisible. Every raw
    # reader was checked: of the six functions that iterate log lines, three expand first
    # (`_load_durable_read_cache`, `_read_all_compacted`, `_read_raw_records`) and the three that
    # do not -- `_seed_intern_tokens_locked`, `_seed_model_registry_seen_locked` and
    # `purge_tombstones` -- read only record_type, scope_key and the model identity fields, which
    # is exactly why those are excluded and these are not.
    #
    # They also cost nothing in sidecars, which is the number to watch: fields share ONE bundle,
    # so a field that varies multiplies the tokens instead of removing bytes. At one distinct
    # value each these add no combinations at all -- the bundle count stays where it was.
    "access_scope",
    "deployment_scope",
    "storage_record_kind",
    "storage_part",
    "dirty_reason",
    "source_ref_type",
    "ref_type",
    "entity_type",
    "event_type",
    "context_event_parent_type",
    "summary_type",
    "summary_identity",
    "status",
    "source_kind",
    "agent_hook",
    "extraction_phase",
    "memory_scope",
    "session_continuity",
    "profile_memory_class",
    "profile_memory_kind",
    # roll-ups a batch writes onto every record it produced
    "source_codex_event_counts",
    "source_codex_events",
    "source_extraction_phases",
    "source_final_session_boundary_count",
    "source_hook_type_counts",
    "source_hook_types",
    "source_memory_layer_counts",
    "source_memory_layers",
    "source_memory_scopes",
    "source_memory_selection_complete_count",
    "source_memory_selection_dropped_line_count",
    "source_memory_selection_dropped_text_chars",
    "source_memory_selection_lossy_count",
    "source_memory_selection_retained_line_ratio_avg",
    "source_memory_selection_retained_text_ratio_avg",
    "source_profile_memory_classes",
    "source_profile_memory_kinds",
    "source_profile_promotion_blockers",
    "source_profile_promotion_policies",
    "source_role_counts",
    "source_roles",
    "source_session_continuities",
)
INTERN_DICT_RECORD_TYPE = "matrixark_intern_dict"
INTERN_TOKEN_KEY = "_im"  # legacy per-record map {field_name: token} (Phase-1 format)

# Bundle interning. The per-field ``_im`` map repeated the field NAMES on every interned record
# (measured ~8% of on-disk memory just for the token map). Because the whole eligible-field bundle
# is near-constant per store, the WHOLE {field: value} bundle hashes to a single token and only
# that token (``_imb``) rides on the data line; the sidecar dict stores the bundle once per distinct
# token.
#
# The per-field format is still READ -- an old log expands unchanged, which is what backward
# compatibility requires. It is no longer WRITTEN. The switch that fell back to it kept a second
# durable encoding alive on the write side, and nothing chose it: no test set it, the portal never
# offered it, and the format it produced is one this reader already understands.
INTERN_BUNDLE_TOKEN_KEY = "_imb"  # bundle token -> sidecar {im_token, im_bundle: {field: value}}
INTERN_BUNDLE_EMIT_KEY = "__bundle__"  # emitted-token namespace for bundle sidecars

# Phase-2 -- model-registry dedup. context_model_registry rows are pure model-metadata REFERENCE
# records (model_ref/model_name/model_hash/provider/execution_mode) re-emitted on every serving batch
# that carries a context_embedding, yet only a handful of distinct models exist per store. The read
# path already latest-state-compacts them (compact_latest_context_state_records), so the duplicates are
# pure durable-log bloat. At most one registry record is appended per distinct semantic identity
# (timestamp excluded); a genuine change to any model field is a new identity and is still recorded.
# Serving/retrieval that reads model info still resolves it (>=1 record per model survives).
#
# This was gated behind MATRIXARK_DEDUP_MODEL_REGISTRY during its rollout. The gate is gone: its OFF
# path was the superseded behavior -- re-emitting every batch -- and keeping a switch for it only
# preserved the option of choosing the worse one.

# Phase-2 -- coalesce transient summary-dirty markers. A context_summary_dirty (status="pending")
# marker means "this node's summary needs regeneration". One is emitted per (node prefix) on EVERY
# event, so between refreshes a hot node accumulates many pending markers -- ~5% of on-disk memory --
# though the refresh reconciliation only ever acts on the LATEST uncompleted pending marker per node
# (it regenerates the node summary from all current events regardless of which marker triggered it) and
# resolves a marker by matching a status="completed" marker on the same dirty_hash. With
# coalescing ON we keep at most ONE outstanding (uncompleted) pending marker per
# (scope, node): a new pending marker is skipped while an uncompleted one is already durable, so the
# node stays flagged for regen. CRASH-SAFE -- the one outstanding marker is durable, so a crash before
# regen still triggers the refresh on recovery; once the summary regenerates (completion marker with
# that dirty_hash) the next event re-marks the node afresh. Completion/refreshed markers are never
# dropped.
#
# This was gated behind MATRIXARK_COALESCE_SUMMARY_DIRTY during its rollout. The gate is gone: its
# OFF path was the superseded behavior -- a marker per event, measured at ~5% of on-disk memory --
# and a switch whose only setting anyone would choose is ON is not a switch.


def _canonical_scope_key_of(record: Json) -> str:
    existing = record.get("scope_key")
    if existing:
        return str(existing)
    scope = record.get("scope")
    if isinstance(scope, dict):
        try:
            return str(canonical_scope_key(scope))
        except Exception:
            return ""
    return ""


def _model_registry_identity(record: Json) -> tuple[Any, ...]:
    return (
        str(record.get("model_kind") or ""),
        str(record.get("model_ref") or ""),
        str(record.get("model_name") or ""),
        record.get("model_hash"),
        str(record.get("provider") or ""),
        str(record.get("execution_mode") or ""),
    )


def _intern_token_for_bundle(bundle: dict[str, Any]) -> str:
    canonical = json.dumps(bundle, sort_keys=True, separators=(",", ":"))
    return _hashlib.blake2b(canonical.encode("utf-8"), digest_size=6).hexdigest()


def encode_interned_records(records: list[Json], emitted_tokens: set[tuple[str, str]]) -> list[Json]:
    """Compress the interned metadata fields on ``records`` for the durable log.

    Returns new ``matrixark_intern_dict`` sidecar records (for any token first seen this call) FOLLOWED
    by the encoded data records; the dict records are emitted first so they precede -- and are durably
    written before -- any data record that references their token. ``emitted_tokens`` tracks the
    ``(field, token)`` pairs already written by this adapter instance and is mutated in place. When the
    flag is OFF the input is returned unchanged (byte-identical to the pre-codec behaviour).
    """
    if not INTERN_RECORD_METADATA:
        return list(records)
    dict_records: list[Json] = []
    encoded_records: list[Json] = []
    for record in records:
        if not isinstance(record, dict) or str(record.get("record_type") or "") == INTERN_DICT_RECORD_TYPE:
            encoded_records.append(record)
            continue
        present = {field: record[field] for field in INTERN_METADATA_FIELDS if field in record}
        if not present:
            encoded_records.append(record)
            continue
        token = _intern_token_for_bundle(present)
        key = (INTERN_BUNDLE_EMIT_KEY, token)
        if key not in emitted_tokens:
            emitted_tokens.add(key)
            dict_records.append({
                "record_type": INTERN_DICT_RECORD_TYPE,
                "im_token": token,
                "im_bundle": present,
            })
        encoded = dict(record)
        for field in present:
            encoded.pop(field, None)
        encoded[INTERN_BUNDLE_TOKEN_KEY] = token
        encoded_records.append(encoded)
    return dict_records + encoded_records


class _SharedInternedList(list):
    """The list counterpart of :class:`_SharedInternedValue`.

    Sharing a list was left out when values were first shared, on the grounds that making it safe
    meant turning it into a tuple, which changes the type a caller sees and breaks anything doing
    ``.append``. A list subclass keeps the type -- ``isinstance``, indexing, iteration and JSON
    encoding all behave -- and refuses the mutation instead, so a path that does append fails
    where it happens rather than silently rewriting records it never looked at.

    Worth 11.8% of a cold read, of which `node_path` alone is 3.6%: 147 objects for THREE distinct
    values.
    """

    __slots__ = ()

    def _refuse(self, *_args, **_kwargs):
        raise TypeError(
            "this list is shared by every record carrying it, so changing it here would change "
            "them all -- copy it first: list(record['node_path'])")

    __setitem__ = _refuse
    __delitem__ = _refuse
    append = _refuse
    extend = _refuse
    insert = _refuse
    pop = _refuse
    remove = _refuse
    clear = _refuse
    sort = _refuse
    reverse = _refuse
    __iadd__ = _refuse
    __imul__ = _refuse

    def __copy__(self):
        return list(self)

    def __deepcopy__(self, memo):
        copied = [_copy.deepcopy(v, memo) for v in self]
        memo[id(self)] = copied
        return copied

    def __reduce__(self):
        return (list, (list(self),))


class _SharedInternedValue(dict):
    """One object, shared by every record that carries this interned value.

    Sharing is the whole saving. Measured over 331 expanded records, storing each distinct value
    once costs 852 KB of real memory against 2,386 KB for a copy per record -- 64% less. Serialised
    size understates this badly: the same records are 787 KB as JSON, so the copies cost about
    three times what the bytes suggest.

    It is only safe while nothing mutates one, because a mutation would reach every record sharing
    it. The expansion previously deep-copied for exactly that reason, but a tripwire that recorded
    every in-place change to an expanded value, run over the whole test suite, found no production
    code that mutates one.

    That is evidence, not proof, so this refuses rather than trusting it. A path that does mutate
    fails where it happens, instead of silently rewriting records it never looked at.
    """

    __slots__ = ()

    def _refuse(self, *_args, **_kwargs):
        raise TypeError(
            "this value is shared by every record carrying it, so changing it here would change "
            "them all -- copy it first: dict(record['storage_route'])")

    __setitem__ = _refuse
    __delitem__ = _refuse
    update = _refuse
    setdefault = _refuse
    pop = _refuse
    popitem = _refuse
    clear = _refuse

    # Taking a copy is exactly what a caller who needs to change one should do, so it has to work.
    # copy.deepcopy rebuilds a dict subclass by assigning into a new instance of the same class,
    # which lands on the refusal above -- the suite found this on a path that deep-copies a whole
    # record and never touches the value itself. Both copies hand back a plain, writable dict.
    def __copy__(self):
        return dict(self)

    def __deepcopy__(self, memo):
        copied = {_copy.deepcopy(k, memo): _copy.deepcopy(v, memo) for k, v in self.items()}
        memo[id(self)] = copied
        return copied

    def __reduce__(self):
        return (dict, (dict(self),))


def _shared_interned_value(value: Any) -> Any:
    """Wrap an interned value so every record can hold the same object.

    Only dicts are shared. A list would have to become a tuple to be safe to share, and that
    changes the type a caller sees -- anything doing .append on it would break, for a field that
    holds little of the memory. Lists keep the copy they had.
    """
    if type(value) is dict:
        return _SharedInternedValue(value)
    return _copy_interned_value(value)


SHARE_REPEATED_VALUES = bool_env("MATRIXARK_SHARE_REPEATED_VALUES", True)

#: A last-resort ceiling on the shared table, not a working limit.
#:
#: It used to be 4096, chosen when "the busiest field held 11 distinct values" on a 60-attachment
#: corpus -- a runaway guard that could not plausibly bind. At 100,105 records it binds hard: the
#: corpus holds 29,657 distinct ``vector`` values, so the table filled and every value after the
#: 4,096th was handed back unshared. The measured cost of that one constant was 66.8 MB of
#: duplicate vectors, 20.9% of a 320 MB cache, and nothing reported it -- the table simply stopped
#: sharing.
#:
#: What keeps the table honest now is :data:`_CONTAINER_FIELD_MIN_HIT_RATE` below, which drops a
#: field that is not actually repeating. A field still in the table is saving more than its
#: entries cost, so growth here is self-justifying and the ceiling only has to stop something
#: nobody foresaw.
#:
#: Worth knowing before raising it further: the table holds strong entries and never shrinks, so
#: it pins its shared values for the life of the process even after the read cache that needed
#: them is dropped. That is bounded by this ceiling -- ~118 MB at the sizes measured here -- and
#: it tracks the process-wide read cache closely enough in practice, but a table that outlives its
#: records is the reason not to treat this number as free.
#:
#: Holding the entries weakly would remove the ceiling entirely, and it is close but not free:
#: both shared types declare ``__slots__ = ()``, which suppresses ``__weakref__``, so they reject
#: a weak reference today and would need the slot declared back before a weak table could hold
#: them. That is 8 bytes on each shared object -- ~250 KB across the 31,549 measured here -- and
#: it is left for whoever needs the ceiling gone rather than folded in here.
_SHARED_VALUE_TABLE_LIMIT = 262144

#: Lookups to watch before judging a field, and the hit rate it has to clear.
#:
#: The string table (:func:`_shared_string`) abandons below 0.10, and containers need a HIGHER bar
#: for a reason worth stating: a string is its own table key, so a hit at any rate is free money,
#: but a container's key is a second copy of its spine -- ``(field, tuple(value))``. Sharing pays
#: when ``hits * value_size > misses * key_size``, i.e. above ``key/(value + key)``. Measured here
#: a shared vector saves ~1,015 B against a ~450 B key, so break-even is ~0.31.
#:
#: The bar is set just above that rather than comfortably above it, because the repetition this
#: exists to catch sits at 0.50 EXACTLY: a chunk body is stored once as a skill_section and once
#: as a resource_chunk, so a perfectly duplicated corpus hits every other lookup and nothing more.
#: A bar of 0.50 would admit that case only by the width of a rounding error and would drop any
#: corpus that is partly unique -- 2x over 70% of its rows and singletons elsewhere hits 0.35 and
#: is still comfortably profitable. So: 0.35, above break-even and below the structural case.
#:
#: Break-even falls as vectors get wider -- the key spine grows with the element count while the
#: floats it points at are shared -- so this bar gets safer at production width, not tighter.
_CONTAINER_FIELD_WARMUP_LOOKUPS = 512
_CONTAINER_FIELD_MIN_HIT_RATE = 0.35

#: Lookups an abandoned field waits before it is judged again.
#:
#: The verdict must not be permanent, because it is made on the FIRST 512 lookups and a corpus can
#: put a field's repeats after them. A store holding one vector per document followed by a second
#: pass of duplicates would show a 0.00 hit rate throughout the warmup and be written off for the
#: life of the process -- the same silent cliff as the fixed 4,096 ceiling this replaced, just
#: reached a different way. Re-arming costs one increment per lookup on a field already being
#: skipped, and bounds the damage of a wrong verdict to one window instead of the whole run.
_CONTAINER_FIELD_REARM_LOOKUPS = 8192

#: Fields that lost the test, and fields that passed it and no longer need counting.
_SHARED_CONTAINERS_ABANDONED: dict = {}
_SHARED_CONTAINERS_EARNED: set = set()
#: field -> [lookups, hits], kept only until the field has earned its place or lost it.
_SHARED_CONTAINER_STATS: dict = {}
#: Only a value whose every entry is one of these can be keyed by its contents.
_SHAREABLE_SCALARS = frozenset({str, int, float, bool, type(None)})
#: The two types :func:`_lookup_shared` hands back. An instance of either is canonical -- the one
#: object the table holds for that value -- which is what makes its identity usable as a key.
_SHARED_CONTAINER_TYPES = (_SharedInternedValue, _SharedInternedList)
#: flat value -> the one object every record carrying it holds. Process-wide, so two adapters
#: over the same store share as well.
_SHARED_VALUE_TABLE: dict = {}


#: How far to look inside a record for a container worth sharing. The repetitive ones sit one
#: level down -- `embedding_meta.node_path` was 100 rows holding ONE value, `envelope.storage_route`
#: 20 rows holding one -- and nothing useful was found below three. A cap keeps a record that nests
#: deeply from costing a walk proportional to its whole shape on every append.
_SHARE_MAX_DEPTH = 3


def _lookup_shared(field, key, value, table, shared_type):
    """Return the one object held for this value, if the field is worth holding a table for."""
    entry = table.get(key)
    if field in _SHARED_CONTAINERS_EARNED:
        if entry is not None:
            return entry
        if len(table) >= _SHARED_VALUE_TABLE_LIMIT:
            return value
        entry = table[key] = shared_type(value)
        return entry
    skipped = _SHARED_CONTAINERS_ABANDONED.get(field)
    if skipped is not None:
        if skipped[0] < _CONTAINER_FIELD_REARM_LOOKUPS:
            skipped[0] += 1
            return value
        # Its window is up. Judge it again on fresh evidence rather than on a verdict reached
        # before the corpus had shown what it holds.
        del _SHARED_CONTAINERS_ABANDONED[field]
    stats = _SHARED_CONTAINER_STATS.get(field)
    if stats is None:
        stats = _SHARED_CONTAINER_STATS[field] = [0, 0]
    stats[0] += 1
    if entry is not None:
        stats[1] += 1
        return entry
    if stats[0] >= _CONTAINER_FIELD_WARMUP_LOOKUPS:
        if stats[1] < stats[0] * _CONTAINER_FIELD_MIN_HIT_RATE:
            # Its values are distinct, not repeated, so every entry is a key with no copy behind
            # it to pay for. Stop, and leave the entries already made -- they are bounded by the
            # warmup and re-deriving which ones to drop would cost more than they hold.
            _SHARED_CONTAINERS_ABANDONED[field] = [0]
            _SHARED_CONTAINER_STATS.pop(field, None)
            return value
        _SHARED_CONTAINERS_EARNED.add(field)
        _SHARED_CONTAINER_STATS.pop(field, None)
    if len(table) >= _SHARED_VALUE_TABLE_LIMIT:
        return value
    entry = table[key] = shared_type(value)
    return entry


def _shared_by_child_identity(field, value, table):
    """Share a dict whose values are all scalars or already-shared containers.

    Returns ``value`` untouched when some value is neither, which is the case that has to stay
    cheap: it is one pass over the items with no hashing.
    """
    key_parts = []
    for name, item in value.items():
        if type(item) in _SHAREABLE_SCALARS:
            key_parts.append((name, 0, item))
        elif isinstance(item, _SHARED_CONTAINER_TYPES):
            key_parts.append((name, 1, id(item)))
        else:
            return value
    try:
        # Keys are distinct within a dict, so sorting never compares past position 0 and the mixed
        # types in position 2 are never ordered against each other.
        return _lookup_shared(field, (field, tuple(sorted(key_parts))), value,
                              table, _SharedInternedValue)
    except TypeError:
        return value        # keys of mixed type do not sort


def _shared_container(field, value, table, depth):
    """Share ``value`` if it is a flat container, else rebuild it around whatever inside it is.

    Returns the SAME object when nothing changed, which is what lets a caller skip the copy: a
    record whose values are all unshareable is passed through untouched rather than duplicated.

    A container is only rebuilt on the path down to a replacement, so the caller's own nested
    dicts are never written to -- the copy stops as soon as there is nothing below worth sharing.
    """
    kind = type(value)          # exact type: an already-shared value is a subclass and is skipped
    if kind is dict:
        if not value:
            return value
        if all(type(v) in _SHAREABLE_SCALARS for v in value.values()):
            try:
                return _lookup_shared(field, (field, tuple(sorted(value.items()))), value,
                                      table, _SharedInternedValue)
            except TypeError:
                return value    # keys of mixed type do not sort
        if depth >= _SHARE_MAX_DEPTH:
            return value
        replacements = None
        for sub_field, sub_value in value.items():
            if type(sub_value) not in (dict, list):
                continue
            shared = _shared_container(str(sub_field), sub_value, table, depth + 1)
            if shared is not sub_value:
                if replacements is None:
                    replacements = {}
                replacements[sub_field] = shared
        if replacements is None:
            rebuilt = value
        else:
            rebuilt = dict(value)
            rebuilt.update(replacements)
        # A dict holding a container could not be keyed by its contents, so it was returned
        # unshared however often it repeated. Once its children have been shared it CAN be: a
        # shared child is the one object held for its value, so the child's identity stands in for
        # its contents and the parent gets a cheap, exact key.
        #
        # This is the whole of what was left. Measured on 100,105 records, every `metadata` value
        # held exactly one container -- `heading_path`, a list -- and its repetition histogram was
        # {2: 2000}: EVERY distinct value appeared exactly twice, once as a skill_section and once
        # as a resource_chunk. 99,320 objects for 49,641 values, 13.4% of the read cache carried at
        # double.
        #
        # Identity is safe as a key here only because the table holds its entries strongly and
        # nothing evicts them, so a shared child outlives every key naming it. A child whose field
        # was abandoned, or that arrived after the table hit its ceiling, is NOT one of these types
        # and falls out of the check below -- which is what keeps the identity honest.
        return _shared_by_child_identity(field, rebuilt, table)
    if kind is list:
        if not value:
            return value
        if all(type(v) in _SHAREABLE_SCALARS for v in value):
            return _lookup_shared(field, (field, tuple(value)), value, table, _SharedInternedList)
        if depth >= _SHARE_MAX_DEPTH:
            return value
        rebuilt = None
        for index, item in enumerate(value):
            if type(item) not in (dict, list):
                continue
            shared = _shared_container(field, item, table, depth + 1)
            if shared is not item:
                if rebuilt is None:
                    rebuilt = list(value)
                rebuilt[index] = shared
        return value if rebuilt is None else rebuilt
    return value


#: What a dict BUILT to a given key count costs on this interpreter, memoised by key count.
#:
#: Measured rather than tabulated, because the growth policy is CPython's and a table written here
#: would silently stop matching. Only a handful of key counts occur, so the cache stays tiny.
_RIGHT_SIZED_BYTES: dict[int, int] = {}


def _right_sized_bytes(key_count: int) -> int:
    cached = _RIGHT_SIZED_BYTES.get(key_count)
    if cached is None:
        probe: dict = {}
        for index in range(key_count):
            probe[index] = None
        cached = _sys.getsizeof(probe)
        _RIGHT_SIZED_BYTES[key_count] = cached
    return cached


def share_repeated_values(records: list[Json], table: dict, already: set | None = None) -> list[Json]:
    """Give every record that carries the same flat dict value the SAME object.

    ``expand_interned_records`` already does this, but only for records it decodes off disk. A
    record that reaches the cache from the append path was built field by field in memory and
    never passed through it, so it holds a private dict for a value the corpus repeats endlessly.
    Measured over 914 cached records from 60 attachments: ``storage_options`` was held as 673
    separate objects for **11 distinct values** and ``storage_route`` as 793 objects for **2**.
    Sharing one object per distinct value reclaims **49.9% of the cache** -- 3,877 B/record down
    to 1,944 B/record.

    Each record is shallow-copied first, so the shared value is only ever reachable through the
    cache. The record the caller passed in keeps its own private dicts and stays writable; only
    the copy the cache holds points at a value that refuses mutation.

    Only flat dicts qualify: a value containing a container cannot be keyed cheaply, and a list
    would have to change type to be safe to share (see :func:`_shared_interned_value`).

    ``vector`` looks like the next candidate -- 363 objects for 145 values, 8.5% of the cache --
    but that is an artefact of a corpus built from repeated text. Real ingest embeds distinct
    documents, so almost every vector is unique: sharing them would hash every vector on the
    append path and collapse nothing. It is left out deliberately, not overlooked.
    """
    if not SHARE_REPEATED_VALUES:
        return records
    shared_out: list[Json] = []
    for record in records:
        if not isinstance(record, dict) or (already is not None and id(record) in already):
            # `already` names records that were shared on the way in. Compaction hands back the
            # SAME objects for everything it did not rebuild, so re-walking their fields only
            # rediscovers that there is nothing to do: measured over 150 skill ingests, the
            # compaction site walked 159,885 rows to change 1,924 of them -- 96.1% wasted, and
            # 18.5% of ingest wall. Membership is one hash against a set built without touching a
            # single field.
            shared_out.append(record)
            continue
        replacements = None
        for field, value in record.items():
            if type(value) not in (dict, list):
                continue
            shared = _shared_container(field, value, table, 0)
            if shared is not value:
                if replacements is None:
                    replacements = {}
                replacements[field] = shared
        # Right-size the table on the way in, whichever branch takes it.
        #
        # CPython grows a dict's table on insert and never shrinks it, and the table a dict ends on
        # depends on HOW it was built rather than on what it holds. Both paths that reach the cache
        # build records the expensive way -- expansion copies the encoded record, drops the intern
        # token and puts the bundle's six fields back; the append path assembles field by field --
        # and a dict that arrives at 21 keys that way keeps a 64-slot table costing 1,176 B where
        # one BUILT to the same 21 keys gets 32 slots and costs 640.
        #
        # 536 B on every cached record, for identical keys and identical values. Rebuilding by
        # ITEMS is what reclaims it: `dict(record)` and `{**record}` both presize from the source's
        # capacity and copy the oversize faithfully.
        #
        # This is the right site because it is the one both paths pass through. Doing it in
        # `expand_interned_records` instead fixed the cold load and left the warm cache -- the one a
        # long-running box actually serves from -- still paying, which is how the shortfall came to light.
        #
        # Measured on a 1 MB skill corpus, where skill_section and resource_chunk are 99.2% of rows:
        # the container falls 1,173 -> 641 B/record, the whole cache 2,759 -> 2,227 B/record
        # (-19.3%), for about 3% on a cold load.
        oversized = _sys.getsizeof(record) > _right_sized_bytes(len(record))
        if replacements is None:
            shared_out.append(dict(record.items()) if oversized else record)
        else:
            # `update` here only replaces values under keys the record already has, so it cannot
            # grow the table it was just given.
            copied = dict(record.items()) if oversized else dict(record)
            copied.update(replacements)
            shared_out.append(copied)
    return shared_out


import json as _json
import sys as _sys


#: A field is abandoned when its lookups keep MISSING -- not when it holds many distinct values.
#:
#: Cardinality was the first test and it is the wrong one. `text` on a chunked document holds 873
#: distinct values over 1,743 rows, so a cardinality limit gives up on it -- yet its repetition
#: histogram is {1: 3, 2: 870}: every value but three appears exactly TWICE, because a chunk is
#: stored once as a skill_section and once as a resource_chunk. High cardinality and perfect
#: repetition at the same time. Sharing those is 13.8% of the read cache.
#:
#: A hit rate separates the two shapes directly: `row_key` misses on essentially every lookup and
#: is dropped after the warmup, while `text` hits half the time and is kept however many distinct
#: values it accumulates.
_STRING_FIELD_WARMUP_LOOKUPS = 512
_STRING_FIELD_MIN_HIT_RATE = 0.10
#: Longer than this and hashing to look the value up is not worth it. Raised from 256 once the
#: cost was measured rather than assumed: hashing 1,743 values averaging 904 characters into a
#: table takes 0.4 ms.
_SHARED_STRING_MAX_LEN = 65536
_SHARED_STRINGS: dict = {}
_SHARED_STRINGS_ABANDONED: set = set()
#: field -> [lookups, hits], kept only until the field has earned its place or lost it.
_SHARED_STRING_STATS: dict = {}


def _shared_string(field, value):
    """Return the one copy of ``value`` held for ``field``, or ``value`` if it is not worth it."""
    if len(value) > _SHARED_STRING_MAX_LEN or field in _SHARED_STRINGS_ABANDONED:
        return value
    table = _SHARED_STRINGS.get(field)
    if table is None:
        table = _SHARED_STRINGS[field] = {}
        _SHARED_STRING_STATS[field] = [0, 0]
    stats = _SHARED_STRING_STATS.get(field)
    shared = table.get(value)
    if stats is not None:
        stats[0] += 1
        if shared is not None:
            stats[1] += 1
        elif stats[0] >= _STRING_FIELD_WARMUP_LOOKUPS:
            if stats[1] < stats[0] * _STRING_FIELD_MIN_HIT_RATE:
                # Its values are distinct, not repeated. Stop paying to find that out.
                _SHARED_STRINGS_ABANDONED.add(field)
                _SHARED_STRINGS.pop(field, None)
                _SHARED_STRING_STATS.pop(field, None)
                return value
            # It has earned its place; stop counting.
            _SHARED_STRING_STATS.pop(field, None)
    if shared is not None:
        return shared
    table[value] = value
    return value


def _interned_pairs(pairs):
    """Build a decoded object with its keys interned.

    The JSON decoder memoises key strings within ONE call, but the log is read a line at a time,
    so every record gets its own copy of every key it carries. Measured over a cold read of 914
    records: **148 distinct key names, backed by 16,743 separate string objects** holding 1,009.7
    KB -- 1,000.3 KB of which is one name repeated. That is close to half the cold cache, spent on
    148 short strings.

    Interning is free of meaning: the strings compare equal either way, so nothing above this can
    tell the difference.
    """
    out = {}
    for key, value in pairs:
        if type(key) is str:
            key = _sys.intern(key)
            if type(value) is str and value:
                value = _shared_string(key, value)
        out[key] = value
    return out


def loads_with_interned_keys(line: str):
    """``json.loads`` for one log line, sharing key strings with every other line."""
    return _json.loads(line, object_pairs_hook=_interned_pairs)


def _copy_interned_value(value: Any) -> Any:
    """Kept for callers that genuinely need their own copy."""
    if type(value) is dict:
        if not any(isinstance(v, (dict, list, set)) for v in value.values()):
            return dict(value)
    elif type(value) is list:
        if not any(isinstance(v, (dict, list, set)) for v in value):
            return list(value)
    elif not isinstance(value, (dict, list, set)):
        return value
    return _copy.deepcopy(value)


#: The packed vector field. A separate key rather than a re-typed ``vector`` so a record is either
#: one form or the other and a reader can tell which by looking, not by guessing.
VECTOR_F32_KEY = "vector_f32"
#: Off by default: a reader from before this change finds no ``vector`` on a packed record and
#: carries on without one, which is lost recall and no error. Every other switch in this family
#: degrades to re-deriving from the log; this one degrades to a quietly worse answer.
LOCAL_BINARY_VECTORS = os.environ.get(
    "MATRIXARK_LOCAL_BINARY_VECTORS", "0"
).strip().lower() not in ("0", "false", "no", "off")


def encode_vector_f32(values: list[float]) -> str:
    """Little-endian float32, base64 -- 5.33 bytes a dimension against 20.96 for the JSON digits."""
    return _base64.b64encode(
        _struct.pack("<%df" % len(values), *[float(v) for v in values])).decode("ascii")


def decode_vector_f32(text: str) -> list[float]:
    raw = _base64.b64decode(text)
    return list(_struct.unpack("<%df" % (len(raw) // 4), raw[: len(raw) // 4 * 4]))


def _is_float_vector(value: Any) -> bool:
    return (isinstance(value, list) and bool(value)
            and all(isinstance(item, (int, float)) and not isinstance(item, bool)
                    for item in value))


def round_vector_to_f32(record: Json) -> Json:
    """Hold the value that will be stored.

    Packing only on the way to the log left the cache and the snapshot holding the original
    float64s, so a warm read answered -0.408248 where a cold read that re-derived from the log
    answered -0.40824800729751587. Same store, two answers, decided by which path served it.

    Called from `_sanitize_jsonl_record`, which is the one thing BOTH append paths run per record.
    The list form below is a wrapper over this; putting the rounding in the list comprehensions
    instead reached only one of the two, because the other spells the same loop with a different
    variable name.
    """
    if not LOCAL_BINARY_VECTORS or not isinstance(record, dict):
        return record
    if not _is_float_vector(record.get("vector")):
        return record
    record = dict(record)
    record["vector"] = decode_vector_f32(encode_vector_f32(record["vector"]))
    return record


def round_vectors_to_f32(records: list[Json]) -> list[Json]:
    """Hold the value that will be stored.

    Packing only on the way to the log left the cache and the snapshot holding the original
    float64s, so a warm read answered -0.408248 where a cold read that re-derived from the log
    answered -0.40824800729751587. Same store, two answers, decided by which path served it --
    exactly the warm-and-cold disagreement that is worth refusing to ship.

    Applied where the record is made, so the cache, the snapshot and the log all carry the same
    float32 value and packing is left with nothing to change but the bytes.
    """
    if not LOCAL_BINARY_VECTORS:
        return records
    return [round_vector_to_f32(record) for record in records]


def pack_record_vectors(records: list[Json]) -> list[Json]:
    """Replace list-valued ``vector`` fields with the packed form, on the way to the log."""
    if not LOCAL_BINARY_VECTORS:
        return records
    out: list[Json] = []
    for record in records:
        vector = record.get("vector") if isinstance(record, dict) else None
        if _is_float_vector(vector):
            record = {key: value for key, value in record.items() if key != "vector"}
            record[VECTOR_F32_KEY] = encode_vector_f32(vector)
        out.append(record)
    return out


def unpack_record_vectors(records: list[Json]) -> list[Json]:
    """The read-side inverse. Applied whatever the flag says, so a log written with it on still
    reads with it off -- the flag chooses what to WRITE, never what can be read."""
    out: list[Json] = []
    for record in records:
        if isinstance(record, dict) and isinstance(record.get(VECTOR_F32_KEY), str):
            packed = record[VECTOR_F32_KEY]
            record = {key: value for key, value in record.items() if key != VECTOR_F32_KEY}
            try:
                record["vector"] = decode_vector_f32(packed)
            except (ValueError, _struct.error):
                # A damaged vector is a lost vector, not a lost record: the row still carries its
                # text and its identity, and retrieval falls through to the lexical path exactly as
                # it does for a row that was never embedded.
                pass
        out.append(record)
    return out


def expand_interned_records(records: list[Json]) -> list[Json]:
    """Re-expand interned metadata tokens to full values and drop the ``matrixark_intern_dict`` sidecar
    records. This is the single read-side inverse of :func:`encode_interned_records` and is applied at
    every raw-read choke point so no downstream consumer ever sees a token. A no-op (other than
    stripping any dict records) when nothing is interned, so old inline-field logs pass through
    unchanged."""
    # Before the fast path below, which returns early when nothing is interned -- a packed vector
    # has nothing to do with interning and would be skipped by it.
    if any(isinstance(record, dict) and isinstance(record.get(VECTOR_F32_KEY), str)
           for record in records):
        records = unpack_record_vectors(records)
    dict_map: dict[tuple[str, str], Any] = {}  # legacy per-field sidecars
    bundle_map: dict[str, dict[str, Any]] = {}  # bundle sidecars: token -> {field: value}
    saw_token = False
    for record in records:
        if not isinstance(record, dict):
            continue
        if str(record.get("record_type") or "") == INTERN_DICT_RECORD_TYPE:
            token = record.get("im_token")
            bundle = record.get("im_bundle")
            if isinstance(token, str) and isinstance(bundle, dict):
                bundle_map[token] = bundle
                continue
            field = record.get("im_field")
            if isinstance(field, str) and isinstance(token, str):
                dict_map[(field, token)] = record.get("im_value")
        elif isinstance(record.get(INTERN_TOKEN_KEY), dict) or isinstance(record.get(INTERN_BUNDLE_TOKEN_KEY), str):
            saw_token = True
    if not dict_map and not bundle_map and not saw_token:
        # Fast path: nothing interned. Still drop any stray dict records (none here) and return as-is.
        return list(records)
    # One wrapper per distinct value, built once here: every record that carries the value then
    # holds the same object, which is where the memory saving comes from.
    shared_bundles: dict[str, dict[str, Any]] = {
        token: {str(field): _shared_interned_value(value) for field, value in bundle.items()}
        for token, bundle in bundle_map.items()
    }
    shared_fields: dict[tuple[str, str], Any] = {
        key: _shared_interned_value(value) for key, value in dict_map.items()
    }
    expanded_out: list[Json] = []
    for record in records:
        if not isinstance(record, dict):
            expanded_out.append(record)
            continue
        if str(record.get("record_type") or "") == INTERN_DICT_RECORD_TYPE:
            continue
        bundle_token = record.get(INTERN_BUNDLE_TOKEN_KEY)
        token_map = record.get(INTERN_TOKEN_KEY)
        if not isinstance(bundle_token, str) and not isinstance(token_map, dict):
            expanded_out.append(record)
            continue
        expanded = dict(record)
        if isinstance(bundle_token, str):
            expanded.pop(INTERN_BUNDLE_TOKEN_KEY, None)
            bundle = shared_bundles.get(bundle_token)
            if isinstance(bundle, dict):
                expanded.update(bundle)
        if isinstance(token_map, dict):
            expanded.pop(INTERN_TOKEN_KEY, None)
            for field, token in token_map.items():
                key = (str(field), str(token))
                if key in shared_fields:
                    expanded[str(field)] = shared_fields[key]
        expanded_out.append(expanded)
    return expanded_out

_LOCAL_READ_CACHE_LOCK = threading.RLock()
_LOCAL_READ_CACHE: dict[str, tuple[int, int, list[Json]]] = {}
# Cache keys whose entry has records appended but not yet compacted. Compacting on every
# append walks the whole entry, which is O(corpus) per write; it is compacted when something
# reads it. Guarded by _LOCAL_READ_CACHE_LOCK, like the cache itself.
_LOCAL_READ_CACHE_DIRTY: set[str] = set()


def profile_promotion_decision(profile_node_hash: int) -> Json:
    scope_available = bool(profile_node_hash)
    return {
        "policy": PROFILE_PROMOTION_POLICY_ALWAYS,
        "importance_gate": False,
        "scope_available": scope_available,
        "blocker": "" if scope_available else PROFILE_PROMOTION_SCOPE_MISSING_BLOCKER,
    }


def should_promote_session_entity_to_profile(entity: Json) -> bool:
    return bool(entity)


# Embedding ref_type -> the owner record type and the field its ref_hash matches. Verified by
# value on a live ingest: an event_text embedding's ref_hash equals the context_event's
# event_id_hash, entity_state equals entity_hash, session_l0 equals summary_hash, node equals
# node_hash. Unlike the Rust engine -- where ref_hash is a one-way hash of (tenant, owner, level)
# and the owner cannot be recovered from it -- python addresses embeddings by the owner's OWN
# hash, which is what makes this join possible at all.
INLINE_VECTOR_OWNER_BY_REF_TYPE = {
    "event": ("context_event", "event_id_hash"),
    "entity": ("context_entity", "entity_hash"),
    "summary": ("context_summary", "summary_hash"),
    "node": ("context_node", "node_hash"),
    "segment": ("context_segment", "segment_hash"),
    "compression": ("context_compression_event", "compression_id_hash"),
    "resource_chunk": ("resource_chunk", "chunk_hash"),
    "skill_section": ("skill_section", "section_hash"),
}



# `embedding_meta` is the embedding record copied wholesale minus a few keys, so it inherits
# whatever the record happened to carry. Two of those are a routing blob:
# `canonical_storage_route(storage_options)` is a pure function of the options beside it, and the
# options are themselves already on the owning record.
#
# Measured by walking the page segments over 300 ingests: `embedding_meta.storage_route` cost
# 3.93 KB per add and `embedding_meta.storage_options` 2.55 KB -- together 63% of everything
# `embedding_meta` cost, and 13x the `vector` the metadata exists to describe (0.78 KB).
#
# Nothing reads either one. Every consumer of `embedding_meta` takes the source aggregates that
# budgeting and recovery need; a search for a read of the nested route or options finds none. The
# record's own top-level `storage_route` is kept and already slimmed to its placement half by
# `slim_persisted_storage_route`, which is where a reader that wants placement looks.
# Fields the owner record carries in its own right. They are stripped from `embedding_meta` when
# they match the owner's, which they do for every writer today -- an embedding is addressed by the
# owner's hash, so it inherits the owner's node and timestamp. Kept when they differ.
_EMBEDDING_META_SAME_AS_OWNER = ("node_path", "updated_at_ms", "node_hash")

# `embedding_type` repeats the owner's record_type whenever the fold's owner map is identity, which
# it is for chunks: skill_section -> skill_section, resource_chunk -> resource_chunk. It is NOT
# identity for the other six -- event -> context_event, summary -> context_summary -- and there the
# value says something the owner cannot (`node_l0` is not `context_summary`). So it is compared
# against record_type rather than assumed equal to it.
_EMBEDDING_META_SAME_AS_RECORD_TYPE = "embedding_type"

_EMBEDDING_META_SKIP = (
    "record_type",
    "ref_type",
    "ref_hash",
    "vector",
    # The retired row's OWN identity, which describes a row that no longer exists once the
    # embedding is folded onto its owner. 36.7% of embedding_meta's bytes over 100,105 records,
    # and the only unique field in a dict whose other members hold one distinct value each -- so
    # it is also what keeps the dict from being shared: 99,371 objects for 99,371 values.
    #
    # Checked for readers the way the rest of this tuple was: no reader in any Python module
    # outside this one, and inside it only the TOP-LEVEL row_key is used; no occurrence anywhere
    # in the Rust crates; and a runtime probe over read_all + retrieve never saw it asked for,
    # while `model` was asked for 99,324 times.
    #
    # It is also wrong to carry. `record_with_embedding_defaults` fills every meta key onto an
    # owner whose value is empty, with no exclusion list, so an owner without a top-level row_key
    # -- 99,280 of 100,122 records -- inherits the RETIRED row's identity, and
    # `latest_value_record_key` prefers a stamped key over deriving one.
    "row_key",
    "storage_route",
    "storage_options",
    # The placement/storage half of the same routing blob, inherited the same way and read by
    # nobody. Measured over 400 memories: placement_key 182.9 KB, scope_key 125.1 KB,
    # storage_record_kind 64.5 KB, placement_hash 56.3 KB, storage_part 50.8 KB -- 479.6 KB of
    # embedding_meta's 1,703.4 KB (28.2%), every one of them a single distinct value across the
    # whole store. Checked for readers rather than assumed: no read of any of them across 101
    # `embedding_meta` sites, and the native layer never reads `embedding_meta` at all.
    #
    # `model` / `model_ref` are deliberately NOT here despite also scanning clean: a mis-set model
    # path falls back to a different vector dimension, and the model identity on an embedding is
    # what makes that detectable.
    "placement_key",
    "placement_hash",
    "scope_key",
    "storage_record_kind",
    "storage_part",
)

try:  # package path
    from tools.matrixark_mcp_ingest_resource_chunk_records import (
        INDEX_SKIP_OWNER_DERIVABLE_TERMS,
    )
except ImportError:  # top-level path (direct tools/ execution)
    from matrixark_mcp_ingest_resource_chunk_records import (
        INDEX_SKIP_OWNER_DERIVABLE_TERMS,
    )


def drop_owner_derivable_postings(records: list[Json], resolve_owner=None) -> list[Json]:
    """Drop a posting whose term the record it points at can derive for itself.

    `context_index_posting_record` has fourteen call sites; this runs once, on the batch, next to
    the fold that already resolves owners. A posting survives unless EVERY ref it carries has an
    owner that both carries a vector -- the condition the retrieve prefilter's owner branch is
    gated on -- and derives the term.

    Conservative in every direction that matters: an owner that cannot be found, or that carries no
    vector, keeps its posting.
    """
    if not INDEX_SKIP_OWNER_DERIVABLE_TERMS:
        return records
    postings = [(i, r) for i, r in enumerate(records)
                if r.get("record_type") == "context_index"]
    if not postings:
        return records

    owners_by_key: dict[tuple[str, Any], Json] = {}
    for record in records:
        record_type = record.get("record_type")
        for ref_type, (owner_type, field) in INLINE_VECTOR_OWNER_BY_REF_TYPE.items():
            if record_type == owner_type and record.get(field) not in (None, ""):
                owners_by_key[(ref_type, record[field])] = record

    def owner_for(ref_type, ref_hash):
        owner = owners_by_key.get((ref_type, ref_hash))
        if owner is None and resolve_owner is not None:
            mapped = INLINE_VECTOR_OWNER_BY_REF_TYPE.get(ref_type)
            if mapped is not None:
                owner = resolve_owner(mapped[0], mapped[1], ref_hash)
        return owner

    drop: set[int] = set()
    for index, record in postings:
        name = str(record.get("index_name") or "")
        ref_type = str(record.get("ref_type") or "")
        refs = context_index_record_ref_hashes(record)
        if not name or not refs or ref_type not in INLINE_VECTOR_OWNER_BY_REF_TYPE:
            continue
        covered = True
        for ref_hash in refs:
            owner = owner_for(ref_type, ref_hash)
            if owner is None or not (owner.get("vector") or owner.get("embedding_meta")):
                covered = False
                break
            if name not in candidate_index_terms(owner, {}, {}):
                covered = False
                break
        if covered:
            drop.add(index)
    if not drop:
        return records
    return [r for i, r in enumerate(records) if i not in drop]


def _embeddings_enabled_for(record: Json) -> bool:
    """Whether this record's tenant stores vectors at all (default ON -- a tenant opts out).

    Imported lazily and failing OPEN: a deployment without the policy module keeps its existing
    behaviour rather than silently storing nothing, which would be a worse failure than the one
    this setting exists to allow.

    Gated here rather than at `embedding_for_text` because that producer has 57 callers and most
    are on the READ path embedding a query. Gating it would stop retrieval working for a tenant who
    only asked not to STORE vectors, which is a different setting entirely.
    """
    try:
        from matrixark_index_growth_bound import generate_embeddings_enabled
    except Exception:  # pragma: no cover - policy module absent
        return True
    # Read the scope the way the store itself does. Interning reduces a written record's scope dict
    # to a `scope_key` holding the tenant HASH, so reading only `scope` sees None on most records,
    # resolves to the default, and fails OPEN -- which is how the first version of this gate ran
    # over 82 records and changed nothing while reporting success.
    scope = record.get("scope") or record.get("access_scope") or record.get("scope_key")
    return bool(generate_embeddings_enabled(scope))


# One function per knob, each naming its gate. Resolving them through getattr would be shorter and
# would defeat the wiring guard, which requires a real call by name -- and that guard is the reason
# these knobs were found doing nothing in the first place. Each falls back to the knob's OWN default
# rather than a blanket True: two of these default ON and one defaults OFF, so a single fail-open
# would silently turn one of them on for everybody.
def _embeddings_enabled(scope: Any) -> bool:
    """Whether this tenant stores vectors at all (default ON -- a tenant opts out)."""
    try:
        from matrixark_index_growth_bound import generate_embeddings_enabled
    except Exception:  # pragma: no cover - policy module absent
        return True
    return bool(generate_embeddings_enabled(scope))


def _node_path_vectors_enabled(scope: Any) -> bool:
    """Whether this tenant stores a vector of the node PATH (default ON).

    Distinct from the knob above: the path is a synthetic string of path segments and a depth
    marker, not anything a customer wrote, so a tenant may want content vectors without these.
    """
    try:
        from matrixark_index_growth_bound import node_path_embeddings_enabled
    except Exception:  # pragma: no cover - policy module absent
        return True
    return bool(node_path_embeddings_enabled(scope))


def _event_summary_text_enabled(scope: Any) -> bool:
    """Whether an event carries its own summary_text (default OFF)."""
    try:
        from matrixark_index_growth_bound import store_event_summary_text_enabled
    except Exception:  # pragma: no cover - policy module absent
        return False
    return bool(store_event_summary_text_enabled(scope))


def _record_scope(record: Json) -> Any:
    """The scope a record is attributed by.

    Interning reduces a written record's scope dict to a `scope_key` holding the tenant hash, so
    reading only `scope` sees None on most records and every gate then falls to its default.
    """
    return record.get("scope") or record.get("access_scope") or record.get("scope_key")


def apply_storage_policy(records: list[Json]) -> list[Json]:
    """Enforce the per-tenant STORAGE knobs on a batch about to be appended.

    Three knobs, one place:

    * `generate_embeddings` (default ON) -- drop the separate embedding record and strip an inline
      vector. Not gated at `embedding_for_text`, which has 57 callers and is shared with the READ
      path: gating there would stop a query being embedded for a tenant who only declined to STORE.
    * `node_path_embeddings` (default ON) -- drop just the `context_node` embeddings, which vectorise
      a synthetic path string rather than anything a customer wrote.
    * `store_event_summary_text` (default OFF) -- strip `summary_text` from an event. Under the
      truncation limit it is a byte-identical copy of `text`, and every reader is written as
      `summary_text or text`, so the field is omitted rather than emptied: absent falls back to the
      text it copied, while "" would read as "summarised to nothing".

    A batch nothing applies to is returned unchanged -- identity, not a copy.
    """
    if not records:
        return records
    out: list[Json] = []
    changed = False
    for record in records:
        scope = _record_scope(record)
        kind = record.get("record_type")

        if kind == "context_embedding":
            if not _embeddings_enabled(scope):
                changed = True
                continue
            if (record.get("embedding_type") == "context_node"
                    and not _node_path_vectors_enabled(scope)):
                changed = True
                continue
            out.append(record)
            continue

        edited = record
        if (record.get("vector") not in (None, "", [])
                and not _embeddings_enabled(scope)):
            edited = dict(edited)
            edited.pop("vector", None)
            # The metadata describes a vector that is no longer there; leaving it would tell a
            # reader this record was embedded when it was not.
            edited.pop("embedding_meta", None)
            changed = True

        if (kind == "context_event" and "summary_text" in edited
                and not _event_summary_text_enabled(scope)):
            edited = dict(edited) if edited is record else edited
            edited.pop("summary_text", None)
            changed = True

        out.append(edited)
    return out if changed else records


def drop_vectors_for_opted_out_tenants(records: list[Json]) -> list[Json]:
    """Remove stored vectors for tenants that turned embeddings off, by BOTH routes.

    A vector reaches storage two ways: as its own `context_embedding` record, and written straight
    onto the owner as an inline `vector` field. Handling only the first is what the first attempt
    did, and it silently left nine of ten records carrying vectors for a tenant that had opted out.

    Applied at every append site alongside the fold, so there is one answer rather than one per
    writer. Records for tenants that have not opted out are returned untouched -- identity, not a
    copy -- so this costs nothing in the ordinary case.
    """
    return apply_storage_policy(records)


def fold_embedding_records(
    records: list[Json],
    resolve_owner=None,
) -> list[Json]:
    """Fold each embedding's vector onto its owner record and DROP the separate record.

    Python embeddings are addressed by the owner's OWN hash (ref_type + ref_hash), so the join
    back to the owner is direct -- which is what makes retiring the separate records possible
    at this one choke point instead of at every writer.

    Three cases, tried in this order:
      * the owner is in the SAME batch: its vector (and embedding metadata) is set in place and
        the embedding record vanishes;
      * the owner was appended EARLIER: ``resolve_owner(record_type, field, ref_hash)`` fetches
        it from the durable view, and an UPDATED copy of the owner replaces the embedding
        record in the batch -- owners are keyed, so the re-append supersedes;
      * no owner can be found (or the ref_type has no mapped owner): the embedding record is
        KEPT exactly as before. A vector is never dropped on the floor.

    Readers keep working across the transition: old logs still hold separate records and every
    read path still accepts them; new logs simply stop growing them.
    """
    embeddings: list[tuple[int, Json]] = []
    owners_by_key: dict[tuple[str, Any], Json] = {}
    for index, record in enumerate(records):
        record_type = record.get("record_type")
        if record_type == "context_embedding":
            embeddings.append((index, record))
            continue
        for ref_type, (owner_type, field) in INLINE_VECTOR_OWNER_BY_REF_TYPE.items():
            if record_type == owner_type and record.get(field) not in (None, ""):
                owners_by_key[(ref_type, record[field])] = record
    if not embeddings:
        return records

    drop: set[int] = set()
    replace: dict[int, Json] = {}
    for index, record in embeddings:
        vector = record.get("vector")
        ref_type = str(record.get("ref_type") or "")
        ref_hash = record.get("ref_hash")
        mapped = INLINE_VECTOR_OWNER_BY_REF_TYPE.get(ref_type)
        if not vector or ref_hash in (None, "") or mapped is None:
            continue
        owner = owners_by_key.get((ref_type, ref_hash))
        if owner is None and resolve_owner is not None:
            owner_type, field = mapped
            resolved = resolve_owner(owner_type, field, ref_hash)
            if resolved is not None:
                owner = dict(resolved)
                replace[index] = owner
        if owner is None:
            continue
        # Decided on the OWNER, not the embedding. An embedding record often carries no scope of
        # its own -- it is addressed by the owner's hash -- so the earlier pass cannot attribute it
        # and lets it through. Here the owner is in hand, so the tenant is knowable: without this,
        # 7 of 19 embeddings folded onto records belonging to a tenant that had opted out.
        if not _embeddings_enabled_for(owner):
            drop.add(index)
            replace.pop(index, None)
            continue
        owner["vector"] = vector
        # The separate record carried more than the vector: serving-layer lineage, scope and
        # source aggregates that budgeting and recovery consume. It rides along under ONE
        # namespaced key so the owner keeps its own shape -- nothing is lost, and nothing
        # leaks into the owner top level.
        meta = {
            key: value
            for key, value in record.items()
            if key not in _EMBEDDING_META_SKIP
            and value not in (None, "", [], {})
        }
        # Three of what survives are fields the owner already carries itself, and the embedding is
        # addressed by the owner's own hash, so they arrive identical: 137.2 KB per 1 MB skill
        # restating the record the meta rides on. Dropped only where they MATCH -- a differing
        # value is the interesting case and is kept.
        for key in _EMBEDDING_META_SAME_AS_OWNER:
            if key in meta and key in owner and owner[key] == meta[key]:
                del meta[key]
        if meta.get(_EMBEDDING_META_SAME_AS_RECORD_TYPE) == owner.get("record_type"):
            del meta[_EMBEDDING_META_SAME_AS_RECORD_TYPE]
        if meta:
            owner.setdefault("embedding_meta", meta)
        if index not in replace:
            drop.add(index)

    if not drop and not replace:
        return records
    out: list[Json] = []
    for index, record in enumerate(records):
        if index in drop:
            continue
        out.append(replace.get(index, record))
    return out


def compact_context_embedding_record(record: Json) -> Json:
    return compact_hot_context_embedding_record(record)


def _ref_list_value(item: Json, field: str) -> list[Any]:
    values = item.get(field)
    if isinstance(values, list):
        return values
    metadata = item.get("metadata")
    if isinstance(metadata, dict):
        values = metadata.get(field)
    return values if isinstance(values, list) else []


def _metadata_value(item: Json, field: str) -> Any:
    value = item.get(field)
    if value not in (None, "", [], {}):
        return value
    metadata = item.get("metadata")
    if isinstance(metadata, dict):
        return metadata.get(field)
    return None


def _source_bucket_names(item: Json, list_field: str, count_field: str, *, normalize_roles: bool = False) -> list[str]:
    values = _metadata_value(item, list_field)
    names: set[str] = set()
    if isinstance(values, list):
        for value in values:
            name = normalize_message_role(value) if normalize_roles else str(value or "").strip()
            if name:
                names.add(name)
    counts = _metadata_value(item, count_field)
    if isinstance(counts, dict):
        for value, count in counts.items():
            try:
                amount = max(0, int(count or 0))
            except (TypeError, ValueError):
                continue
            if amount <= 0:
                continue
            name = normalize_message_role(value) if normalize_roles else str(value or "").strip()
            if name:
                names.add(name)
    return sorted(names)


def _selected_ref_tokens(item: Json) -> int:
    try:
        return max(1, int(item.get("token_estimate") or 0))
    except (TypeError, ValueError):
        return max(1, token_count(str(item.get("text") or "")))


def _refresh_selected_counter_policy(
    *,
    selected: list[Json],
    dropped_over_budget: Json,
    policy_field: str,
    token_field: str,
    count_field: str,
    bucket_names,
) -> None:
    policy = dropped_over_budget.get(policy_field)
    if not isinstance(policy, dict):
        return
    budget_tokens = policy.get("budget_tokens")
    if not isinstance(budget_tokens, dict) or not budget_tokens:
        return
    selected_tokens = {str(name): 0 for name in budget_tokens.keys()}
    selected_counts = {str(name): 0 for name in budget_tokens.keys()}
    for item in selected:
        ref_tokens = _selected_ref_tokens(item)
        for name in bucket_names(item):
            if name in selected_tokens:
                selected_tokens[name] += ref_tokens
                selected_counts[name] += 1
    policy[token_field] = {key: value for key, value in selected_tokens.items() if value > 0 or key in budget_tokens}
    policy[count_field] = {key: value for key, value in selected_counts.items() if value > 0}
    policy["selected_counter_source"] = "final_context_pack_selection_after_profile_pending_dedupe"


def refresh_final_selected_budget_policies(selected: list[Json], dropped_over_budget: Json) -> None:
    _refresh_selected_counter_policy(
        selected=selected,
        dropped_over_budget=dropped_over_budget,
        policy_field="source_role_budget_policy",
        token_field="selected_tokens_by_role",
        count_field="selected_ref_count_by_role",
        bucket_names=lambda item: _source_bucket_names(
            item,
            "budget_source_roles" if _metadata_value(item, "budget_source_roles") else "source_roles",
            "budget_source_role_counts" if _metadata_value(item, "budget_source_role_counts") else "source_role_counts",
            normalize_roles=True,
        ),
    )
    _refresh_selected_counter_policy(
        selected=selected,
        dropped_over_budget=dropped_over_budget,
        policy_field="memory_selection_policy_budget_policy",
        token_field="selected_tokens_by_policy",
        count_field="selected_ref_count_by_policy",
        bucket_names=lambda item: _source_bucket_names(
            item,
            "source_memory_selection_policies",
            "source_memory_selection_policy_counts",
        ),
    )
    _refresh_selected_counter_policy(
        selected=selected,
        dropped_over_budget=dropped_over_budget,
        policy_field="memory_layer_budget_policy",
        token_field="selected_tokens_by_layer",
        count_field="selected_ref_count_by_layer",
        bucket_names=lambda item: [candidate_memory_layer_name(item)],
    )
    _refresh_selected_counter_policy(
        selected=selected,
        dropped_over_budget=dropped_over_budget,
        policy_field="extraction_phase_budget_policy",
        token_field="selected_tokens_by_phase",
        count_field="selected_ref_count_by_phase",
        bucket_names=lambda item: [
            str(_metadata_value(item, "extraction_phase") or "").strip()
            or "unknown"
        ],
    )


def suppress_extracted_represented_pending_events(selected: list[Json], dropped_over_budget: Json) -> tuple[list[Json], int]:
    extracted_selected_event_ids: set[int] = set()
    for item in selected:
        if is_pending_async_candidate(item):
            continue
        for field in [
            "source_event_ids",
            "extraction_context_event_ids",
            "source_ref_hashes",
        ]:
            for value in _ref_list_value(item, field):
                try:
                    event_id = int(value or 0)
                except (TypeError, ValueError):
                    event_id = 0
                if event_id:
                    extracted_selected_event_ids.add(event_id)
        if str(item.get("ref_type") or "") == "event":
            try:
                event_id = int(item.get("ref_hash") or 0)
            except (TypeError, ValueError):
                event_id = 0
            if event_id:
                extracted_selected_event_ids.add(event_id)
    if not extracted_selected_event_ids:
        return selected, 0
    extracted_preferred_selected: list[Json] = []
    removed_tokens = 0
    removed_pending_count = 0
    for item in selected:
        try:
            metadata = item.get("metadata")
            metadata_ref_hash = metadata.get("ref_hash") if isinstance(metadata, dict) else 0
            pending_event_id = int(item.get("ref_hash") or metadata_ref_hash or 0)
        except (TypeError, ValueError):
            pending_event_id = 0
        if (
            is_pending_async_candidate(item)
            and pending_event_id
            and pending_event_id in extracted_selected_event_ids
        ):
            removed_pending_count += 1
            removed_tokens += int(item.get("token_estimate") or max(1, token_count(str(item.get("text") or ""))))
            continue
        extracted_preferred_selected.append(item)
    if removed_pending_count and extracted_preferred_selected:
        dropped_over_budget["pending_async_event_superseded_by_extracted_refs"] = (
            int(dropped_over_budget.get("pending_async_event_superseded_by_extracted_refs") or 0)
            + removed_pending_count
        )
        return extracted_preferred_selected, removed_tokens
    return selected, 0


def suppress_overlapping_profile_current_entities(selected: list[Json], dropped_over_budget: Json) -> tuple[list[Json], int]:
    kept: list[Json] = []
    removed_tokens = 0
    profile_text_tokens_by_type: dict[str, list[set[str]]] = {}
    for item in selected:
        entity_type = str(item.get("entity_type") or "").strip().lower()
        is_profile_current_entity = (
            item.get("ref_type") == "entity"
            and str(item.get("memory_scope") or "").strip().lower() == "user_profile"
            and str(item.get("session_continuity") or "").strip().lower() == "cross_session"
            and bool(item.get("profile_current_state_representative"))
            and entity_type
        )
        if not is_profile_current_entity:
            kept.append(item)
            continue
        item_tokens = {token for token in tokens(str(item.get("text") or "")) if len(token) > 2}
        overlaps_existing = False
        for prior_tokens in profile_text_tokens_by_type.get(entity_type, []):
            if not item_tokens or not prior_tokens:
                continue
            intersection = len(item_tokens.intersection(prior_tokens))
            smaller = max(1, min(len(item_tokens), len(prior_tokens)))
            if intersection / smaller >= 0.60:
                overlaps_existing = True
                break
        if overlaps_existing:
            item_token_count = max(1, token_count(str(item.get("text") or "")))
            removed_tokens += item_token_count
            dropped_over_budget.setdefault("profile_current_entity_overlap_suppressed", 0)
            dropped_over_budget["profile_current_entity_overlap_suppressed"] += 1
            record_dropped_candidate(
                dropped_over_budget,
                item,
                reason="duplicate",
                token_estimate=item_token_count,
            )
            continue
        kept.append(item)
        profile_text_tokens_by_type.setdefault(entity_type, []).append(item_tokens)
    return kept, removed_tokens


def suppress_profile_shadowed_session_entities(selected: list[Json], dropped_over_budget: Json) -> tuple[list[Json], int]:
    profile_entity_source_hashes: set[Any] = set()
    profile_entity_identity_keys: set[tuple[str, str]] = set()
    for item in selected:
        if (
            item.get("ref_type") == "entity"
            and item.get("memory_scope") == "user_profile"
            and item.get("session_continuity") == "cross_session"
        ):
            entity_type = str(item.get("entity_type") or "").strip().lower()
            if is_codex_outcome_entity_type(entity_type) or str(item.get("profile_memory_kind") or "").strip().lower() == "codex_outcome":
                continue
            profile_entity_source_hashes.update(_ref_list_value(item, "source_entity_hashes"))
            entity_name = str(item.get("entity_name") or "").strip().lower()
            if entity_type and entity_name:
                profile_entity_identity_keys.add((entity_type, entity_name))
    if not profile_entity_source_hashes and not profile_entity_identity_keys:
        return selected, 0

    deduped_selected: list[Json] = []
    removed_tokens = 0
    removed_count = 0
    for item in selected:
        if (
            item.get("ref_type") == "entity"
            and item.get("memory_scope") == "session"
            and item.get("session_continuity") == "same_session"
        ):
            item_entity_type = str(item.get("entity_type") or "").strip().lower()
            if is_codex_outcome_entity_type(item_entity_type) or str(item.get("profile_memory_kind") or "").strip().lower() == "codex_outcome":
                deduped_selected.append(item)
                continue
            item_key = (
                item_entity_type,
                str(item.get("entity_name") or "").strip().lower(),
            )
            represented_by_profile = item.get("ref_hash") in profile_entity_source_hashes or (
                bool(item_key[0] and item_key[1]) and item_key in profile_entity_identity_keys
            )
            if represented_by_profile:
                token_estimate = int(item.get("token_estimate") or max(1, token_count(str(item.get("text") or ""))))
                removed_tokens += token_estimate
                removed_count += 1
                record_dropped_candidate(
                    dropped_over_budget,
                    {
                        **item,
                        "profile_shadowed_reason": "selected_profile_entity_supersedes_session_entity",
                    },
                    reason="profile_entity_shadowed_session_entity",
                    token_estimate=token_estimate,
                )
                continue
        deduped_selected.append(item)
    if not removed_count or not deduped_selected:
        return selected, 0
    dropped_over_budget["profile_entity_shadowed_session_entities"] = (
        int(dropped_over_budget.get("profile_entity_shadowed_session_entities") or 0) + removed_count
    )
    return deduped_selected, removed_tokens


def codex_session_identity_policy(session_id_source: str) -> Json:
    source = str(session_id_source or "").strip()
    strong_sources = {"explicit", "payload_field", "payload_path_hash"}
    fallback_sources = {"state_file", "state_file_created", "workspace_hash"}
    strong = source in strong_sources or source.startswith(("payload.", "env."))
    fallback = source in fallback_sources
    return {
        "session_id_source": source,
        "strong_session_identity": strong,
        "fallback_session_identity": fallback,
        "risk": "workspace_fallback_may_merge_multiple_codex_tasks" if fallback else "",
    }


AUTO_BUDGET_QUERY_TYPES = {
    "current_state",
    "latest",
    "profile_memory",
    "multi_hop",
    "date",
    "broad_exploration",
    "evidence",
    "benchmark_quality",
}

FEATURE_MEMORY_BUDGET_QUERY_RE = re.compile(
    r"\b(?:mem0|feature parity|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionalit(?:y|ies)|algorithms?|memory feature|session memory|profile memory|cross[- ]session memory|long[- ]term memory|threshold|idle batch|batch extraction)\b"
)


def _explicit_cross_session_requested(args: Json, ranking: Json) -> bool:
    raw = args.get("cross_session", ranking.get("cross_session"))
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, dict):
        return bool(raw.get("enabled"))
    return False


def feature_profile_memory_budget_query(args: Json, ranking: Json, *, question_type: str = "fact") -> bool:
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type == "profile_memory":
        return True
    query = str(args.get("query") or ranking.get("query") or "").strip()
    if not query:
        return False
    lower = query.lower()
    return bool(
        PROFILE_MEMORY_QUERY_RE.search(lower)
        or PROFILE_MEMORY_STANDING_RULE_QUERY_RE.search(lower)
        or FEATURE_MEMORY_BUDGET_QUERY_RE.search(lower)
        or feature_scope_excludes_outcome_evidence(query)
    )


def effective_retrieval_question_type(query: str, requested_question_type: Any = "") -> str:
    question_type = str(requested_question_type or infer_query_type(query)).strip().lower()
    if question_type in {"", "fact"} and PROFILE_MEMORY_STANDING_RULE_QUERY_RE.search(query.lower()):
        return "profile_memory"
    return question_type or "fact"


def _default_memory_budget_mode(args: Json, ranking: Json, *, field: str, question_type: str) -> str:
    mode = str(args.get(field) or ranking.get(field) or "").strip().lower()
    if mode:
        return mode
    normalized_question_type = str(question_type or "fact").strip().lower()
    if (
        normalized_question_type in AUTO_BUDGET_QUERY_TYPES
        or feature_profile_memory_budget_query(args, ranking, question_type=question_type)
        or _explicit_cross_session_requested(args, ranking)
    ):
        return "auto"
    return ""


def codex_outcome_budget_query(args: Json, ranking: Json, *, question_type: str = "fact") -> bool:
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type not in {"evidence", "current_state", "latest", "benchmark_quality", "profile_memory", "multi_hop", "date"}:
        return False
    query = str(args.get("query") or ranking.get("query") or "").strip()
    if not query:
        return False
    lower = query.lower()
    return bool(CODEX_OUTCOME_QUERY_RE.search(lower) or re.search(
        r"\b(?:assistant decision|tool evidence|validation evidence|pushed commit|blocked work|next action|what did codex|what was done)\b",
        lower,
    ))


def codex_user_goal_budget_query(args: Json, ranking: Json, *, question_type: str = "fact") -> bool:
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type not in {"profile_memory", "current_state", "latest", "multi_hop", "date"}:
        return False
    query = str(args.get("query") or ranking.get("query") or "").strip()
    if not query:
        return False
    lower = query.lower()
    return bool(
        re.search(
            r"\b(?:what|which|show|list|recall|remember|find)\b.{0,80}\b(?:goal|task|plan|requirement|request|asked|ask|instruction|directive)\b",
            lower,
        )
        or re.search(
            r"\b(?:goal|task|plan|requirement|request|instruction|directive)\b.{0,80}\b(?:codex|implement|fix|add|remove|replace|move|build|work)\b",
            lower,
        )
        or re.search(r"\b(?:what did i ask|what have i asked|user asked|user request|current plan)\b", lower)
    )


def feature_scope_budget_query(args: Json, ranking: Json) -> bool:
    query = str(args.get("query") or ranking.get("query") or ranking.get("question") or "")
    return feature_scope_excludes_outcome_evidence(query)


def auto_source_role_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="source_role_budget_mode",
        question_type=question_type,
    )
    if mode not in {"auto", "balanced", "codex_auto"}:
        return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = optional_object(args, "source_role_budget_fractions") or optional_object(ranking, "source_role_budget_fractions")
    defaults = {"assistant": 0.45, "tool": 0.35, "user": 0.60}
    normalized_question_type = str(question_type or "fact").strip().lower()
    if codex_outcome_budget_query(args, ranking, question_type=question_type):
        defaults.update({"assistant": 0.55, "tool": 0.55, "user": 0.40})
    elif codex_user_goal_budget_query(args, ranking, question_type=question_type):
        defaults.update({"assistant": 0.35, "tool": 0.25, "user": 0.70})
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update({"assistant": 0.50, "tool": 0.40, "user": 0.50})
    elif normalized_question_type == "profile_memory":
        defaults.update({"assistant": 0.50, "tool": 0.45, "user": 0.50})
    elif normalized_question_type == "evidence":
        defaults.update({"assistant": 0.35, "tool": 0.50, "user": 0.45})
    elif normalized_question_type == "benchmark_quality":
        defaults.update({"assistant": 0.50, "tool": 0.60, "user": 0.30})
    elif normalized_question_type in {"broad_exploration", "multi_hop", "date"}:
        defaults.update({"assistant": 0.45, "tool": 0.45, "user": 0.50})
    if feature_scope_budget_query(args, ranking):
        defaults["tool"] = 0.0
    budgets: Json = {}
    for role, default_fraction in defaults.items():
        raw_fraction = fractions.get(role, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        if fraction <= 0.0:
            continue
        amount = max(1, int(remote_budget * fraction))
        if amount:
            budgets[role] = amount
    return budgets, mode


def memory_layer_budget_question_reason(question_type: str) -> str:
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type == "profile_memory":
        return "profile_memory_queries_prioritize user_profile entities, profile summaries, and cross-session bridges"
    if normalized_question_type in {"current_state", "latest"}:
        return "current_state_or_latest_queries_prioritize_profile_entity and cross-session current state"
    if normalized_question_type in {"multi_hop", "date"}:
        return "multi_hop_or_date_queries_expand cross-session events, segments, summaries, and profile bridges"
    if normalized_question_type == "benchmark_quality":
        return "benchmark_quality_queries_prioritize tool evidence, assistant outcomes, quality metrics, and cross-session/profile summaries"
    if normalized_question_type in {"broad_exploration", "evidence"}:
        return "broad_or_evidence_queries_expand summaries, cross-session evidence, and profile bridges"
    return "normal_queries_keep_profile_and_cross_session_budget compact so same-session context dominates"


def auto_memory_selection_policy_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="memory_selection_policy_budget_mode",
        question_type=question_type,
    )
    if mode not in {"auto", "balanced", "codex_auto"}:
        sibling_mode = str(
            args.get("source_role_budget_mode")
            or ranking.get("source_role_budget_mode")
            or args.get("memory_layer_budget_mode")
            or ranking.get("memory_layer_budget_mode")
            or ""
        ).strip().lower()
        if sibling_mode in {"auto", "balanced", "codex_auto"}:
            mode = sibling_mode
        else:
            return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = (
        optional_object(args, "memory_selection_policy_budget_fractions")
        or optional_object(ranking, "memory_selection_policy_budget_fractions")
    )
    defaults = {
        "selected_user_prompt": 0.45,
        "selected_user_profile_fact": 0.35,
        "selected_assistant_profile_fact": 0.35,
        "selected_assistant_decision_outcome_only": 0.30,
        "selected_tool_evidence_only": 0.30,
    }
    normalized_question_type = str(question_type or "fact").strip().lower()
    if codex_outcome_budget_query(args, ranking, question_type=question_type):
        defaults.update(
            {
                "selected_user_prompt": 0.35,
                "selected_user_profile_fact": 0.45,
                "selected_assistant_profile_fact": 0.35,
                "selected_assistant_decision_outcome_only": 0.58,
                "selected_tool_evidence_only": 0.55,
                "selected_profile_current_state": 0.55,
            }
        )
    elif codex_user_goal_budget_query(args, ranking, question_type=question_type):
        defaults.update(
            {
                "selected_user_prompt": 0.70,
                "selected_user_profile_fact": 0.55,
                "selected_assistant_profile_fact": 0.45,
                "selected_assistant_decision_outcome_only": 0.30,
                "selected_tool_evidence_only": 0.25,
                "selected_profile_current_state": 0.55,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update(
            {
                "selected_user_prompt": 0.40,
                "selected_user_profile_fact": 0.60,
                "selected_assistant_profile_fact": 0.55,
                "selected_assistant_decision_outcome_only": 0.45,
                "selected_tool_evidence_only": 0.30,
                "selected_profile_current_state": 0.50,
            }
        )
    elif normalized_question_type == "profile_memory":
        defaults.update(
            {
                "selected_user_prompt": 0.35,
                "selected_user_profile_fact": 0.70,
                "selected_assistant_profile_fact": 0.65,
                "selected_assistant_decision_outcome_only": 0.40,
                "selected_tool_evidence_only": 0.30,
                "selected_profile_current_state": 0.65,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        defaults.update(
            {
                "selected_user_prompt": 0.25,
                "selected_user_profile_fact": 0.35,
                "selected_assistant_profile_fact": 0.30,
                "selected_assistant_decision_outcome_only": 0.50,
                "selected_tool_evidence_only": 0.65,
                "selected_profile_current_state": 0.40,
            }
        )
    elif normalized_question_type in {"multi_hop", "date", "broad_exploration", "evidence"}:
        defaults.update(
            {
                "selected_user_prompt": 0.35,
                "selected_user_profile_fact": 0.45,
                "selected_assistant_profile_fact": 0.45,
                "selected_assistant_decision_outcome_only": 0.45,
                "selected_tool_evidence_only": 0.50,
            }
        )
    if feature_scope_budget_query(args, ranking):
        defaults["selected_assistant_decision_outcome_only"] = 0.0
        defaults["selected_tool_evidence_only"] = 0.0
    budgets: Json = {}
    for policy, default_fraction in defaults.items():
        raw_fraction = fractions.get(policy, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        if fraction <= 0.0:
            continue
        amount = max(1, int(remote_budget * fraction))
        if amount:
            budgets[policy] = amount
    return budgets, mode


def codex_outcome_event_segment_layer_fractions(question_type: str, *, outcome_query: bool = False) -> Json:
    normalized_question_type = str(question_type or "fact").strip().lower()
    defaults: Json = {
        "same_session_codex_outcome_event": 0.22,
        "cross_session_codex_outcome_event": 0.20,
        "same_session_codex_outcome_segment": 0.20,
        "cross_session_codex_outcome_segment": 0.18,
    }
    if outcome_query:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.45,
                "cross_session_codex_outcome_event": 0.42,
                "same_session_codex_outcome_segment": 0.38,
                "cross_session_codex_outcome_segment": 0.36,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.35,
                "cross_session_codex_outcome_event": 0.30,
                "same_session_codex_outcome_segment": 0.30,
                "cross_session_codex_outcome_segment": 0.28,
            }
        )
    elif normalized_question_type == "profile_memory":
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.25,
                "cross_session_codex_outcome_event": 0.35,
                "same_session_codex_outcome_segment": 0.22,
                "cross_session_codex_outcome_segment": 0.32,
            }
        )
    elif normalized_question_type in {"multi_hop", "date"}:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.35,
                "cross_session_codex_outcome_event": 0.35,
                "same_session_codex_outcome_segment": 0.32,
                "cross_session_codex_outcome_segment": 0.32,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.42,
                "cross_session_codex_outcome_event": 0.45,
                "same_session_codex_outcome_segment": 0.35,
                "cross_session_codex_outcome_segment": 0.40,
            }
        )
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.38,
                "cross_session_codex_outcome_event": 0.35,
                "same_session_codex_outcome_segment": 0.34,
                "cross_session_codex_outcome_segment": 0.32,
            }
        )
    return defaults


def auto_memory_layer_budget_tokens(args: Json, ranking: Json, *, remote_budget_tokens: int, question_type: str = "fact") -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="memory_layer_budget_mode",
        question_type=question_type,
    )
    if mode not in {"auto", "balanced", "codex_auto"}:
        return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = optional_object(args, "memory_layer_budget_fractions") or optional_object(ranking, "memory_layer_budget_fractions")
    defaults = {
        "summary": 0.20,
        "profile_summary": 0.30,
        "same_session_summary": 0.20,
        "cross_session_summary": 0.20,
        "compression": 0.25,
        "profile_compression": 0.25,
        "same_session_compression": 0.20,
        "cross_session_compression": 0.20,
        "pending_async_event": 0.20,
        "pending_async_codex_outcome_event": 0.20,
        "pending_async_memory_feature_event": 0.20,
        "same_session_event": 0.45,
        "same_session_memory_feature_event": 0.35,
        "cross_session_memory_feature_event": 0.25,
        "cross_session_event": 0.25,
        "same_session_segment": 0.35,
        "same_session_memory_feature_segment": 0.30,
        "cross_session_memory_feature_segment": 0.25,
        "cross_session_segment": 0.25,
        "same_session_memory_feature_entity": 0.35,
        "profile_entity": 0.40,
        "cross_session_codex_outcome_entity": 0.25,
        "cross_session_memory_feature_entity": 0.25,
        "cross_session_codex_outcome_summary": 0.25,
        "cross_session_codex_outcome_compression": 0.25,
    }
    normalized_question_type = str(question_type or "fact").strip().lower()
    outcome_query = codex_outcome_budget_query(args, ranking, question_type=question_type)
    feature_profile_query = feature_profile_memory_budget_query(args, ranking, question_type=question_type)
    if outcome_query:
        defaults.update(
            {
                "summary": 0.18,
                "profile_summary": 0.35,
                "same_session_summary": 0.18,
                "cross_session_summary": 0.32,
                "compression": 0.25,
                "profile_compression": 0.35,
                "same_session_compression": 0.20,
                "cross_session_compression": 0.32,
                "pending_async_event": 0.20,
                "pending_async_codex_outcome_event": 0.42,
                "same_session_event": 0.35,
                "cross_session_event": 0.38,
                "same_session_segment": 0.30,
                "cross_session_segment": 0.35,
                "profile_entity": 0.45,
                "cross_session_codex_outcome_entity": 0.62,
                "cross_session_memory_feature_entity": 0.35,
                "cross_session_codex_outcome_summary": 0.45,
                "cross_session_codex_outcome_compression": 0.45,
            }
        )
    elif feature_profile_query:
        defaults.update(
            {
                "summary": 0.15,
                "profile_summary": 0.50,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.45,
                "compression": 0.20,
                "profile_compression": 0.45,
                "same_session_compression": 0.15,
                "cross_session_compression": 0.40,
                "pending_async_event": 0.12,
                "pending_async_codex_outcome_event": 0.10,
                "pending_async_memory_feature_event": 0.55,
                "same_session_event": 0.25,
                "same_session_memory_feature_event": 0.55,
                "cross_session_memory_feature_event": 0.70,
                "cross_session_event": 0.35,
                "same_session_segment": 0.25,
                "same_session_memory_feature_segment": 0.50,
                "cross_session_memory_feature_segment": 0.68,
                "cross_session_segment": 0.35,
                "profile_entity": 0.65,
                "cross_session_codex_outcome_entity": 0.20,
                "cross_session_memory_feature_entity": 0.75,
                "cross_session_codex_outcome_summary": 0.20,
                "cross_session_codex_outcome_compression": 0.20,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update(
            {
                "summary": 0.15,
                "profile_summary": 0.20,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.15,
                "compression": 0.20,
                "profile_compression": 0.25,
                "same_session_compression": 0.15,
                "cross_session_compression": 0.20,
                "pending_async_event": 0.15,
                "same_session_event": 0.35,
                "cross_session_event": 0.30,
                "same_session_segment": 0.30,
                "cross_session_segment": 0.30,
                "profile_entity": 0.55,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.50,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "profile_memory":
        defaults.update(
            {
                "summary": 0.15,
                "profile_summary": 0.45,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.40,
                "compression": 0.25,
                "profile_compression": 0.40,
                "same_session_compression": 0.20,
                "cross_session_compression": 0.35,
                "pending_async_event": 0.15,
                "pending_async_memory_feature_event": 0.50,
                "same_session_event": 0.25,
                "same_session_memory_feature_event": 0.50,
                "cross_session_memory_feature_event": 0.62,
                "cross_session_event": 0.40,
                "same_session_segment": 0.25,
                "same_session_memory_feature_segment": 0.48,
                "cross_session_memory_feature_segment": 0.60,
                "cross_session_segment": 0.40,
                "profile_entity": 0.60,
                "cross_session_codex_outcome_entity": 0.30,
                "cross_session_memory_feature_entity": 0.65,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type in {"multi_hop", "date"}:
        defaults.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "same_session_summary": 0.20,
                "cross_session_summary": 0.35,
                "compression": 0.30,
                "profile_compression": 0.35,
                "same_session_compression": 0.25,
                "cross_session_compression": 0.35,
                "pending_async_event": 0.20,
                "same_session_event": 0.40,
                "cross_session_event": 0.35,
                "same_session_segment": 0.35,
                "cross_session_segment": 0.35,
                "profile_entity": 0.45,
                "cross_session_codex_outcome_entity": 0.40,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        defaults.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "same_session_summary": 0.20,
                "cross_session_summary": 0.35,
                "compression": 0.30,
                "profile_compression": 0.35,
                "same_session_compression": 0.20,
                "cross_session_compression": 0.35,
                "pending_async_event": 0.20,
                "same_session_event": 0.35,
                "cross_session_event": 0.35,
                "same_session_segment": 0.30,
                "cross_session_segment": 0.35,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.58,
                "cross_session_memory_feature_entity": 0.35,
                "cross_session_codex_outcome_summary": 0.45,
                "cross_session_codex_outcome_compression": 0.45,
            }
        )
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        defaults.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "same_session_summary": 0.25,
                "cross_session_summary": 0.30,
                "compression": 0.30,
                "profile_compression": 0.35,
                "same_session_compression": 0.30,
                "cross_session_compression": 0.30,
                "pending_async_event": 0.25,
                "same_session_event": 0.45,
                "cross_session_event": 0.30,
                "same_session_segment": 0.40,
                "cross_session_segment": 0.30,
                "profile_entity": 0.45,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    defaults.update(
        codex_outcome_event_segment_layer_fractions(
            normalized_question_type,
            outcome_query=outcome_query,
        )
    )
    if feature_scope_budget_query(args, ranking):
        for outcome_layer in [
            "same_session_codex_outcome_event",
            "pending_async_codex_outcome_event",
            "cross_session_codex_outcome_event",
            "same_session_codex_outcome_segment",
            "cross_session_codex_outcome_segment",
            "cross_session_codex_outcome_entity",
            "cross_session_codex_outcome_summary",
            "cross_session_codex_outcome_compression",
        ]:
            defaults[outcome_layer] = 0.0
    defaults["cross_session_memory_feature_summary"] = max(
        defaults.get("cross_session_memory_feature_entity", 0.25),
        defaults.get("profile_summary", 0.30),
    )
    defaults["cross_session_memory_feature_compression"] = max(
        defaults.get("cross_session_memory_feature_entity", 0.25),
        defaults.get("profile_compression", 0.25),
    )
    defaults["same_session_memory_feature_summary"] = max(
        defaults.get("same_session_memory_feature_entity", 0.25),
        defaults.get("same_session_summary", 0.20),
    )
    defaults["same_session_memory_feature_compression"] = max(
        defaults.get("same_session_memory_feature_entity", 0.25),
        defaults.get("same_session_compression", 0.20),
    )
    budgets: Json = {}
    for layer, default_fraction in defaults.items():
        raw_fraction = fractions.get(layer, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        if fraction <= 0.0:
            continue
        amount = max(1, int(remote_budget * fraction))
        if amount:
            budgets[layer] = amount
    return budgets, mode


def auto_extraction_phase_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="extraction_phase_budget_mode",
        question_type=question_type,
    )
    if not mode:
        sibling_mode = str(
            args.get("source_role_budget_mode")
            or ranking.get("source_role_budget_mode")
            or args.get("memory_layer_budget_mode")
            or ranking.get("memory_layer_budget_mode")
            or args.get("memory_selection_policy_budget_mode")
            or ranking.get("memory_selection_policy_budget_mode")
            or ""
        ).strip().lower()
        if sibling_mode in {"auto", "balanced", "codex_auto"}:
            mode = sibling_mode
    if mode not in {"auto", "balanced", "codex_auto"}:
        return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = optional_object(args, "extraction_phase_budget_fractions") or optional_object(
        ranking,
        "extraction_phase_budget_fractions",
    )
    defaults = {
        "pending_async": 0.12,
        "provisional": 0.25,
        "final": 0.70,
    }
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type in {"current_state", "latest"}:
        defaults.update({"pending_async": 0.12, "provisional": 0.25, "final": 0.75})
    elif normalized_question_type == "profile_memory":
        defaults.update({"pending_async": 0.10, "provisional": 0.20, "final": 0.80})
    elif normalized_question_type in {"multi_hop", "date"}:
        defaults.update({"pending_async": 0.15, "provisional": 0.30, "final": 0.70})
    elif normalized_question_type == "benchmark_quality":
        defaults.update({"pending_async": 0.12, "provisional": 0.25, "final": 0.75})
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        defaults.update({"pending_async": 0.15, "provisional": 0.35, "final": 0.70})
    budgets: Json = {}
    for phase, default_fraction in defaults.items():
        raw_fraction = fractions.get(phase, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        if fraction <= 0.0:
            continue
        amount = max(1, int(remote_budget * fraction))
        if amount:
            budgets[phase] = amount
    return budgets, mode


def pre_retrieval_summary_refresh_memory_layer_budget_tokens(
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
    outcome_query: bool = False,
    args: Json | None = None,
    ranking: Json | None = None,
) -> tuple[Json, str]:
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    normalized_question_type = str(question_type or "fact").strip().lower()
    mode = "pre_retrieval_summary_refresh_balanced"
    if normalized_question_type in {"current_state", "latest"}:
        mode = "pre_retrieval_summary_refresh_current_state"
    elif normalized_question_type == "profile_memory":
        mode = "pre_retrieval_summary_refresh_profile_memory"
    elif normalized_question_type in {"multi_hop", "date"}:
        mode = "pre_retrieval_summary_refresh_multi_hop"
    elif normalized_question_type == "benchmark_quality":
        mode = "pre_retrieval_summary_refresh_benchmark_quality"
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        mode = "pre_retrieval_summary_refresh_evidence"
    if remote_budget <= 0:
        return {}, mode
    args = args if isinstance(args, dict) else {}
    ranking = ranking if isinstance(ranking, dict) else {}
    feature_profile_query = feature_profile_memory_budget_query(args, ranking, question_type=question_type)
    fractions = {
        "summary": 0.15,
        "profile_summary": 0.30,
        "same_session_summary": 0.20,
        "cross_session_summary": 0.25,
        "compression": 0.20,
        "profile_compression": 0.25,
        "same_session_compression": 0.20,
        "cross_session_compression": 0.25,
        "pending_async_event": 0.20,
        "pending_async_codex_outcome_event": 0.20,
        "pending_async_memory_feature_event": 0.20,
        "same_session_event": 0.45,
        "same_session_memory_feature_event": 0.35,
        "cross_session_memory_feature_event": 0.25,
        "cross_session_event": 0.25,
        "same_session_segment": 0.30,
        "same_session_memory_feature_segment": 0.30,
        "cross_session_memory_feature_segment": 0.25,
        "cross_session_segment": 0.25,
        "same_session_memory_feature_entity": 0.35,
        "profile_entity": 0.45,
        "cross_session_codex_outcome_entity": 0.25,
        "cross_session_memory_feature_entity": 0.25,
        "cross_session_codex_outcome_summary": 0.25,
        "cross_session_codex_outcome_compression": 0.25,
    }
    if feature_profile_query:
        fractions.update(
            {
                "summary": 0.15,
                "profile_summary": 0.50,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.45,
                "profile_compression": 0.45,
                "cross_session_compression": 0.40,
                "pending_async_codex_outcome_event": 0.10,
                "pending_async_memory_feature_event": 0.55,
                "same_session_event": 0.25,
                "same_session_memory_feature_event": 0.55,
                "cross_session_memory_feature_event": 0.70,
                "cross_session_event": 0.35,
                "same_session_segment": 0.25,
                "same_session_memory_feature_segment": 0.50,
                "cross_session_memory_feature_segment": 0.68,
                "cross_session_segment": 0.35,
                "profile_entity": 0.65,
                "cross_session_codex_outcome_entity": 0.20,
                "cross_session_memory_feature_entity": 0.75,
                "cross_session_codex_outcome_summary": 0.20,
                "cross_session_codex_outcome_compression": 0.20,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        fractions.update(
            {
                "profile_summary": 0.35,
                "cross_session_summary": 0.30,
                "profile_compression": 0.35,
                "cross_session_compression": 0.30,
                "cross_session_event": 0.30,
                "cross_session_segment": 0.30,
                "profile_entity": 0.55,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.50,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "profile_memory":
        fractions.update(
            {
                "summary": 0.15,
                "profile_summary": 0.45,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.40,
                "profile_compression": 0.40,
                "cross_session_compression": 0.35,
                "pending_async_codex_outcome_event": 0.15,
                "pending_async_memory_feature_event": 0.50,
                "same_session_event": 0.25,
                "same_session_memory_feature_event": 0.50,
                "cross_session_memory_feature_event": 0.62,
                "cross_session_event": 0.40,
                "same_session_segment": 0.25,
                "same_session_memory_feature_segment": 0.48,
                "cross_session_memory_feature_segment": 0.60,
                "cross_session_segment": 0.40,
                "profile_entity": 0.60,
                "cross_session_codex_outcome_entity": 0.30,
                "cross_session_memory_feature_entity": 0.65,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type in {"multi_hop", "date"}:
        fractions.update(
            {
                "cross_session_summary": 0.35,
                "cross_session_compression": 0.35,
                "cross_session_event": 0.35,
                "cross_session_segment": 0.35,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.40,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        fractions.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "cross_session_summary": 0.35,
                "profile_compression": 0.35,
                "cross_session_compression": 0.35,
                "cross_session_event": 0.35,
                "cross_session_segment": 0.35,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.58,
                "cross_session_memory_feature_entity": 0.35,
                "cross_session_codex_outcome_summary": 0.45,
                "cross_session_codex_outcome_compression": 0.45,
            }
        )
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        fractions.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "cross_session_summary": 0.30,
                "profile_compression": 0.30,
                "cross_session_compression": 0.30,
                "same_session_event": 0.35,
                "cross_session_event": 0.30,
                "same_session_segment": 0.35,
                "cross_session_segment": 0.30,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    fractions.update(
        codex_outcome_event_segment_layer_fractions(
            normalized_question_type,
            outcome_query=outcome_query,
        )
    )
    if feature_scope_budget_query(args, ranking):
        for outcome_layer in [
            "same_session_codex_outcome_event",
            "cross_session_codex_outcome_event",
            "same_session_codex_outcome_segment",
            "cross_session_codex_outcome_segment",
            "cross_session_codex_outcome_entity",
            "cross_session_codex_outcome_summary",
            "cross_session_codex_outcome_compression",
        ]:
            fractions[outcome_layer] = 0.0
    fractions["cross_session_memory_feature_summary"] = max(
        fractions.get("cross_session_memory_feature_entity", 0.25),
        fractions.get("profile_summary", 0.30),
    )
    fractions["cross_session_memory_feature_compression"] = max(
        fractions.get("cross_session_memory_feature_entity", 0.25),
        fractions.get("profile_compression", 0.25),
    )
    fractions["same_session_memory_feature_summary"] = max(
        fractions.get("same_session_memory_feature_entity", 0.25),
        fractions.get("same_session_summary", 0.20),
    )
    fractions["same_session_memory_feature_compression"] = max(
        fractions.get("same_session_memory_feature_entity", 0.25),
        fractions.get("same_session_compression", 0.20),
    )
    return {
        layer: max(1, int(remote_budget * fraction))
        for layer, fraction in fractions.items()
        if fraction > 0.0
    }, mode


def auto_memory_selection_policy_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    return shared_auto_memory_selection_policy_budget_tokens(
        args,
        ranking,
        remote_budget_tokens=remote_budget_tokens,
        question_type=question_type,
    )


def auto_memory_layer_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    return shared_auto_memory_layer_budget_tokens(
        args,
        ranking,
        remote_budget_tokens=remote_budget_tokens,
        question_type=question_type,
    )


def auto_extraction_phase_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    return shared_auto_extraction_phase_budget_tokens(
        args,
        ranking,
        remote_budget_tokens=remote_budget_tokens,
        question_type=question_type,
    )


def pre_retrieval_summary_refresh_memory_layer_budget_tokens(
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
    outcome_query: bool = False,
    args: Json | None = None,
    ranking: Json | None = None,
) -> tuple[Json, str]:
    del outcome_query
    return shared_pre_retrieval_summary_refresh_memory_layer_budget_tokens(
        remote_budget_tokens=remote_budget_tokens,
        question_type=question_type,
        args=args,
        ranking=ranking,
    )


def pre_retrieval_summary_refresh_enabled(args: Json, ranking: Json) -> bool:
    value = (
        args.get("pre_retrieval_summary_refresh")
        if "pre_retrieval_summary_refresh" in args
        else ranking.get("pre_retrieval_summary_refresh")
        if "pre_retrieval_summary_refresh" in ranking
        else PRE_RETRIEVAL_SUMMARY_REFRESH
    )
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "auto", "bounded"}
    return bool(value)


def pre_retrieval_summary_refresh_explicitly_configured(args: Json, ranking: Json) -> bool:
    return "pre_retrieval_summary_refresh" in args or "pre_retrieval_summary_refresh" in ranking


def pre_retrieval_summary_refresh_limit(args: Json, ranking: Json) -> int:
    raw_limit = (
        args.get("pre_retrieval_summary_refresh_limit")
        or ranking.get("pre_retrieval_summary_refresh_limit")
        or PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT
    )
    try:
        return max(1, int(raw_limit))
    except (TypeError, ValueError):
        return PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT


def session_event_message_count(records: list[Json]) -> int:
    return sum(len(messages_from_event_record(record)) for record in records)


def session_events_by_message_limit(records: list[Json], limit: int | None) -> list[Json]:
    if limit is None:
        return records
    selected: list[Json] = []
    message_count = 0
    for record in records:
        selected.append(record)
        message_count += max(1, len(messages_from_event_record(record)))
        if message_count >= limit:
            break
    return selected


def deferred_idle_auto_batch_result(
    *,
    idle_commit_result: Json | None,
    pending_event_count: int,
    pending_message_count: int,
    threshold_messages: int,
    idle_commit_timeout_ms: int | None,
) -> Json | None:
    if not isinstance(idle_commit_result, dict):
        return None
    if idle_commit_result.get("status") != "deferred":
        return None
    if str(idle_commit_result.get("commit_reason") or "") != "idle_timeout":
        return None
    if idle_commit_timeout_ms is None or idle_commit_timeout_ms <= 0:
        return None
    return {
        "status": "deferred",
        "trigger_policy": "idle_timeout",
        "commit_reason": "idle_timeout",
        "reason": "session_buffer_idle_deadline_armed",
        "pending_event_count": pending_event_count,
        "pending_message_count": pending_message_count,
        "threshold_messages": threshold_messages,
        "idle_commit_timeout_ms": idle_commit_timeout_ms,
        "idle_elapsed_ms": idle_commit_result.get("idle_elapsed_ms", 0),
        "idle_commit_scheduled": pending_event_count > 0,
        "extraction_phase": "provisional",
        "final_session_boundary": False,
        "trigger_evidence": {
            **(idle_commit_result.get("trigger_evidence") if isinstance(idle_commit_result.get("trigger_evidence"), dict) else {}),
            "pending_event_count": pending_event_count,
            "pending_message_count": pending_message_count,
            "threshold_messages": threshold_messages,
            "threshold_ready": False,
            "idle_timeout_ms": idle_commit_timeout_ms,
            "idle_ready": False,
            "force": False,
            "commit_reason": "idle_timeout",
        },
    }


def idle_commit_scheduled_task_record(
    *,
    event_id_hash: int,
    node_hash: int,
    node_path: list[str],
    scope: Json,
    storage_options: Json | None = None,
    ingestion_time_ms: int,
    idle_commit_timeout_ms: int,
    pending_event_count: int,
    pending_message_count: int,
    threshold_messages: int,
) -> Json:
    deadline_ms = int(ingestion_time_ms or 0) + max(0, int(idle_commit_timeout_ms or 0))
    requested_storage_options = dict(storage_options or {})
    return {
        "record_type": "matrixark_async_pipeline_task",
        "task_hash": stable_hash(f"async_pipeline_idle_commit:{event_id_hash}"),
        "event_id_hash": event_id_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "scope": scope,
        "status": "idle_commit_scheduled",
        "stages": ["extraction", "summary", "compression", "embedding"],
        "reason": "session_buffer_idle_deadline",
        "trigger_policy": "idle_timeout",
        "auto_batch_extract": True,
        "threshold_messages": threshold_messages,
        "idle_commit_timeout_ms": idle_commit_timeout_ms,
        "idle_commit_deadline_ms": deadline_ms,
        "idle_commit_cutoff_ms": int(ingestion_time_ms or 0),
        "idle_commit_pending_event_count": pending_event_count,
        "idle_commit_pending_message_count": pending_message_count,
        "requested_storage_options": requested_storage_options,
        "storage_options": requested_storage_options,
        "source_extraction_phases": ["provisional"],
        "extraction_phase": "provisional",
        "final_session_boundary": False,
        "created_at_ms": int(ingestion_time_ms or 0),
        "updated_at_ms": int(ingestion_time_ms or 0),
    }


ASSISTANT_PROFILE_FACT_LINEAGE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\b(?:user|you)\b.{0,96}\b(?:prefer|prefers|preference|likes|wants|needs|asked|requires|required|always|never|avoid|remember)\b",
        r"\b(?:i(?:'ll| will)? remember|remembered|noted|got it|understood)\b.{0,140}\b(?:prefer|preference|want|need|always|never|avoid|profile|memory|workspace|repo|branch|reply|respond|format|style)\b",
        r"\b(?:i(?:'ll| will)|codex will|assistant will)\b.{0,64}\b(?:remember|keep|use|follow|prefer|avoid|not use|always use|make sure)\b",
        r"\b(?:standing instruction|standing preference|user profile|long[- ]term memor(?:y|ies)|saved preference|persistent instruction)\b",
        r"\b(?:call me|my name is|user(?:'s)? name is|user goes by|pronouns?|address (?:me|the user))\b",
        r"\b(?:reply|respond|answer|write|communication style|response style|answer style|preferred language|preferred format|timezone|time zone|locale)\b.{0,120}\b(?:concise|brief|detailed|bullets?|markdown|language|tone|style|format|timezone|locale)\b",
        r"\b(?:workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|build|deploy|deployment|rustraft|temporalstore|matrixark)\b.{0,140}\b(?:always|prefer|use|keep|must|should|avoid|never|don't|push|build|deploy)\b",
        r"\b(?:i(?:'ll| will)|codex will|assistant will|going forward|from now on)\b.{0,80}\b(?:use|keep|follow|prefer|avoid|never use|not use|always use|push|build|deploy)\b.{0,140}\b(?:workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|build|deploy|deployment|rustraft|temporalstore|matrixark)\b",
    ]
]


def assistant_profile_fact_lineage_text(text: Any) -> bool:
    compact = " ".join(str(text or "").split())
    return bool(compact and any(pattern.search(compact) for pattern in ASSISTANT_PROFILE_FACT_LINEAGE_PATTERNS))


def user_profile_fact_lineage_text(text: Any) -> bool:
    compact = " ".join(str(text or "").split())
    return bool(compact and any(pattern.search(compact) for pattern in ASSISTANT_PROFILE_FACT_LINEAGE_PATTERNS))


def context_source_lineage(envelope: Json, hook: Json | None = None) -> Json:
    metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
    role_counts: Json = {}
    assistant_text_parts: list[str] = []
    user_text_parts: list[str] = []
    for message in envelope.get("messages", []):
        if not isinstance(message, dict):
            continue
        role = normalize_message_role(message.get("role"))
        if role:
            role_counts[role] = int(role_counts.get(role, 0)) + 1
        if role == "assistant":
            assistant_text_parts.append(str(message.get("content") or ""))
        elif role == "user":
            user_text_parts.append(str(message.get("content") or ""))
    roles = set(role_counts)
    metadata_scalar_role = normalize_message_role(metadata.get("source_role"))
    if metadata_scalar_role:
        roles.add(metadata_scalar_role)
        role_counts[metadata_scalar_role] = max(1, int(role_counts.get(metadata_scalar_role, 0)))
    for value in metadata.get("source_roles", []) if isinstance(metadata.get("source_roles"), list) else []:
        role = normalize_message_role(value)
        if role:
            roles.add(role)
            role_counts[role] = max(1, int(role_counts.get(role, 0)))
    if isinstance(metadata.get("source_role_counts"), dict):
        for value, count in metadata["source_role_counts"].items():
            role = normalize_message_role(value)
            if not role:
                continue
            try:
                amount = max(0, int(count or 0))
            except (TypeError, ValueError):
                amount = 0
            if amount:
                roles.add(role)
                role_counts[role] = max(int(role_counts.get(role, 0)), amount)
    hook_types = set()
    hook_type = str(metadata.get("hook_type") or "").strip()
    if hook_type:
        hook_types.add(hook_type)
    for value in metadata.get("source_hook_types", []) if isinstance(metadata.get("source_hook_types"), list) else []:
        if str(value or "").strip():
            hook_types.add(str(value).strip())
    codex_events = set()
    codex_event = str(metadata.get("codex_event") or "").strip()
    if codex_event:
        codex_events.add(codex_event)
    for value in metadata.get("source_codex_events", []) if isinstance(metadata.get("source_codex_events"), list) else []:
        if str(value or "").strip():
            codex_events.add(str(value).strip())
    agent_event = str(metadata.get("agent_event") or "").strip()
    if agent_event:
        codex_events.add(agent_event)
    if isinstance(hook, dict):
        hook_type = str(hook.get("hook_type") or "").strip()
        if hook_type:
            hook_types.add(hook_type)
        trigger = str(hook.get("trigger") or "").strip()
        if trigger:
            codex_events.add(trigger)
    if not hook_types:
        for event in sorted(codex_events):
            legacy_hook_type = legacy_hook_type_from_codex_event(event)
            if legacy_hook_type:
                hook_types.add(legacy_hook_type)
    source_lineage_count = max(1, sum(int(value or 0) for value in role_counts.values()))
    hook_type_counts: Json = {}
    if isinstance(metadata.get("source_hook_type_counts"), dict):
        for value, count in metadata["source_hook_type_counts"].items():
            hook_name = str(value or "").strip()
            if not hook_name:
                continue
            try:
                amount = max(0, int(count or 0))
            except (TypeError, ValueError):
                amount = 0
            if amount:
                hook_types.add(hook_name)
                hook_type_counts[hook_name] = max(int(hook_type_counts.get(hook_name, 0)), amount)
    for hook_name in hook_types:
        hook_type_counts.setdefault(hook_name, source_lineage_count)
    codex_event_counts: Json = {}
    if isinstance(metadata.get("source_codex_event_counts"), dict):
        for value, count in metadata["source_codex_event_counts"].items():
            event_name = str(value or "").strip()
            if not event_name:
                continue
            try:
                amount = max(0, int(count or 0))
            except (TypeError, ValueError):
                amount = 0
            if amount:
                codex_events.add(event_name)
                codex_event_counts[event_name] = max(int(codex_event_counts.get(event_name, 0)), amount)
    for event_name in codex_events:
        codex_event_counts.setdefault(event_name, source_lineage_count)
    memory_selection_policy_counts: Json = {}
    explicit_policy_counts = (
        metadata.get("source_memory_selection_policy_counts")
        if isinstance(metadata.get("source_memory_selection_policy_counts"), dict)
        else {}
    )
    for policy, count in explicit_policy_counts.items():
        policy_name = str(policy or "").strip()
        if not policy_name:
            continue
        try:
            amount = max(0, int(count or 0))
        except (TypeError, ValueError):
            amount = 0
        if amount:
            memory_selection_policy_counts[policy_name] = int(memory_selection_policy_counts.get(policy_name, 0)) + amount
    for policy in metadata.get("source_memory_selection_policies", []) if isinstance(metadata.get("source_memory_selection_policies"), list) else []:
        policy_name = str(policy or "").strip()
        if policy_name and policy_name not in memory_selection_policy_counts:
            memory_selection_policy_counts[policy_name] = 1
    selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
    if isinstance(selection.get("policies"), list):
        for policy in selection.get("policies", []):
            policy_name = str(policy or "").strip()
            if policy_name and policy_name not in memory_selection_policy_counts:
                memory_selection_policy_counts[policy_name] = 1
    selection_policy = str(selection.get("policy") or "").strip()
    if selection_policy and selection_policy not in memory_selection_policy_counts:
        memory_selection_policy_counts[selection_policy] = 1
    selection_lossy_count = 0
    selection_complete_count = 0
    selection_retained_text_ratio_sum = 0.0
    selection_retained_line_ratio_sum = 0.0
    selection_stats_count = 0
    selection_dropped_text_chars = 0
    selection_dropped_line_count = 0
    if selection:
        try:
            selection_dropped_text_chars += max(0, int(selection.get("dropped_text_chars") or 0))
        except (TypeError, ValueError):
            pass
        try:
            selection_dropped_line_count += max(0, int(selection.get("dropped_line_count") or 0))
        except (TypeError, ValueError):
            pass
        try:
            selection_retained_text_ratio_sum += float(selection.get("retained_text_ratio"))
            selection_retained_line_ratio_sum += float(selection.get("retained_line_ratio"))
            selection_stats_count += 1
        except (TypeError, ValueError):
            pass
        if bool(selection.get("selection_lossy")):
            selection_lossy_count += 1
        else:
            selection_complete_count += 1
    assistant_policies: list[str] = []
    assistant_lineage_text = "\n".join(assistant_text_parts) or metadata.get("text") or envelope.get("text")
    assistant_feature_memory_only = feature_scope_excludes_outcome_evidence(assistant_lineage_text)
    if assistant_profile_fact_lineage_text(assistant_lineage_text):
        assistant_policies.append("selected_assistant_profile_fact")
    if assistant_lineage_text and not assistant_feature_memory_only:
        assistant_policies.append("selected_assistant_decision_outcome_only")
    if not assistant_policies:
        assistant_policies.append(
            "selected_assistant_profile_fact"
            if assistant_feature_memory_only
            else "selected_assistant_decision_outcome_only"
        )
    user_lineage_text = "\n".join(user_text_parts) or (metadata.get("text") if "user" in roles else "")
    user_policies = ["selected_user_prompt"]
    if user_profile_fact_lineage_text(user_lineage_text):
        user_policies.append("selected_user_profile_fact")
    inferred_policy_by_role = {
        "assistant": assistant_policies,
        "tool": "selected_tool_evidence_only",
        "user": user_policies,
    }
    for role, count in role_counts.items():
        policies = inferred_policy_by_role.get(role)
        if isinstance(policies, str):
            policies = [policies]
        if not policies:
            continue
        for policy in policies:
            if not policy or policy in memory_selection_policy_counts:
                continue
            memory_selection_policy_counts[policy] = max(1, int(count or 0))
    entity_type = ""
    if "tool" in roles:
        entity_type = "tool_evidence"
    elif "assistant" in roles:
        entity_type = "memory_feature_profile" if assistant_feature_memory_only else "assistant_decision"
    memory_layer_counts: Json = {}
    for layer in metadata.get("source_memory_layers", []) if isinstance(metadata.get("source_memory_layers"), list) else []:
        layer_name = str(layer or "").strip()
        if layer_name:
            memory_layer_counts[layer_name] = int(memory_layer_counts.get(layer_name, 0)) + source_lineage_count
    if isinstance(metadata.get("source_memory_layer_counts"), dict):
        for layer, count in metadata["source_memory_layer_counts"].items():
            layer_name = str(layer or "").strip()
            if not layer_name:
                continue
            try:
                amount = max(0, int(count or 0))
            except (TypeError, ValueError):
                amount = 0
            if amount:
                memory_layer_counts[layer_name] = int(memory_layer_counts.get(layer_name, 0)) + amount
    explicit_memory_layer = str(metadata.get("memory_layer") or "").strip()
    if explicit_memory_layer:
        memory_layer_counts.setdefault(explicit_memory_layer, source_lineage_count)
    inferred_memory_layer = candidate_memory_layer_name(
        {
            "record_type": "context_event",
            "ref_type": "event",
            "memory_scope": "session",
            "session_continuity": "same_session",
            "entity_type": entity_type,
            "event_type": entity_type,
        }
    )
    if inferred_memory_layer:
        memory_layer_counts.setdefault(inferred_memory_layer, source_lineage_count)
    return {
        "memory_scope": "session",
        "session_continuity": "same_session",
        **({"entity_type": entity_type} if entity_type else {}),
        "source_roles": sorted(roles),
        "source_role_counts": {role: int(role_counts.get(role, 0)) for role in sorted(roles) if int(role_counts.get(role, 0)) > 0},
        "source_hook_types": sorted(hook_types),
        "source_hook_type_counts": {name: int(hook_type_counts.get(name, 0)) for name in sorted(hook_types) if int(hook_type_counts.get(name, 0)) > 0},
        "source_codex_events": sorted(codex_events),
        "source_codex_event_counts": {name: int(codex_event_counts.get(name, 0)) for name in sorted(codex_events) if int(codex_event_counts.get(name, 0)) > 0},
        "source_memory_selection_policies": sorted(memory_selection_policy_counts),
        "source_memory_selection_policy_counts": memory_selection_policy_counts,
        "source_memory_layers": sorted(memory_layer_counts),
        "source_memory_layer_counts": memory_layer_counts,
        "source_memory_selection_lossy_count": selection_lossy_count,
        "source_memory_selection_complete_count": selection_complete_count,
        "source_memory_selection_dropped_text_chars": selection_dropped_text_chars,
        "source_memory_selection_dropped_line_count": selection_dropped_line_count,
        "source_memory_selection_retained_text_ratio_avg": round(selection_retained_text_ratio_sum / selection_stats_count, 6) if selection_stats_count else 1.0,
        "source_memory_selection_retained_line_ratio_avg": round(selection_retained_line_ratio_sum / selection_stats_count, 6) if selection_stats_count else 1.0,
    }


def context_event_type_for_message(message: Json, default_event_type: str) -> str:
    role = normalize_message_role(message.get("role")) if isinstance(message, dict) else ""
    content = str(message.get("content") or "") if isinstance(message, dict) else ""
    metadata = message.get("metadata") if isinstance(message, dict) and isinstance(message.get("metadata"), dict) else {}
    selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
    policies = {
        str(policy or "").strip()
        for policy in (selection.get("policies") if isinstance(selection.get("policies"), list) else [])
        if str(policy or "").strip()
    }
    policy = str(selection.get("policy") or "").strip()
    if policy:
        policies.add(policy)
    if role == "assistant" and feature_scope_excludes_outcome_evidence(content):
        return "memory_feature"
    if role == "user" and feature_scope_excludes_outcome_evidence(content):
        return "memory_feature"
    by_role = {
        "user": "user_prompt",
        "assistant": "assistant_response",
        "tool": "tool_evidence",
    }
    if role in by_role:
        return by_role[role]
    by_policy = {
        "selected_user_prompt": "user_prompt",
        "selected_user_profile_fact": "user_prompt",
        "selected_assistant_profile_fact": "assistant_response",
        "selected_assistant_decision_outcome_only": "assistant_response",
        "selected_tool_evidence_only": "tool_evidence",
    }
    for policy_value in policies:
        event_type = by_policy.get(policy_value)
        if event_type:
            return event_type
    return default_event_type or "conversation_event"


def memory_selection_policy_counts_for_message(message: Json, *, default_counts: Json | None = None) -> Json:
    role = normalize_message_role(message.get("role")) if isinstance(message, dict) else ""
    content = str(message.get("content") or "") if isinstance(message, dict) else ""
    metadata = message.get("metadata") if isinstance(message, dict) and isinstance(message.get("metadata"), dict) else {}
    selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
    policies: list[str] = []
    if isinstance(selection.get("policies"), list):
        policies.extend(str(policy or "").strip() for policy in selection.get("policies", []))
    selection_policy = str(selection.get("policy") or "").strip()
    if selection_policy:
        policies.append(selection_policy)
    if not policies and role == "user":
        policies.append("selected_user_prompt")
        if user_profile_fact_lineage_text(content):
            policies.append("selected_user_profile_fact")
    elif not policies and role == "assistant":
        feature_memory_only = feature_scope_excludes_outcome_evidence(content)
        if assistant_profile_fact_lineage_text(content) or feature_memory_only:
            policies.append("selected_assistant_profile_fact")
        if content and not feature_memory_only:
            policies.append("selected_assistant_decision_outcome_only")
    elif not policies and role == "tool":
        policies.append("selected_tool_evidence_only")
    counts: Json = {}
    for policy in ordered_unique_any([policy for policy in policies if policy]):
        counts[policy] = 1
    if counts:
        return counts
    return dict(default_counts or {})


def memory_selection_retention_for_message(message: Json, *, default_retention: Json | None = None) -> Json:
    metadata = message.get("metadata") if isinstance(message, dict) and isinstance(message.get("metadata"), dict) else {}
    selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
    if not selection:
        return dict(default_retention or {})
    retention: Json = {
        "source_memory_selection_lossy_count": 1 if bool(selection.get("selection_lossy")) else 0,
        "source_memory_selection_complete_count": 0 if bool(selection.get("selection_lossy")) else 1,
    }
    for source_key, target_key in [
        ("dropped_text_chars", "source_memory_selection_dropped_text_chars"),
        ("dropped_line_count", "source_memory_selection_dropped_line_count"),
        ("retained_text_ratio", "source_memory_selection_retained_text_ratio_avg"),
        ("retained_line_ratio", "source_memory_selection_retained_line_ratio_avg"),
    ]:
        if selection.get(source_key) not in (None, ""):
            retention[target_key] = selection.get(source_key)
    return retention


def source_event_lineage_summary(records: list[Json]) -> Json:
    role_counts: Json = {}
    hook_type_counts: Json = {}
    codex_event_counts: Json = {}
    memory_scopes: list[str] = []
    session_continuities: list[str] = []
    extraction_phases: list[str] = []
    target_memory_scopes: list[str] = []
    target_session_continuities: list[str] = []
    target_extraction_phases: list[str] = []
    memory_selection_policy_counts: Json = {}
    memory_selection_lossy_count = 0
    memory_selection_complete_count = 0
    memory_selection_dropped_text_chars = 0
    memory_selection_dropped_line_count = 0
    memory_selection_retained_text_ratio_sum = 0.0
    memory_selection_retained_line_ratio_sum = 0.0
    memory_selection_retained_ratio_count = 0
    profile_promotion_policies: list[str] = []
    profile_promotion_blockers: list[str] = []
    profile_memory_classes: list[str] = []
    profile_memory_kinds: list[str] = []
    memory_layer_counts: Json = {}
    final_session_boundary_count = 0

    def add_count(counts: Json, name: Any, count: Any = 1) -> None:
        label = str(name or "").strip()
        if not label:
            return
        try:
            amount = max(0, int(count or 0))
        except (TypeError, ValueError):
            amount = 0
        if amount:
            counts[label] = int(counts.get(label, 0)) + amount

    def add_role_count(name: Any, count: Any = 1) -> None:
        role = normalize_message_role(name)
        if role:
            add_count(role_counts, role, count)

    def add_values(values: list[str], source: Any) -> None:
        if isinstance(source, list):
            for item in source:
                label = str(item or "").strip()
                if label:
                    values.append(label)
        else:
            label = str(source or "").strip()
            if label:
                values.append(label)

    for record in records:
        if not isinstance(record, dict):
            continue
        record_messages = messages_from_event_record(record)
        existing_role_counts = record.get("source_role_counts") if isinstance(record.get("source_role_counts"), dict) else {}
        if len(record_messages) > 1:
            for message in record_messages:
                add_role_count(message.get("role"), 1)
        elif existing_role_counts:
            for role, count in existing_role_counts.items():
                add_role_count(role, count)
        else:
            roles = record.get("source_roles") if isinstance(record.get("source_roles"), list) else []
            if roles:
                for role in roles:
                    add_role_count(role, 1)
            else:
                event_role = str(record.get("source_role") or "").strip()
                if event_role:
                    add_role_count(event_role, 1)
                else:
                    for message in record_messages:
                        add_role_count(message.get("role"), 1)

        existing_hook_counts = record.get("source_hook_type_counts") if isinstance(record.get("source_hook_type_counts"), dict) else {}
        if existing_hook_counts:
            for hook_type, count in existing_hook_counts.items():
                add_count(hook_type_counts, hook_type, count)
        else:
            hook_values: list[str] = []
            add_values(hook_values, record.get("source_hook_types"))
            envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
            metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
            hook = record.get("agent_hook") if isinstance(record.get("agent_hook"), dict) else {}
            add_values(hook_values, envelope.get("hook_type"))
            add_values(hook_values, metadata.get("hook_type"))
            add_values(hook_values, metadata.get("source_hook_types"))
            add_values(hook_values, hook.get("hook_type"))
            for hook_type in ordered_unique_any(hook_values):
                add_count(hook_type_counts, hook_type, 1)

        existing_codex_counts = record.get("source_codex_event_counts") if isinstance(record.get("source_codex_event_counts"), dict) else {}
        if existing_codex_counts:
            for codex_event, count in existing_codex_counts.items():
                add_count(codex_event_counts, codex_event, count)
        else:
            codex_values: list[str] = []
            add_values(codex_values, record.get("source_codex_events"))
            envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
            metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
            hook = record.get("agent_hook") if isinstance(record.get("agent_hook"), dict) else {}
            add_values(codex_values, envelope.get("codex_event"))
            add_values(codex_values, metadata.get("codex_event"))
            add_values(codex_values, metadata.get("source_codex_events"))
            add_values(codex_values, hook.get("codex_event"))
            add_values(codex_values, hook.get("trigger"))
            for codex_event in ordered_unique_any(codex_values):
                add_count(codex_event_counts, codex_event, 1)
        if not hook_type_counts:
            for codex_event in sorted(codex_event_counts):
                add_count(hook_type_counts, legacy_hook_type_from_codex_event(codex_event), codex_event_counts[codex_event])

        existing_selection_counts = (
            record.get("source_memory_selection_policy_counts")
            if isinstance(record.get("source_memory_selection_policy_counts"), dict)
            else {}
        )
        if existing_selection_counts:
            for policy, count in existing_selection_counts.items():
                add_count(memory_selection_policy_counts, policy, count)
        else:
            selection_values: list[str] = []
            add_values(selection_values, record.get("source_memory_selection_policies"))
            envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
            metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
            selection = record.get("codex_memory_selection") if isinstance(record.get("codex_memory_selection"), dict) else {}
            envelope_selection = (
                envelope.get("codex_memory_selection")
                if isinstance(envelope.get("codex_memory_selection"), dict)
                else {}
            )
            metadata_selection = (
                metadata.get("codex_memory_selection")
                if isinstance(metadata.get("codex_memory_selection"), dict)
                else {}
            )
            add_values(selection_values, selection.get("policy"))
            add_values(selection_values, envelope_selection.get("policy"))
            add_values(selection_values, metadata_selection.get("policy"))
            for policy in ordered_unique_any(selection_values):
                add_count(memory_selection_policy_counts, policy, 1)
        record_has_retention_counts = False
        for field, target in [
            ("source_memory_selection_lossy_count", "lossy"),
            ("source_memory_selection_complete_count", "complete"),
        ]:
            try:
                amount = max(0, int(record.get(field) or 0))
            except (TypeError, ValueError):
                amount = 0
            if target == "lossy":
                memory_selection_lossy_count += amount
            else:
                memory_selection_complete_count += amount
            record_has_retention_counts = record_has_retention_counts or amount > 0
        if not record_has_retention_counts:
            seen_selection_sources: set[tuple[Any, ...]] = set()
            for source in [
                record.get("codex_memory_selection") if isinstance(record.get("codex_memory_selection"), dict) else {},
                (record.get("envelope") or {}).get("codex_memory_selection")
                if isinstance(record.get("envelope"), dict)
                and isinstance((record.get("envelope") or {}).get("codex_memory_selection"), dict)
                else {},
                ((record.get("envelope") or {}).get("metadata") or {}).get("codex_memory_selection")
                if isinstance(record.get("envelope"), dict)
                and isinstance((record.get("envelope") or {}).get("metadata"), dict)
                and isinstance(((record.get("envelope") or {}).get("metadata") or {}).get("codex_memory_selection"), dict)
                else {},
            ]:
                if not source:
                    continue
                selection_key = (
                    source.get("policy"),
                    bool(source.get("selection_lossy")),
                    source.get("dropped_text_chars"),
                    source.get("dropped_line_count"),
                    source.get("retained_text_ratio"),
                    source.get("retained_line_ratio"),
                )
                if selection_key in seen_selection_sources:
                    continue
                seen_selection_sources.add(selection_key)
                if bool(source.get("selection_lossy")):
                    memory_selection_lossy_count += 1
                else:
                    memory_selection_complete_count += 1
                try:
                    memory_selection_dropped_text_chars += max(0, int(source.get("dropped_text_chars") or 0))
                except (TypeError, ValueError):
                    pass
                try:
                    memory_selection_dropped_line_count += max(0, int(source.get("dropped_line_count") or 0))
                except (TypeError, ValueError):
                    pass
                try:
                    memory_selection_retained_text_ratio_sum += float(source.get("retained_text_ratio"))
                    memory_selection_retained_line_ratio_sum += float(source.get("retained_line_ratio"))
                    memory_selection_retained_ratio_count += 1
                except (TypeError, ValueError):
                    pass
        for field, accumulator in [
            ("source_memory_selection_dropped_text_chars", "text"),
            ("source_memory_selection_dropped_line_count", "line"),
        ]:
            try:
                amount = max(0, int(record.get(field) or 0))
            except (TypeError, ValueError):
                amount = 0
            if accumulator == "text":
                memory_selection_dropped_text_chars += amount
            else:
                memory_selection_dropped_line_count += amount
        if "source_memory_selection_retained_text_ratio_avg" in record:
            try:
                memory_selection_retained_text_ratio_sum += float(record.get("source_memory_selection_retained_text_ratio_avg"))
                memory_selection_retained_line_ratio_sum += float(record.get("source_memory_selection_retained_line_ratio_avg", 1.0))
                memory_selection_retained_ratio_count += 1
            except (TypeError, ValueError):
                pass

        add_values(memory_scopes, record.get("source_memory_scopes"))
        add_values(memory_scopes, record.get("memory_scope"))
        add_values(session_continuities, record.get("source_session_continuities"))
        add_values(session_continuities, record.get("session_continuity"))
        add_values(extraction_phases, record.get("source_extraction_phases"))
        add_values(extraction_phases, record.get("extraction_phase"))
        add_values(target_memory_scopes, record.get("memory_scope"))
        add_values(target_session_continuities, record.get("session_continuity"))
        add_values(target_extraction_phases, record.get("extraction_phase"))
        add_values(profile_promotion_policies, record.get("source_profile_promotion_policies"))
        add_values(profile_promotion_policies, record.get("profile_promotion_policy"))
        add_values(profile_promotion_blockers, record.get("source_profile_promotion_blockers"))
        add_values(profile_promotion_blockers, record.get("profile_promotion_blocker"))
        add_values(profile_memory_classes, record.get("source_profile_memory_classes"))
        add_values(profile_memory_classes, record.get("profile_memory_class"))
        add_values(profile_memory_kinds, record.get("source_profile_memory_kinds"))
        add_values(profile_memory_kinds, record.get("profile_memory_kind"))
        existing_layer_counts = (
            record.get("source_memory_layer_counts")
            if isinstance(record.get("source_memory_layer_counts"), dict)
            else {}
        )
        if existing_layer_counts:
            for layer, count in existing_layer_counts.items():
                add_count(memory_layer_counts, layer, count)
        else:
            layer_values: list[str] = []
            add_values(layer_values, record.get("source_memory_layers"))
            add_values(layer_values, record.get("memory_layer"))
            inferred_layer = candidate_memory_layer_name(record)
            if inferred_layer:
                add_values(layer_values, inferred_layer)
            for layer in ordered_unique_any(layer_values):
                add_count(memory_layer_counts, layer, 1)
        try:
            final_session_boundary_count += max(0, int(record.get("source_final_session_boundary_count") or 0))
        except (TypeError, ValueError):
            pass
        if bool(record.get("final_session_boundary")):
            final_session_boundary_count += 1

    source_roles = sorted(role_counts)
    source_hook_types = sorted(hook_type_counts)
    source_codex_events = sorted(codex_event_counts)
    source_memory_scopes = ordered_unique_any(memory_scopes)
    source_session_continuities = ordered_unique_any(session_continuities)
    source_extraction_phases = ordered_unique_any(extraction_phases)
    source_memory_selection_policies = sorted(memory_selection_policy_counts)
    explicit_memory_scopes = ordered_unique_any(target_memory_scopes)
    explicit_session_continuities = ordered_unique_any(target_session_continuities)
    explicit_extraction_phases = ordered_unique_any(target_extraction_phases)
    source_profile_promotion_policies = ordered_unique_any(profile_promotion_policies)
    source_profile_promotion_blockers = ordered_unique_any(profile_promotion_blockers)
    source_profile_memory_classes = ordered_unique_any(profile_memory_classes)
    source_profile_memory_kinds = ordered_unique_any(profile_memory_kinds)
    source_memory_layers = sorted(memory_layer_counts)
    memory_scope = (
        explicit_memory_scopes[0]
        if len(explicit_memory_scopes) == 1
        else "user_profile"
        if source_memory_scopes == ["user_profile"]
        else "session"
        if "session" in source_memory_scopes
        else source_memory_scopes[0]
        if source_memory_scopes
        else ""
    )
    session_continuity = (
        explicit_session_continuities[0]
        if len(explicit_session_continuities) == 1
        else "cross_session"
        if source_session_continuities == ["cross_session"]
        else "same_session"
        if "same_session" in source_session_continuities
        else source_session_continuities[0]
        if source_session_continuities
        else ""
    )
    extraction_phase = (
        explicit_extraction_phases[0]
        if len(explicit_extraction_phases) == 1
        else "final"
        if source_extraction_phases == ["final"]
        else "provisional"
        if "provisional" in source_extraction_phases
        else source_extraction_phases[0]
        if source_extraction_phases
        else ""
    )
    lineage = {
        "source_roles": source_roles,
        "source_role_counts": role_counts,
        "source_hook_types": source_hook_types,
        "source_hook_type_counts": hook_type_counts,
        "source_codex_events": source_codex_events,
        "source_codex_event_counts": codex_event_counts,
        "source_memory_selection_policies": source_memory_selection_policies,
        "source_memory_selection_policy_counts": memory_selection_policy_counts,
        "source_memory_selection_lossy_count": memory_selection_lossy_count,
        "source_memory_selection_complete_count": memory_selection_complete_count,
        "source_memory_selection_dropped_text_chars": memory_selection_dropped_text_chars,
        "source_memory_selection_dropped_line_count": memory_selection_dropped_line_count,
        "source_memory_selection_retained_text_ratio_avg": round(memory_selection_retained_text_ratio_sum / memory_selection_retained_ratio_count, 6) if memory_selection_retained_ratio_count else 1.0,
        "source_memory_selection_retained_line_ratio_avg": round(memory_selection_retained_line_ratio_sum / memory_selection_retained_ratio_count, 6) if memory_selection_retained_ratio_count else 1.0,
        "source_memory_scopes": source_memory_scopes,
        "source_session_continuities": source_session_continuities,
        "source_extraction_phases": source_extraction_phases,
        "source_profile_promotion_policies": source_profile_promotion_policies,
        "source_profile_promotion_blockers": source_profile_promotion_blockers,
        "source_profile_memory_classes": source_profile_memory_classes,
        "source_profile_memory_kinds": source_profile_memory_kinds,
        "source_memory_layers": source_memory_layers,
        "source_memory_layer_counts": memory_layer_counts,
        "source_final_session_boundary_count": final_session_boundary_count,
    }
    if memory_scope:
        lineage["memory_scope"] = memory_scope
    if session_continuity:
        lineage["session_continuity"] = session_continuity
    if extraction_phase:
        lineage["extraction_phase"] = extraction_phase
    if final_session_boundary_count:
        lineage["final_session_boundary"] = True
    return lineage


def compression_profile_layer_values(records: list[Json]) -> Json:
    profile_classes: set[str] = set()
    profile_kinds: set[str] = set()
    for record in records:
        for value in record.get("source_profile_memory_classes", []) if isinstance(record.get("source_profile_memory_classes"), list) else []:
            text = str(value or "").strip()
            if text:
                profile_classes.add(text)
        for value in record.get("source_profile_memory_kinds", []) if isinstance(record.get("source_profile_memory_kinds"), list) else []:
            text = str(value or "").strip()
            if text:
                profile_kinds.add(text)
        profile_class = str(record.get("profile_memory_class") or "").strip()
        profile_kind = str(record.get("profile_memory_kind") or "").strip()
        if profile_class:
            profile_classes.add(profile_class)
        if profile_kind:
            profile_kinds.add(profile_kind)
        event_type = str(record.get("event_type") or "").strip().lower()
        policies = record.get("source_memory_selection_policies") if isinstance(record.get("source_memory_selection_policies"), list) else []
        if event_type in {"assistant_response", "tool_evidence", "assistant_decision"} or any(
            str(policy or "") in {"selected_assistant_decision_outcome_only", "selected_tool_evidence_only"}
            for policy in policies
        ):
            profile_classes.add("codex_outcome")
            profile_kinds.add("codex_outcome")
    classes = sorted(profile_classes)
    kinds = sorted(profile_kinds)
    return {
        "source_profile_memory_classes": classes,
        "source_profile_memory_kinds": kinds,
        "profile_memory_class": classes[0] if len(classes) == 1 else ("mixed" if classes else ""),
        "profile_memory_kind": "codex_outcome" if "codex_outcome" in kinds else (kinds[0] if len(kinds) == 1 else ("mixed" if kinds else "")),
    }


def compression_context_index_terms(record: Json) -> list[str]:
    try:
        from tools.matrixark_mcp_indexing import benchmark_quality_index_terms
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_indexing import benchmark_quality_index_terms
    terms = ["operator:TIME_COMPRESS", "context_class:compression", "source_type:message"]
    terms.extend(benchmark_quality_index_terms(record.get("summary_text"), record.get("text")))
    for token in tokens(str(record.get("summary_text") or "")):
        if token:
            terms.append(f"keyword:{token}")
    for field, prefix in [
        ("source_roles", "source_role"),
        ("source_hook_types", "hook_type"),
        ("source_codex_events", "codex_event"),
        ("source_memory_selection_policies", "memory_selection_policy"),
        ("source_memory_scopes", "source_memory_scope"),
        ("source_session_continuities", "source_session_continuity"),
        ("source_extraction_phases", "extraction_phase"),
        ("source_profile_promotion_policies", "profile_promotion_policy"),
        ("source_profile_promotion_blockers", "profile_promotion_blocker"),
        ("source_profile_memory_classes", "profile_memory_class"),
        ("source_profile_memory_kinds", "profile_memory_kind"),
    ]:
        values = record.get(field)
        if isinstance(values, list):
            terms.extend(f"{prefix}:{str(value).strip()}" for value in values if str(value or "").strip())
    for field, prefix in [
        ("memory_scope", "memory_scope"),
        ("session_continuity", "session_continuity"),
        ("extraction_phase", "extraction_phase"),
        ("profile_memory_class", "profile_memory_class"),
        ("profile_memory_kind", "profile_memory_kind"),
    ]:
        value = str(record.get(field) or "").strip()
        if value:
            terms.append(f"{prefix}:{value}")
    try:
        source_final_session_boundary_count = int(record.get("source_final_session_boundary_count") or 0)
    except (TypeError, ValueError):
        source_final_session_boundary_count = 0
    if bool(record.get("final_session_boundary")) or source_final_session_boundary_count > 0:
        terms.append("final_session_boundary:true")
    return ordered_unique_any(terms)


def compression_context_index_records(record: Json) -> list[Json]:
    compression_hash = record.get("compression_id_hash")
    if compression_hash is None:
        return []
    scope = candidate_access_scope(record)
    return [
        context_index_posting_record(
            index_name=index_name,
            data_model="context_compression_event",
            ref_type="compression",
            ref_hashes=[compression_hash],
            node_hash=record.get("node_hash"),
            scope=scope,
            updated_at_ms=record.get("compressed_time_ms", record.get("updated_at_ms", now_ms())),
        )
        for index_name in compression_context_index_terms(record)
    ]


# One field name for the identity of a WAL row, on every row.
#
# The same concept -- what makes this row supersede an earlier one -- was spelled nine ways
# across twelve record types: node_hash, child_ref_hash, event_id_hash, summary_hash,
# ref_hash, entity_hash, dirty_hash, skill_hash, resource_hash, resource_import_task_hash,
# node_id. Every reader that wanted a row identity had to know all twelve, and a new type
# meant editing each of them.
#
# A row now carries its identity under ROW_KEY_FIELD. The per-type knowledge stays in one
# function, is applied once at write, and readers use the field.
ROW_KEY_FIELD = "row_key"


def canonical_row_key(record: Json) -> str | None:
    """The identity of a WAL row as one string, or None when the row has no usable identity.

    A row whose key has an empty part is NOT compacted -- compact_latest_value_records leaves
    it alone rather than letting an absent hash collide with another absent one. Collapsing the
    key to a single string would hide that from the guard, so the same test is applied here and
    such a row simply gets no key.
    """
    key = _latest_value_record_key_by_type(record)
    if key is None:
        return None
    if any(part in (None, "") for part in key[1:]):
        return None
    try:
        return json.dumps(list(key), separators=(",", ":"), default=str, sort_keys=False)
    except (TypeError, ValueError):
        return None


def stamp_row_keys(records: list[Json]) -> list[Json]:
    """Give every row its identity under the shared field name, once, at write time."""
    for record in records or ():
        if not isinstance(record, dict) or ROW_KEY_FIELD in record:
            continue
        key = canonical_row_key(record)
        if key is not None:
            record[ROW_KEY_FIELD] = key
    return records


def latest_value_record_key(record: Json) -> tuple[Any, ...] | None:
    """Prefer the row's own key. Rows written before it existed are keyed by type."""
    stamped = record.get(ROW_KEY_FIELD)
    if isinstance(stamped, str) and stamped:
        return (stamped,)
    return _latest_value_record_key_by_type(record)


def _latest_value_record_key_by_type(record: Json) -> tuple[Any, ...] | None:
    record_type = str(record.get("record_type") or "")
    if record_type == "context_node":
        return (record_type, record.get("node_hash"))
    if record_type == "context_child_ref":
        return (record_type, record.get("child_ref_hash"))
    if record_type == "context_event":
        return (record_type, record.get("event_id_hash"))
    if record_type == "context_summary":
        return (record_type, record.get("summary_type"), record.get("summary_hash") or record.get("node_hash"))
    if record_type == "context_embedding":
        return (record_type, record.get("embedding_type"), record.get("ref_type"), record.get("ref_hash"))
    if record_type == "context_index":
        return (
            record_type,
            record.get("index_name"),
            record.get("scope_key") or canonical_scope_key(record.get("scope", {})) if isinstance(record.get("scope", {}), dict) else record.get("scope_key"),
            record.get("node_hash") or record.get("node_id"),
            record.get("data_model") or record.get("ref_type"),
            record.get("timestamp_key_ms") or record.get("updated_at_ms"),
        )
    if record_type == "context_entity":
        return (record_type, record.get("entity_hash"))
    if record_type == "context_summary_dirty":
        return (record_type, record.get("dirty_hash"))
    if record_type == "session_buffer_event":
        return (record_type, tuple(record.get("buffer_key", [])), record.get("event_id_hash"))
    if record_type == "resource_manifest":
        return (record_type, record.get("resource_hash"))
    if record_type == "skill_registry_update":
        return (record_type, record.get("skill_hash"))
    if record_type == "resource_import_task":
        # Every writer of this row writes `task_hash`; nothing writes
        # `resource_import_task_hash`, which is what this asked for. So the key was
        # (record_type, None) for every one of them, and compaction skips a key with an empty
        # part -- these rows were never superseded and accumulated three per attachment. The old
        # name is still accepted, for a log that somehow carries it.
        return (
            record_type,
            record.get("task_hash")
            if record.get("task_hash") is not None
            else record.get("resource_import_task_hash"),
        )
    return None


def compact_latest_value_records(records: list[Json]) -> list[Json]:
    latest: dict[tuple[Any, ...], Json] = {}
    output: list[Json] = []
    latest_positions: dict[tuple[Any, ...], int] = {}
    for record in records:
        key = latest_value_record_key(record)
        if key is None or any(part in (None, "") for part in key[1:]):
            output.append(record)
            continue
        existing = latest.get(key)
        if existing is None:
            latest[key] = record
            latest_positions[key] = len(output)
            output.append(record)
            continue
        record_ts = int(record.get("updated_at_ms") or record.get("created_at_ms") or 0)
        existing_ts = int(existing.get("updated_at_ms") or existing.get("created_at_ms") or 0)
        record_revision = int(record.get("profile_revision") or record.get("revision") or 0)
        existing_revision = int(existing.get("profile_revision") or existing.get("revision") or 0)
        if (record_ts, record_revision) >= (existing_ts, existing_revision):
            latest[key] = record
            output[latest_positions[key]] = record
    return output


# ================================================================================================
# Memory delete / forget: tombstone (soft-delete) machinery
# ------------------------------------------------------------------------------------------------
# Delete/forget are recorded as durable tombstone records appended to the SAME JSONL event log, so
# they survive reload with the rest of the store (no separate index to keep in sync). A tombstone is
# applied by `apply_memory_tombstones` -- called at the single read choke point `read_all()` -- which
# drops the matching records so nothing deleted can resurface in retrieve, get_all, entity caches, or
# summaries. Tombstones are order-aware: a tombstone only removes records that appear BEFORE it in the
# append log, so re-ingesting for a subject AFTER a forget produces fresh, live memories again.
MEMORY_TOMBSTONE_RECORD_TYPE = "matrixark_memory_tombstone"

# Record types a scope-level forget / tenant reset wipes (the user-visible "memory" of a subject).
# Access/identity/audit/idempotency records are intentionally excluded -- forget removes context data,
# not the tenant's control plane.
_MEMORY_SCOPED_RECORD_TYPES = {
    "context_event",
    "context_entity",
    "context_summary",
    "context_segment",
    "context_embedding",
    "context_compression_event",
    "context_index",
    "context_node",
    "context_child_ref",
    "context_batch_commit",
    "session_buffer_event",
    "context_summary_dirty",
    "matrixark_async_pipeline_task",
}


def _record_memory_ids(record: Json) -> set[str]:
    """The addressable ids by which a single-record `delete(memory_id)` can match `record`.

    A memory's public id is its ``event_id_hash`` (what ``/v1/ingest`` returns). We also match the
    event's own embedding (``ref_type == 'event'`` + ``ref_hash``) and the event's own index/vector
    postings (``ref_hash == memory_id``) so the deleted memory does not resurface through its vector
    or a secondary index. Deleting the wider *derived* provenance closure (entities/summaries built
    FROM the event) is handled separately by the closure-aware delete tombstone (`closure: true`)."""
    ids: set[str] = set()
    event_hash = record.get("event_id_hash")
    if event_hash not in (None, ""):
        ids.add(str(event_hash))
    ref_type = str(record.get("record_type") or "")
    if ref_type in {"context_embedding", "context_index"} and str(record.get("ref_type") or "") == "event":
        ref_hash = record.get("ref_hash")
        if ref_hash not in (None, ""):
            ids.add(str(ref_hash))
    return ids


# Record types that are DERIVED from source events (their content is extracted/summarized from one
# or more `context_event`s). Closure-aware delete walks these via their provenance so removing a
# source event also removes the single-source segments/entities/summaries built solely from it.
_MEMORY_DERIVATIVE_RECORD_TYPES = {
    "context_entity",
    "context_summary",
    "context_segment",
    "context_summary_dirty",
}


def _record_provenance_source_ids(record: Json) -> set[int] | None:
    """The set of source-event ids a DERIVED record was built from, or ``None`` when `record` is not
    a provenance-carrying derivative.

    A derivative points back at its sources through ``source_event_ids`` (list), ``source_refs``
    (list of stringified event ids), and/or ``source_event_hash`` (a single id). Returns a set of the
    resolved integer ids; ``None`` signals "not a derivative" so callers can leave leaf records (the
    event itself, audit/index/embedding rows) to the plain-id match path."""
    if str(record.get("record_type") or "") not in _MEMORY_DERIVATIVE_RECORD_TYPES:
        return None
    ids: set[int] = set()
    found = False
    values = record.get("source_event_ids")
    if isinstance(values, list):
        for value in values:
            try:
                ids.add(int(value))
                found = True
            except (TypeError, ValueError):
                continue
    refs = record.get("source_refs")
    if isinstance(refs, list):
        for value in refs:
            try:
                ids.add(int(value))
                found = True
            except (TypeError, ValueError):
                continue
    single = record.get("source_event_hash")
    if single not in (None, ""):
        try:
            ids.add(int(single))
            found = True
        except (TypeError, ValueError):
            pass
    return ids if found else None


# A derivative's OWN addressable identity hashes -- the ids that its embeddings (``context_embedding``
# ref_hash) and secondary-index postings (``context_index`` ref_hash / ref_hashes) point at. These
# are what must be swept when the derivative itself is closure-deleted so no orphan embedding / index
# posting survives referencing a removed entity / summary / segment.
_MEMORY_DERIVATIVE_IDENTITY_FIELDS = ("entity_hash", "summary_hash", "segment_hash")


def _record_derivative_identity_ids(record: Json) -> set[str]:
    """The identity hashes (entity_hash / summary_hash / segment_hash) a DERIVED record is addressed
    by. Its embeddings + index postings reference these, so tombstoning the derivative must also sweep
    any posting/embedding whose ref target is one of them."""
    ids: set[str] = set()
    for field in _MEMORY_DERIVATIVE_IDENTITY_FIELDS:
        value = record.get(field)
        if value not in (None, ""):
            ids.add(str(value))
    return ids


def _record_own_identity_id(record: Json) -> str | None:
    """The single addressable id `record` is a *member* under, for the event-membership index:
    an event's ``event_id_hash`` or a derivative's identity hash. Embeddings / index postings are
    members *by reference* (their ref target is one of these ids), so they carry no own member id."""
    event_hash = record.get("event_id_hash")
    record_type = str(record.get("record_type") or "")
    if record_type == "context_event" and event_hash not in (None, ""):
        return str(event_hash)
    if record_type in _MEMORY_DERIVATIVE_RECORD_TYPES:
        for field in _MEMORY_DERIVATIVE_IDENTITY_FIELDS:
            value = record.get(field)
            if value not in (None, ""):
                return str(value)
    return None


def build_event_member_index(records: list[Json]) -> dict[str, set[str]]:
    """Build the durable event-membership map ``event_id_hash -> {member identity hashes}`` from the
    live record set.

    A member of an event is: the ``context_event`` itself (member id = its own ``event_id_hash``) plus
    every derivative (entity / summary / segment / summary_dirty) built from it (member id = the
    derivative's identity hash). The event's and the derivatives' OWN embeddings + secondary-index
    postings are members *by reference*: their ref target is one of these member ids, so a delete that
    tombstones every member id (matching embeddings / postings by ref_hash ∈ member set) reclaims them
    too -- this is why the member set is precisely the closure identity set to sweep on delete."""
    index: dict[str, set[str]] = {}
    for record in records:
        record_type = str(record.get("record_type") or "")
        if record_type == "context_event":
            event_hash = record.get("event_id_hash")
            if event_hash not in (None, ""):
                index.setdefault(str(event_hash), set()).add(str(event_hash))
            continue
        if record_type in _MEMORY_DERIVATIVE_RECORD_TYPES:
            provenance = _record_provenance_source_ids(record)
            if not provenance:
                continue
            identity_ids = _record_derivative_identity_ids(record)
            if not identity_ids:
                continue
            for source_id in provenance:
                bucket = index.setdefault(str(source_id), set())
                bucket.update(identity_ids)
    return index


def _safe_int(value: Any) -> int | None:
    """Best-effort ``int`` coercion (accepts numeric strings); ``None`` on failure."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _record_scope_hashes(record: Json) -> tuple[int, int]:
    """Return ``(tenant_hash, user_hash)`` for `record` from its access scope, resolving each either
    from an explicit hash field or by parsing the record's ``scope_key``. Missing -> 0."""
    scope = candidate_access_scope(record)
    if not isinstance(scope, dict):
        return 0, 0
    try:
        tenant_hash = int(scope.get("tenant_hash") or 0)
    except (TypeError, ValueError):
        tenant_hash = 0
    try:
        user_hash = int(scope.get("user_hash") or 0)
    except (TypeError, ValueError):
        user_hash = 0
    if not tenant_hash or not user_hash:
        parts = parse_scope_key(str(scope.get("scope_key") or ""))
        tenant_hash = tenant_hash or int(parts.get("t") or 0)
        user_hash = user_hash or int(parts.get("u") or 0)
    return tenant_hash, user_hash


def _tombstone_kills_record(tombstone: Json, record: Json) -> bool:
    """True when `tombstone` (appended after `record`) removes `record`."""
    kind = str(tombstone.get("tombstone_kind") or "")
    if kind == "delete":
        target = str(tombstone.get("target_memory_id") or "")
        if not target:
            return False
        if target in _record_memory_ids(record):
            return True
        # Provenance closure (opt-in per tombstone): when the deleted id is a source EVENT, also
        # remove derived records built SOLELY from it (single-source segments/entities/summaries).
        # Multi-source derivatives are NOT matched here -- delete_memory rewrites those in place with
        # the source dropped, so their surviving copy no longer lists `target`.
        if tombstone.get("closure"):
            # Secondary-posting sweep (event-membership closure): the deleted event AND its
            # single-source derivatives' OWN embeddings / secondary-index postings orphan otherwise,
            # because they are matched by neither the plain-id path (they are not ref_type=="event")
            # nor the provenance path (embeddings/postings carry no source_event_ids). ``closure_ref_ids``
            # is the closure identity set (anchor event_id_hash + each killed derivative's identity
            # hash); a posting/embedding whose ref target is in it is a member being deleted, regardless
            # of ref_type. Shared parent nodes (ref_type=="node") are never in the set, so they survive.
            ref_ids = tombstone.get("closure_ref_ids")
            if ref_ids and str(record.get("record_type") or "") in {"context_embedding", "context_index"}:
                closure_ids = ref_ids if isinstance(ref_ids, (set, frozenset)) else set(str(x) for x in ref_ids)
                ref_hash = record.get("ref_hash")
                if ref_hash not in (None, "") and str(ref_hash) in closure_ids:
                    return True
                ref_hashes = record.get("ref_hashes")
                if isinstance(ref_hashes, list) and any(str(x) in closure_ids for x in ref_hashes):
                    return True
            try:
                target_int = int(target)
            except (TypeError, ValueError):
                return False
            provenance = _record_provenance_source_ids(record)
            if provenance is not None and provenance == {target_int}:
                return True
        return False
    record_type = str(record.get("record_type") or "")
    if record_type not in _MEMORY_SCOPED_RECORD_TYPES:
        return False
    rec_tenant_hash, rec_user_hash = _record_scope_hashes(record)
    if kind == "forget":
        target_tenant = int(tombstone.get("target_tenant_hash") or 0)
        target_user = int(tombstone.get("target_user_hash") or 0)
        if not target_tenant or not target_user:
            return False
        return rec_tenant_hash == target_tenant and rec_user_hash == target_user
    if kind == "reset":
        target_tenant = int(tombstone.get("target_tenant_hash") or 0)
        return bool(target_tenant) and rec_tenant_hash == target_tenant
    return False


def _records_contain_memory_tombstone(records: list[Json]) -> bool:
    for record in records:
        if str(record.get("record_type") or "") == MEMORY_TOMBSTONE_RECORD_TYPE:
            return True
    return False


def apply_memory_tombstones(records: list[Json]) -> list[Json]:
    """Drop records removed by any memory tombstone, and strip the tombstone markers themselves.

    Order-aware single pass: a tombstone only removes matching records that precede it in the log, so
    re-ingesting for a forgotten subject after the forget yields live memories again. Fast-path
    returns the input unchanged when the log carries no tombstone (the overwhelmingly common case)."""
    if not _records_contain_memory_tombstone(records):
        return records
    live: list[Json] = []
    for record in records:
        if str(record.get("record_type") or "") == MEMORY_TOMBSTONE_RECORD_TYPE:
            live = [kept for kept in live if not _tombstone_kills_record(record, kept)]
            continue
        live.append(record)
    return live


def surviving_source_event_ids(records: list[Json]) -> set[str] | None:
    """Order-aware set of ``context_event`` ids (as str) that SURVIVE the memory-tombstone sweep.

    Returns ``None`` when the log carries no memory tombstone -- the fast path, signalling the caller
    to keep every pending event unchanged. When tombstones exist, a ``context_event`` is *surviving*
    only when no delete/forget tombstone appended AFTER it removes it.

    This is the delete-before-extract forward guard's source of truth: async batch extraction runs at
    commit time and materializes derivatives (entities / summaries / segments + embeddings + index
    postings) from the still-PENDING ``session_buffer_event``s. If the source event (or, for a forget,
    the whole subject scope) was deleted while pending, its ``context_event`` -- and the matching
    ``session_buffer_event`` -- are killed by the delete/forget tombstone, so they are absent here and
    the commit path skips extraction, never re-materializing the deleted content after the tombstone.
    Because the sweep is order-aware (a tombstone only removes records that PRECEDE it), a LATER
    re-ingest of the same content (a fresh ``event_id_hash``, appended after the tombstone) still
    survives and materializes normally -- the suppression is per deleted event / tombstone, not a
    permanent block on the content. Durable + cross-process: the signal is read from the JSONL log, so
    a delete in one process is honored by a commit run in a freshly reloaded process."""
    if not _records_contain_memory_tombstone(records):
        return None
    surviving: set[str] = set()
    for record in apply_memory_tombstones(records):
        if str(record.get("record_type") or "") == "context_event":
            event_hash = record.get("event_id_hash")
            if event_hash not in (None, ""):
                surviving.add(str(event_hash))
    return surviving


def compact_and_apply_tombstones(records: list[Json]) -> list[Json]:
    """The serving pipeline, in the ONE order that is correct for both orphan sweeping AND supersede:

        compact_latest_value  ->  apply_memory_tombstones  ->  compact_latest_context_state

    Both boundaries are load-bearing:

    * ``apply_memory_tombstones`` must run BEFORE ``compact_latest_context_state_records`` (which calls
      ``compact_context_index_postings``): that step rebuilds ``context_index`` postings into fresh
      coalesced rows appended at the TAIL of its output, so a tombstone applied AFTER it sits
      positionally *before* those rebuilt rows and the order-aware sweep (a tombstone only removes
      records that precede it) never reaches them -- the deleted event's / derivative's own postings
      orphan. Sweeping first removes each posting in its true append position; the rebuild then
      coalesces only survivors (also correct for mixed-ref buckets).

    * ``apply_memory_tombstones`` must run AFTER ``compact_latest_value_records``: a multi-source
      derivative is DEMOTED by appending a newer copy with the deleted source trimmed
      (source_event_ids [A,B] -> [B]); the original [A,B] copy is meant to be hidden by latest-value
      compaction. If tombstones ran on the raw (un-collapsed) log, a later delete of the remaining
      source B would match the demoted [B] copy by provenance closure (== {B}) but NOT the still-present
      original [A,B] copy (!= {B}); compaction would then surface the stale original and the derivative
      would survive deletion of its own last source. Collapsing duplicate copies to the newest FIRST
      leaves exactly one copy (the demoted one) for the tombstone to match -- deterministic regardless
      of read-cache vs full-recompute path or PYTHONHASHSEED.

    A tombstone-free log short-circuits ``apply_memory_tombstones`` unchanged, so the middle step is a
    no-op for the overwhelmingly common no-tombstone case (and ``compact_latest_value`` then
    ``compact_latest_context_state`` is exactly the historical composition)."""
    # Audit/pipeline-task footprint bounding runs FIRST. It was written as this pipeline's entry
    # (`bound_pipeline_task_footprint`, "Lever A, lever B, audit-payload retention, then value
    # sharing") and was reachable from nowhere -- while a comment in matrixark_mcp_summary_runtime
    # states serving already applies it. Its knob is defaulted, not off: audit payloads are retained
    # for the newest 20 rows per scope and aged out beyond that.
    #
    # Safe at this position: it touches only pipeline-task and audit rows, never context_index
    # postings and never memory records, so neither load-bearing boundary above is disturbed. The
    # audit ROW always survives (retrieval and session-commit look it up by record_type + scope);
    # only the diagnostic payload ages out.
    try:
        from tools.matrixark_pipeline_task_slim import bound_pipeline_task_footprint
    except ImportError:  # Direct script execution from tools/.
        from matrixark_pipeline_task_slim import bound_pipeline_task_footprint
    records = bound_pipeline_task_footprint(records)
    return compact_latest_context_state_records(apply_memory_tombstones(compact_latest_value_records(records)))


# --------------------------------------------------------------------------------------------- #
# PurchaseMemory Phase 1: per-record TTL / retention-cutoff (expire-only) read-time enforcement.
#
# A record carries ``expires_at`` (float unix seconds) + ``expires_at_ms`` (int ms) + ``ephemeral``
# when it was ingested with a TTL. A scope-level retention cutoff is a durable marker record
# (``matrixark_retention_cutoff``) carrying the target tenant/user hashes + ``cutoff_ms``. Both are
# enforced lazily at READ time (records never surface once expired / older than an active cutoff)
# and reclaimed physically by the existing tombstone-purge path (see ``sweep_expired_memories``).
# The read filter is applied on every read (never baked into the size/mtime-keyed cache) so a
# record that expires with no intervening write still disappears on the next read.
# --------------------------------------------------------------------------------------------- #
MEMORY_RETENTION_CUTOFF_RECORD_TYPE = "matrixark_retention_cutoff"
# Ephemeral (TTL) records are stamped on every scoped record of the ingestion EXCEPT summaries,
# which aggregate across ingestions and must not inherit a single ingest's expiry.
_MEMORY_SUMMARY_RECORD_TYPES = {"context_summary", "context_summary_dirty"}


def _memory_clock_now_ms() -> int:
    """Current wall-clock in ms for TTL/cutoff checks, with a test override.

    ``MATRIXARK_MEMORY_NOW_MS`` (integer ms) lets tests advance the expiry clock deterministically
    without monkeypatching every ``now_ms`` call site; unset -> real ``now_ms()``."""
    override = os.environ.get("MATRIXARK_MEMORY_NOW_MS")
    if override:
        try:
            return int(override)
        except (TypeError, ValueError):
            pass
    return now_ms()


def _record_expires_at_ms(record: Json) -> int:
    try:
        return int(record.get("expires_at_ms") or 0)
    except (TypeError, ValueError):
        return 0


def _record_is_time_expired(record: Json, now_ms_value: int) -> bool:
    expires_at_ms = _record_expires_at_ms(record)
    return expires_at_ms > 0 and expires_at_ms <= now_ms_value


def _record_occurred_ms(record: Json) -> int:
    """The record's occurrence time in ms (``updated_at_ms``, else the ingestion time / event
    time), used for retention-cutoff comparison. 0 when unknown (never cut by a cutoff)."""
    for field in ("updated_at_ms", "timestamp_key_ms", "event_time_ms", "created_at_ms"):
        value = record.get(field)
        if value not in (None, ""):
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    envelope = record.get("envelope")
    if isinstance(envelope, dict):
        try:
            return int(envelope.get("ingestion_time_ms") or 0)
        except (TypeError, ValueError):
            return 0
    return 0


def _retention_cutoffs_by_subject(records: list[Json]) -> dict[tuple[int, int], int]:
    """Highest active cutoff (ms) per ``(tenant_hash, user_hash)`` from cutoff-marker records."""
    cutoffs: dict[tuple[int, int], int] = {}
    for record in records:
        if str(record.get("record_type") or "") != MEMORY_RETENTION_CUTOFF_RECORD_TYPE:
            continue
        try:
            key = (int(record.get("target_tenant_hash") or 0), int(record.get("target_user_hash") or 0))
            cutoff_ms = int(record.get("cutoff_ms") or 0)
        except (TypeError, ValueError):
            continue
        if cutoff_ms > cutoffs.get(key, 0):
            cutoffs[key] = cutoff_ms
    return cutoffs


def _record_cut_by_retention(record: Json, cutoffs: dict[tuple[int, int], int]) -> bool:
    if not cutoffs:
        return False
    key = _record_scope_hashes(record)
    cutoff_ms = cutoffs.get(key, 0)
    if not cutoff_ms:
        return False
    occurred = _record_occurred_ms(record)
    return occurred > 0 and occurred < cutoff_ms


def _memory_records_need_expiry_filter(records: list[Json]) -> bool:
    for record in records:
        if record.get("ephemeral") or record.get("expires_at_ms"):
            return True
        if str(record.get("record_type") or "") == MEMORY_RETENTION_CUTOFF_RECORD_TYPE:
            return True
    return False


def filter_live_memory_records(records: list[Json], *, now_ms_value: int | None = None) -> list[Json]:
    """Drop expired / pre-cutoff records and the internal cutoff markers themselves.

    Fast-path returns the input unchanged when no record carries a TTL or a cutoff marker exists
    (the overwhelmingly common case). Applied per-read on top of the compacted+tombstoned view."""
    if not _memory_records_need_expiry_filter(records):
        return records
    now = now_ms_value if now_ms_value is not None else _memory_clock_now_ms()
    cutoffs = _retention_cutoffs_by_subject(records)
    live: list[Json] = []
    for record in records:
        if str(record.get("record_type") or "") == MEMORY_RETENTION_CUTOFF_RECORD_TYPE:
            continue  # internal marker: never surfaced to callers
        if _record_is_time_expired(record, now):
            continue
        if _record_cut_by_retention(record, cutoffs):
            continue
        live.append(record)
    return live


def _drop_time_expired_records(records: list[Json], *, now_ms_value: int | None = None) -> list[Json]:
    """Lightweight expiry-only re-filter for cache layers below ``read_all`` (retrieval hot-path
    cache) so a record that expires between writes stops surfacing without a new write."""
    if not any(record.get("expires_at_ms") for record in records):
        return records
    now = now_ms_value if now_ms_value is not None else _memory_clock_now_ms()
    return [record for record in records if not _record_is_time_expired(record, now)]


try:  # mixin
    from tools.matrixark_local_adapter_retrieve import _LocalAdapterRetrieveMixin
except ImportError:
    from matrixark_local_adapter_retrieve import _LocalAdapterRetrieveMixin

try:  # mixin
    from tools.matrixark_local_adapter_ingest import _LocalAdapterIngestMixin
except ImportError:
    from matrixark_local_adapter_ingest import _LocalAdapterIngestMixin

try:  # mixin
    from tools.matrixark_local_adapter_dashboard import _LocalAdapterDashboardMixin
except ImportError:
    from matrixark_local_adapter_dashboard import _LocalAdapterDashboardMixin

try:  # mixin
    from tools.matrixark_local_adapter_summaries import _LocalAdapterSummariesMixin
except ImportError:
    from matrixark_local_adapter_summaries import _LocalAdapterSummariesMixin

try:  # mixin
    from tools.matrixark_local_adapter_session_commit import _LocalAdapterSessionCommitMixin
except ImportError:
    from matrixark_local_adapter_session_commit import _LocalAdapterSessionCommitMixin

try:  # mixin
    from tools.matrixark_local_adapter_context_node import _LocalAdapterContextNodeMixin
except ImportError:
    from matrixark_local_adapter_context_node import _LocalAdapterContextNodeMixin

try:  # mixin
    from tools.matrixark_local_adapter_retrieval import _LocalAdapterRetrievalMixin
except ImportError:
    from matrixark_local_adapter_retrieval import _LocalAdapterRetrievalMixin

@dataclass
class MatrixArkLocalAdapter(_LocalAdapterRetrieveMixin, _LocalAdapterIngestMixin, _LocalAdapterDashboardMixin, _LocalAdapterSummariesMixin, _LocalAdapterSessionCommitMixin, _LocalAdapterContextNodeMixin, _LocalAdapterRetrievalMixin):
    event_log: Path

    def __post_init__(self) -> None:
        self._init_local_runtime_state()

    def _init_local_runtime_state(self) -> None:
        self.event_log.parent.mkdir(parents=True, exist_ok=True)
        # Per-instance JSONL toggle. The proxy/direct-backed adapters persist
        # durably through their Rust client and construct the base adapter with a
        # sentinel "…-unused-…" event_log path to signal that the local JSONL
        # mirror should not be used. Honor that intent: without this, the global
        # LOCAL_JSONL_ENABLED default left the inherited append()/read_all() writing
        # and, crucially, re-reading + re-compacting that redundant log on every
        # call. It grew to the rotation cap (hundreds of MB / tens of thousands of
        # records) and made each retrieve/ingest take tens of seconds -- blowing the
        # request deadline so context never committed. The pure-local adapter (real
        # event_log path) keeps the JSONL; MATRIXARK_LOCAL_JSONL_ENABLED still forces
        # it off globally.
        self._local_jsonl_enabled = LOCAL_JSONL_ENABLED and "-unused-" not in self.event_log.name
        self._write_batch_local = threading.local()
        # Per-ingestion TTL / identity stamp propagated from the envelope to every record produced
        # during a single ingest() (thread-local so concurrent ingests never bleed into each other).
        self._ingest_stamp_local = threading.local()
        self._event_log_lock = threading.RLock()
        # Metadata-interning: (field, token) pairs whose durable dict record this instance has already
        # emitted, so a value's dict record is written once. Lazily seeded from the log on first write.
        self._intern_emitted_tokens: set[tuple[str, str]] = set()
        self._intern_tokens_seeded = False
        self._resource_import_worker_count = max(1, int(os.environ.get("MATRIXARK_RESOURCE_IMPORT_WORKERS", "2")))
        self._resource_import_queue_max = max(1, int(os.environ.get("MATRIXARK_RESOURCE_IMPORT_QUEUE_MAX", "64")))
        self._resource_import_queue: thread_queue.Queue[Json] = thread_queue.Queue(maxsize=self._resource_import_queue_max)
        self._resource_import_workers_started = False
        self._resource_import_worker_lock = threading.RLock()
        self._resource_import_stop = threading.Event()
        self._resource_import_threads: list[threading.Thread] = []
        self._latest_entity_by_hash: dict[int, Json] = {}
        self._entity_cache_loaded = False
        self._session_buffer_cache_lock = threading.RLock()
        self._context_event_by_hash: dict[int, Json] = {}
        self._session_pending_event_ids_by_key: dict[tuple[str, str, str, str], list[int]] = {}
        self._session_committed_event_ids_by_key: dict[tuple[str, str, str, str], set[int]] = {}
        self._context_node_hashes: set[int] = set()
        self._context_child_ref_hashes: set[int] = set()
        self._context_node_cache_loaded = False
        self._read_cache_lock = threading.RLock()
        self._read_cache_records: list[Json] | None = None
        # Set when records were appended to the cache without compacting it -- see
        # _compact_read_cache_if_dirty_locked.
        self._read_cache_dirty = False
        # (record_type, field) -> {value: record}, kept current as records are appended so an
        # embedding can find its owner without reading the whole set. None until first use.
        self._embedding_owner_index: dict[tuple[str, str], dict[Any, Json]] | None = None
        # dirty_hash -> the newest summary-dirty or refresh-audit row, so the outstanding set
        # can be answered without reading everything. None until first use.
        self._summary_dirty_index: dict[Any, Json] | None = None
        #: Pipeline-task rows, kept for the same reason the summary-dirty rows are: the
        #: pre-retrieval idle-commit flush was scanning the WHOLE live view twice per query
        #: to find them, and they are a handful of rows in a store of millions.
        self._pipeline_task_index: list[Json] | None = None
        # model_ref -> {node_hash}, so 'is this node already embedded' does not read the
        # store. None until first use; see _existing_node_embedding_refs.
        self._node_embedding_refs_index: dict[str, set[int]] | None = None
        # The resolved event-log path, which never changes. Path.resolve walks the path and
        # stats each component, and this was recomputed at a dozen sites on every append.
        self._resolved_cache_key: str | None = None
        self._resolved_paths: dict[Path, str] = {}
        # The keys the compacted cache already holds, kept current so a write can tell whether
        # it supersedes anything without scanning. None means unknown -- rebuilt on the next
        # compaction.
        self._read_cache_value_keys: set[Any] | None = None
        self._read_cache_state_keys: set[Any] | None = None
        self._read_cache_size = -1
        self._read_cache_mtime_ns = -1
        self._read_cache_source = "empty"
        self._durable_read_cache_last_write_ms = 0.0
        # (cache_key, base_count, delta_count, epoch) this instance last wrote, or None.
        self._durable_read_cache_state: tuple[str, int, int, int | None] | None = None
        # Bumped whenever compaction rewrites the cached record list, so a persisted tail can
        # tell "only grew at the end" from "the prefix moved".
        self._read_cache_compaction_epoch = 0
        self._retrieval_records_cache_lock = threading.RLock()
        self._retrieval_records_cache_generation = 0
        self._retrieval_records_cache: dict[tuple[Any, ...], Json] = {}
        self._context_pack_cache_lock = threading.RLock()
        self._context_pack_cache: dict[tuple[Any, ...], tuple[float, Json]] = {}
        self._context_pack_cache_max_entries = max(0, int(os.environ.get("MATRIXARK_CONTEXT_PACK_CACHE_MAX_ENTRIES", "256")))
        self._context_pack_cache_ttl_s = max(0.0, float(os.environ.get("MATRIXARK_CONTEXT_PACK_CACHE_TTL_S", "30")))
        # Event-membership index: event_id_hash -> {member identity hashes} (see
        # `build_event_member_index`). The authoritative O(1) enumeration of what a delete/update must
        # sweep; rebuilt lazily from the live view and invalidated whenever the read caches clear. An
        # engine-backed adapter also persists it as a durable hash so delete never needs a rescan.
        self._event_member_index: dict[str, set[str]] | None = None
        self._event_member_index_lock = threading.RLock()
        # Instrumentation (tests assert the fast path is taken): counts index-served vs scan-fallback
        # member lookups on delete/update.
        self._event_member_index_hits = 0
        self._event_member_index_misses = 0

    def _write_batch_stack(self) -> list[list[Json]]:
        local = getattr(self, "_write_batch_local", None)
        if local is None:
            self._write_batch_local = threading.local()
            local = self._write_batch_local
        stack = getattr(local, "stack", None)
        if stack is None:
            stack = []
            local.stack = stack
        return stack

    def _current_write_batch(self) -> list[Json] | None:
        stack = self._write_batch_stack()
        return stack[-1] if stack else None

    # ---- Per-ingestion TTL / identity stamp -------------------------------------------------- #
    def _ingest_stamp_stack(self) -> list[Json]:
        local = getattr(self, "_ingest_stamp_local", None)
        if local is None:
            self._ingest_stamp_local = threading.local()
            local = self._ingest_stamp_local
        stack = getattr(local, "stack", None)
        if stack is None:
            stack = []
            local.stack = stack
        return stack

    def _push_ingest_stamp(self, envelope: Json) -> None:
        """Capture the envelope's TTL / identity fields for stamping onto this ingestion's records.
        Always pushes (possibly an empty dict) so pop stays balanced in a try/finally."""
        stamp: Json = {}
        if envelope.get("ephemeral"):
            stamp["expires_at"] = envelope.get("expires_at")
            stamp["expires_at_ms"] = envelope.get("expires_at_ms")
            stamp["ephemeral"] = True
        identity_key = envelope.get("identity_key")
        if isinstance(identity_key, str) and identity_key:
            stamp["identity_key"] = identity_key
            stamp["truth_rank"] = int(envelope.get("truth_rank") or 0)
            if envelope.get("truth_class"):
                stamp["truth_class"] = envelope.get("truth_class")
        self._ingest_stamp_stack().append(stamp)

    def _pop_ingest_stamp(self) -> None:
        stack = self._ingest_stamp_stack()
        if stack:
            stack.pop()

    def _current_ingest_stamp(self) -> Json:
        stack = self._ingest_stamp_stack()
        return stack[-1] if stack else {}

    def _stamp_ingest_fields(self, records: list[Json]) -> list[Json]:
        """Stamp the active per-ingestion TTL / identity fields onto scoped records (in place).

        TTL (expires_at / ephemeral) lands on every scoped record EXCEPT summaries (which aggregate
        across ingestions). identity_key / truth_rank lands on the ``context_event`` only. No-op when
        no ingest stamp is active (i.e. every write outside an ephemeral/keyed ingest)."""
        stamp = self._current_ingest_stamp()
        if not stamp:
            return records
        has_ttl = "expires_at_ms" in stamp
        has_identity = "identity_key" in stamp
        for record in records:
            record_type = str(record.get("record_type") or "")
            if record_type not in _MEMORY_SCOPED_RECORD_TYPES:
                continue
            if has_ttl and record_type not in _MEMORY_SUMMARY_RECORD_TYPES:
                if stamp.get("expires_at") is not None:
                    record.setdefault("expires_at", stamp.get("expires_at"))
                record.setdefault("expires_at_ms", stamp.get("expires_at_ms"))
                record.setdefault("ephemeral", True)
            if has_identity and record_type == "context_event":
                record.setdefault("identity_key", stamp.get("identity_key"))
                record.setdefault("truth_rank", int(stamp.get("truth_rank") or 0))
                if stamp.get("truth_class"):
                    record.setdefault("truth_class", stamp.get("truth_class"))
        return records

    def _queue_batched_records(self, records: list[Json]) -> bool:
        batch = self._current_write_batch()
        if batch is None:
            return False
        batch.extend(records)
        return True

    def _local_jsonl_guardrails(self) -> Json:
        return {
            "enabled": self._local_jsonl_enabled,
            "max_bytes": LOCAL_JSONL_MAX_BYTES,
            "retention_count": LOCAL_JSONL_RETENTION_COUNT,
            "retention_age_ms": LOCAL_JSONL_RETENTION_AGE_MS,
            "include_bulky_fields": LOCAL_JSONL_INCLUDE_BULKY_FIELDS,
            "durable_read_cache": {
                "enabled": LOCAL_DURABLE_READ_CACHE_ENABLED,
                "path": str(self._durable_read_cache_path()),
                "schema_version": LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION,
                "last_load_source": self._read_cache_source,
                "min_write_ms": LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS,
            },
            "usage": "testing_debug_only",
        }

    def _sanitize_jsonl_record(self, record: Json) -> Json:
        # Above the early return, so the bulky-fields case is covered too. Both append paths run
        # this per record, which is what makes it the right place -- see round_vector_to_f32.
        record = round_vector_to_f32(record)
        if LOCAL_JSONL_INCLUDE_BULKY_FIELDS:
            return record
        sanitized = dict(record)
        bulky = LOCAL_JSONL_BULKY_FIELDS
        if sanitized.get("record_type") == "context_debug_record":
            # On this record type debug_payload is the entire content, not incidental bulk riding
            # on a larger row. The writer refuses to emit the record at all without a payload
            # (materialize_serving_record returns early), and the record only exists when the
            # caller opts in via MATRIXARK_CONTEXT_DEBUG_RECORDS. Stripping the payload here left
            # a husk that costs bytes and tells every reader nothing -- so the opt-in bought no
            # diagnostics unless the caller also knew to set the unrelated bulky-fields flag.
            # Every other record type keeps the default treatment.
            bulky = bulky - {"debug_payload"}
        dropped = sorted(field for field in bulky if field in sanitized)
        for field in dropped:
            sanitized.pop(field, None)
        if dropped:
            metadata = dict(sanitized.get("jsonl_guardrails", {})) if isinstance(sanitized.get("jsonl_guardrails"), dict) else {}
            metadata["dropped_bulky_fields"] = dropped
            sanitized["jsonl_guardrails"] = metadata
        return sanitized

    def _jsonl_rotated_path(self, index: int) -> Path:
        return self.event_log.with_name(f"{self.event_log.name}.{index}")

    def _log_append_form(self) -> bytes:
        """Which form to append in: whatever the log ALREADY is, else the configured one.

        Taken from the FILE rather than from the flag. That is what keeps two forms out of one file:
        a log written by hand -- which is how a good many fixtures are built -- stays plain and is
        appended to as plain, and a store that crosses the flag changes form only when rotation
        hands it a new file. Turning the flag off again leaves every existing log readable, because
        nothing rewrites one.
        """
        try:
            with self.event_log.open("rb") as handle:
                head = handle.read(len(_SHARD_CONTAINER_MAGIC) + 1)
        except (FileNotFoundError, OSError):
            head = b""
        if head[:len(_SHARD_CONTAINER_MAGIC)] == _SHARD_CONTAINER_MAGIC:
            return head[len(_SHARD_CONTAINER_MAGIC):len(_SHARD_CONTAINER_MAGIC) + 1]
        if head:
            return b""
        return _SHARD_CODEC_BLOCKS if LOCAL_JSONL_BLOCK_LOG else b""

    def _append_records_to_log(self, jsonl_records: list[Json]) -> None:
        """Append one batch to the log, in whichever form the log is written.

        BOTH append paths come through here. They used to encode and write separately, which was
        duplication while there was one form and is a correctness hazard with two -- the same shape
        of defect the snapshot tail had when only one of its two writers knew about blocks.

        The batch is the block, and that is why this costs no durability: the batch is already the
        unit acked together, so a crash loses exactly what it loses today. Per-record blocks would
        be durability-free too and are worth almost nothing -- 1.43x against 9.68x.

        Rotation is decided on the bytes actually written, so a block log holds proportionally more
        history before rotating. That is a behaviour change and a deliberate one: retention stays a
        disk budget, and compressing the log spends it on more history rather than on less disk.

        The form is re-read after rotation, because rotation hands back an EMPTY log -- which adopts
        the configured form, and that need not be the form the old file had.
        """
        form = self._log_append_form()
        block = _encode_log_block(jsonl_records) if form == _SHARD_CODEC_BLOCKS else None
        lines = ([json.dumps(record, separators=(",", ":")) + "\n" for record in jsonl_records]
                 if block is None else [])
        size = len(block) if block is not None else sum(len(line.encode("utf-8")) for line in lines)
        self._rotate_jsonl_if_needed_locked(size)

        after = self._log_append_form()
        if after != form:
            form = after
            block = _encode_log_block(jsonl_records) if form == _SHARD_CODEC_BLOCKS else None
            lines = ([json.dumps(record, separators=(",", ":")) + "\n" for record in jsonl_records]
                     if block is None else [])

        if block is not None:
            fresh = not self.event_log.exists() or self.event_log.stat().st_size == 0
            with self.event_log.open("ab") as handle:
                if fresh:
                    handle.write(_SHARD_CONTAINER_MAGIC + _SHARD_CODEC_BLOCKS)
                handle.write(block)
            return
        with self.event_log.open("a", encoding="utf-8") as handle:
            for line in lines:
                handle.write(line)

    def _seal_rotated_shard(self, path: Path) -> None:
        """Store a just-rotated shard compressed.

        Called AFTER the rename that rotated it, never instead of it: the rename is the commit
        point and stays one atomic operation. This is a follow-up -- temp write, fsync, atomic
        replace -- and a crash anywhere in it leaves the plain shard, which every reader here still
        accepts. The worst outcome is a shard that was not compressed, never one that was lost.

        The temp file is dot-prefixed so it sits outside the <event log name>* namespace, for the
        reason the read-cache files are: a sidecar picked up under that prefix is replayed as
        durable history.

        Sealing happens AT rotation, in the same moment as the rename, so the shard's mtime still
        marks when it was rotated -- which is what the age-based retention prune reads. A lazy sweep
        that sealed shards later would reset that clock and keep them past their age.

        Sealing happens AT rotation, in the same moment as the rename, so the shard's mtime still
        marks when it was rotated -- which is what the age-based retention prune reads. A lazy sweep
        that sealed shards later would reset that clock and keep them past their age.
        """
        if not LOCAL_JSONL_COMPRESS_SEALED:
            return
        try:
            raw = path.read_bytes()
            if raw.startswith(_SHARD_CONTAINER_MAGIC):
                return
            tmp = path.with_name(f".{path.name}.seal.{os.getpid()}.tmp")
            with tmp.open("wb") as handle:
                handle.write(_SHARD_CONTAINER_MAGIC + _SHARD_CODEC_ZLIB)
                handle.write(zlib.compress(raw, LOCAL_DURABLE_READ_CACHE_COMPRESS_LEVEL))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        except OSError:
            pass

    def _retained_jsonl_paths(self) -> list[Path]:
        if not self._local_jsonl_enabled:
            return []
        max_rotated = max(0, LOCAL_JSONL_RETENTION_COUNT - 1)
        paths = [self._jsonl_rotated_path(index) for index in range(max_rotated, 0, -1)]
        paths.append(self.event_log)
        return [path for path in paths if path.exists()]

    def _durable_read_cache_path(self) -> Path:
        return self.event_log.with_name(f"{self.event_log.name}.read-cache.json")

    def _durable_read_cache_binary_path(self) -> Path:
        """The compressed container, when one is written.

        Deliberately NOT the same name as the JSON form. `_load_durable_read_cache` opens the
        snapshot with `encoding="utf-8"` and catches `(FileNotFoundError, json.JSONDecodeError,
        OSError)`; compressed bytes raise `UnicodeDecodeError`, which is a ValueError but not a
        JSONDecodeError, so a reader from before this change would crash on them. Under its own
        name it simply sees no snapshot -- which it already knows how to handle.
        """
        return self.event_log.with_name(f"{self.event_log.name}.read-cache.bin")

    def _durable_read_cache_snapshot_path(self) -> Path:
        """Where the snapshot is, whichever form it was written in.

        Callers that want the snapshot itself -- rather than one encoding of it -- ask here, so a
        change of container does not read as a missing file.
        """
        # Which form this build WRITES, not whichever happens to be on disk. A caller asking
        # "where is the snapshot" usually asks before there is one -- a test fixture, or the gate
        # deciding whether a base exists -- and an answer that changes once the file appears would
        # have them looking at the wrong path. The LOADER still checks both explicitly, so a store
        # carrying the older JSON form keeps loading.
        if LOCAL_DURABLE_READ_CACHE_COMPRESS:
            return self._durable_read_cache_binary_path()
        return self._durable_read_cache_path()

    def _durable_read_cache_delta_path(self) -> Path:
        """Records appended since the base snapshot, one JSON object per line.

        Dot-prefixed so it sits OUTSIDE the <event log name>* namespace: callers glob that
        prefix to enumerate retained shards, and a cache file picked up there would be replayed
        as durable history.
        """
        return self.event_log.with_name(f".{self.event_log.name}.read-cache-delta.jsonl")

    def _durable_read_cache_delta_binary_path(self) -> Path:
        """The block-framed tail, when one is written.

        Its own name, for the reason the base container has one: the plain reader splits this file
        on newlines, and a compressed payload contains them. Under a separate name an older reader
        finds no tail and re-derives, which it already knows how to do.
        """
        return self.event_log.with_name(f".{self.event_log.name}.read-cache-delta.bin")

    def _durable_read_cache_tail_path(self) -> Path:
        """Where the tail is, in whichever form this build writes -- see the base's counterpart."""
        if LOCAL_DURABLE_READ_CACHE_COMPRESS:
            return self._durable_read_cache_delta_binary_path()
        return self._durable_read_cache_delta_path()

    def _append_tail_records(self, appended_records: list[Json]) -> None:
        """Append one batch to the tail, in whichever form this build writes.

        BOTH append paths come through here -- the one a write takes, holding the batch it just
        appended, and the one a read takes, slicing it out of the record set. They used to write the
        tail separately. While the tail had a single form that was duplication; with two forms it is
        a correctness hazard, because the writer would extend the block-framed tail while the reader
        extended the plain one, the loader would read whichever it prefers, and its count would
        disagree with the head -- so every read would re-derive from the log with a perfectly good
        snapshot sitting beside it.

        One block per appended batch. The batch is the boundary the caller already holds, so a block
        costs no buffering and does not move the moment a record becomes durable -- and it reaches
        15.6x where per-record framing reaches 3.3x, because one record carries no dictionary worth
        the name.

        A tail already on disk in the OTHER form is refused rather than joined. Raising here is the
        answer both callers already have: they catch ValueError and fall through to a full rewrite,
        which clears both forms and leaves exactly one tail behind.
        """
        binary = self._durable_read_cache_delta_binary_path()
        plain = self._durable_read_cache_delta_path()
        wanted, other = (binary, plain) if LOCAL_DURABLE_READ_CACHE_COMPRESS else (plain, binary)
        if other.exists() and other.stat().st_size:
            raise ValueError("a tail in the other form is on disk")
        if wanted is binary:
            with binary.open("ab") as handle:
                handle.write(_encode_delta_block(appended_records))
            return
        with plain.open("a", encoding="utf-8") as handle:
            for record in appended_records:
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")

    def _durable_read_cache_head_path(self) -> Path:
        """Signature and counts only -- never the records.

        The signature changes on every append, so whichever file holds it is rewritten every
        time. Keeping it out of the base is the point: the base holds the records and is
        rewritten only when the delta is folded back in.

        Counts are read from HERE rather than kept on the instance. Several adapters can share
        one log, and an instance that has not written yet believes the base is empty -- trusting
        that would append a tail against the wrong offset and duplicate or skip records.
        """
        return self.event_log.with_name(f".{self.event_log.name}.read-cache-head.json")

    def _jsonl_cache_signature_detail(self, paths: list[Path] | None = None) -> Json:
        total_size = 0
        max_mtime_ns = -1
        entries: list[Json] = []
        for path in paths if paths is not None else self._retained_jsonl_paths():
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            size = int(stat.st_size)
            mtime_ns = int(stat.st_mtime_ns)
            total_size += size
            max_mtime_ns = max(max_mtime_ns, mtime_ns)
            # Resolving is a walk over every component; the retained paths do not move, so the
            # resolved form is remembered per path rather than recomputed on each signature.
            resolved = self._resolved_paths.get(path)
            if resolved is None:
                resolved = str(path.resolve())
                self._resolved_paths[path] = resolved
            entries.append({"path": resolved, "size": size, "mtime_ns": mtime_ns})
        if total_size <= 0 and max_mtime_ns < 0:
            return {"total_size": -1, "max_mtime_ns": -1, "paths": []}
        return {"total_size": total_size, "max_mtime_ns": max_mtime_ns, "paths": entries}

    def _jsonl_cache_signature(self) -> tuple[int, int]:
        signature = self._jsonl_cache_signature_detail()
        return int(signature.get("total_size", -1)), int(signature.get("max_mtime_ns", -1))

    def _load_durable_read_cache(self, signature: Json) -> list[Json] | None:
        if not self._local_jsonl_enabled or not LOCAL_DURABLE_READ_CACHE_ENABLED:
            return None
        try:
            with self._durable_read_cache_head_path().open("r", encoding="utf-8") as handle:
                head = json.load(handle)
            binary_path = self._durable_read_cache_binary_path()
            if binary_path.exists():
                payload = _decode_snapshot_bytes(binary_path.read_bytes())
            else:
                with self._durable_read_cache_path().open("r", encoding="utf-8") as handle:
                # Decode the snapshot exactly as the log is decoded. Both paths return the same
                # records, so they have to return them at the same cost, and a bare json.load
                # here does not: it gives every record a private copy of every repeated VALUE.
                # Key names are not the problem on this path -- the whole snapshot is one decode
                # call and the decoder memoises key strings within a call -- which is exactly why
                # this went unnoticed. Values get no such treatment, and they are the larger half:
                # a chunk body is stored once as a skill_section and once as a resource_chunk, so
                # the text of a chunked document arrives twice and was held twice.
                #
                # Measured on a 217 MB snapshot of 100,105 records: 4,233 B/record bare against
                # 3,196 B/record through the hook -- 24.5%, for 0.8 s of decode paid once per
                # process. The delta tail below already used the hook, so until now a store served
                # from its snapshot held a cache a third larger than the same store served from
                # its log, for byte-identical content.
                    payload = json.load(handle, object_pairs_hook=_interned_pairs)
        # zlib.error and ValueError join the list for the container: a truncated or unknown-codec
        # snapshot is derived state like any other unreadable one, and re-deriving from the log is
        # the answer for all of them.
        except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError, zlib.error):
            return None
        if not isinstance(head, dict) or not isinstance(payload, dict):
            return None
        if head.get("schema_version") != LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION:
            return None
        if head.get("cache_key") != self._cache_key_str():
            return None
        if head.get("signature") != signature:
            return None
        records = payload.get("records")
        if not isinstance(records, list):
            return None
        records = [record for record in records if isinstance(record, dict)]
        # Undo the interning the writer applied, dropping the sidecars, so what comes back is the
        # record set the caller stored -- the same inverse the log path applies at its own read.
        # Called unconditionally rather than behind the intern flag, because the flag can differ
        # between the process that wrote the file and the one reading it; with nothing interned
        # this takes the no-op path. It runs before the count check, which counts data records.
        records = expand_interned_records(records)
        if len(records) != head.get("record_count"):
            return None
        delta_count = head.get("delta_count") or 0
        if delta_count:
            try:
                binary_tail = self._durable_read_cache_delta_binary_path()
                if binary_tail.exists():
                    tail = _decode_delta_blocks(binary_tail.read_bytes())
                else:
                    with self._durable_read_cache_delta_path().open("r", encoding="utf-8") as handle:
                        tail = [loads_with_interned_keys(line) for line in handle if line.strip()]
            except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError, zlib.error):
                return None
            if len(tail) != delta_count:
                return None
            records.extend(record for record in tail if isinstance(record, dict))
        # A tail appended against the wrong base would still satisfy the counts, so check the
        # record the head named. A mismatch returns None and the caller re-derives from the log,
        # which is the same fallback a missing snapshot takes.
        recorded = head.get("tail_fingerprint")
        if recorded and records and _snapshot_prefix_fingerprint(records[-1]) != recorded:
            return None
        # Compact what was stitched. The base is a checkpoint and the delta is what has been
        # appended since, so together they can hold rows the base recorded and a later append
        # superseded. Compaction is what makes that correct, and it is what lets the writer stop
        # caring whether the persisted prefix is still a prefix.
        return compact_and_apply_tombstones(expand_interned_records(records))

    def _durable_read_cache_tail_fingerprint(self) -> str:
        """The fingerprint the head recorded, or "" when there is none to trust."""
        try:
            with self._durable_read_cache_head_path().open("r", encoding="utf-8") as handle:
                head = json.load(handle)
            if head.get("cache_key") != self._cache_key_str():
                return ""
            if head.get("schema_version") != LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION:
                return ""
            recorded = head.get("tail_fingerprint")
            return recorded if isinstance(recorded, str) else ""
        except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError, ValueError):
            return ""

    def _durable_read_cache_counts(self) -> tuple[int, int]:
        """(base_count, delta_count) as recorded on disk, or (0, 0)."""
        try:
            with self._durable_read_cache_head_path().open("r", encoding="utf-8") as handle:
                head = json.load(handle)
            if head.get("cache_key") != self._cache_key_str():
                return (0, 0)
            if head.get("schema_version") != LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION:
                return (0, 0)
            return (int(head.get("record_count") or 0), int(head.get("delta_count") or 0))
        except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError, ValueError):
            return (0, 0)

    def _durable_read_cache_signature(self) -> Json | None:
        """The signature the head recorded, or None when there is none to trust."""
        try:
            with self._durable_read_cache_head_path().open("r", encoding="utf-8") as handle:
                head = json.load(handle)
            if head.get("cache_key") != self._cache_key_str():
                return None
            if head.get("schema_version") != LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION:
                return None
            return head.get("signature")
        except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError, ValueError):
            return None

    def _refresh_durable_read_cache_if_behind(
        self, records: list[Json], signature: Json, epoch: int | None
    ) -> None:
        """Bring the snapshot up to the log this read just served.

        A write only ever continues the tail and leaves a snapshot it cannot continue for "the
        next read" to refresh. That only happened on the branch that re-derives from the log --
        but once a cache is warm, every read is served from it and that branch is never reached
        again. The snapshot froze at whatever the first read wrote, and because a stale snapshot
        no longer matches the log's signature, the write path stopped being able to continue it
        either: the two paths each waited for the other and every restart re-derived the log.

        Reading only the head keeps this to one small file per read, and a read that added
        nothing since the snapshot does no work at all.
        """
        if not self._local_jsonl_enabled or not LOCAL_DURABLE_READ_CACHE_ENABLED:
            return
        if self._durable_read_cache_signature() == signature:
            return
        self._write_durable_read_cache(list(records), signature, epoch=epoch)

    def _write_durable_read_cache(
        self,
        records: list[Json],
        signature: Json,
        *,
        force: bool = False,
        epoch: int | None = None,
        tail_only: bool = False,
        appended_records: list[Json] | None = None,
        pre_size: int | None = None,
    ) -> None:
        """Persist the read snapshot.

        epoch identifies the caller's compaction generation for a list that only ever grows
        at the end. Only then can the tail be written on its own -- compaction rewrites the list,
        and a shorter or reordered prefix makes a slice-based tail wrong. Callers holding a list
        they cannot make that promise for pass None and get a full rewrite.
        """
        if not self._local_jsonl_enabled or not LOCAL_DURABLE_READ_CACHE_ENABLED:
            return
        if int(signature.get("total_size", -1)) < 0:
            return
        now = now_ms()
        path = self._durable_read_cache_path()
        delta_path = self._durable_read_cache_delta_path()
        head_path = self._durable_read_cache_head_path()
        base_count, delta_count = self._durable_read_cache_counts()
        # Matching counts are not proof the bytes match: several adapters share one log, and a
        # list of the same length can hold different records. Continue the tail only when the
        # counts on disk are still exactly the ones THIS instance last wrote AND no compaction
        # has run since -- together those mean the persisted base really is the prefix of the
        # list in hand, whatever happened in between.
        appended = len(records) - (base_count + delta_count)
        contiguous = (
            epoch is not None
            and appended > 0
            and self._durable_read_cache_state
            == (self._cache_key_str(), base_count, delta_count, epoch)
        )
        if not contiguous and appended > 0 and base_count > 0:
            # The list handed over is often the process-wide one, shared by every adapter over this
            # log, and no instance can vouch for it from its own bookkeeping -- so `epoch` arrives
            # as None and the whole base was being rewritten on every read. Measured with two
            # adapters on one log, 290 of 291 writes took that path.
            #
            # Disk can answer the question that bookkeeping cannot: the head names the last record
            # it persisted, so if this list carries that same record at that same position, the
            # file is a prefix of it and only the tail is missing.
            persisted = base_count + delta_count
            recorded = self._durable_read_cache_tail_fingerprint()
            if recorded and recorded == _snapshot_prefix_fingerprint(records[persisted - 1]):
                contiguous = True

        def write_head(record_count: int, deltas: int, last_record: Json | None = None) -> None:
            # The record this snapshot really ends on. It is records[-1] for a full rewrite, but
            # the append path hands over what it wrote, which is not a slice of `records`.
            persisted_last = last_record if last_record is not None else (
                records[-1] if records else None)
            tmp = head_path.with_name(f"{head_path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
            with tmp.open("w", encoding="utf-8") as handle:
                json.dump({
                    "schema_version": LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION,
                    "cache_key": self._cache_key_str(),
                    "signature": signature,
                    "record_count": record_count,
                    "delta_count": deltas,
                    # The last record this snapshot holds, so another adapter over the same log
                    # can prove the file is a prefix of its own list -- see the contiguity check.
                    "tail_fingerprint": (
                        _snapshot_prefix_fingerprint(persisted_last)
                        if persisted_last is not None else ""
                    ),
                }, handle, separators=(",", ":"))
                handle.write("\n")
            tmp.replace(head_path)

        # The append path writes exactly the records it was handed, and asks nothing about
        # whether the persisted prefix is still a prefix.
        #
        # That question is what forced the rewrite: ingest compacts, compaction removes rows
        # from the middle, so the live list is usually NOT an extension of what is on disk and
        # the slice below cannot apply. The load path compacts what it stitches instead, so a
        # base holding rows that have since been superseded is reconciled rather than wrong.
        #
        # Safe because the append path never writes a record twice: what base+delta can contain
        # is SUPERSEDED rows, and those are keyed, so compaction removes them. Duplicates would
        # not be -- 56% of records have no latest-value key -- which is why the delta must carry
        # what was appended and never a slice.
        #
        # Writing the head with the current signature is also what stops the READ path
        # rewriting: `_refresh_durable_read_cache_if_behind` returns early when the recorded
        # signature already matches.
        #
        # The one thing it must still establish is that the snapshot on disk describes the log as
        # it stood BEFORE this write. Another writer -- in this process or another -- can have
        # appended records that never passed through this instance, and extending the delta over
        # them would publish a view that silently drops them: 55 records where the log holds 75.
        # The head records the log it was stamped for, so requiring that to equal `pre_size` is
        # exactly the question 'was I already behind?', and a no sends this write down the
        # rewrite path where the full record set is reconciled.
        head_signature = self._durable_read_cache_signature() or {}
        covers_log_before_this_write = (
            pre_size is not None
            and int(head_signature.get("total_size", -1)) == int(pre_size)
        )
        if (
            appended_records
            and base_count > 0
            # The snapshot in whichever form it was written, not one encoding of it. Asking
            # `path.exists()` here tied the tail to the JSON file, so a container base answered
            # False and no tail was ever written.
            and self._durable_read_cache_snapshot_path().exists()
            and covers_log_before_this_write
            and delta_count + len(appended_records) <= LOCAL_DURABLE_READ_CACHE_MAX_DELTA
        ):
            try:
                self._append_tail_records(appended_records)
                write_head(base_count, delta_count + len(appended_records),
                           appended_records[-1])
                self._durable_read_cache_state = (
                    self._cache_key_str(), base_count,
                    delta_count + len(appended_records), epoch
                )
                self._durable_read_cache_last_write_ms = now
                return
            except (OSError, TypeError, ValueError):
                pass   # fall through; a full rewrite also clears the partial tail

        # Append-only fast path. The base holds the whole record set, so rewriting it costs
        # O(corpus) JSON on every append -- and retrieval appends recall-reinforcement markers,
        # so every query paid it. Writing just the tail keeps the cost proportional to what
        # actually changed, and base + delta reconstruct the same view, so the snapshot stays
        # current for a restart.
        if (
            contiguous
            and base_count > 0
            and delta_count + appended <= LOCAL_DURABLE_READ_CACHE_MAX_DELTA
            and self._durable_read_cache_snapshot_path().exists()
        ):
            try:
                self._append_tail_records(records[base_count + delta_count:])
                write_head(base_count, delta_count + appended)
                self._durable_read_cache_state = (
                    self._cache_key_str(), base_count, delta_count + appended, epoch
                )
                self._durable_read_cache_last_write_ms = now
                return
            except (OSError, TypeError, ValueError):
                pass   # fall through to a full rewrite, which also clears the partial tail

        if tail_only:
            # The caller is a write. A full rewrite here is O(corpus) per append; leave the snapshot
            # as it stands and let the next read refresh it.
            return
        # Everything below rewrites the WHOLE record set, so the floor belongs here and nowhere
        # earlier. It used to sit at the top of this function, where it gated the tail append above
        # as well -- and the tail append is what keeps the head's signature current after a write,
        # so a floor there made a snapshot unusable for a restart and broke four tests asserting
        # exactly that. Guarding only the rewrite leaves appends as prompt as they were.
        if not force and LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS > 0:
            if now - self._durable_read_cache_last_write_ms < LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS:
                return
        tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        payload = {
            "schema_version": LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION,
            "cache_key": self._cache_key_str(),
            "signature": signature,
            "record_count": len(records),
            # Store what the log stores. The log writes each record with its interned metadata
            # replaced by a bundle token and one sidecar per distinct bundle; the snapshot was
            # writing the same records fully expanded, so the largest file in the store was the
            # one copy of the data that had opted out of the compression.
            #
            # What it is worth depends on how much of a record is metadata rather than body, so
            # the range matters more than any single figure. Re-encoding snapshots two real runs
            # left behind: 13.4 MB -> 4.3 MB (67.8%) on a store of 3,022 small records, and
            # 207.2 MB -> 186.3 MB (10.1%) on 100,105 records whose text and vectors dominate.
            # The sidecar count barely moves with the corpus -- 22 for 99,919 tokened records --
            # so this is the whole per-record cost of these fields, not a ratio that decays.
            #
            # `record_count` above counts data records, which is what the load path recovers
            # after expansion; the encoded list is longer by the sidecars it carries.
            "records": encode_interned_records(records, set()),
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            binary_path = self._durable_read_cache_binary_path()
            if LOCAL_DURABLE_READ_CACHE_COMPRESS:
                _write_blocked_snapshot(tmp_path, payload)
                tmp_path.replace(binary_path)
                stale = path
            else:
                with tmp_path.open("w", encoding="utf-8") as handle:
                    json.dump(payload, handle, separators=(",", ":"))
                    handle.write("\n")
                tmp_path.replace(path)
                stale = binary_path
            # Only one form is the snapshot. Leaving the other behind would let a reader that
            # prefers it serve a record set from a different write.
            try:
                stale.unlink()
            except FileNotFoundError:
                pass
            for stale_tail in (delta_path, self._durable_read_cache_delta_binary_path()):
                try:
                    stale_tail.unlink()
                except FileNotFoundError:
                    pass
            write_head(len(records), 0)
            self._durable_read_cache_state = (
                self._cache_key_str(), len(records), 0, epoch
            )
            self._durable_read_cache_last_write_ms = now
        except OSError:
            try:
                tmp_path.unlink()
            except OSError:
                pass

    def _clear_jsonl_read_caches(self) -> None:
        cache_key = self._cache_key_str()
        with self._read_cache_lock:
            self._read_cache_records = None
            self._read_cache_value_keys = None
            self._read_cache_state_keys = None
            self._summary_dirty_index = None
            self._pipeline_task_index = None
            self._node_embedding_refs_index = None
            self._read_cache_size = -1
            self._read_cache_mtime_ns = -1
            self._read_cache_source = "empty"
        with _LOCAL_READ_CACHE_LOCK:
            _LOCAL_READ_CACHE.pop(cache_key, None)
            _LOCAL_READ_CACHE_DIRTY.discard(cache_key)
        for cache_file in (self._durable_read_cache_path(),
                           self._durable_read_cache_binary_path(),
                           self._durable_read_cache_delta_path(),
                           self._durable_read_cache_delta_binary_path(),
                           self._durable_read_cache_head_path()):
            try:
                cache_file.unlink()
            except FileNotFoundError:
                pass

    def _prune_jsonl_retention_locked(self) -> None:
        max_rotated = max(0, LOCAL_JSONL_RETENTION_COUNT - 1)
        now_timestamp = max(0.0, now_ms() / 1000.0)
        max_age_s = max(0.0, LOCAL_JSONL_RETENTION_AGE_MS / 1000.0)
        index = max_rotated + 1
        while True:
            path = self._jsonl_rotated_path(index)
            if not path.exists():
                break
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            index += 1
        if max_age_s <= 0:
            return
        for path in [self._jsonl_rotated_path(index) for index in range(1, max_rotated + 1)]:
            try:
                if now_timestamp - float(path.stat().st_mtime) > max_age_s:
                    path.unlink()
            except FileNotFoundError:
                continue

    def _rotate_jsonl_if_needed_locked(self, incoming_bytes: int) -> None:
        if not self._local_jsonl_enabled:
            return
        self._prune_jsonl_retention_locked()
        max_bytes = max(1, LOCAL_JSONL_MAX_BYTES)
        try:
            current_size = int(self.event_log.stat().st_size)
        except FileNotFoundError:
            current_size = 0
        if current_size <= 0 or current_size + max(0, incoming_bytes) <= max_bytes:
            return
        max_rotated = max(0, LOCAL_JSONL_RETENTION_COUNT - 1)
        if max_rotated <= 0:
            try:
                self.event_log.unlink()
            except FileNotFoundError:
                pass
            self._clear_jsonl_read_caches()
            return
        oldest = self._jsonl_rotated_path(max_rotated)
        try:
            oldest.unlink()
        except FileNotFoundError:
            pass
        for index in range(max_rotated - 1, 0, -1):
            source = self._jsonl_rotated_path(index)
            if source.exists():
                source.replace(self._jsonl_rotated_path(index + 1))
        if self.event_log.exists():
            self.event_log.replace(self._jsonl_rotated_path(1))
            self._seal_rotated_shard(self._jsonl_rotated_path(1))
        self._clear_jsonl_read_caches()

    @contextmanager
    def write_batch(self, label: str = "hot_path"):
        stack = self._write_batch_stack()
        batch: list[Json] = []
        stack.append(batch)
        try:
            yield batch
        except Exception:
            stack.pop()
            raise
        else:
            stack.pop()
            if batch:
                self.append_many(batch)

    def ensure_backend_ready(self, *, reason: str = "manual", probe: bool = True, timeout_ms: int | None = None) -> Json:
        return {
            "status": "ready",
            "backend": "local",
            "reason": reason,
            "probe": bool(probe),
            "attempts": 1,
            "topology": {"mode": "local-jsonl", "event_log": str(self.event_log), "jsonl_guardrails": self._local_jsonl_guardrails()},
            "checks": {
                "mcp_process_started": True,
                "namespace_table_opened": True,
                "slot_coverage_verified_by_warmup_hset_hget": True,
            },
        }

    def backend_metrics(self) -> Json:
        return {
            "backend": getattr(self, "_backend_label", lambda: "local")(),
            "metrics_format": "json",
            "metrics": {
                "mode": "local-jsonl",
                "event_log": str(self.event_log),
                "jsonl_guardrails": self._local_jsonl_guardrails(),
            },
        }

    def _observe_model_latency(self, stage: str, elapsed_ms: float) -> None:
        metrics = getattr(self, "_matrixark_service_metrics", None)
        if metrics is not None:
            try:
                metrics.observe_model_latency(stage, elapsed_ms)
            except Exception:
                pass

    #: A batch carrying one of these cannot be applied by appending: a tombstone or cutoff
    #: removes records that came BEFORE it, a context_index posting is rebuilt and coalesced
    #: across the whole set, and a pipeline-task or audit row is footprint-bounded across it.
    _COMPACTION_IS_NOT_LOCAL = frozenset({
        "context_index",
        "matrixark_async_pipeline_task",
        "context_extraction_audit",
    })

    @staticmethod
    def _batch_is_local(records: list[Json]) -> bool:
        """True when nothing in the batch can affect a record already in the cache."""
        for record in records:
            if not isinstance(record, dict):
                return False
            record_type = str(record.get("record_type") or "")
            if record_type in MatrixArkLocalAdapter._COMPACTION_IS_NOT_LOCAL:
                return False
            if "tombstone" in record_type or "retention_cutoff" in record_type:
                return False
        return True

    def _cache_keys_locked(self):
        """The keys the compacted cache holds, built once and then kept current."""
        if self._read_cache_value_keys is None or self._read_cache_state_keys is None:
            records = self._read_cache_records or []
            self._read_cache_value_keys = {latest_value_record_key(r) for r in records}
            self._read_cache_state_keys = {latest_context_state_key(r) for r in records}
            self._read_cache_value_keys.discard(None)
            self._read_cache_state_keys.discard(None)
        return self._read_cache_value_keys, self._read_cache_state_keys

    def _note_embedding_owners(self, records: list[Json]) -> None:
        """Fold newly appended records into whichever owner buckets have been built.

        Only existing buckets are updated: a (type, field) nobody has asked about is built
        from a read when first needed, and includes these records by then. Later records
        overwrite earlier ones, which is the newest-wins answer the backwards scan gave.
        """
        index = self._embedding_owner_index
        if not index:
            return
        for record in records:
            if not isinstance(record, dict):
                continue
            record_type = record.get("record_type")
            for (indexed_type, field), bucket in index.items():
                if indexed_type != record_type:
                    continue
                value = record.get(field)
                if value is not None:
                    bucket[value] = record

    def _cache_key_str(self) -> str:
        """The resolved event-log path, computed once.

        Path.resolve() walks every component and stats it. This value is the same for the life
        of the adapter, and it was being recomputed at each of a dozen call sites on every
        append -- 428 filesystem stats per attachment came through here and the retained-path
        scan beside it.
        """
        if self._resolved_cache_key is None:
            self._resolved_cache_key = str(self.event_log.resolve())
        return self._resolved_cache_key

    def _compact_read_cache_if_dirty_locked(self) -> None:
        """Compact the cache if records were appended since the last compaction.

        Callers hold ``_read_cache_lock``. Compaction only ever REMOVES -- tombstoned and
        superseded records -- so a total that did not change means nothing was dropped and any
        persisted prefix still stands, which is what the epoch records.

        Deferring is the point: the read path serves this list without re-compacting, so it has to
        be compact when READ, not after every append.
        """
        if not self._read_cache_dirty or self._read_cache_records is None:
            self._read_cache_dirty = False
            return
        before = len(self._read_cache_records)
        # Everything in the cache was shared when it entered, and compaction returns those same
        # objects for every row it does not rebuild. Only the rebuilt ones need looking at.
        # The ids cannot be recycled underneath this: `already` is built from a list this frame
        # still holds, so every object in it stays alive until the call returns.
        already_shared = {id(record) for record in self._read_cache_records}
        self._read_cache_records = share_repeated_values(
            compact_and_apply_tombstones(self._read_cache_records),
            _SHARED_VALUE_TABLE,
            already_shared,
        )
        self._read_cache_dirty = False
        if len(self._read_cache_records) != before:
            self._read_cache_compaction_epoch += 1

    def _update_read_cache_after_append(
        self, records: list[Json], *, pre_size: int | None = None
    ) -> None:
        """Fold freshly appended records into every read cache -- but only into a view that
        actually covered the log up to this write.

        ``pre_size`` is the retained-log byte total as it stood before this instance's write,
        captured under the event-log lock. A cached view is only allowed to absorb the append
        when the bytes it covers equal that number: the signature alone cannot catch a stale
        view, because it describes the log at WRITE time, so a list missing another writer's
        records still stamps the current signature and gets served to cold readers as if it
        were complete.
        """
        if not records:
            return
        cache_key = self._cache_key_str()
        signature = self._jsonl_cache_signature_detail()
        size = int(signature.get("total_size", -1))
        mtime_ns = int(signature.get("max_mtime_ns", -1))
        durable_records: list[Json] | None = None
        # Compaction generation of the list handed to the durable writer, or None when the list
        # is not this instance's own append-only one -- see _write_durable_read_cache.
        durable_epoch: int | None = None
        with self._read_cache_lock:
            if (
                pre_size is not None
                and self._read_cache_records is not None
                and self._read_cache_size >= 0
                and self._read_cache_size != pre_size
            ):
                # Another writer appended since this view was established. Extending it would
                # stamp the current signature onto a list missing their records, and a cold
                # reader would silently lose them. Drop it; the next read re-derives from disk.
                self._read_cache_records = None
                self._read_cache_value_keys = None
                self._read_cache_state_keys = None
                self._summary_dirty_index = None
                self._pipeline_task_index = None
                self._node_embedding_refs_index = None
                self._read_cache_size = -1
                self._read_cache_mtime_ns = -1
                self._read_cache_source = "empty"
            if self._read_cache_records is not None:
                # Extend now, compact when something reads. Compacting here walked the whole cache
                # on every append, which is what made ingest quadratic in the corpus: 27 records
                # land per attachment, so 50 attachments took 105 s against 12 s for 20.
                # A batch that supersedes nothing needs no compaction at all: every stage of
                # the pipeline leaves the existing records exactly as they were, so the cache
                # is still compact after appending. Measured over an attachment ingest, half
                # of all cache updates are of that shape. The keys are kept current, so
                # deciding costs the size of the BATCH, not of the cache.
                appends_only = False
                if self._batch_is_local(records):
                    value_keys, state_keys = self._cache_keys_locked()
                    new_value = [latest_value_record_key(r) for r in records]
                    new_state = [latest_context_state_key(r) for r in records]
                    appends_only = not (
                        any(k is not None and k in value_keys for k in new_value)
                        or any(k is not None and k in state_keys for k in new_state)
                    )
                    # Extend either way. The sets exist only to answer 'could this batch
                    # supersede something', and a SUPERSET answers that safely: it can say yes
                    # when the answer is no, which costs a compaction that was not needed, but
                    # it can never say no when the answer is yes. Discarding them on a
                    # colliding batch meant rebuilding from the whole cache on the next one --
                    # 3.7 million key computations over 250 attachments, 36% of the time.
                    value_keys.update(k for k in new_value if k is not None)
                    state_keys.update(k for k in new_state if k is not None)
                self._read_cache_records.extend(
                    share_repeated_values(records, _SHARED_VALUE_TABLE)
                )
                if not appends_only:
                    self._read_cache_dirty = True
                self._note_embedding_owners(records)
                if self._summary_dirty_index is not None:
                    for record in records:
                        self._note_summary_dirty_row(self._summary_dirty_index, record)
                if self._pipeline_task_index is not None:
                    for record in records:
                        if (isinstance(record, dict)
                                and str(record.get("record_type") or "")
                                == "matrixark_async_pipeline_task"):
                            self._pipeline_task_index.append(record)
                if self._node_embedding_refs_index is not None:
                    for record in records:
                        self._note_node_embedding_ref(self._node_embedding_refs_index, record)
                durable_epoch = self._read_cache_compaction_epoch
            if size >= 0:
                self._read_cache_size = size
                self._read_cache_mtime_ns = mtime_ns
                if self._read_cache_records is not None and not self._read_cache_dirty:
                    # A cold reader is served the snapshot without re-compacting, so only a
                    # COMPACT cache may be copied into it.
                    #
                    # Compacting here to make that true undid the deferral: the copy is taken on
                    # every append, so every append compacted after all. It was 322 compactions
                    # over 40 attachments, 112,965 records visited, 28% of the time an ingest
                    # took. A write already declines to rewrite the base and lets the next read
                    # refresh the snapshot; declining to extend it while the cache is dirty is
                    # the same trade, and the read that compacts will refresh it.
                    durable_epoch = self._read_cache_compaction_epoch
                    durable_records = list(self._read_cache_records)
            else:
                self._read_cache_records = None
                self._read_cache_value_keys = None
                self._read_cache_state_keys = None
                self._summary_dirty_index = None
                self._pipeline_task_index = None
                self._node_embedding_refs_index = None
                self._read_cache_size = -1
                self._read_cache_mtime_ns = -1
                self._read_cache_source = "empty"
        with _LOCAL_READ_CACHE_LOCK:
            cached = _LOCAL_READ_CACHE.get(cache_key)
            if cached is not None and pre_size is not None and cached[0] != pre_size:
                # The shared entry does not cover the log as it stood before this write either
                # (a writer in another process got in), so it is stale the same way.
                _LOCAL_READ_CACHE.pop(cache_key, None)
                _LOCAL_READ_CACHE_DIRTY.discard(cache_key)
                cached = None
            if cached is not None:
                _, _, cached_records = cached
                # Extend now, compact when something reads -- see _LOCAL_READ_CACHE_DIRTY.
                _LOCAL_READ_CACHE[cache_key] = (
                    self._read_cache_size, self._read_cache_mtime_ns,
                    list(cached_records) + list(records))
                _LOCAL_READ_CACHE_DIRTY.add(cache_key)
                # This entry is uncompacted, so no BASE may be written from it -- a cold reader
                # would be served superseded and tombstoned records. The appended records are
                # still handed to the snapshot below: they go to the delta, which is compacted
                # at load, so they carry no such claim.
            elif self._read_cache_records is not None:
                with self._read_cache_lock:
                    self._compact_read_cache_if_dirty_locked()
                    _LOCAL_READ_CACHE[cache_key] = (
                        self._read_cache_size,
                        self._read_cache_mtime_ns,
                        list(self._read_cache_records),
                    )
                _LOCAL_READ_CACHE_DIRTY.discard(cache_key)
        # Only continue the tail here; never rewrite the whole base from a write.
        #
        # Ingest does not just append: compaction rewrites earlier records, so the list is
        # usually not an extension of what is persisted and the tail cannot apply. Measured over
        # 40 ingests, 173 snapshot writes, 146 of them full rewrites of the entire record set,
        # which came to two thirds of the time an ingest took.
        #
        # The snapshot is derived state -- _load_durable_read_cache checks it against the log's
        # signature and returns None on any mismatch, and the caller re-derives. Skipping a
        # rewrite here costs a slower cold start until the next read, and the read path installs
        # the records and writes the snapshot anyway.
        #
        # The appended records are handed over whether or not this instance holds a compact list.
        # They were gated on one because the delta was replayed onto the base as-is; it is now
        # compacted at load, so a delta carrying a since-superseded row is reconciled rather than
        # wrong. That gate is what left the snapshot behind on every append made while the shared
        # entry was dirty -- and a snapshot that is behind is rebuilt whole by the next read.
        #
        # `durable_records` remains the base-rewrite argument and stays empty when this instance
        # cannot vouch for a compact list. With tail_only set, an empty list writes no base.
        self._write_durable_read_cache(
            list(durable_records or []), signature, epoch=durable_epoch, tail_only=True,
            appended_records=list(records), pre_size=pre_size)
        if any(str(record.get("record_type") or "") in RETRIEVAL_HOT_RECORD_TYPES for record in records):
            with self._retrieval_records_cache_lock:
                self._retrieval_records_cache_generation += 1
                self._retrieval_records_cache.clear()
                with self._context_pack_cache_lock:
                    self._context_pack_cache.clear()

    def _seed_intern_tokens_locked(self) -> None:
        """Seed ``_intern_emitted_tokens`` from the durable log so a fresh adapter appending to an
        existing interned log does not re-emit dict records already present. Best-effort: duplicate
        dict records are harmless (content-addressed), so any read error just leaves the set empty."""
        if self._intern_tokens_seeded or not INTERN_RECORD_METADATA:
            self._intern_tokens_seeded = True
            return
        self._intern_tokens_seeded = True
        if not self._local_jsonl_enabled:
            return
        try:
            for path in self._retained_jsonl_paths():
                for line in _iter_shard_lines(path):
                    line = line.strip()
                    if not line or INTERN_DICT_RECORD_TYPE not in line:
                        continue
                    try:
                        record = loads_with_interned_keys(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(record, dict) and str(record.get("record_type") or "") == INTERN_DICT_RECORD_TYPE:
                        token = record.get("im_token")
                        if isinstance(token, str) and isinstance(record.get("im_bundle"), dict):
                            self._intern_emitted_tokens.add((INTERN_BUNDLE_EMIT_KEY, token))
                            continue
                        field = record.get("im_field")
                        if isinstance(field, str) and isinstance(token, str):
                            self._intern_emitted_tokens.add((field, token))
        except OSError:
            pass

    def _encode_records_for_log(self, sanitized: list[Json]) -> list[Json]:
        """Intern the metadata fields for durable storage. ``sanitized`` records are already
        bulky-field-stripped; the returned list interleaves any new ``matrixark_intern_dict`` sidecar
        records (first) ahead of the encoded data records. No-op when the flag is OFF."""
        if not INTERN_RECORD_METADATA:
            return pack_record_vectors(sanitized)
        if not self._intern_tokens_seeded:
            self._seed_intern_tokens_locked()
        return pack_record_vectors(
            encode_interned_records(sanitized, self._intern_emitted_tokens))

    def _seed_model_registry_seen_locked(self) -> None:
        """Seed the seen-identity set from the durable log so a fresh adapter does not re-append a
        registry record whose identity is already present. Reads raw log lines (registry records carry
        no interned fields) so it never re-enters the read/compaction path. Best-effort."""
        if getattr(self, "_model_registry_seeded", False):
            return
        self._model_registry_seeded = True
        if not hasattr(self, "_model_registry_seen"):
            self._model_registry_seen = set()
        if not self._local_jsonl_enabled:
            return
        try:
            for path in self._retained_jsonl_paths():
                for line in _iter_shard_lines(path):
                    line = line.strip()
                    if not line or "context_model_registry" not in line:
                        continue
                    try:
                        record = loads_with_interned_keys(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(record, dict) and str(record.get("record_type") or "") == "context_model_registry":
                        self._model_registry_seen.add(_model_registry_identity(record))
        except OSError:
            pass

    def _filter_duplicate_model_registry(self, records: list[Json]) -> list[Json]:
        """Drop context_model_registry records whose semantic identity is already durably present.
        Keeps at least one record per distinct model; a changed field is a new identity."""
        if not any(isinstance(r, dict) and r.get("record_type") == "context_model_registry" for r in records):
            return records
        if not getattr(self, "_model_registry_seeded", False):
            self._seed_model_registry_seen_locked()
        if not hasattr(self, "_model_registry_seen"):
            self._model_registry_seen = set()
        kept: list[Json] = []
        for record in records:
            if isinstance(record, dict) and str(record.get("record_type") or "") == "context_model_registry":
                identity = _model_registry_identity(record)
                if identity in self._model_registry_seen:
                    continue
                self._model_registry_seen.add(identity)
            kept.append(record)
        return kept

    #: The only two row types the outstanding-dirty answer depends on.
    _SUMMARY_DIRTY_TYPES = ("context_summary_dirty", "context_summary_refresh_audit")

    def _summary_dirty_rows(self) -> list[Json]:
        """The newest summary-dirty and refresh-audit row per dirty_hash.

        This was answered by scanning the whole live view twice, once per append -- 40 of the 85
        full reads a twenty-attachment ingest performs. Those two types are a small fraction of the
        store, so the adapter keeps just them, built from one read the first time and folded
        forward on every append. Newest wins per dirty_hash, which is what compaction does with
        these rows.
        """
        index = self._summary_dirty_index
        if index is None:
            index = {}
            try:
                live = self.read_all()
            except (OSError, ValueError):
                live = []
            for record in live:
                self._note_summary_dirty_row(index, record)
            self._summary_dirty_index = index
        return list(index.values())

    def idle_commit_task_records(self, scope: Json) -> list[Json]:
        """Only the pipeline-task rows, without walking the store.

        `pre_retrieval_idle_commit_flush` looks for scheduled idle-commit tasks before every
        retrieve. It prefers this method and falls back to `read_all()`, and the local adapter did
        not offer it -- so the fallback ran, scanning the entire live view TWICE per query to find
        rows that number in the handful. Profiled on a 1 MB corpus at 8,548 records, that flush was
        68% of a settled retrieve.

        The native reader has offered this since it was written; this is the same contract for the
        python one. Scope is accepted and not filtered on here: the caller already applies
        `scope_matches` to every row it considers, and narrowing twice would mean rebuilding this
        index per scope rather than per cache generation.

        Kept the way `_summary_dirty_rows` is kept -- built from one read the first time, folded
        forward on append, and dropped whenever the cache it describes is.
        """
        index = self._pipeline_task_index
        if index is None:
            index = []
            try:
                live = self.read_all()
            except (OSError, ValueError):
                live = []
            for record in live:
                if (isinstance(record, dict)
                        and str(record.get("record_type") or "")
                        == "matrixark_async_pipeline_task"):
                    index.append(record)
            self._pipeline_task_index = index
        return list(index)

    @staticmethod
    def _note_summary_dirty_row(index: dict[Any, Json], record: Json) -> None:
        if not isinstance(record, dict):
            return
        if str(record.get("record_type") or "") not in MatrixArkLocalAdapter._SUMMARY_DIRTY_TYPES:
            return
        dirty_hash = record.get("dirty_hash")
        if dirty_hash is None:
            return
        index[dirty_hash] = record

    def _outstanding_dirty_nodes(self) -> set[tuple[str, Any]]:
        """(scope_key, node_hash) pairs with an uncompleted pending context_summary_dirty marker.

        A node is reported outstanding only if a pending marker is really present, so coalescing can
        never drop the last marker for a node that still needs regeneration.
        """
        rows = self._summary_dirty_rows()
        completed: set[Any] = set()
        for record in rows:
            dirty_hash = record.get("dirty_hash")
            if dirty_hash is not None and record.get("status") in ("completed", "refreshed"):
                completed.add(dirty_hash)
        pending: set[tuple[str, Any]] = set()
        for record in rows:
            if str(record.get("record_type") or "") != "context_summary_dirty":
                continue
            if record.get("status") != "pending":
                continue
            dirty_hash = record.get("dirty_hash")
            node_hash = record.get("node_hash")
            if node_hash is None or dirty_hash in completed:
                continue
            pending.add(
                (str(record.get("scope_key") or _canonical_scope_key_of(record)), node_hash)
            )
        return pending

    def _coalesce_summary_dirty(self, records: list[Json]) -> list[Json]:
        """Drop redundant pending summary-dirty markers for a (scope, node) that already has an
        outstanding uncompleted marker. Completion / non-pending markers pass through. No-op when the
        batch carries no pending markers."""
        pending_in_batch = [
            r for r in records
            if isinstance(r, dict) and str(r.get("record_type") or "") == "context_summary_dirty"
            and r.get("status") == "pending"
        ]
        if not pending_in_batch:
            return records
        outstanding = self._outstanding_dirty_nodes()
        kept: list[Json] = []
        for record in records:
            if (
                isinstance(record, dict)
                and str(record.get("record_type") or "") == "context_summary_dirty"
                and record.get("status") == "pending"
            ):
                node_hash = record.get("node_hash")
                if node_hash is not None:
                    key = (str(record.get("scope_key") or _canonical_scope_key_of(record)), node_hash)
                    if key in outstanding:
                        continue  # a pending marker for this (scope, node) is already durable
                    outstanding.add(key)  # coalesce duplicates within this same batch too
            kept.append(record)
        return kept

    def _apply_serving_dedup(self, records: list[Json]) -> list[Json]:
        return self._coalesce_summary_dirty(self._filter_duplicate_model_registry(records))

    @property
    def _append_coalesce_tls(self) -> threading.local:
        tls = getattr(self, "_append_coalesce_tls_obj", None)
        if tls is None:
            tls = threading.local()
            self._append_coalesce_tls_obj = tls
        return tls

    def _begin_append_coalescing(self) -> None:
        """Buffer this THREAD's subsequent append() calls until flush.

        For a run of consecutive appends with no interleaved read, one append_many is
        semantically identical (same records, same order, same batch pipeline) and costs one
        durable engine batch instead of one per record. Thread-local on purpose: the adapter is
        shared across request threads, and one request's buffer must never receive another's
        records. The caller owns the flush point (before its first read) and the abort on
        failure (so a reused pool thread cannot inherit an active buffer).
        """
        tls = self._append_coalesce_tls
        tls.buffer = []
        tls.active = True

    def _flush_append_coalescing(self) -> None:
        tls = self._append_coalesce_tls
        if not getattr(tls, "active", False):
            return
        tls.active = False
        buffered = tls.buffer
        tls.buffer = []
        if buffered:
            self.append_many(buffered)

    def _abort_append_coalescing(self) -> None:
        """Drop this thread's buffered records without writing (failed-request cleanup)."""
        tls = self._append_coalesce_tls
        tls.active = False
        tls.buffer = []

    def append(self, record: Json) -> None:
        tls = getattr(self, "_append_coalesce_tls_obj", None)
        if tls is not None and getattr(tls, "active", False):
            tls.buffer.append(record)
            return
        records = self._apply_serving_dedup(
            self._stamp_ingest_fields(materialize_serving_record_batch([record]))
        )
        records = drop_vectors_for_opted_out_tenants(records)
        # One resolver for both, so the record set is read once per write rather than once per
        # fold. Each resolver reads on first use and indexes; two of them read twice.
        # Every row leaves here carrying its identity under the one shared name.
        stamp_row_keys(records)
        resolve_owner = self._embedding_owner_resolver()
        records = fold_embedding_records(records, resolve_owner=resolve_owner)
        records = drop_owner_derivable_postings(records, resolve_owner=resolve_owner)
        if not records:
            return
        if self._queue_batched_records(records):
            return
        sanitized = [self._sanitize_jsonl_record(item) for item in records]
        pre_size: int | None = None
        if self._local_jsonl_enabled:
            with self._event_log_lock:
                # Log bytes as they stand BEFORE this write. If this differs from the bytes the
                # cached view covers, another writer appended in between and the cached view is
                # missing their records -- see _update_read_cache_after_append.
                pre_size = int(self._jsonl_cache_signature_detail().get("total_size", -1))
                self._append_records_to_log(self._encode_records_for_log(sanitized))
                self._prune_jsonl_retention_locked()
        self._update_latest_entity_cache(records)
        # The read caches hold the fully-expanded (interning-free) view, so serve the sanitized
        # records -- expansion of the on-disk interned form yields exactly these.
        self._update_read_cache_after_append(sanitized, pre_size=pre_size)
        self._maintain_event_membership_after_append(records)

    def append_many(self, records: list[Json]) -> None:
        records = self._apply_serving_dedup(
            self._stamp_ingest_fields(materialize_serving_record_batch(records))
        )
        # Embeddings fold onto their owners here and the separate records are dropped -- the
        # owners are the only place vectors live in new logs. Cross-batch embeddings update
        # their earlier owner through the durable view. The fold runs AFTER the serving
        # materialization so the metadata that rides along under embedding_meta is exactly the
        # shape the separate record used to persist in.
        records = drop_vectors_for_opted_out_tenants(records)
        # One resolver for both, so the record set is read once per write rather than once per
        # fold. Each resolver reads on first use and indexes; two of them read twice.
        # Every row leaves here carrying its identity under the one shared name.
        stamp_row_keys(records)
        resolve_owner = self._embedding_owner_resolver()
        records = fold_embedding_records(records, resolve_owner=resolve_owner)
        records = drop_owner_derivable_postings(records, resolve_owner=resolve_owner)
        if not records:
            return
        if self._queue_batched_records(records):
            return
        sanitized = [self._sanitize_jsonl_record(record) for record in records]
        pre_size: int | None = None
        if self._local_jsonl_enabled:
            with self._event_log_lock:
                # Same interloper capture as append() -- see _update_read_cache_after_append.
                pre_size = int(self._jsonl_cache_signature_detail().get("total_size", -1))
                self._append_records_to_log(self._encode_records_for_log(sanitized))
                self._prune_jsonl_retention_locked()
        self._update_latest_entity_cache(records)
        self._update_read_cache_after_append(sanitized, pre_size=pre_size)

    def _resolve_embedding_owner(
        self, record_type: str, field: str, ref_hash: Any
    ) -> Json | None:
        """The newest durable record of ``record_type`` whose ``field`` equals ``ref_hash`` --
        the owner a late-arriving embedding folds onto. None when no such owner exists yet, in
        which case the embedding record is kept as-is rather than losing the vector."""
        return self._embedding_owner_resolver()(record_type, field, ref_hash)

    def _embedding_owner_resolver(self):
        """A resolver that reads the record set ONCE and indexes it.

        The per-embedding form read the whole set and scanned it backwards for every embedding
        it was asked about: 24 full reads per attachment, 480 of the 561 a twenty-attachment
        ingest performed. Each of those reads compacts the cache when it is dirty, so the cost
        grew with the corpus once per embedding rather than once per ingest.

        The index is built per (record_type, field) on first use, and later records overwrite
        earlier ones, which is the same newest-wins answer the backwards scan gave.
        """
        def resolve(record_type: str, field: str, ref_hash: Any) -> Json | None:
            index = self._embedding_owner_index
            if index is None:
                index = {}
                self._embedding_owner_index = index
            key = (record_type, field)
            bucket = index.get(key)
            if bucket is None:
                # First question about this (type, field): build it from one read, then keep
                # it current on every append. Rebuilding per write meant 765 resolvers and
                # 400 full reads over twenty attachments.
                bucket = {}
                try:
                    known = self.read_all()
                except Exception:
                    known = []
                for record in known:
                    if record.get("record_type") != record_type:
                        continue
                    value = record.get(field)
                    if value is not None:
                        bucket[value] = record
                index[key] = bucket
            return bucket.get(ref_hash)

        return resolve

    def _update_latest_entity_cache(self, records: list[Json]) -> None:
        if not hasattr(self, "_session_buffer_cache_lock"):
            self._session_buffer_cache_lock = threading.RLock()
        if not hasattr(self, "_context_event_by_hash"):
            self._context_event_by_hash = {}
        if not hasattr(self, "_session_pending_event_ids_by_key"):
            self._session_pending_event_ids_by_key = {}
        if not hasattr(self, "_session_committed_event_ids_by_key"):
            self._session_committed_event_ids_by_key = {}
        for record in records:
            record_type = record.get("record_type")
            if record_type == "context_event":
                try:
                    event_hash = int(record.get("event_id_hash", 0))
                except (TypeError, ValueError):
                    event_hash = 0
                if event_hash:
                    with self._session_buffer_cache_lock:
                        self._context_event_by_hash[event_hash] = record
                continue
            if record_type == "session_buffer_event":
                try:
                    event_hash = int(record.get("event_id_hash", 0))
                except (TypeError, ValueError):
                    event_hash = 0
                raw_key = record.get("buffer_key", [])
                if event_hash and isinstance(raw_key, list) and len(raw_key) == 4:
                    key = tuple(str(item) for item in raw_key)
                    with self._session_buffer_cache_lock:
                        committed = self._session_committed_event_ids_by_key.setdefault(key, set())
                        pending = self._session_pending_event_ids_by_key.setdefault(key, [])
                        if event_hash in self._context_event_by_hash:
                            enriched_event = dict(self._context_event_by_hash[event_hash])
                            if isinstance(record.get("envelope"), dict) and "envelope" not in enriched_event:
                                enriched_event["envelope"] = record["envelope"]
                            if isinstance(record.get("agent_hook"), dict) and "agent_hook" not in enriched_event:
                                enriched_event["agent_hook"] = record["agent_hook"]
                            self._context_event_by_hash[event_hash] = enriched_event
                        if event_hash not in committed and event_hash not in pending:
                            pending.append(event_hash)
                continue
            if record_type == "context_batch_commit":
                key = session_buffer_key_from_scope(record.get("scope", {}))
                source_ids: list[int] = []
                for ref in record.get("source_event_ids", []):
                    try:
                        source_ids.append(int(ref))
                    except (TypeError, ValueError):
                        continue
                if source_ids:
                    with self._session_buffer_cache_lock:
                        committed = self._session_committed_event_ids_by_key.setdefault(key, set())
                        committed.update(source_ids)
                        pending = self._session_pending_event_ids_by_key.setdefault(key, [])
                        if pending:
                            source_set = set(source_ids)
                            self._session_pending_event_ids_by_key[key] = [event_id for event_id in pending if event_id not in source_set]
                        # A committed event's body is dead weight here. `_context_event_by_hash`
                        # exists so the session buffer can hand back the events it is still
                        # holding; every consumer of it looks up PENDING ids. Nothing was ever
                        # removed, so the map kept one parsed record per event for the life of the
                        # process: gateway RSS grew 31 -> 59 MB over 800 ingests, about 35 KB a
                        # memory, and it did not stop.
                        #
                        # Dropping a committed body is safe because it is a cache, not a store:
                        # the read path that populates it re-reads and re-populates on a miss.
                        # The committed ID set stays -- it is what marks an event done, and it is
                        # eight bytes rather than a record.
                        for event_id in source_ids:
                            self._context_event_by_hash.pop(event_id, None)
                continue
            if record_type == "context_node":
                try:
                    node_hash = int(record.get("node_hash", 0))
                except (TypeError, ValueError):
                    node_hash = 0
                if node_hash:
                    self._context_node_hashes.add(node_hash)
                continue
            if record_type == "context_child_ref":
                try:
                    child_ref_hash = int(record.get("child_ref_hash", 0))
                except (TypeError, ValueError):
                    child_ref_hash = 0
                if child_ref_hash:
                    self._context_child_ref_hashes.add(child_ref_hash)
                continue
            if record_type != "context_entity":
                continue
            try:
                entity_hash = int(record.get("entity_hash", 0))
            except (TypeError, ValueError):
                continue
            if entity_hash:
                self._latest_entity_by_hash[entity_hash] = record

    def _ensure_context_node_cache_loaded(self) -> None:
        if self._context_node_cache_loaded:
            return
        self._context_node_hashes = set()
        self._context_child_ref_hashes = set()
        for record in self.read_all():
            if record.get("record_type") == "context_node" and record.get("node_hash") is not None:
                try:
                    self._context_node_hashes.add(int(record.get("node_hash")))
                except (TypeError, ValueError):
                    pass
            elif record.get("record_type") == "context_child_ref" and record.get("child_ref_hash") is not None:
                try:
                    self._context_child_ref_hashes.add(int(record.get("child_ref_hash")))
                except (TypeError, ValueError):
                    pass
        self._context_node_cache_loaded = True

    def _ensure_latest_entity_cache_loaded(self) -> None:
        if self._entity_cache_loaded:
            return
        records = self.read_all()
        self._latest_entity_by_hash = {}
        for record in records:
            if record.get("record_type") != "context_entity":
                continue
            try:
                entity_hash = int(record.get("entity_hash", 0))
            except (TypeError, ValueError):
                continue
            if entity_hash:
                self._latest_entity_by_hash[entity_hash] = record
        self._entity_cache_loaded = True

    def append_audit(self, record: Json) -> None:
        self.append(record)

    def telemetry_record_for_context_pack(
        self,
        pack: Json,
        *,
        query: str,
        scope: Json,
        audit_mode: str,
        request_metadata: Json | None = None,
    ) -> Json:
        recall_policy = pack.get("recall_policy", {}) if isinstance(pack.get("recall_policy"), dict) else {}
        retrieval_metrics = pack.get("retrieval_metrics", {}) if isinstance(pack.get("retrieval_metrics"), dict) else {}
        memory_layer_budget = (
            retrieval_metrics.get("memory_layer_budget")
            if isinstance(retrieval_metrics.get("memory_layer_budget"), dict)
            else recall_policy.get("memory_layer_budget")
            if isinstance(recall_policy.get("memory_layer_budget"), dict)
            else {}
        )
        dropped_memory_layer_budget = (
            retrieval_metrics.get("dropped_memory_layer_budget")
            if isinstance(retrieval_metrics.get("dropped_memory_layer_budget"), dict)
            else recall_policy.get("dropped_memory_layer_budget")
            if isinstance(recall_policy.get("dropped_memory_layer_budget"), dict)
            else {}
        )
        memory_layer_pressure = (
            retrieval_metrics.get("memory_layer_pressure")
            if isinstance(retrieval_metrics.get("memory_layer_pressure"), dict)
            else recall_policy.get("memory_layer_pressure")
            if isinstance(recall_policy.get("memory_layer_pressure"), dict)
            else {}
        )
        if not memory_layer_pressure:
            memory_layer_pressure = memory_layer_pressure_summary(
                memory_layer_budget,
                dropped_memory_layer_budget,
            )
        stage_budgets = recall_policy.get("stage_latency_budgets", {}) if isinstance(recall_policy.get("stage_latency_budgets"), dict) else {}
        async_pipeline_readiness = (
            retrieval_metrics.get("async_pipeline_readiness")
            if isinstance(retrieval_metrics.get("async_pipeline_readiness"), dict)
            else recall_policy.get("async_pipeline_readiness")
            if isinstance(recall_policy.get("async_pipeline_readiness"), dict)
            else {}
        )
        memory_selection_policy_budget = (
            recall_policy.get("memory_selection_policy_budget_policy")
            if isinstance(recall_policy.get("memory_selection_policy_budget_policy"), dict)
            else {}
        )
        tree = recall_policy.get("tree_traversal", {}) if isinstance(recall_policy.get("tree_traversal"), dict) else {}
        secondary = recall_policy.get("secondary_index_filter", {}) if isinstance(recall_policy.get("secondary_index_filter"), dict) else {}
        rerank = recall_policy.get("rerank", {}) if isinstance(recall_policy.get("rerank"), dict) else {}
        time_weighted = recall_policy.get("time_weighted_recall", {}) if isinstance(recall_policy.get("time_weighted_recall"), dict) else {}
        session_identity = recall_policy.get("session_identity", {}) if isinstance(recall_policy.get("session_identity"), dict) else {}
        dropped_refs = pack.get("dropped_refs", {}) if isinstance(pack.get("dropped_refs"), dict) else {}
        metric_bucket_counts = (
            retrieval_metrics.get("dropped_ref_bucket_counts")
            if isinstance(retrieval_metrics.get("dropped_ref_bucket_counts"), dict)
            else {}
        )
        dropped_ref_bucket_counts = {
            str(key): int(value)
            for key, value in (
                metric_bucket_counts.items()
                if metric_bucket_counts
                else ((key, value) for key, value in dropped_refs.items() if isinstance(value, int))
            )
            if str(key) != "deadline_exceeded" and int(value) > 0
        }
        dropped_ref_count = int(retrieval_metrics.get("dropped_refs") or dropped_refs.get("dropped_ref_count") or 0)
        if not dropped_ref_count and isinstance(dropped_refs.get("refs"), list):
            dropped_ref_count = len(dropped_refs.get("refs") or [])
        if not dropped_ref_count:
            dropped_ref_count = sum(dropped_ref_bucket_counts.values())
        record = {
            "record_type": "context_pack_telemetry",
            "context_pack_id": pack.get("context_pack_id", ""),
            "query_hash": stable_hash(query),
            "scope": scope,
            "audit_mode": audit_mode,
            "question_type": pack.get("question_type", ""),
            "query_plan": recall_policy.get("query_plan", {}),
            "selected_ref_count": len(pack.get("selected_refs", []) or []),
            "selected_ref_counts": pack.get("selected_ref_counts", {}),
            "dropped_ref_count": dropped_ref_count,
            "dropped_ref_bucket_counts": dropped_ref_bucket_counts,
            "stale_dropped_refs": int(
                retrieval_metrics.get("stale_dropped_refs")
                or dropped_ref_bucket_counts.get("stale", 0)
            ),
            "used_local_context_tokens": pack.get("used_local_context_tokens", 0),
            "used_remote_context_tokens": pack.get("used_remote_context_tokens", 0),
            "total_prompt_context_tokens": pack.get("total_prompt_context_tokens", 0),
            "remote_context_budget_tokens": pack.get("remote_context_budget_tokens", 0),
            "requested_max_context_tokens": pack.get("requested_max_context_tokens", 0),
            "memory_layer_budget": memory_layer_budget,
            "dropped_memory_layer_budget": dropped_memory_layer_budget,
            "memory_layer_pressure": memory_layer_pressure,
            "memory_selection_policy_budget": memory_selection_policy_budget,
            "async_pipeline_readiness": async_pipeline_readiness,
            "session_identity": session_identity,
            "quality_warnings": pack.get("quality_warnings", []) or [],
            "partial_context_pack": bool(pack.get("partial_context_pack", False)),
            "insufficient_context": bool(pack.get("insufficient_context", False)),
            "quality_warning_count": len(pack.get("quality_warnings", []) or []),
            "primary_candidate_count": pack.get("primary_candidate_count", 0),
            "auxiliary_candidate_count": pack.get("auxiliary_candidate_count", 0),
            "tree_fallback_to_flat": bool(tree.get("fallback_to_flat", False)),
            "tree_selected_node_count": tree.get("selected_node_count", 0),
            "secondary_index_matched_candidate_count": secondary.get("matched_candidate_count", 0),
            "secondary_index_dropped_candidate_count": secondary.get("dropped_candidate_count", 0),
            "rerank_mode": rerank.get("mode", ""),
            "rerank_candidate_count": rerank.get("reranked_candidate_count", 0),
            "time_weighted_recall": time_weighted,
            "stage_latency_budgets": stage_budgets,
            "created_at_ms": now_ms(),
        }
        if request_metadata:
            record["retrieval_request_metadata"] = {
                key: request_metadata.get(key)
                for key in [
                    "source",
                    "retrieval_source",
                    "codex_event",
                    "hook_type",
                    "codex_session_id_source",
                    "session_id_source",
                    "lifecycle_stage",
                ]
                if request_metadata.get(key) not in (None, "", [], {})
            }
        return record

    def append_context_pack_visibility(
        self,
        *,
        pack: Json,
        audit_record: Json,
        query: str,
        scope: Json,
        audit_mode: str,
        request_metadata: Json | None = None,
        audit_sample_rate: float = 1.0,
    ) -> Json:
        telemetry_write_mode = CONTEXT_TELEMETRY_WRITE_MODE
        if telemetry_write_mode not in {"inline", "async", "sync", "off"}:
            raise MatrixArkError("MATRIXARK_CONTEXT_TELEMETRY_WRITE_MODE must be inline, async, sync, or off")
        force_rich_audit = bool(
            pack.get("partial_context_pack")
            or pack.get("insufficient_context")
            or pack.get("quality_warnings")
        )
        sample_basis = stable_hash(f"{pack.get('context_pack_id', '')}:{query}") % 1_000_000
        sample_value = sample_basis / 1_000_000.0
        rich_audit_sampled = bool(audit_mode == "full" and (force_rich_audit or sample_value < audit_sample_rate))
        telemetry_enabled = audit_mode != "off" and telemetry_write_mode != "off"
        visibility_decision = {
            "audit_mode": audit_mode,
            "audit_sample_rate": round(audit_sample_rate, 6),
            "audit_sample_value": round(sample_value, 6),
            "rich_replay_audit": rich_audit_sampled,
            "full_replay_audit_enabled": audit_mode == "full",
            "rich_replay_audit_force_reason": (
                "partial_or_warning" if force_rich_audit and audit_mode == "full" else "sampled" if rich_audit_sampled else "not_sampled"
            ),
            "telemetry_record": telemetry_enabled,
            "telemetry_write_mode": telemetry_write_mode,
            "serving_blocked_on_full_audit": False,
            "full_replay_audit_requires_full_mode": True,
        }
        telemetry = self.telemetry_record_for_context_pack(
            pack,
            query=query,
            scope=scope,
            audit_mode=audit_mode,
            request_metadata=request_metadata,
        )
        telemetry["visibility_decision"] = visibility_decision
        if telemetry_enabled and telemetry_write_mode in {"inline", "sync"}:
            self.append(telemetry)
        elif telemetry_enabled and telemetry_write_mode == "async":
            self.append_audit(telemetry)
        if rich_audit_sampled:
            audit_record["operational_visibility_policy"] = visibility_decision
            if isinstance(telemetry.get("memory_layer_budget"), dict) and "memory_layer_budget" not in audit_record:
                audit_record["memory_layer_budget"] = telemetry["memory_layer_budget"]
            if isinstance(telemetry.get("dropped_memory_layer_budget"), dict) and "dropped_memory_layer_budget" not in audit_record:
                audit_record["dropped_memory_layer_budget"] = telemetry["dropped_memory_layer_budget"]
            if isinstance(telemetry.get("async_pipeline_readiness"), dict) and "async_pipeline_readiness" not in audit_record:
                audit_record["async_pipeline_readiness"] = telemetry["async_pipeline_readiness"]
            if audit_mode == "full":
                self.append_audit(audit_record)
            else:
                self.append_audit(compact_context_pack_audit_record(audit_record))
        return visibility_decision

    def flush_audits(self) -> None:
        return

    def find_idempotency_record(self, key_hash: int) -> Json | None:
        for record in reversed(self.read_all()):
            if record.get("record_type") == "matrixark_idempotency" and record.get("key_hash") == key_hash:
                return record
        return None

    def append_idempotency_record(self, *, key_hash: int, tool_name: str, raw_key: str, identity: Json, response: Json) -> None:
        # Phase-2: the stored response is slimmed to identity/status fields (+ a full-response content
        # hash) when MATRIXARK_SLIM_IDEMPOTENCY_RESPONSE is ON; flag OFF stores the full response
        # (byte-identical to prior behaviour). Dedup is unaffected -- it keys on key_hash. See
        # matrixark_mcp_local_idempotency.build_idempotency_record.
        self.append(
            _build_idempotency_record(
                key_hash=key_hash,
                tool_name=tool_name,
                raw_key=raw_key,
                identity=identity,
                response=response,
            )
        )

    def ensure_backend_ready(self, *, reason: str = "matrixark") -> Json:
        return {"status": "ready", "backend": "local", "reason": reason}

    def recent_records(self, limit: int = 128) -> list[Json]:
        limit = max(1, int(limit or 1))
        records = self.read_all()
        if len(records) <= limit:
            return records
        # Slicing a list copies it, so the caller cannot reach the cache through this. The
        # branch that used to wrap this in `list(...)` copied a copy.
        return records[-limit:]

    def read_all(self) -> list[Json]:
        """Live view: the compacted, tombstone-filtered log with expired / pre-cutoff records and
        internal retention-cutoff markers removed. Expiry is enforced on every read (never cached)
        so a TTL record disappears once its ``expires_at`` passes, even with no intervening write."""
        return filter_live_memory_records(self._read_all_compacted())

    def _read_all_compacted(self) -> list[Json]:
        cache_key = self._cache_key_str()
        paths = self._retained_jsonl_paths()
        if not paths:
            with self._read_cache_lock:
                self._read_cache_records = []
                self._read_cache_value_keys = None
                self._read_cache_state_keys = None
                self._summary_dirty_index = None
                self._pipeline_task_index = None
                self._node_embedding_refs_index = None
                self._read_cache_size = -1
                self._read_cache_mtime_ns = -1
                self._read_cache_source = "empty"
            with _LOCAL_READ_CACHE_LOCK:
                _LOCAL_READ_CACHE.pop(cache_key, None)
                _LOCAL_READ_CACHE_DIRTY.discard(cache_key)
            for cache_file in (self._durable_read_cache_path(),
                               self._durable_read_cache_binary_path(),
                               self._durable_read_cache_delta_path(),
                               self._durable_read_cache_delta_binary_path(),
                               self._durable_read_cache_head_path()):
                try:
                    cache_file.unlink()
                except FileNotFoundError:
                    pass
            return []
        signature = self._jsonl_cache_signature_detail(paths)
        size = int(signature.get("total_size", -1))
        mtime_ns = int(signature.get("max_mtime_ns", -1))
        served: list[Json] | None = None
        served_epoch: int | None = None
        with self._read_cache_lock:
            if (
                self._read_cache_records is not None
                and self._read_cache_size == size
                and self._read_cache_mtime_ns == mtime_ns
            ):
                self._compact_read_cache_if_dirty_locked()
                self._read_cache_source = "instance"
                served = list(self._read_cache_records)
                served_epoch = self._read_cache_compaction_epoch
        if served is not None:
            # Served from cache, so the re-derive branch below never runs -- refresh here or the
            # snapshot never advances again. See _refresh_durable_read_cache_if_behind.
            self._refresh_durable_read_cache_if_behind(served, signature, served_epoch)
            return served
        with _LOCAL_READ_CACHE_LOCK:
            cached = _LOCAL_READ_CACHE.get(cache_key)
            if cached is not None and cache_key in _LOCAL_READ_CACHE_DIRTY:
                # Appends left it uncompacted. This is the read, so compact it once here.
                cached = (cached[0], cached[1], compact_and_apply_tombstones(cached[2]))
                _LOCAL_READ_CACHE[cache_key] = cached
                _LOCAL_READ_CACHE_DIRTY.discard(cache_key)
            if cached is not None:
                cached_size, cached_mtime_ns, cached_records = cached
                if cached_size == size and cached_mtime_ns == mtime_ns:
                    # Share ONCE, then hand the same list to every holder. Sharing only
                    # into the adapter's cache left the process cache, the durable snapshot and
                    # the returned list holding the unshared originals, so both were alive at
                    # once and a cold read kept 120 copies of a value with ONE distinct value.
                    records = share_repeated_values(
                        list(cached_records), _SHARED_VALUE_TABLE
                    )
                    with self._read_cache_lock:
                        self._read_cache_records = list(records)
                        self._read_cache_value_keys = None
                        self._read_cache_state_keys = None
                        self._read_cache_size = size
                        self._read_cache_mtime_ns = mtime_ns
                        self._read_cache_source = "process"
                        served_epoch = self._read_cache_compaction_epoch
                    # Same reason as the instance branch above.
                    self._refresh_durable_read_cache_if_behind(records, signature, served_epoch)
                    return list(records)
                _LOCAL_READ_CACHE.pop(cache_key, None)
                _LOCAL_READ_CACHE_DIRTY.discard(cache_key)
        durable_records = self._load_durable_read_cache(signature)
        if durable_records is not None:
            records = share_repeated_values(list(durable_records), _SHARED_VALUE_TABLE)
            with self._read_cache_lock:
                self._read_cache_records = list(records)
                self._read_cache_value_keys = None
                self._read_cache_state_keys = None
                self._summary_dirty_index = None
                self._pipeline_task_index = None
                self._node_embedding_refs_index = None
                self._read_cache_size = size
                self._read_cache_mtime_ns = mtime_ns
                self._read_cache_source = "durable"
            with _LOCAL_READ_CACHE_LOCK:
                _LOCAL_READ_CACHE[cache_key] = (size, mtime_ns, list(records))
                _LOCAL_READ_CACHE_DIRTY.discard(cache_key)
            with self._retrieval_records_cache_lock:
                self._retrieval_records_cache_generation += 1
                self._retrieval_records_cache.clear()
            with self._context_pack_cache_lock:
                self._context_pack_cache.clear()
            return list(records)
        records = []
        with self._event_log_lock:
            for path in paths:
                for line in _iter_shard_lines(path):
                    line = line.strip()
                    if line:
                        records.append(loads_with_interned_keys(line))
        # Expand interned metadata BEFORE compaction/caching so the read cache, durable cache, and
        # every downstream consumer see fully-expanded, token-free records.
        records = expand_interned_records(records)
        records = compact_and_apply_tombstones(records)
        with self._read_cache_lock:
            cache_changed = (
                self._read_cache_records is None
                or self._read_cache_size != size
                or self._read_cache_mtime_ns != mtime_ns
            )
            records = share_repeated_values(list(records), _SHARED_VALUE_TABLE)
            self._read_cache_records = list(records)
            self._read_cache_value_keys = None
            self._read_cache_state_keys = None
            self._summary_dirty_index = None
            self._pipeline_task_index = None
            self._node_embedding_refs_index = None
            self._read_cache_size = size
            self._read_cache_mtime_ns = mtime_ns
            self._read_cache_source = "jsonl"
        with _LOCAL_READ_CACHE_LOCK:
            _LOCAL_READ_CACHE[cache_key] = (size, mtime_ns, list(records))
            _LOCAL_READ_CACHE_DIRTY.discard(cache_key)
        # These records were just installed as the cached list, so the epoch describes them
        # exactly -- passing it lets the next append continue this base instead of
        # rewriting it once before the tail path can start.
        self._write_durable_read_cache(
            list(records), signature, force=True, epoch=self._read_cache_compaction_epoch
        )
        if cache_changed:
            with self._retrieval_records_cache_lock:
                self._retrieval_records_cache_generation += 1
                self._retrieval_records_cache.clear()
            with self._context_pack_cache_lock:
                self._context_pack_cache.clear()
        return list(records)

    # ============================================================================================
    # Memory management: forget (delete_all) / delete / get_all / reset  (mem0 conformance)
    # --------------------------------------------------------------------------------------------
    # These implement the mem0 memory-management surface against the local JSONL store via durable
    # tombstones (see `apply_memory_tombstones`). subject = `scope.user_id`. A tombstone is appended
    # to the same event log, so a "deleted"/"forgotten" memory never resurfaces from retrieve /
    # get_all / caches and the removal survives reload.
    #
    # This pass ALSO adds: get/update(=supersede)/history (mem0 read/update conformance), provenance-
    # closure delete (a source-event delete cascades to its single-source derivatives, and demotes
    # multi-source derivatives by trimming the deleted source from their evidence), and a crash-safe
    # physical PURGE that rewrites the JSONL log without tombstoned records + markers to reclaim space
    # (the purged log replays to the same logical state). DEFERRED (separate parallel workstream):
    # rust-datanode-native StringDelete/CommonDelete/FeatureDelete wiring, and true re-derivation
    # (re-extraction) of a demoted multi-source entity/summary -- we trim evidence, not re-summarize.
    def prior_context_records(self, scope: Json | None = None) -> list[Json]:
        """The live records prior-context collection reads. Base implementation: the whole store.

        `collect_prior_context` and the caller-supplied-fields carry-over consume only three
        record types (context_event, context_summary, context_pack_audit), in append order, with
        live-view semantics. A backend that can fetch that subset cheaply overrides this; the
        contract is that the result is indistinguishable FROM THOSE CONSUMERS' point of view from
        `read_all()`.
        """
        return self.read_all()

    def surviving_ids_for_pending_events(self, pending: list[Json]) -> set[str] | None:
        """Which of `pending`'s event ids survive the tombstone sweep? None = all of them.

        The delete-before-extract guard's question, asked as a method so a backend can answer it
        without reading the whole log. This base implementation IS the old behaviour: skip when no
        tombstone can exist, otherwise run the order-aware sweep over the full raw log.
        """
        if not pending:
            return None
        if not self.memory_tombstones_may_exist():
            return None
        return surviving_source_event_ids(self._read_raw_records())

    def memory_tombstones_may_exist(self) -> bool:
        """Could the durable log hold a memory tombstone? Conservative: True means "look properly".

        The delete-before-extract guard reads the whole raw log on every commit, and on a log with
        no tombstone that read changes nothing -- `surviving_source_event_ids` returns None. This
        gives a backend a chance to answer the question without the read. Here, on the JSONL log,
        the read IS the answer, so this is exactly what the guard used to do.
        """
        return _records_contain_memory_tombstone(self._read_raw_records())

    def _resolve_subject_hashes(self, scope: Json) -> tuple[int, int]:
        """Return ``(tenant_hash, user_hash)`` for an already-identity-enriched request scope,
        resolving each from an explicit hash field or by parsing the scope_key."""
        try:
            tenant_hash = int(scope.get("tenant_hash") or 0)
        except (TypeError, ValueError):
            tenant_hash = 0
        try:
            user_hash = int(scope.get("user_hash") or 0)
        except (TypeError, ValueError):
            user_hash = 0
        if not tenant_hash or not user_hash:
            parts = parse_scope_key(str(scope.get("scope_key") or canonical_scope_key(scope)))
            tenant_hash = tenant_hash or int(parts.get("t") or 0)
            user_hash = user_hash or int(parts.get("u") or 0)
        return tenant_hash, user_hash

    # ============================================================================================
    # Event-membership index (event_id_hash -> {member identity hashes}).
    # --------------------------------------------------------------------------------------------
    # The authoritative, O(1)-lookup enumeration of everything a delete/update of a source event must
    # sweep: the event, its derivatives (entity/summary/segment/summary_dirty), and -- by reference --
    # the embeddings + secondary-index postings of the event AND each derivative (matched on
    # ref_hash / ref_hashes ∈ member set). The member set IS the closure identity set carried on the
    # delete tombstone (``closure_ref_ids``), so a complete membership => zero orphans by construction.
    #
    # LOCAL adapter: an in-memory dict rebuilt lazily from the live view and invalidated on every
    # append (so it always reflects committed derivatives, including async-extracted ones, at the time
    # a delete runs). ENGINE adapter: additionally persisted as a durable ``{prefix}:event_members``
    # hash (hset on append, hget on delete, hdel after) for a true O(1) lookup with no rescan.
    def _ensure_event_member_index(self) -> dict[str, set[str]]:
        with self._event_member_index_lock:
            if self._event_member_index is None:
                self._event_member_index = build_event_member_index(self.read_all())
            return self._event_member_index

    def _invalidate_event_member_index(self) -> None:
        index = getattr(self, "_event_member_index", None)
        lock = getattr(self, "_event_member_index_lock", None)
        if lock is None:
            self._event_member_index = None
            return
        with lock:
            self._event_member_index = None

    def _merge_event_member_index(self, records: list[Json]) -> None:
        """Fold an appended batch into the membership index instead of dropping it.

        Sound because membership is ADDITIVE for these record types: an event contributes
        itself, and a derivative contributes its identity ids to each source it names. Neither
        can retract an entry another record already earned, so a union with what the batch
        builds equals what a full rebuild would produce.

        When no index is built yet this does nothing, and must: the next
        ``_ensure_event_member_index`` builds from ``read_all()``, which already contains these
        records. That is the property that makes the fast path unable to lose one."""
        if not records:
            return
        # Read both through getattr, as `_invalidate_event_member_index` already does: an instance
        # that never ran __init__ has neither, and no lock means no index, which by the rule above
        # makes this a no-op rather than an error.
        lock = getattr(self, "_event_member_index_lock", None)
        if lock is None or getattr(self, "_event_member_index", None) is None:
            return
        with lock:
            if self._event_member_index is None:
                return
            for event_id, members in build_event_member_index(records).items():
                self._event_member_index.setdefault(event_id, set()).update(members)

    def _maintain_event_membership_after_append(self, records: list[Json]) -> None:
        """Keep the membership index coherent after a batch lands.

        Base (LOCAL) behavior used to invalidate the whole index on every append that carried an
        event, a tombstone or a derivative -- which is nearly every write. The next delete or
        update then rebuilt it with ``build_event_member_index(self.read_all())``, a full-store
        scan, so N writes interleaved with N deletes cost N full scans.

        Appends now MERGE (membership is additive, see ``_merge_event_member_index``); only a
        tombstone still invalidates, because it REMOVES live records and the index is built from
        the live view -- folding a removal in as a union would leave members that no longer
        exist, and a delete that consulted them would sweep the wrong identity set.

        The engine adapter overrides this to also write-through the durable ``event_members``
        hash."""
        if not records:
            return
        additive: list[Json] = []
        for record in records:
            record_type = str(record.get("record_type") or "")
            if record_type == MEMORY_TOMBSTONE_RECORD_TYPE:
                self._invalidate_event_member_index()
                return
            if record_type == "context_event" or record_type in _MEMORY_DERIVATIVE_RECORD_TYPES:
                additive.append(record)
        if additive:
            self._merge_event_member_index(additive)

    # --- Durable-persistence seam (engine adapter overrides these; LOCAL is in-memory only) --------
    def _lookup_persisted_event_members(self, event_id: str) -> set[str] | None:
        """Engine O(1) member fetch (``hget {prefix}:event_members event_id``); LOCAL -> None so the
        in-memory index is used."""
        return None

    def _persist_event_members(self, event_id: str, member_ids: set[str]) -> None:
        """Engine write-through of an event's member set; LOCAL is a no-op (in-memory index is truth)."""
        return None

    def _forget_persisted_event_members(self, event_id: str) -> None:
        """Engine hdel of an event's member set after delete; LOCAL is a no-op."""
        return None

    def _resolve_event_members(self, event_id: str, records: list[Json]) -> tuple[set[str], str]:
        """The member identity-hash set for a source event, plus which path served it. Fast path first:
        the durable engine hash (O(1)), then the in-memory index (O(1) after an amortized build); a
        scan of ``records`` is the correctness fallback used only when the index has no entry (e.g. a
        pre-index log on first upgrade)."""
        persisted = self._lookup_persisted_event_members(event_id)
        if persisted is not None:
            self._event_member_index_hits += 1
            return set(str(x) for x in persisted), "index_persisted"
        members = self._ensure_event_member_index().get(str(event_id))
        if members is not None:
            self._event_member_index_hits += 1
            return set(members), "index_memory"
        self._event_member_index_misses += 1
        scanned = build_event_member_index(records).get(str(event_id), set())
        return set(scanned), "scan_fallback"

    def _closure_ref_ids_for_event(self, records: list[Json], event_id_str: str, event_id_int: int) -> list[str]:
        """The closure identity set to sweep when a source EVENT is closure-removed WITHOUT demotion
        (keyed-upsert supersede, TTL expiry, update): the event id + every SINGLE-source derivative's
        identity hash. Multi-source derivatives survive (their provenance no longer equals just this
        event under the closure tombstone), so their identities are excluded and their embeddings /
        postings are preserved. Uses the membership index for enumeration; the provenance walk splits
        single- vs multi-source and is the fallback when the index has no entry."""
        member_hashes, _src = self._resolve_event_members(event_id_str, records)
        single_source_ids: set[str] = set()
        multi_source_ids: set[str] = set()
        for record in records:
            provenance = _record_provenance_source_ids(record)
            if provenance is None or event_id_int not in provenance:
                continue
            identity_ids = _record_derivative_identity_ids(record)
            if provenance == {event_id_int}:
                single_source_ids |= identity_ids
            else:
                multi_source_ids |= identity_ids
        ref_ids = {str(event_id_str)} | single_source_ids | (member_hashes - multi_source_ids)
        ref_ids -= multi_source_ids
        return sorted(ref_ids)

    def forget(self, args: Json, hook: Json | None = None) -> Json:
        """Delete ALL memory for a scope (mem0 ``delete_all(user_id=...)``) -- the primary, complete
        deletion primitive. subject = ``scope.user_id``; requires ``confirm == scope.user_id`` (exact
        match, no wildcard). Records a durable forget tombstone plus a payload-free, hashed-subject
        audit entry. Returns the count of live memories removed."""
        scope = optional_object(args, "scope")
        user_id = str(scope.get("user_id") or "").strip()
        confirm = str(args.get("confirm") or "").strip()
        if not user_id:
            raise MatrixArkError("forget requires scope.user_id (the subject to forget)")
        if confirm != user_id:
            raise MatrixArkError("forget requires confirm to equal scope.user_id (exact match, no wildcard)")
        tenant_hash, user_hash = self._resolve_subject_hashes(scope)
        if not tenant_hash or not user_hash:
            raise MatrixArkError("forget could not resolve the subject scope (tenant_hash/user_hash)")
        removed = 0
        for record in self.read_all():
            if str(record.get("record_type") or "") not in _MEMORY_SCOPED_RECORD_TYPES:
                continue
            rec_tenant, rec_user = _record_scope_hashes(record)
            if rec_tenant == tenant_hash and rec_user == user_hash:
                removed += 1
        subject_sha256 = hashlib.sha256(user_id.encode("utf-8")).hexdigest()
        scope_key = canonical_scope_key(scope)
        created_at_ms = now_ms()
        tombstone = {
            "record_type": MEMORY_TOMBSTONE_RECORD_TYPE,
            "tombstone_kind": "forget",
            "target_tenant_hash": tenant_hash,
            "target_user_hash": user_hash,
            "target_scope_key": scope_key,
            "subject_sha256": subject_sha256,
            "removed_count": removed,
            "created_at_ms": created_at_ms,
        }
        forget_audit = {
            "record_type": "matrixark_memory_forget_audit",
            "subject_sha256": subject_sha256,
            "target_scope_key": scope_key,
            "removed_count": removed,
            "created_at_ms": created_at_ms,
        }
        self.append_many([tombstone, forget_audit])
        result = {
            "forgotten": True,
            "subject_sha256": subject_sha256,
            "removed_count": removed,
            "scope_key": scope_key,
        }
        purge = self._maybe_auto_purge()
        if purge is not None:
            result["purge"] = purge
        return result

    def records_for_delete(self, memory_id: str) -> list[Json]:
        """The live records a delete reasons over: the event and everything pointing at it.

        Base implementation: the whole store. delete's own predicates run over whatever this
        returns, so an override only has to produce a SUPERSET of the id's live records --
        missing one would leave a derivative pointing at a deleted source.
        """
        return self.read_all()

    def delete_memory(self, args: Json, hook: Json | None = None) -> Json:
        """Delete a single memory by id/hash (mem0 ``delete``), with provenance-closure cascade.

        The id is the ``event_id_hash`` that ``/v1/ingest`` returns. A durable delete tombstone always
        removes the addressed record (and its own event embedding / index postings). When the id is a
        source EVENT, the delete is closure-aware:

          * derived records built SOLELY from it (single-source segments / entities / summaries) are
            cascaded out by the SAME tombstone (``closure: true``);
          * MULTI-source derivatives are NOT hard-deleted -- instead their surviving copy is rewritten
            with the deleted source trimmed from ``source_event_ids`` / ``source_refs`` /
            ``source_event_hash`` (best-effort demote). A derivative whose evidence becomes empty after
            trimming is tombstoned outright.

        Deleting a leaf record (a bare event, or a non-derivative) still just tombstones that record --
        backward compatible with the pre-closure behavior."""
        memory_id = str(args.get("memory_id") or args.get("id") or "").strip()
        if not memory_id:
            raise MatrixArkError("delete requires a memory_id")
        records = self.records_for_delete(memory_id)
        try:
            memory_id_int: int | None = int(memory_id)
        except (TypeError, ValueError):
            memory_id_int = None
        is_source_event = any(
            str(record.get("record_type") or "") == "context_event"
            and str(record.get("event_id_hash")) == memory_id
            for record in records
        )
        closure = bool(is_source_event and memory_id_int is not None)
        superseded: list[Json] = []
        member_source = "n/a"
        # Closure identity set = the event-membership member set to sweep (event_id + each SINGLE-source
        # derivative's identity hash). Multi-source derivatives are DEMOTED (survive with this source
        # trimmed), so their identities are excluded and their embeddings/postings are preserved.
        closure_ref_ids: set[str] = set()
        if closure:
            closure_ref_ids.add(memory_id)
            member_hashes, member_source = self._resolve_event_members(memory_id, records)
            demoted_identity_ids: set[str] = set()
            single_source_ids: set[str] = set()
            for record in records:
                provenance = _record_provenance_source_ids(record)
                if provenance is None or memory_id_int not in provenance:
                    continue
                identity_ids = _record_derivative_identity_ids(record)
                if provenance == {memory_id_int}:
                    # single-source -> the closure tombstone removes it; sweep its own embeddings/postings.
                    single_source_ids |= identity_ids
                    continue
                demoted = self._demote_derivative_source(record, memory_id_int)
                if demoted is not None:
                    superseded.append(demoted)
                    demoted_identity_ids |= _record_derivative_identity_ids(demoted)
            # The index (authoritative member enumeration) minus the demoted-survivor identities is the
            # single-source kill set; union with the provenance-walk result so a stale/missing index
            # entry can never UNDER-sweep (the scan is the safety net) and a shared identity is never
            # OVER-swept (demoted ids are excluded).
            closure_ref_ids |= single_source_ids
            closure_ref_ids |= (member_hashes - demoted_identity_ids - {memory_id})
            closure_ref_ids -= demoted_identity_ids
        tombstone = {
            "record_type": MEMORY_TOMBSTONE_RECORD_TYPE,
            "tombstone_kind": "delete",
            "target_memory_id": memory_id,
            "closure": closure,
            "created_at_ms": now_ms(),
        }
        if closure_ref_ids:
            tombstone["closure_ref_ids"] = sorted(closure_ref_ids)
        removed = sum(1 for record in records if _tombstone_kills_record(tombstone, record))
        if superseded:
            self.append_many(superseded + [tombstone])
        else:
            self.append(tombstone)
        if closure:
            self._forget_persisted_event_members(memory_id)
            self._invalidate_event_member_index()
        result = {
            "deleted": removed > 0,
            "memory_id": memory_id,
            "removed_count": removed,
            "closure": closure,
            "superseded_count": len(superseded),
            "member_count": len(closure_ref_ids),
            "member_source": member_source,
            # The identity set this delete covers. Deciding what belongs in it is the subtle part
            # -- single-source derivatives are removed, multi-source ones are demoted instead --
            # and it is decided exactly once, here. Reported so a backend can apply the same set
            # to its own copy without re-deriving the rule and drifting from it.
            "closure_ref_ids": sorted(closure_ref_ids | {memory_id}),
        }
        self._maybe_auto_purge()
        return result

    def _demote_derivative_source(self, record: Json, source_id: int) -> Json | None:
        """Return a superseding copy of a MULTI-source derivative with ``source_id`` trimmed from its
        provenance/evidence, or ``None`` when nothing changed. The copy keeps the record's identity
        (entity_hash / summary_hash / ...) and bumps ``updated_at_ms`` so it wins latest-value
        compaction; the deleted source no longer appears in its lineage."""
        demoted = dict(record)
        changed = False
        values = demoted.get("source_event_ids")
        if isinstance(values, list):
            trimmed = [value for value in values if _safe_int(value) != source_id]
            if len(trimmed) != len(values):
                demoted["source_event_ids"] = trimmed
                changed = True
        refs = demoted.get("source_refs")
        if isinstance(refs, list):
            trimmed_refs = [value for value in refs if _safe_int(value) != source_id]
            if len(trimmed_refs) != len(refs):
                demoted["source_refs"] = trimmed_refs
                changed = True
        if _safe_int(demoted.get("source_event_hash")) == source_id:
            remaining = demoted.get("source_event_ids") if isinstance(demoted.get("source_event_ids"), list) else []
            demoted["source_event_hash"] = int(remaining[0]) if remaining else 0
            changed = True
        if not changed:
            return None
        demoted["updated_at_ms"] = now_ms()
        demoted["demoted_removed_source_id"] = source_id
        return demoted

    # ============================================================================================
    # PurchaseMemory Phase 2: keyed-upsert (identity_key) + truth-rank guard, and TTL sweep/purge.
    # ============================================================================================
    @staticmethod
    def _record_scope_key_str(record: Json) -> str:
        scope = candidate_access_scope(record)
        if isinstance(scope, dict):
            scope_key = str(scope.get("scope_key") or "")
            if scope_key:
                return scope_key
            canonical = canonical_scope_key(scope)
            if canonical:
                return canonical
        return ""

    def _apply_identity_upsert(self, result: Json, *, identity_key: str, envelope: Json) -> Json:
        """Keyed-upsert truth-rank guard for a just-ingested event.

        Finds OTHER live ``context_event``s that share ``identity_key`` within the SAME subject scope
        and applies the guard against the highest surviving rank:

          * new_rank >= existing_rank -> SUPERSEDE: closure-tombstone every older keyed record
            (``superseded_by`` = the new id), so recall returns the new value.
          * new_rank <  existing_rank -> RANK-GUARDED: roll this lower-confidence write back
            (closure-tombstone the new id) and keep the existing highest-rank fact untouched.
        """
        new_id = str(result.get("event_id_hash") or "")
        if not new_id:
            return result
        records = self._read_all_compacted()
        new_record: Json | None = None
        for record in records:
            if str(record.get("record_type") or "") == "context_event" and str(record.get("event_id_hash")) == new_id:
                new_record = record
                break
        if new_record is None:
            return result
        subject_scope_key = self._record_scope_key_str(new_record)
        new_rank = int(new_record.get("truth_rank") or 0)
        now_value = _memory_clock_now_ms()
        existing: list[Json] = []
        for record in records:
            if str(record.get("record_type") or "") != "context_event":
                continue
            if str(record.get("event_id_hash")) == new_id:
                continue
            if str(record.get("identity_key") or "") != identity_key:
                continue
            if subject_scope_key and self._record_scope_key_str(record) != subject_scope_key:
                continue
            if _record_is_time_expired(record, now_value):
                continue
            existing.append(record)
        if not existing:
            result["identity_key"] = identity_key
            result["truth_rank"] = new_rank
            result["identity_upsert"] = "created"
            result["upsert_outcome"] = "add"
            return result
        existing_rank = max(int(record.get("truth_rank") or 0) for record in existing)
        if new_rank >= existing_rank:
            tombstones: list[Json] = []
            superseded_ids: list[str] = []
            for record in existing:
                old_id = str(record.get("event_id_hash") or "")
                if not old_id:
                    continue
                superseded_ids.append(old_id)
                old_tombstone = {
                    "record_type": MEMORY_TOMBSTONE_RECORD_TYPE,
                    "tombstone_kind": "delete",
                    "target_memory_id": old_id,
                    "closure": True,
                    # "supersede" so mem0 history() labels it a supersede (not a plain delete);
                    # identity_supersede_kind records that it was a keyed-upsert supersede.
                    "tombstone_reason": "supersede",
                    "identity_supersede_kind": "identity_key",
                    "identity_key": identity_key,
                    "superseded_by": new_id,
                    "created_at_ms": now_ms(),
                }
                old_id_int = _safe_int(old_id)
                if old_id_int is not None:
                    old_tombstone["closure_ref_ids"] = self._closure_ref_ids_for_event(records, old_id, old_id_int)
                    self._forget_persisted_event_members(old_id)
                tombstones.append(old_tombstone)
            if tombstones:
                self._invalidate_event_member_index()
                self.append_many(tombstones)
                self._maybe_auto_purge()
            result["identity_key"] = identity_key
            result["truth_rank"] = new_rank
            result["identity_upsert"] = "superseded"
            result["upsert_outcome"] = "update"
            result["superseded_memory_ids"] = superseded_ids
            return result
        # RANK-GUARDED: keep the highest-rank / most-recent existing fact; discard the new write.
        keep = max(existing, key=lambda record: (int(record.get("truth_rank") or 0), _record_occurred_ms(record)))
        keep_id = str(keep.get("event_id_hash") or "")
        rank_guard_tombstone = {
            "record_type": MEMORY_TOMBSTONE_RECORD_TYPE,
            "tombstone_kind": "delete",
            "target_memory_id": new_id,
            "closure": True,
            "tombstone_reason": "identity_rank_guarded",
            "identity_key": identity_key,
            "superseded_by": keep_id,
            "created_at_ms": now_ms(),
        }
        new_id_int = _safe_int(new_id)
        if new_id_int is not None:
            rank_guard_tombstone["closure_ref_ids"] = self._closure_ref_ids_for_event(records, new_id, new_id_int)
            self._forget_persisted_event_members(new_id)
            self._invalidate_event_member_index()
        self.append(rank_guard_tombstone)
        self._maybe_auto_purge()
        return {
            "ingested": False,
            "rank_guarded": True,
            "upsert_outcome": "rank_guarded",
            "identity_key": identity_key,
            "event_id_hash": keep.get("event_id_hash"),
            "current_memory_id": keep.get("event_id_hash"),
            "existing_memory_id": keep.get("event_id_hash"),
            "existing_rank": existing_rank,
            "new_rank": new_rank,
            "rejected_memory_id": new_id,
            "access": result.get("access", {}),
        }

    def _write_retention_cutoff(self, result: Json, envelope: Json) -> None:
        """Persist a durable scope-level retention-cutoff marker; records in the subject scope whose
        occurrence time < cutoff are hidden at read-time and reclaimed by the expiry sweep."""
        try:
            cutoff_ms = int(envelope.get("retention_cutoff_ms") or 0)
        except (TypeError, ValueError):
            cutoff_ms = 0
        if cutoff_ms <= 0:
            return
        scope = envelope.get("scope") if isinstance(envelope.get("scope"), dict) else {}
        tenant_hash, user_hash = self._resolve_subject_hashes(scope)
        if not tenant_hash or not user_hash:
            new_id = str(result.get("event_id_hash") or "")
            for record in self._read_all_compacted():
                if str(record.get("record_type") or "") == "context_event" and str(record.get("event_id_hash")) == new_id:
                    resolved_tenant, resolved_user = _record_scope_hashes(record)
                    tenant_hash = tenant_hash or resolved_tenant
                    user_hash = user_hash or resolved_user
                    break
        if not tenant_hash or not user_hash:
            return
        self.append({
            "record_type": MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
            "target_tenant_hash": tenant_hash,
            "target_user_hash": user_hash,
            "cutoff_ms": cutoff_ms,
            "cutoff_ts": envelope.get("retention_cutoff_ts"),
            "created_at_ms": now_ms(),
        })

    def sweep_expired_memories(self, *, now_ms_value: int | None = None, force_purge: bool = False) -> Json:
        """Lazy expiry reclamation: closure-tombstone expired / pre-cutoff memories, then purge.

        Idempotent and crash-safe -- it reuses the same durable delete-tombstone + ``purge_tombstones``
        machinery as ``delete``, so a re-run tombstones nothing new and recovery replays to the same
        state. ``force_purge`` physically rewrites the log immediately; otherwise purge is left to the
        ``MATRIXARK_MEMORY_PURGE_THRESHOLD`` gate."""
        now_value = now_ms_value if now_ms_value is not None else _memory_clock_now_ms()
        records = self._read_all_compacted()
        cutoffs = _retention_cutoffs_by_subject(records)
        expired_ids: list[str] = []
        seen: set[str] = set()
        for record in records:
            if str(record.get("record_type") or "") != "context_event":
                continue
            if not (_record_is_time_expired(record, now_value) or _record_cut_by_retention(record, cutoffs)):
                continue
            event_id = str(record.get("event_id_hash") or "")
            if not event_id or event_id in seen:
                continue
            seen.add(event_id)
            expired_ids.append(event_id)
        tombstones = []
        for event_id in expired_ids:
            ttl_tombstone = {
                "record_type": MEMORY_TOMBSTONE_RECORD_TYPE,
                "tombstone_kind": "delete",
                "target_memory_id": event_id,
                "closure": True,
                "tombstone_reason": "ttl_expired",
                "created_at_ms": now_ms(),
            }
            event_id_int = _safe_int(event_id)
            if event_id_int is not None:
                ttl_tombstone["closure_ref_ids"] = self._closure_ref_ids_for_event(records, event_id, event_id_int)
                self._forget_persisted_event_members(event_id)
            tombstones.append(ttl_tombstone)
        purge: Json | None = None
        if tombstones:
            self._invalidate_event_member_index()
            self.append_many(tombstones)
            if force_purge:
                purge = self.purge_tombstones(force=True)
            else:
                purge = self._maybe_auto_purge()
        return {"swept": len(expired_ids), "expired_memory_ids": expired_ids, "purge": purge}

    def _maybe_sweep_expired(self) -> Json | None:
        """Auto-sweep expired memories when purge is enabled; never raises into the write path."""
        if MEMORY_PURGE_THRESHOLD <= 0 or not self._local_jsonl_enabled:
            return None
        try:
            return self.sweep_expired_memories(force_purge=False)
        except OSError:
            return None

    def get_memory_by_identity_key(self, args: Json) -> Json:
        """Recall the single current LIVE keyed value for ``identity_key`` in a scope (Phase 2).

        The highest-``truth_rank`` surviving record wins (ties -> most recent occurrence). Reads
        through the live view so superseded / expired / forgotten keyed records never surface."""
        identity_key = str(args.get("identity_key") or "").strip()
        if not identity_key:
            raise MatrixArkError("get by key requires identity_key")
        scope = optional_object(args, "scope")
        tenant_hash, user_hash = self._resolve_subject_hashes(scope)
        candidates: list[Json] = []
        for record in self.read_all():
            if str(record.get("record_type") or "") != "context_event":
                continue
            if str(record.get("identity_key") or "") != identity_key:
                continue
            record_tenant, record_user = _record_scope_hashes(record)
            if tenant_hash and record_tenant != tenant_hash:
                continue
            if user_hash and record_user != user_hash:
                continue
            candidates.append(record)
        if not candidates:
            return {"found": False, "identity_key": identity_key}
        best = max(candidates, key=lambda record: (int(record.get("truth_rank") or 0), _record_occurred_ms(record)))
        return {
            "found": True,
            "identity_key": identity_key,
            "id": best.get("event_id_hash"),
            "memory_id": best.get("event_id_hash"),
            "memory": best.get("summary_text") or best.get("text") or "",
            "text": best.get("text") or "",
            "truth_rank": int(best.get("truth_rank") or 0),
            "truth_class": best.get("truth_class"),
            "expires_at": best.get("expires_at"),
            "scope_key": best.get("scope_key") or canonical_scope_key(scope),
            "updated_at_ms": best.get("updated_at_ms"),
        }

    # mem0 calls these "users", but the list is really every identity that holds memories:
    # a user, an agent, or a run (a session here). The tuple is (mem0 type, scope field).
    MEMORY_SUBJECT_FIELDS = (("user", "user_id"), ("agent", "agent_id"), ("run", "session_id"))

    @classmethod
    def memory_subjects_in_record(cls, record: Json) -> list[tuple[str, str]]:
        """The (type, name) identities a single record attributes memory to."""
        scope = record.get("scope") if isinstance(record, dict) else None
        if not isinstance(scope, dict):
            return []
        found = []
        for kind, field in cls.MEMORY_SUBJECT_FIELDS:
            value = str(scope.get(field) or "").strip()
            if value:
                found.append((kind, value))
        return found

    def list_memory_subjects(self, args: Json) -> Json:
        """Every user / agent / run that holds at least one live memory (mem0 ``users``).

        Derived from the live view, so a subject whose memories were all forgotten or expired
        stops being listed -- which is the point: mem0's `users()` answers "who has memories",
        not "who was ever provisioned". The account-level user list answers that other question
        and is deliberately not reused here.
        """
        limit = args.get("limit") if isinstance(args, dict) else None
        limit = int(limit) if isinstance(limit, int) and limit > 0 else 0
        seen: dict[tuple[str, str], int] = {}
        for record in self.read_all():
            for subject in self.memory_subjects_in_record(record):
                seen[subject] = seen.get(subject, 0) + 1
        results = [{"type": kind, "name": name, "memory_count": count}
                   for (kind, name), count in sorted(seen.items())]
        if limit:
            results = results[:limit]
        return {"results": results, "count": len(results)}

    def get_all(self, args: Json) -> Json:
        """List a scope's active (non-forgotten, non-deleted) memories (mem0 ``get_all(user_id=...)``).
        Projects live ``context_event`` records for the subject scope to ``{id, memory, ...}``. Because
        it reads through ``read_all`` (tombstone-filtered), forgotten/deleted memories are excluded."""
        scope = optional_object(args, "scope")
        tenant_hash, user_hash = self._resolve_subject_hashes(scope)
        try:
            limit = int(args.get("limit") or 0)
        except (TypeError, ValueError):
            raise MatrixArkError("limit must be an integer")
        memories: list[Json] = []
        for record in self.records_for_get_all(scope):
            if str(record.get("record_type") or "") != "context_event":
                continue
            rec_tenant, rec_user = _record_scope_hashes(record)
            if tenant_hash and rec_tenant != tenant_hash:
                continue
            if user_hash and rec_user != user_hash:
                continue
            memories.append({
                "id": record.get("event_id_hash"),
                "memory": record.get("summary_text") or record.get("text") or "",
                "text": record.get("text") or "",
                "user_id": scope.get("user_id"),
                "scope_key": record.get("scope_key") or canonical_scope_key(scope),
                "created_at_ms": record.get("updated_at_ms") or record.get("timestamp_key_ms"),
            })
        memories.sort(key=lambda item: int(item.get("created_at_ms") or 0))
        if limit > 0:
            memories = memories[:limit]
        return {"memories": memories, "count": len(memories)}

    def reset(self, args: Json, hook: Json | None = None) -> Json:
        """Wipe ALL memory for the caller's tenant (mem0 ``reset``). Guarded by an explicit
        ``confirm`` that must equal the resolved ``tenant_id`` or the literal ``"RESET"`` sentinel."""
        scope = optional_object(args, "scope")
        tenant_id = str(scope.get("tenant_id") or "").strip()
        confirm = str(args.get("confirm") or "").strip()
        if not confirm:
            raise MatrixArkError("reset requires an explicit confirm (the tenant_id or 'RESET')")
        if confirm != "RESET" and (not tenant_id or confirm != tenant_id):
            raise MatrixArkError("reset requires confirm to equal the tenant_id or the literal 'RESET'")
        tenant_hash, _ = self._resolve_subject_hashes(scope)
        if not tenant_hash:
            raise MatrixArkError("reset could not resolve the caller's tenant scope")
        removed = 0
        for record in self.read_all():
            if str(record.get("record_type") or "") not in _MEMORY_SCOPED_RECORD_TYPES:
                continue
            rec_tenant, _ = _record_scope_hashes(record)
            if rec_tenant == tenant_hash:
                removed += 1
        tombstone = {
            "record_type": MEMORY_TOMBSTONE_RECORD_TYPE,
            "tombstone_kind": "reset",
            "target_tenant_hash": tenant_hash,
            "removed_count": removed,
            "created_at_ms": now_ms(),
        }
        self.append(tombstone)
        # Reset is a bulk wipe -- reclaim space immediately by physically compacting the tombstoned
        # records + markers out of the log (crash-safe; the purged log replays to the same state).
        purge = self.purge_tombstones(force=True)
        return {"reset": True, "tenant_hash": tenant_hash, "removed_count": removed, "purge": purge}

    def records_for_get_memory(self, memory_id: str) -> list[Json]:
        """The live records get_memory filters for one id. Base implementation: the whole store.

        get_memory's own matching (id equality and the provenance check) runs over whatever this
        returns, so an override only has to produce a SUPERSET of the id's live records -- being
        slow is recoverable, answering {found: false} for a live memory is not.
        """
        return self.read_all()

    def get_memory(self, args: Json) -> Json:
        """Fetch a single memory by id (mem0 ``get``). Returns the live ``context_event`` for
        ``memory_id`` projected to ``{id, memory, text, metadata, ...}`` plus the derived records
        (entities/summaries/embeddings) whose provenance points at it. Reads through ``read_all`` so a
        forgotten/deleted memory returns ``{found: false}``."""
        memory_id = str(args.get("memory_id") or args.get("id") or "").strip()
        if not memory_id:
            raise MatrixArkError("get requires a memory_id")
        memory_id_int = _safe_int(memory_id)
        event: Json | None = None
        derived: list[Json] = []
        for record in self.records_for_get_memory(memory_id):
            record_type = str(record.get("record_type") or "")
            if record_type == "context_event" and str(record.get("event_id_hash")) == memory_id:
                event = record
                continue
            if memory_id_int is not None:
                provenance = _record_provenance_source_ids(record)
                if provenance is not None and memory_id_int in provenance:
                    derived.append({
                        "record_type": record_type,
                        "entity_hash": record.get("entity_hash"),
                        "entity_name": record.get("entity_name"),
                        "summary_hash": record.get("summary_hash"),
                        "summary_type": record.get("summary_type"),
                        "text": record.get("summary_text") or record.get("text"),
                    })
        if event is None:
            return {"found": False, "memory_id": memory_id}
        metadata = {
            field: event.get(field)
            for field in ("event_type", "memory_scope", "memory_layer", "session_continuity",
                          "node_path", "classification", "profile_memory_class", "profile_memory_kind")
            if event.get(field) not in (None, "", [], {})
        }
        return {
            "found": True,
            "id": event.get("event_id_hash"),
            "memory_id": memory_id,
            "memory": event.get("summary_text") or event.get("text") or "",
            "text": event.get("text") or "",
            "metadata": metadata,
            "scope_key": event.get("scope_key"),
            "created_at_ms": event.get("timestamp_key_ms") or event.get("event_time_ms"),
            "updated_at_ms": event.get("updated_at_ms"),
            "derived": derived,
            "derived_count": len(derived),
        }

    def update_memory(self, args: Json, hook: Json | None = None) -> Json:
        """Update a memory's content (mem0 ``update``), implemented as a SUPERSEDE: the new text is
        ingested as a fresh memory in the SAME scope as the old one, then the old id is tombstoned, so
        retrieve / get_all return the new version and the old never resurfaces.

        ``memory_id`` addresses the existing ``context_event``; ``data`` / ``text`` / ``content`` is
        the new content. The re-ingest scope is reconstructed from the old record's ``scope_key`` (its
        tenant/user/session hashes), so mem0's scope-less ``update(memory_id, data)`` lands in the
        right subject. When the request carries a tenant, it must match the old record's tenant
        (cross-tenant update is refused)."""
        memory_id = str(args.get("memory_id") or args.get("id") or "").strip()
        if not memory_id:
            raise MatrixArkError("update requires a memory_id")
        new_text = args.get("data")
        if new_text in (None, ""):
            new_text = args.get("text")
        if new_text in (None, ""):
            new_text = args.get("content")
        if not isinstance(new_text, str) or not new_text.strip():
            raise MatrixArkError("update requires new content (data / text)")
        # The id's own records, not the store's. An update reasons over exactly what a delete
        # does -- the addressed event plus everything pointing at it -- and the same subset serves
        # both the lookup here and the supersede closure below, so one id-scoped fetch replaces
        # two full-store reads. The seam's contract is a SUPERSET of the id's live records, which
        # is what both uses need; on an adapter without the index it still returns the whole store.
        update_records = self.records_for_delete(memory_id)
        old: Json | None = None
        for record in update_records:
            if str(record.get("record_type") or "") == "context_event" and str(record.get("event_id_hash")) == memory_id:
                old = record
                break
        if old is None:
            raise MatrixArkNotFoundError("update target memory not found (already deleted, or not a memory id)")
        old_scope_key = str(old.get("scope_key") or "")
        parts = parse_scope_key(old_scope_key)
        # Tenant isolation: an authenticated request scope pins tenant_hash; refuse cross-tenant edits.
        request_scope = optional_object(args, "scope")
        request_tenant, _ = self._resolve_subject_hashes(request_scope) if request_scope else (0, 0)
        if request_tenant and int(parts.get("t") or 0) and request_tenant != int(parts.get("t") or 0):
            raise MatrixArkError("update refused: memory belongs to a different tenant")
        explicit_keys = [name for name, part in (("user_id", "u"), ("session_id", "s"), ("agent_id", "a")) if parts.get(part)]
        reingest_scope: Json = {"scope_key": old_scope_key or None}
        if parts.get("t"):
            reingest_scope["tenant_hash"] = int(parts["t"])
        if parts.get("u"):
            reingest_scope["user_hash"] = int(parts["u"])
        if parts.get("s"):
            reingest_scope["session_hash"] = int(parts["s"])
        if parts.get("a"):
            reingest_scope["agent_hash"] = int(parts["a"])
        reingest_scope["_explicit_scope_keys"] = explicit_keys
        clean_scope = {key: value for key, value in reingest_scope.items() if value is not None}
        ingested = self.ingest(
            {
                "messages": [{"role": "user", "content": new_text}],
                "scope": clean_scope,
                "finalize": True,
            },
            hook=hook,
        )
        new_memory_id = ingested.get("event_id_hash")
        tombstone = {
            "record_type": MEMORY_TOMBSTONE_RECORD_TYPE,
            "tombstone_kind": "delete",
            "target_memory_id": memory_id,
            "closure": True,
            "tombstone_reason": "supersede",
            "superseded_by": new_memory_id,
            "created_at_ms": now_ms(),
        }
        memory_id_int = _safe_int(memory_id)
        if memory_id_int is not None:
            # Sweep the superseded version's own embeddings / index postings so the old text can't leak
            # via retrieval after the update (same closure identity set as delete).
            tombstone["closure_ref_ids"] = self._closure_ref_ids_for_event(update_records, memory_id, memory_id_int)
            self._forget_persisted_event_members(memory_id)
            self._invalidate_event_member_index()
        self.append(tombstone)
        superseded_ids = sorted(set(tombstone.get("closure_ref_ids") or []) | {memory_id})
        return {
            "updated": True,
            "memory_id": memory_id,
            "new_memory_id": new_memory_id,
            "superseded": True,
            "text": new_text,
            # The identity set the old version covered. Reported for the same reason delete
            # reports its closure: a backend has to remove its own copy, and re-deriving the rule
            # there would put two versions of it in the tree.
            "closure_ref_ids": superseded_ids,
            # The scope the replacement was ingested into, so a backend that needs to finish the
            # write itself does not have to reconstruct it from the old record a second time.
            "reingest_scope": clean_scope,
        }

    MEMORY_FEEDBACK_RECORD_TYPE = "matrixark_memory_feedback"
    MEMORY_FEEDBACK_RATINGS = ("POSITIVE", "NEGATIVE", "VERY_NEGATIVE")

    def memory_feedback(self, args: Json, hook: Json | None = None) -> Json:
        """Attach a rating to an existing memory (mem0 ``feedback``).

        Deliberately NOT a `context_event`: a rating is not a memory, and storing it as one would
        make it show up in `get_all` and compete for retrieval. It is its own record type, and
        `history(memory_id)` surfaces it beside the ingest / supersede / delete events -- which is
        where a caller looks for what happened to a memory, and the only place this is readable.

        The rating vocabulary is closed. An unrecognised value is refused rather than stored,
        because a rating nobody can interpret is worse than no rating: it reads as feedback that
        was recorded and understood.
        """
        memory_id = str(args.get("memory_id") or args.get("id") or "").strip()
        if not memory_id:
            raise MatrixArkInvalidRequestError("feedback requires a memory_id")
        rating = str(args.get("feedback") or args.get("rating") or "").strip().upper()
        if not rating:
            raise MatrixArkInvalidRequestError(
                "feedback requires a feedback value (%s)"
                % ", ".join(self.MEMORY_FEEDBACK_RATINGS))
        if rating not in self.MEMORY_FEEDBACK_RATINGS:
            raise MatrixArkInvalidRequestError(
                "feedback must be one of %s (got %r)"
                % (", ".join(self.MEMORY_FEEDBACK_RATINGS), rating))
        reason = args.get("feedback_reason")
        reason = str(reason).strip() if isinstance(reason, str) and reason.strip() else None

        target: Json | None = None
        for record in self.read_all():
            if (str(record.get("record_type") or "") == "context_event"
                    and str(record.get("event_id_hash")) == memory_id):
                target = record
                break
        if target is None:
            raise MatrixArkNotFoundError(
                "feedback target memory not found (already deleted, or not a memory id)")

        # Tenant isolation, the same rule `update` applies: an authenticated request pins a tenant,
        # and a rating must not cross from one tenant to another's memory.
        request_scope = optional_object(args, "scope")
        request_tenant, _ = self._resolve_subject_hashes(request_scope) if request_scope else (0, 0)
        target_tenant, _ = _record_scope_hashes(target)
        if request_tenant and target_tenant and request_tenant != target_tenant:
            raise MatrixArkError("feedback refused: the memory belongs to another tenant")

        record = {
            "record_type": self.MEMORY_FEEDBACK_RECORD_TYPE,
            "target_memory_id": memory_id,
            "feedback": rating,
            "scope_key": str(target.get("scope_key") or ""),
            "created_at_ms": now_ms(),
        }
        if reason:
            record["feedback_reason"] = reason
        self.append(record)
        return {"recorded": True, "memory_id": memory_id, "feedback": rating,
                "feedback_reason": reason}

    def records_for_get_all(self, scope: Json) -> list[Json]:
        """The live records get_all filters. Base implementation: the whole store.

        get_all's own scope filter (hash equality) runs over whatever this returns, so an
        override only has to produce a SUPERSET of the subject's live events -- being slow is
        recoverable, dropping a memory from the listing is not.
        """
        return self.read_all()

    def records_for_summary_refresh(self) -> list[Json]:
        """The live records a summary-refresh pass reads. Base implementation: the whole store.

        The pass's consumers touch a closed set of record types; a backend that can fetch that
        subset cheaply overrides this. The contract is that a pass fed the subset produces the
        same refreshes as one fed `read_all()`.
        """
        return self.read_all()

    def raw_records_for_history(self, memory_id: str | None = None) -> list[Json]:
        """The records `history` walks. Base implementation: the whole raw log.

        `memory_id` is a scoping HINT: a backend that can fetch one memory's records cheaply may
        use it, and history filters by id either way, so ignoring it is always correct.

        History consumes three record types -- the memory's event rows, the tombstones that
        target or created it, and its feedback ratings -- in append order. A backend that can
        fetch that subset cheaply overrides this; the contract is that history's OUTPUT for any
        memory id is unchanged.
        """
        return self._read_raw_records()

    def history(self, args: Json) -> Json:
        """Return the ordered change history for a memory id (mem0 ``history``). Because the store is
        event-sourced, this is the RAW (un-compacted, un-tombstoned) event log filtered to the id:
        each ingest of the id, any supersede/delete tombstone that targets it, and the supersede link
        when the id is the NEW version produced by an update. Ordered oldest-first by log position."""
        memory_id = str(args.get("memory_id") or args.get("id") or "").strip()
        if not memory_id:
            raise MatrixArkError("history requires a memory_id")
        events: list[Json] = []
        seen_ingested = False
        for record in self.raw_records_for_history(memory_id):
            record_type = str(record.get("record_type") or "")
            ts = record.get("updated_at_ms") or record.get("timestamp_key_ms") or record.get("event_time_ms") or record.get("created_at_ms")
            if record_type == "context_event" and str(record.get("event_id_hash")) == memory_id:
                # The ingest pipeline may persist the event row more than once (pending + committed);
                # collapse to a single "ingested" entry so the history reads as one create per id.
                if seen_ingested:
                    continue
                seen_ingested = True
                events.append({"event": "ingested", "record_type": record_type,
                               "memory_id": memory_id, "created_at_ms": ts,
                               "text": record.get("text") or record.get("summary_text") or ""})
            elif record_type == MEMORY_TOMBSTONE_RECORD_TYPE and str(record.get("tombstone_kind") or "") == "delete":
                if str(record.get("target_memory_id") or "") == memory_id:
                    op = "superseded" if str(record.get("tombstone_reason") or "") == "supersede" else "deleted"
                    entry = {"event": op, "record_type": record_type, "memory_id": memory_id, "created_at_ms": ts}
                    if record.get("superseded_by") is not None:
                        entry["superseded_by"] = record.get("superseded_by")
                    events.append(entry)
                elif str(record.get("superseded_by")) == memory_id:
                    events.append({"event": "created", "record_type": record_type, "memory_id": memory_id,
                                   "created_at_ms": ts, "supersedes_memory_id": record.get("target_memory_id")})
            elif (record_type == self.MEMORY_FEEDBACK_RECORD_TYPE
                    and str(record.get("target_memory_id") or "") == memory_id):
                entry = {"event": "feedback", "record_type": record_type, "memory_id": memory_id,
                         "created_at_ms": ts, "feedback": record.get("feedback")}
                if record.get("feedback_reason"):
                    entry["feedback_reason"] = record.get("feedback_reason")
                events.append(entry)
        return {"memory_id": memory_id, "history": events, "count": len(events)}

    # --------------------------------------------------------------------------------------------
    # Physical purge: reclaim space by rewriting the JSONL log without tombstoned records + markers.
    # The purged log replays (via read_all) to the SAME logical state -- value/state compaction is
    # left to read_all, so purge only removes what a tombstone already hides. Crash-safe: survivors
    # are written to a temp file, fsync'd, then atomically os.replace'd onto the primary log (the same
    # durability the durable read-cache uses); rotated shards are folded in and removed afterward.
    # --------------------------------------------------------------------------------------------
    def _read_raw_records(self) -> list[Json]:
        """All records across the retained JSONL shards in append order -- NOT compacted and NOT
        tombstone-filtered (the durable event history). Empty when the local JSONL is disabled.

        Interned metadata is expanded and the ``matrixark_intern_dict`` sidecar records are stripped,
        so callers see the same fully-expanded logical history as ``read_all`` (the dict records are a
        storage-representation detail, not part of the event history)."""
        raw: list[Json] = []
        if not self._local_jsonl_enabled:
            return raw
        with self._event_log_lock:
            for path in self._retained_jsonl_paths():
                for line in _iter_shard_lines(path):
                    line = line.strip()
                    if line:
                        raw.append(loads_with_interned_keys(line))
        return expand_interned_records(raw)

    def _count_raw_tombstones(self) -> int:
        return sum(
            1 for record in self._read_raw_records()
            if str(record.get("record_type") or "") == MEMORY_TOMBSTONE_RECORD_TYPE
        )

    def _maybe_auto_purge(self) -> Json | None:
        """Purge when the raw tombstone count crosses ``MATRIXARK_MEMORY_PURGE_THRESHOLD`` (>0 to
        enable; default 0 = off). Best-effort: never raises into the caller's write path."""
        if MEMORY_PURGE_THRESHOLD <= 0 or not self._local_jsonl_enabled:
            return None
        try:
            if self._count_raw_tombstones() < MEMORY_PURGE_THRESHOLD:
                return None
            return self.purge_tombstones(force=True)
        except OSError:
            return None

    def purge_tombstones(self, *, force: bool = False) -> Json:
        """Physically rewrite the JSONL event log without tombstoned records or tombstone markers,
        reclaiming space. No-op (``purged: false``) when the local JSONL is disabled or the log holds
        no tombstone (and ``force`` only controls the threshold gate, not correctness). Crash-safe via
        temp-write + fsync + atomic ``os.replace`` onto the primary shard."""
        if not self._local_jsonl_enabled:
            return {"purged": False, "reason": "jsonl_disabled"}
        with self._event_log_lock:
            paths = self._retained_jsonl_paths()
            if not paths:
                return {"purged": False, "reason": "empty"}
            raw: list[Json] = []
            for path in paths:
                for line in _iter_shard_lines(path):
                    line = line.strip()
                    if line:
                        raw.append(loads_with_interned_keys(line))
            tombstone_count = sum(
                1 for record in raw
                if str(record.get("record_type") or "") == MEMORY_TOMBSTONE_RECORD_TYPE
            )
            if tombstone_count == 0:
                return {"purged": False, "reason": "no_tombstones", "records": len(raw)}
            survivors = apply_memory_tombstones(raw)
            bytes_before = sum(path.stat().st_size for path in paths if path.exists())
            tmp_path = self.event_log.with_name(f"{self.event_log.name}.purge.{os.getpid()}.{threading.get_ident()}.tmp")
            lines = [json.dumps(self._sanitize_jsonl_record(record), separators=(",", ":")) + "\n" for record in survivors]
            with tmp_path.open("w", encoding="utf-8") as handle:
                for line in lines:
                    handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self.event_log)  # atomic commit point
            # Fold rotated shards into the (now consolidated) primary: remove them post-commit.
            for path in paths:
                if path != self.event_log and path.exists():
                    try:
                        path.unlink()
                    except OSError:
                        pass
            bytes_after = self.event_log.stat().st_size
        self._clear_jsonl_read_caches()
        self._reset_derived_caches()
        return {
            "purged": True,
            "removed_tombstones": tombstone_count,
            "records_before": len(raw),
            "records_after": len(survivors),
            "bytes_before": bytes_before,
            "bytes_after": bytes_after,
        }

    def _reset_derived_caches(self) -> None:
        """Invalidate the in-memory derived caches so the next read rebuilds them from the purged log."""
        self._entity_cache_loaded = False
        self._latest_entity_by_hash = {}
        self._context_node_cache_loaded = False
        self._context_node_hashes = set()
        self._context_child_ref_hashes = set()
        if hasattr(self, "_context_event_by_hash"):
            self._context_event_by_hash = {}
        with self._retrieval_records_cache_lock:
            self._retrieval_records_cache_generation += 1
            self._retrieval_records_cache.clear()
        with self._context_pack_cache_lock:
            self._context_pack_cache.clear()









