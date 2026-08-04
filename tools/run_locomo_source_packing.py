"""Rust-TemporalStore source packing/splitting helpers, extracted from run_locomo_ingest_once.py."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def pack_rust_temporalstore_sources(path: Path, pack_size: int) -> dict[str, Any]:
    from run_locomo_ingest_once import iter_jsonl_cases  # lazy (avoid circular import)
    if pack_size <= 0:
        return {"enabled": False, "pack_size": pack_size}
    packed_lines: list[str] = []
    original_source_count = 0
    packed_source_count = 0
    max_original_sources_per_case = 0
    max_packed_sources_per_case = 0
    case_count = 0
    for case in iter_jsonl_cases(path):
        case_count += 1
        sources = [source for source in case.get("sources", []) if isinstance(source, dict)]
        original_source_count += len(sources)
        max_original_sources_per_case = max(max_original_sources_per_case, len(sources))
        if len(sources) > pack_size:
            case["sources"] = packed_rust_temporalstore_sources(sources, pack_size)
            case["rust_temporalstore_sources_packed"] = True
            case["rust_temporalstore_original_source_count"] = len(sources)
            case["rust_temporalstore_source_pack_size"] = pack_size
        else:
            case["rust_temporalstore_sources_packed"] = False
        packed_sources = [source for source in case.get("sources", []) if isinstance(source, dict)]
        packed_source_count += len(packed_sources)
        max_packed_sources_per_case = max(max_packed_sources_per_case, len(packed_sources))
        packed_lines.append(json.dumps(case, ensure_ascii=False))
    path.write_text("\n".join(packed_lines) + ("\n" if packed_lines else ""), encoding="utf-8")
    return {
        "enabled": True,
        "pack_size": pack_size,
        "case_count": case_count,
        "original_source_count": original_source_count,
        "packed_source_count": packed_source_count,
        "source_count_reduction": original_source_count - packed_source_count,
        "max_original_sources_per_case": max_original_sources_per_case,
        "max_packed_sources_per_case": max_packed_sources_per_case,
        "preserves_all_source_text": True,
    }


def packed_rust_temporalstore_sources(sources: list[dict[str, Any]], pack_size: int) -> list[dict[str, str]]:
    packed: list[dict[str, str]] = []
    for start in range(0, len(sources), pack_size):
        chunk = sources[start : start + pack_size]
        first_title = str(chunk[0].get("title") or chunk[0].get("id") or start + 1)
        last_title = str(chunk[-1].get("title") or chunk[-1].get("id") or start + len(chunk))
        body_parts: list[str] = []
        kinds: list[str] = []
        source_refs: list[str] = []
        for source in chunk:
            title = str(source.get("title") or source.get("id") or source.get("source_ref") or "source")
            body = str(source.get("body") or source.get("text") or "")
            kind = str(source.get("kind") or source.get("source_kind") or "document")
            kinds.append(kind)
            source_refs.append(title)
            body_parts.append(f"[source_ref: {title}]\n{body}")
        packed.append(
            {
                "kind": most_common_source_kind(kinds),
                "title": (
                    f"packed sources {start + 1}-{start + len(chunk)}: {first_title} .. {last_title}; "
                    f"refs: {' | '.join(source_refs)}"
                ),
                "body": "\n\n".join(body_parts),
                "packed_source_count": str(len(chunk)),
            }
        )
    return packed


def most_common_source_kind(kinds: list[str]) -> str:
    counts: dict[str, int] = {}
    for kind in kinds:
        counts[kind] = counts.get(kind, 0) + 1
    if not counts:
        return "document"
    return sorted(counts.items(), key=lambda row: (-row[1], row[0]))[0][0]


def split_rust_temporalstore_jsonl(path: Path, batch_size: int) -> list[Path]:
    from run_locomo_ingest_once import iter_jsonl_cases  # lazy (avoid circular import)
    cases = list(iter_jsonl_cases(path))
    if batch_size <= 0 or len(cases) <= batch_size:
        return [path]
    groups: list[tuple[str, list[dict[str, Any]]]] = []
    for case in cases:
        signature = rust_temporalstore_source_signature(case)
        if not groups or groups[-1][0] != signature:
            groups.append((signature, [case]))
        else:
            groups[-1][1].append(case)
    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for _signature, group in groups:
        if current and len(current) + len(group) > batch_size:
            batches.append(current)
            current = []
        current.extend(group)
    if current:
        batches.append(current)
    batch_paths: list[Path] = []
    stem = path.with_suffix("")
    for index, batch in enumerate(batches, start=1):
        batch_path = stem.with_name(f"{stem.name}.batch-{index:04d}.jsonl")
        compact_batch = compact_rust_temporalstore_batch(batch)
        batch_path.write_text(
            "".join(json.dumps(case, ensure_ascii=False) + "\n" for case in compact_batch),
            encoding="utf-8",
        )
        batch_paths.append(batch_path)
    return batch_paths


def compact_rust_temporalstore_batch(batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep full sources once per source set and reference them from later cases."""

    seen_source_sets: set[str] = set()
    compacted: list[dict[str, Any]] = []
    for case in batch:
        signature = rust_temporalstore_source_signature(case)
        if not signature:
            compacted.append(case)
            continue
        out = dict(case)
        out["source_set_id"] = signature
        if signature in seen_source_sets:
            out.pop("sources", None)
            out["source_set_ref"] = signature
            out["sources_compacted"] = True
        else:
            seen_source_sets.add(signature)
            out["sources_compacted"] = False
        compacted.append(out)
    return compacted


def rust_temporalstore_source_signature(case: dict[str, Any]) -> str:
    sources = case.get("sources")
    if not isinstance(sources, list):
        return ""
    digest = hashlib.sha256()
    for source in sources:
        if not isinstance(source, dict):
            continue
        digest.update(str(source.get("id") or source.get("source_ref") or source.get("title") or "").encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(len(str(source.get("body") or ""))).encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


