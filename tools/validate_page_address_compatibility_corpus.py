#!/usr/bin/env python3
"""Validate the shared PageAddress compatibility corpus.

This is intentionally lightweight so C++ and Rust CI can both run it before
their native engine-specific tests. Native tests should consume the same JSON
corpus and prove the storage engine implements these semantics.
"""

from __future__ import annotations

import json
import pathlib
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
CORPUS_PATH = ROOT / "compat" / "page_address_compatibility_corpus.json"
REQUIRED_ADDRESS_FIELDS = {
    "shard_id",
    "zone_id",
    "segment_id",
    "page_id",
    "offset",
    "length",
    "generation",
}


def _load_corpus() -> dict[str, Any]:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))


def _address_key(address: dict[str, Any]) -> tuple[int, int, int, int, int]:
    return (
        int(address["shard_id"]),
        int(address["zone_id"]),
        int(address["segment_id"]),
        int(address["page_id"]),
        int(address["offset"]),
    )


def _records(page: dict[str, Any]) -> list[tuple[int, str]]:
    return [(int(record["timestamp_ms"]), str(record["value"])) for record in page["records"]]


def validate_encode_decode(addresses: list[dict[str, Any]]) -> None:
    for address in addresses:
        missing = REQUIRED_ADDRESS_FIELDS - set(address)
        if missing:
            raise AssertionError(f"{address.get('id', '<missing id>')} missing fields: {sorted(missing)}")
        encoded = json.dumps(address, sort_keys=True, separators=(",", ":"))
        decoded = json.loads(encoded)
        if decoded != address:
            raise AssertionError(f"{address['id']} JSON encode/decode changed the address")
        if int(address["length"]) <= 0:
            raise AssertionError(f"{address['id']} length must be positive")


def validate_stable_order(corpus: dict[str, Any], addresses: list[dict[str, Any]]) -> None:
    ordered = [address["id"] for address in sorted(addresses, key=_address_key)]
    expected = corpus["expected_stable_order"]
    if ordered != expected:
        raise AssertionError(f"stable address order mismatch: got {ordered}, expected {expected}")


def validate_timestamp_range_lookup(corpus: dict[str, Any]) -> None:
    page_index = corpus["timestamp_page_index"]
    entries = page_index["entries"]
    for query in page_index["queries"]:
        selected = [
            entry["address_id"]
            for entry in entries
            if int(entry["start_ms"]) <= int(query["end_ms"])
            and int(entry["end_ms"]) >= int(query["start_ms"])
        ]
        if selected != query["expected_address_ids"]:
            raise AssertionError(
                f"{query['name']} page index lookup mismatch: got {selected}, expected {query['expected_address_ids']}"
            )


def validate_page_split(corpus: dict[str, Any]) -> None:
    case = corpus["page_split_case"]
    target = int(case["target_page_bytes"])
    pages: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_bytes = 0
    for record in case["records"]:
        record_bytes = int(record["encoded_bytes"])
        if current and current_bytes + record_bytes > target:
            pages.append(current)
            current = []
            current_bytes = 0
        current.append(record)
        current_bytes += record_bytes
    if current:
        pages.append(current)

    actual = [
        {
            "start_ms": page[0]["timestamp_ms"],
            "end_ms": page[-1]["timestamp_ms"],
            "record_count": len(page),
        }
        for page in pages
    ]
    if actual != case["expected_pages"]:
        raise AssertionError(f"page split mismatch: got {actual}, expected {case['expected_pages']}")
    if [record["timestamp_ms"] for page in pages for record in page] != [
        record["timestamp_ms"] for record in case["records"]
    ]:
        raise AssertionError("page split changed timestamp order")


def validate_compaction_rewrite(corpus: dict[str, Any]) -> None:
    case = corpus["compaction_rewrite_case"]
    before_records = sorted(record for page in case["before"] for record in _records(page))
    after_records = sorted(record for page in case["after"] for record in _records(page))
    if before_records != after_records:
        raise AssertionError(f"compaction rewrite changed logical records: {before_records} != {after_records}")

    before_ids = {page["address_id"] for page in case["before"]}
    after_ids = {page["address"]["id"] for page in case["after"]}
    stale_ids = set(case["stale_address_ids"])
    if not stale_ids.issubset(before_ids):
        raise AssertionError(f"stale ids must come from pre-compaction pages: {stale_ids - before_ids}")
    if stale_ids & after_ids:
        raise AssertionError(f"stale ids must not remain live after compaction: {stale_ids & after_ids}")

    max_before_generation = max(
        int(address["generation"])
        for address in corpus["addresses"]
        if address["id"] in before_ids
    )
    min_after_generation = min(int(page["address"]["generation"]) for page in case["after"])
    if min_after_generation <= max_before_generation:
        raise AssertionError(
            f"compaction generation did not advance: before={max_before_generation}, after={min_after_generation}"
        )


def main() -> int:
    corpus = _load_corpus()
    if corpus.get("schema_version") != 1:
        raise AssertionError("page address corpus schema_version must be 1")
    addresses = corpus["addresses"]

    validate_encode_decode(addresses)
    validate_stable_order(corpus, addresses)
    validate_timestamp_range_lookup(corpus)
    validate_page_split(corpus)
    validate_compaction_rewrite(corpus)

    print("page address compatibility corpus passed:")
    print("- encode/decode PageAddress")
    print("- stable ordering by shard/zone/segment/page/offset")
    print("- timestamp range to page address lookup")
    print("- page split behavior")
    print("- compaction rewrite preserves logical records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
