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
REQUIRED_BLOCK_ADDRESS_FIELDS = {
    "shard_id",
    "zone_id",
    "block_id",
    "offset",
    "length",
    "checksum",
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


def _block_address_key(address: dict[str, Any]) -> tuple[int, int, int, int]:
    return (
        int(address["shard_id"]),
        int(address["zone_id"]),
        int(address["block_id"]),
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


def validate_block_encode_decode(block_addresses: list[dict[str, Any]]) -> None:
    for address in block_addresses:
        missing = REQUIRED_BLOCK_ADDRESS_FIELDS - set(address)
        if missing:
            raise AssertionError(f"{address.get('id', '<missing id>')} missing block fields: {sorted(missing)}")
        encoded = json.dumps(address, sort_keys=True, separators=(",", ":"))
        decoded = json.loads(encoded)
        if decoded != address:
            raise AssertionError(f"{address['id']} JSON encode/decode changed the block address")
        if int(address["length"]) <= 0:
            raise AssertionError(f"{address['id']} block length must be positive")
        if not str(address["checksum"]).startswith("sha256:"):
            raise AssertionError(f"{address['id']} block checksum must be explicit")


def validate_stable_order(corpus: dict[str, Any], addresses: list[dict[str, Any]]) -> None:
    ordered = [address["id"] for address in sorted(addresses, key=_address_key)]
    expected = corpus["expected_stable_order"]
    if ordered != expected:
        raise AssertionError(f"stable address order mismatch: got {ordered}, expected {expected}")


def validate_block_stable_order(corpus: dict[str, Any], block_addresses: list[dict[str, Any]]) -> None:
    ordered = [address["id"] for address in sorted(block_addresses, key=_block_address_key)]
    expected = corpus["expected_block_stable_order"]
    if ordered != expected:
        raise AssertionError(f"stable block address order mismatch: got {ordered}, expected {expected}")


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


def _query_records(case: dict[str, Any], query: dict[str, Any]) -> list[str]:
    include_tombstoned = bool(query.get("include_tombstoned", False))
    selected = []
    for record in case["records"]:
        timestamp_ms = int(record["timestamp_ms"])
        if timestamp_ms < int(query["start_ms"]) or timestamp_ms > int(query["end_ms"]):
            continue
        if record.get("tombstone") and not include_tombstoned:
            continue
        selected.append(str(record["value"]))
    return selected


def validate_tombstone_filtering(corpus: dict[str, Any]) -> None:
    case = corpus["tombstone_case"]
    selected = _query_records(case, case["query"])
    if selected != case["query"]["expected_values"]:
        raise AssertionError(
            f"tombstone filtered query mismatch: got {selected}, expected {case['query']['expected_values']}"
        )
    replay_selected = _query_records(case, case["debug_replay_query"])
    if replay_selected != case["debug_replay_query"]["expected_values"]:
        raise AssertionError(
            "debug/replay query must be able to include tombstoned records when policy allows it"
        )


def validate_cold_scan_no_cache_promotion(corpus: dict[str, Any]) -> None:
    case = corpus["cold_scan_case"]
    if not case["read_options"].get("no_cache_fill") or not case["read_options"].get("no_promote"):
        raise AssertionError("cold scans must use no_cache_fill and no_promote by default")

    page_index = corpus["timestamp_page_index"]
    scan_range = case["timestamp_range"]
    selected = [
        entry["address_id"]
        for entry in page_index["entries"]
        if int(entry["start_ms"]) <= int(scan_range["end_ms"])
        and int(entry["end_ms"]) >= int(scan_range["start_ms"])
    ]
    expected_scan = case["expected_scan"]
    if selected != expected_scan["page_address_ids"]:
        raise AssertionError(f"cold scan page selection mismatch: got {selected}, expected {expected_scan['page_address_ids']}")

    decode_limit = int(case["read_options"]["decode_batch_limit"])
    if decode_limit > int(expected_scan["max_decoded_per_batch"]):
        raise AssertionError("cold scan decode batch limit exceeds expected bound")

    if case["cache_before"] != case["expected_cache_after"]:
        raise AssertionError("cold scan must not change serving hot-cache keys or admission counters")


def validate_restart_rebuild_indexes(corpus: dict[str, Any]) -> None:
    case = corpus["restart_rebuild_case"]
    manifest = case["manifest_before_crash"]
    expected = case["expected_after_restart"]

    page_entries = manifest["page_index_entries"]
    block_entries = manifest["block_index_entries"]
    object_entries = manifest["object_index_entries"]
    if len(page_entries) != int(expected["page_index_entry_count"]):
        raise AssertionError("restart page index entry count mismatch")
    if len(block_entries) != int(expected["block_index_entry_count"]):
        raise AssertionError("restart block index entry count mismatch")
    if len(object_entries) != int(expected["object_index_entry_count"]):
        raise AssertionError("restart object index entry count mismatch")

    address_by_id = {address["id"]: address for address in corpus["addresses"]}
    block_address_by_id = {address["id"]: address for address in corpus["block_addresses"]}
    block_by_id = {entry["address_id"]: entry for entry in block_entries}
    for entry in page_entries:
        address_id = entry["address_id"]
        if address_id not in address_by_id:
            raise AssertionError(f"page index references unknown address {address_id}")
        block_entry = block_by_id.get(address_id)
        if not block_entry:
            raise AssertionError(f"block index missing durable location for {address_id}")
        block_address_id = block_entry.get("block_address_id")
        if block_address_id not in block_address_by_id:
            raise AssertionError(f"block index references unknown BlockAddress {block_address_id}")
        if block_entry.get("checksum") != address_by_id[address_id].get("checksum"):
            raise AssertionError(f"checksum mismatch for rebuilt address {address_id}")

    page_ids = {entry["address_id"] for entry in page_entries}
    for entry in object_entries:
        referenced = set(entry["page_address_ids"])
        if not referenced.issubset(page_ids):
            raise AssertionError(f"object index references missing page addresses: {sorted(referenced - page_ids)}")

    lookup_query_name = expected["lookup_query_name"]
    query = next(
        query
        for query in corpus["timestamp_page_index"]["queries"]
        if query["name"] == lookup_query_name
    )
    rebuilt_lookup = [
        entry["address_id"]
        for entry in page_entries
        if int(entry["start_ms"]) <= int(query["end_ms"])
        and int(entry["end_ms"]) >= int(query["start_ms"])
    ]
    if rebuilt_lookup != expected["expected_address_ids"]:
        raise AssertionError(
            f"rebuilt page index lookup mismatch: got {rebuilt_lookup}, expected {expected['expected_address_ids']}"
        )
    if int(expected["unreadable_page_refs"]) != 0 or int(expected["checksum_mismatches"]) != 0:
        raise AssertionError("restart rebuild corpus expects all pages readable with no checksum mismatch")


def main() -> int:
    corpus = _load_corpus()
    if corpus.get("schema_version") != 1:
        raise AssertionError("page address corpus schema_version must be 1")
    addresses = corpus["addresses"]
    block_addresses = corpus["block_addresses"]

    validate_encode_decode(addresses)
    validate_block_encode_decode(block_addresses)
    validate_stable_order(corpus, addresses)
    validate_block_stable_order(corpus, block_addresses)
    validate_timestamp_range_lookup(corpus)
    validate_page_split(corpus)
    validate_compaction_rewrite(corpus)
    validate_tombstone_filtering(corpus)
    validate_cold_scan_no_cache_promotion(corpus)
    validate_restart_rebuild_indexes(corpus)

    print("page address compatibility corpus passed:")
    print("- encode/decode PageAddress")
    print("- encode/decode BlockAddress")
    print("- stable ordering by shard/zone/segment/page/offset")
    print("- stable BlockAddress ordering by shard/zone/block/offset")
    print("- timestamp range to page address lookup")
    print("- page split behavior")
    print("- compaction rewrite preserves logical records")
    print("- tombstone skips stale records")
    print("- cold scan does not warm serving cache")
    print("- crash/restart rebuilds page/block/object index")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
