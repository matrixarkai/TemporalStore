#!/usr/bin/env python3
"""Summarize OSS memory benchmark misses into actionable gap buckets."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


DATE_RE = re.compile(
    r"\b(?:\d{1,2}\s+[A-Z][a-z]+(?:,?\s+\d{4})?|[A-Z][a-z]+\s+\d{1,2}(?:,?\s+\d{4})?|\d{4})\b"
)


def norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def tokens(value: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]+", value.lower()) if len(token) > 1}


def classify(row: dict[str, Any]) -> str:
    if not row.get("retrieval_hit"):
        return "retrieval_missing_expected_ref"
    expected = " ".join(str(v) for v in row.get("answer_terms") or [])
    actual = norm(row.get("reader_answer"))
    if not actual:
        return "reader_empty_answer"
    expected_tokens = tokens(expected)
    actual_tokens = tokens(actual)
    if expected_tokens and actual_tokens:
        overlap = len(expected_tokens & actual_tokens) / max(1, len(expected_tokens))
        if overlap >= 0.67:
            if DATE_RE.search(expected) and DATE_RE.search(actual):
                expected_dates = set(DATE_RE.findall(expected))
                actual_dates = set(DATE_RE.findall(actual))
                if expected_dates and actual_dates and expected_dates.isdisjoint(actual_dates):
                    return "reader_wrong_date_or_distractor"
            return "answer_equivalence_or_format"
        if overlap > 0:
            return "reader_partial_answer"
    if DATE_RE.search(expected) and DATE_RE.search(actual):
        return "reader_wrong_date_or_distractor"
    if len(actual) > 120:
        return "reader_copied_long_context_span"
    return "reader_wrong_or_distractor"


def summarize(path: Path) -> dict[str, Any]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_bucket[classify(row)].append(row)
    category_counts = Counter(str(row.get("category") or "unknown") for row in rows)
    bucket_counts = {bucket: len(items) for bucket, items in sorted(by_bucket.items())}
    examples = {}
    for bucket, items in sorted(by_bucket.items()):
        examples[bucket] = [
            {
                "query_id": row.get("query_id"),
                "question": row.get("question"),
                "expected": row.get("answer_terms"),
                "reader_answer": norm(row.get("reader_answer"))[:180],
                "rank": row.get("rank"),
                "retrieval_hit": row.get("retrieval_hit"),
            }
            for row in items[:5]
        ]
    return {
        "miss_file": str(path),
        "miss_count": len(rows),
        "category_counts": dict(sorted(category_counts.items())),
        "gap_bucket_counts": bucket_counts,
        "gap_examples": examples,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--miss-file", action="append", required=True, help="Path to a benchmark misses JSONL file.")
    parser.add_argument("--output-json", help="Optional output path for the combined summary.")
    args = parser.parse_args()

    summaries = [summarize(Path(path)) for path in args.miss_file]
    result = {"summaries": summaries}
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output_json:
        Path(args.output_json).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
