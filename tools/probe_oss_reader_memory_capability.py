#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Probe whether an OSS reader is strong enough for memory benchmarks.

The benchmark harness can run with tiny local readers, but final MatrixArk vs
The baseline memory system's claims need a reader that handles temporal and personal
memory questions with compact retrieved context. This probe is intentionally
small, deterministic, and fail-closed.
"""

from __future__ import annotations

import argparse
import json
import re
import time
import urllib.request
from pathlib import Path
from typing import Any

WORD_RE = re.compile(r"[A-Za-z0-9]+")

CASES = [
    {
        "id": "locomo_temporal_support_group",
        "category": "locomo_temporal",
        "question": "When did Caroline go to the LGBTQ support group?",
        "context": "[D1:3] 1:56 pm on 8 May, 2023 Caroline: I went to a LGBTQ support group yesterday and it was so powerful.",
        "accepted": ["7 may 2023", "may 7 2023", "2023 05 07", "yesterday"],
    },
    {
        "id": "locomo_temporal_year",
        "category": "locomo_temporal",
        "question": "When did Melanie paint a sunrise?",
        "context": "[D1:12] 1:56 pm on 8 May, 2023 Melanie: I painted a sunrise last year, in 2022, and it made me feel hopeful.",
        "accepted": ["2022"],
    },
    {
        "id": "longmem_user_degree",
        "category": "longmemeval_personal_fact",
        "question": "What degree did I graduate with?",
        "context": "[answer_280352e9] user: I graduated with a degree in Business Administration before moving to digital task management.",
        "accepted": ["business administration"],
    },
    {
        "id": "longmem_commute_duration",
        "category": "longmemeval_personal_fact",
        "question": "How long is my daily commute to work?",
        "context": "[answer_40a90d51] user: My daily commute to work is 45 minutes each way on weekdays.",
        "accepted": ["45 minutes each way", "45 minutes"],
    },
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reader-base-url", required=True)
    parser.add_argument("--reader-model", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--min-hit-rate", type=float, default=0.90)
    parser.add_argument("--max-p95-ms", type=float, default=30000.0)
    parser.add_argument("--report", default="/tmp/oss_reader_memory_capability_probe.json")
    args = parser.parse_args()

    started = time.time()
    report: dict[str, Any] = {
        "reader_base_url": args.reader_base_url,
        "reader_model": args.reader_model,
        "min_hit_rate": args.min_hit_rate,
        "max_p95_ms": args.max_p95_ms,
        "cases": [],
        "blockers": [],
    }
    if not probe_reader(args.reader_base_url):
        report["blockers"].append("reader_endpoint_unreachable")
        return finish(report, args.report, started, 2)

    hits = 0
    latencies = []
    for case in CASES:
        before = time.perf_counter()
        answer = call_reader(
            args.reader_base_url,
            args.reader_model,
            case["question"],
            case["context"],
            timeout=args.timeout_seconds,
            max_tokens=args.max_tokens,
        )
        elapsed_ms = (time.perf_counter() - before) * 1000.0
        latencies.append(elapsed_ms)
        hit = answer_matches(answer, case["accepted"])
        hits += int(hit)
        report["cases"].append(
            {
                "id": case["id"],
                "category": case["category"],
                "question": case["question"],
                "accepted": case["accepted"],
                "answer": answer,
                "hit": hit,
                "latency_ms": round(elapsed_ms, 3),
            }
        )

    hit_rate = hits / max(1, len(CASES))
    p95 = percentile(latencies, 95)
    competitive = hit_rate >= args.min_hit_rate and p95 <= args.max_p95_ms
    report.update(
        {
            "case_count": len(CASES),
            "hit_rate": hit_rate,
            "p95_ms": p95,
            "competitive_reader_ready": competitive,
            "claim_status": "competitive_reader_ready" if competitive else "reader_smoke_only",
        }
    )
    if hit_rate < args.min_hit_rate:
        report["blockers"].append("reader_quality_below_threshold")
    if p95 > args.max_p95_ms:
        report["blockers"].append("reader_latency_above_threshold")
    return finish(report, args.report, started, 0 if competitive else 1)


def call_reader(base_url: str, model: str, question: str, context: str, *, timeout: float, max_tokens: int) -> str:
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Answer using only the context. Return a short direct answer to the question. "
                    "If the question asks for a date, year, or when something happened, "
                    "resolve relative phrases against the context timestamp and return the explicit date or year: "
                    "yesterday means one calendar day before the context timestamp, and last year means the calendar year before the timestamp. "
                    "If the question asks for a fact such as a degree, owner, place, or duration, "
                    "copy the exact answer span from the context. For `degree in X`, return X, "
                    "not the credential level. Do not substitute an unrelated date."
                ),
            },
            {"role": "user", "content": f"Question: {question}\nContext:\n{context}\nAnswer:"},
        ],
        "temperature": 0,
        "max_tokens": max_tokens,
    }
    req = urllib.request.Request(
        base_url.rstrip("/") + "/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
        return str(data["choices"][0]["message"]["content"]).strip()
    except Exception as exc:
        return f"reader_error:{type(exc).__name__}:{exc}"


def probe_reader(base_url: str) -> bool:
    try:
        with urllib.request.urlopen(base_url.rstrip("/") + "/models", timeout=10) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def answer_matches(answer: str, accepted: list[str]) -> bool:
    norm = normalize(answer)
    return any(normalize(item) in norm for item in accepted)


def normalize(text: str) -> str:
    return " ".join(WORD_RE.findall(str(text).lower()))


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = int(rank)
    high = min(len(ordered) - 1, low + 1)
    frac = rank - low
    return round(ordered[low] * (1 - frac) + ordered[high] * frac, 3)


def finish(report: dict[str, Any], path: str, started: float, code: int) -> int:
    report["duration_seconds"] = round(time.time() - started, 3)
    report["ready"] = code == 0 and not report.get("blockers")
    Path(path).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(path)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
