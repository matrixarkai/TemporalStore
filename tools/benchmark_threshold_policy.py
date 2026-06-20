#!/usr/bin/env python3
"""Shared benchmark threshold profiles for context-memory gates."""

from __future__ import annotations

import argparse
from typing import Any


CUSTOM_DEFAULTS = {
    "min_case_count": 1,
    "min_hit_rate": 0.90,
    "min_reader_hit_rate": 0.0,
    "min_token_reduction_percent": 0.0,
    "max_retrieval_p95_ms": 1000.0,
    "max_reader_p95_ms": 30000.0,
    "require_open_source_reader": False,
}


THRESHOLD_PROFILES = {
    "custom": CUSTOM_DEFAULTS,
    "fixture": {
        "min_case_count": 4,
        "min_hit_rate": 1.0,
        "min_reader_hit_rate": 1.0,
        "min_token_reduction_percent": 0.0,
        "max_retrieval_p95_ms": 1000.0,
        "max_reader_p95_ms": 30000.0,
        "require_open_source_reader": False,
    },
    "locomo_full": {
        "min_case_count": 1542,
        "min_hit_rate": 0.94,
        "min_reader_hit_rate": 0.58,
        "min_token_reduction_percent": 80.0,
        "max_retrieval_p95_ms": 250.0,
        "max_reader_p95_ms": 50.0,
        "require_open_source_reader": False,
    },
    "longmemeval_full": {
        "min_case_count": 500,
        "min_hit_rate": 0.90,
        "min_reader_hit_rate": 0.58,
        "min_token_reduction_percent": 80.0,
        "max_retrieval_p95_ms": 2000.0,
        "max_reader_p95_ms": 200.0,
        "require_open_source_reader": False,
    },
    "oss_reader_full": {
        "min_case_count": 1542,
        "min_hit_rate": 0.94,
        "min_reader_hit_rate": 0.58,
        "min_token_reduction_percent": 80.0,
        "max_retrieval_p95_ms": 250.0,
        "max_reader_p95_ms": 30000.0,
        "require_open_source_reader": True,
    },
}


def add_threshold_policy_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--threshold-profile",
        choices=sorted(THRESHOLD_PROFILES),
        default="custom",
        help="Named benchmark threshold policy. Explicit threshold flags override profile defaults.",
    )
    parser.add_argument("--min-hit-rate", type=float, default=None)
    parser.add_argument("--min-case-count", type=int, default=None)
    parser.add_argument("--min-reader-hit-rate", type=float, default=None)
    parser.add_argument("--min-token-reduction-percent", type=float, default=None)
    parser.add_argument("--max-retrieval-p95-ms", type=float, default=None)
    parser.add_argument("--max-reader-p95-ms", type=float, default=None)
    parser.add_argument("--require-open-source-reader", action="store_true")


def resolve_threshold_policy(args: argparse.Namespace) -> dict[str, Any]:
    thresholds = dict(THRESHOLD_PROFILES[args.threshold_profile])
    for key in (
        "min_hit_rate",
        "min_case_count",
        "min_reader_hit_rate",
        "min_token_reduction_percent",
        "max_retrieval_p95_ms",
        "max_reader_p95_ms",
    ):
        value = getattr(args, key)
        if value is not None:
            thresholds[key] = value
    if args.require_open_source_reader:
        thresholds["require_open_source_reader"] = True
    return thresholds
