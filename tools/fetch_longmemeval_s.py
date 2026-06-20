#!/usr/bin/env python3
"""Fetch the real LongMemEval_s artifact used by the benchmark runner.

The dataset is intentionally not committed to this repository. This helper
downloads it into the runner's default location and validates that it has a
LongMemEval-like JSON shape before replacing the target file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT = Path("/tmp/longmemeval_s.json")
DEFAULT_URLS = (
    "https://huggingface.co/datasets/xiaowu0162/longmemeval/resolve/main/longmemeval_s",
    "https://huggingface.co/datasets/LIXINYI33/longmemeval-s/resolve/main/longmemeval_s_cleaned.json",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--url", action="append", default=[], help="Override/add download URL. May be repeated.")
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--min-records", type=int, default=100)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    urls = tuple(args.url) if args.url else DEFAULT_URLS
    if args.output.exists() and not args.force:
        report = validate_artifact(args.output, args.min_records)
        print(json.dumps({"status": "already_present", **report}, indent=2, sort_keys=True))
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []
    for url in urls:
        try:
            report = fetch_one(url, args.output, args.timeout_seconds, args.min_records)
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0
        except Exception as exc:  # noqa: BLE001 - downloader should try all mirrors and report exact causes.
            errors.append(f"{url}: {exc}")

    print(
        json.dumps(
            {
                "status": "failed",
                "output": str(args.output),
                "errors": errors,
                "hint": (
                    "Network access to Hugging Face may be blocked. Download LongMemEval_s manually "
                    "and place it at /tmp/longmemeval_s.json, then rerun "
                    "tools/run_longmemeval_s_full_path.py --threshold-profile longmemeval_full."
                ),
            },
            indent=2,
            sort_keys=True,
        ),
        file=sys.stderr,
    )
    return 2


def fetch_one(url: str, output: Path, timeout_seconds: float, min_records: int) -> dict[str, Any]:
    fd, tmp_name = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".tmp", dir=str(output.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "TemporalStore LongMemEval fetcher"})
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            with tmp_path.open("wb") as handle:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
        report = validate_artifact(tmp_path, min_records)
        tmp_path.replace(output)
        return {
            "status": "downloaded",
            "source_url": url,
            **validate_artifact(output, min_records),
        }
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}") from exc
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def validate_artifact(path: Path, min_records: int) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    records = extract_records(data)
    if len(records) < min_records:
        raise ValueError(f"expected at least {min_records} records, found {len(records)}")
    valid = sum(1 for record in records if is_longmemeval_record(record))
    if valid < min_records:
        raise ValueError(f"expected at least {min_records} LongMemEval-shaped records, found {valid}")
    return {
        "output": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
        "record_count": len(records),
        "longmemeval_shaped_records": valid,
    }


def extract_records(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("conversations", "data", "items"):
            value = data.get(key)
            if isinstance(value, list):
                return value
        return [data]
    return []


def is_longmemeval_record(record: Any) -> bool:
    if not isinstance(record, dict):
        return False
    has_history = isinstance(record.get("haystack_sessions"), list) or isinstance(record.get("conversation"), dict)
    has_questions = isinstance(record.get("questions"), list) or isinstance(record.get("qa"), list)
    return has_history and has_questions


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
