#!/usr/bin/env python3
"""Validate benchmark result docs do not overclaim production parity."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
BENCHMARK_DOC_PATTERNS = (
    "*benchmark*.md",
    "*locomo*.md",
    "*longmem*.md",
    "context_benchmarks*.md",
)
CLAIM_PATTERN = re.compile(
    r"\b(?:production\s+(?:parity|ready)|production-ready|paper-equivalent)\b",
    re.IGNORECASE,
)
NEGATION_PATTERN = re.compile(
    r"\b(?:no|not|cannot|blocked|unless|only|must not|is not|not a|requires?|requirement)\b",
    re.IGNORECASE,
)
REQUIRED_EVIDENCE = (
    ("real dataset", re.compile(r"\breal (?:dataset|artifact)|full dataset\b", re.IGNORECASE)),
    ("real reader", re.compile(r"\breal (?:reader|OSS call|open-source reader call)|live .*reader\b", re.IGNORECASE)),
    (
        "passing thresholds",
        re.compile(
            r"benchmark_threshold_passed|Threshold violations\s*\|\s*`\[\]`|threshold violations.*\[\]",
            re.IGNORECASE | re.DOTALL,
        ),
    ),
)


def main() -> int:
    validate_temporal_reasoning_rules()
    failures: list[str] = []
    for path in benchmark_docs():
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line_no, line in enumerate(text.splitlines(), start=1):
            if not CLAIM_PATTERN.search(line):
                continue
            if NEGATION_PATTERN.search(line):
                continue
            missing = [name for name, pattern in REQUIRED_EVIDENCE if not pattern.search(text)]
            if missing:
                failures.append(
                    f"{relative(path)}:{line_no}: benchmark production claim lacks "
                    f"{', '.join(missing)} evidence: {line.strip()}"
                )
    if failures:
        print("benchmark claim validation failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("benchmark claim validation passed")
    return 0


def validate_temporal_reasoning_rules() -> None:
    runner = load_locomo_runner()
    texts = [
        "2023-01-10 I joined the running club.",
        "2023-01-12 I bought new running shoes.",
        "2023-01-20 I ran my first race.",
        "2023-02-01 I watered the orchids.",
        "2023-02-03 I watered the orchids again.",
        "2023-03-01 I scheduled the workshop in two weeks.",
    ]
    checks = [
        (
            "Did I buy new running shoes before I ran my first race?",
            "Yes",
            "before/after comparison",
        ),
        (
            "When did I run my first race after I bought new running shoes?",
            "20 January 2023",
            "anchored after-date selection",
        ),
        (
            "What did I do before I ran my first race?",
            "bought new running shoes",
            "nearest prior event selection",
        ),
        (
            "When did I first water the orchids?",
            "1 February 2023",
            "first occurrence selection",
        ),
        (
            "When did I last water the orchids?",
            "3 February 2023",
            "last occurrence selection",
        ),
    ]
    for question, expected, label in checks:
        answer = runner.temporal_ordering_answer(question, texts)
        if expected.lower() not in answer.lower():
            raise SystemExit(f"temporal rule failed {label}: expected {expected!r}, got {answer!r}")
    relative_entries = runner.dated_text_entries([texts[-1]])
    if not any(runner.format_date(entry.date) == "15 March 2023" for entry in relative_entries):
        raise SystemExit("temporal rule failed future relative-date normalization")


def load_locomo_runner():
    path = ROOT / "tools" / "run_locomo_ingest_once.py"
    spec = importlib.util.spec_from_file_location("run_locomo_ingest_once_for_validation", path)
    if spec is None or spec.loader is None:
        raise SystemExit("unable to load run_locomo_ingest_once.py for temporal validation")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def benchmark_docs() -> list[Path]:
    docs = set()
    for pattern in BENCHMARK_DOC_PATTERNS:
        docs.update(DOCS.glob(pattern))
    return sorted(path for path in docs if path.is_file())


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT)).replace("\\", "/")


if __name__ == "__main__":
    raise SystemExit(main())
