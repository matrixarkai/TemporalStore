#!/usr/bin/env python3
"""Validate benchmark result docs do not overclaim production parity."""

from __future__ import annotations

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


def benchmark_docs() -> list[Path]:
    docs = set()
    for pattern in BENCHMARK_DOC_PATTERNS:
        docs.update(DOCS.glob(pattern))
    return sorted(path for path in docs if path.is_file())


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT)).replace("\\", "/")


if __name__ == "__main__":
    raise SystemExit(main())
