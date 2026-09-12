#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The shape checks the `validate_*` gates share, with one copy of each.

Two vocabularies live here, and they differ in how they refuse:

* the ``require_*`` report-field checks raise ``ValueError``, which their caller's ``main``
  catches and turns into that script's own ``fail`` line, so the message keeps naming the gate
  it came from;
* ``require_snippets`` reads a FILE rather than a report field and raises ``SystemExit``
  directly, because its callers count return values and have no error funnel.

They are together because they are one vocabulary for one kind of script, not because the
contracts match -- and saying which is which here is cheaper than discovering it at a call site.

``ROOT`` is the repository root, computed the way every gate that used to carry its own copy of
``require_snippets`` computed it. That is not incidental: a body that reads a module-scope
constant only moves safely when the constant resolves to the same value in its new home, and
this module sits in ``tools/`` beside the scripts it serves, so ``parents[1]`` is the same
directory it was.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def load_report(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            report = json.load(handle)
    except FileNotFoundError:
        raise ValueError(f"report not found: {path}")
    except json.JSONDecodeError as exc:
        raise ValueError(f"report is not valid JSON: {exc}") from exc
    if not isinstance(report, dict):
        raise ValueError("report root must be a JSON object")
    return report


def require_bool(report: dict[str, Any], field: str) -> bool:
    value = report.get(field)
    if value is not True:
        raise ValueError(f"{field} must be true, got {value!r}")
    return True


def require_int_at_least(report: dict[str, Any], field: str, minimum: int) -> int:
    value = report.get(field)
    if not isinstance(value, int):
        raise ValueError(f"{field} must be an integer, got {value!r}")
    if value < minimum:
        raise ValueError(f"{field} must be >= {minimum}, got {value}")
    return value


def require_int_between(report: dict[str, Any], field: str, minimum: int, maximum: int) -> int:
    value = report.get(field)
    if not isinstance(value, int):
        raise ValueError(f"{field} must be an integer, got {value!r}")
    if value < minimum or value > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}, got {value}")
    return value


def require_int_equal(report: dict[str, Any], field: str, expected: int) -> None:
    value = report.get(field)
    if value != expected:
        raise ValueError(f"{field} must be {expected}, got {value!r}")


def require_string_set(report: dict[str, Any], field: str, required: set[str]) -> set[str]:
    value = report.get(field)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{field} must be a string array")
    observed = set(value)
    missing = sorted(required - observed)
    if missing:
        raise ValueError(f"{field} missing required entries: {missing}")
    return observed


def require_int_map(report: dict[str, Any], field: str) -> dict[str, int]:
    value = report.get(field)
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    bad_items = {
        key: item
        for key, item in value.items()
        if not isinstance(key, str) or not isinstance(item, int)
    }
    if bad_items:
        raise ValueError(f"{field} must map strings to integers, got {bad_items!r}")
    return value


def require_map_keys_at_least(
    report: dict[str, Any], field: str, required: set[str], minimum: int
) -> dict[str, Any]:
    value = report.get(field)
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    missing = sorted(required - set(value))
    if missing:
        raise ValueError(f"{field} missing required keys: {missing}")
    too_small = {
        key: value.get(key)
        for key in sorted(required)
        if not isinstance(value.get(key), int) or value.get(key) < minimum
    }
    if too_small:
        raise ValueError(f"{field} entries must be >= {minimum}: {too_small}")
    return value


def require_distribution_count(report: dict[str, Any], field: str, key: str, expected: int) -> None:
    value = report.get(field)
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    observed = value.get(key, 0)
    if observed != expected:
        raise ValueError(f"{field}[{key!r}] must be {expected}, got {observed!r}")


def require_snippets(path: Path, snippets: tuple[str, ...], label: str) -> int:
    if not path.exists():
        raise SystemExit(f"{label}: missing {path.relative_to(ROOT)}")
    text = path.read_text(encoding="utf-8", errors="ignore")
    missing = [snippet for snippet in snippets if snippet not in text]
    if missing:
        raise SystemExit(
            f"{label}: {path.relative_to(ROOT)} missing snippets: {', '.join(missing)}"
        )
    return len(snippets)
