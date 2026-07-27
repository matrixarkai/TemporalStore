#!/usr/bin/env python3
"""Validate apples-to-apples OSS model contracts across benchmark reports.

MatrixArk, OpenViking, VikingMem, and similar baselines may have different
storage and retrieval logic, but benchmark claims are only comparable when the
reader model, embedding model, retrieval budget, reader evidence policy, and reader context budget are
held constant. This validator fails closed when a report omits the contract or
when any run drifts from the shared OSS model contract.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


CONTRACT_KEYS = (
    "reader_model",
    "embedding_model",
    "encoding_model",
    "max_events",
    "reader_max_context_chars",
    "reader_max_tokens",
)

READER_POLICY_KEYS = (
    "reader_include_extractive_hint",
    "reader_candidate_only",
    "reader_candidate_first",
    "reader_focus_evidence",
)

RETRIEVAL_POLICY_KEYS = (
    "adaptive_max_events",
    "adaptive_base_max_events",
    "retrieval_same_session_percent",
    "retrieval_cross_session_percent",
    "retrieval_summary_percent",
    "retrieval_entity_percent",
    "retrieval_event_percent",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", action="append", required=True, help="Benchmark JSON report. May be repeated.")
    parser.add_argument("--label", action="append", default=[], help="Optional label matching --report order.")
    parser.add_argument(
        "--allow-diagnostic",
        action="store_true",
        help="Allow diagnostic-only reports as long as their model contract matches.",
    )
    parser.add_argument(
        "--allow-reader-fallback",
        action="store_true",
        help="Allow reader fallback/error rows. By default, fair OSS reader comparisons must use the configured OSS reader.",
    )
    parser.add_argument(
        "--allow-reader-policy-drift",
        action="store_true",
        help=(
            "Allow prompt-shaping policy drift. By default, apples-to-apples reader-quality comparisons "
            "must use the same candidate-only, candidate-first, focus-evidence, and extractive-hint policy."
        ),
    )
    parser.add_argument("--output-json", default="", help="Optional normalized validation summary path.")
    args = parser.parse_args()

    labels = list(args.label)
    rows = []
    errors: list[str] = []
    for index, report in enumerate(args.report):
        label = labels[index] if index < len(labels) else Path(report).stem
        row_errors: list[str] = []
        row = normalize_report(Path(report), label, row_errors)
        rows.append(row)
        errors.extend(row_errors)

    expected = first_complete_contract(rows)
    if expected is None:
        errors.append("no_complete_shared_oss_model_contract")
    else:
        for row in rows:
            for key in CONTRACT_KEYS:
                if row.get(key) != expected.get(key):
                    errors.append(
                        f"{row['label']}: {key}_mismatch expected={expected.get(key)!r} actual={row.get(key)!r}"
                    )
            if not row.get("provider_name"):
                errors.append(f"{row['label']}: provider_identity_missing")
            if row.get("diagnostic_only") and not args.allow_diagnostic:
                errors.append(f"{row['label']}: diagnostic_only_report")
            if not args.allow_reader_fallback:
                if row.get("reader_fallback_count", 0) > 0:
                    errors.append(f"{row['label']}: reader_fallback_used count={row.get('reader_fallback_count')}")
                if row.get("reader_error_count", 0) > 0:
                    errors.append(f"{row['label']}: reader_errors count={row.get('reader_error_count')}")
                if row.get("reader_open_source_calls") is not None and row.get("reader_open_source_calls", 0) <= 0:
                    errors.append(f"{row['label']}: no_oss_reader_calls")
            if not args.allow_reader_policy_drift:
                for key in READER_POLICY_KEYS:
                    if row.get(key) != rows[0].get(key):
                        errors.append(
                            f"{row['label']}: {key}_mismatch expected={rows[0].get(key)!r} actual={row.get(key)!r}"
                        )
            for key in RETRIEVAL_POLICY_KEYS:
                if row.get(key) != rows[0].get(key):
                    errors.append(
                        f"{row['label']}: {key}_mismatch expected={rows[0].get(key)!r} actual={row.get(key)!r}"
                    )

    result = {
        "schema": "matrixark_shared_oss_model_contract_validation_v1",
        "passed": not errors,
        "report_count": len(rows),
        "expected_contract": expected or {},
        "rows": rows,
        "errors": errors,
        "rule": (
            "MatrixArk, OpenViking, VikingMem, and peer rows must use the same OSS "
            "reader model, embedding/encoding model, retrieved event budget, reader evidence policy, and "
            "reader context/output-token budget before token-savings or reader-quality claims are comparable. "
            "Reader fallback, reader errors, diagnostic-only rows, and reader prompt-policy drift fail "
            "by default so MatrixArk and VikingMem/OpenViking use the same OSS encoder and reader setup."
        ),
    }
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print("shared_oss_model_contract_passed")
    return 0


def normalize_report(path: Path, label: str, errors: list[str]) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        errors.append(f"{label}: invalid_json {exc}")
        return {"label": label, "path": str(path)}

    metrics = data.get("harness") if isinstance(data.get("harness"), dict) else data
    quality_gate = data.get("quality_gate") if isinstance(data.get("quality_gate"), dict) else {}
    contract = extract_contract(data, metrics, quality_gate)
    if not contract:
        errors.append(f"{label}: benchmark_model_contract_missing")

    matrixark_like = label.lower().startswith("matrixark") or str(data.get("baseline") or "").lower() == "matrixark"
    prefix = "matrixark" if matrixark_like else "baseline"
    row = {
        "label": label,
        "path": str(path),
        "provider_name": str(contract.get(f"{prefix}_provider_name") or data.get("reader_provider_name") or "").strip(),
        "reader_model": normalized(contract.get(f"{prefix}_reader_model") or data.get("reader_model") or data.get("model")),
        "embedding_model": normalized(contract.get(f"{prefix}_embedding_model") or data.get("embedding_model")),
        "encoding_model": normalized(
            contract.get(f"{prefix}_encoding_model")
            or contract.get(f"{prefix}_embedding_model")
            or data.get("encoding_model")
            or data.get("embedding_model")
        ),
        "max_events": to_int(contract.get(f"{prefix}_max_events") or data.get("max_events")),
        "reader_max_context_chars": to_int(
            contract.get(f"{prefix}_reader_max_context_chars") or data.get("reader_max_context_chars")
        ),
        "reader_max_tokens": to_int(contract.get(f"{prefix}_reader_max_tokens") or data.get("reader_max_tokens")),
        "reader_include_extractive_hint": bool(data.get("reader_include_extractive_hint")),
        "reader_candidate_only": bool(data.get("reader_candidate_only")),
        "reader_candidate_first": bool(data.get("reader_candidate_first")),
        "reader_focus_evidence": bool(data.get("reader_focus_evidence")),
        "adaptive_max_events": bool(
            contract.get(f"{prefix}_adaptive_max_events")
            if f"{prefix}_adaptive_max_events" in contract
            else data.get("adaptive_max_events")
        ),
        "adaptive_base_max_events": to_int(
            contract.get(f"{prefix}_adaptive_base_max_events")
            if f"{prefix}_adaptive_base_max_events" in contract
            else data.get("adaptive_base_max_events")
        )
        or 0,
        "retrieval_same_session_percent": to_float_policy(
            contract.get(f"{prefix}_retrieval_same_session_percent"),
            data,
            "same_session_percent",
        ),
        "retrieval_cross_session_percent": to_float_policy(
            contract.get(f"{prefix}_retrieval_cross_session_percent"),
            data,
            "cross_session_percent",
        ),
        "retrieval_summary_percent": to_float_policy(
            contract.get(f"{prefix}_retrieval_summary_percent"),
            data,
            "summary_percent",
        ),
        "retrieval_entity_percent": to_float_policy(
            contract.get(f"{prefix}_retrieval_entity_percent"),
            data,
            "entity_percent",
        ),
        "retrieval_event_percent": to_float_policy(
            contract.get(f"{prefix}_retrieval_event_percent"),
            data,
            "event_percent",
        ),
        "reader_fallback_count": to_int(data.get("reader_fallback_count")) or 0,
        "reader_error_count": to_int(data.get("reader_error_count")) or 0,
        "reader_open_source_calls": to_int(data.get("reader_open_source_calls")),
        "shared_oss_model_contract_required": bool(contract.get("shared_oss_model_contract_required")),
        "shared_oss_model_contract_passed": bool(contract.get("shared_oss_model_contract_passed")),
        "diagnostic_only": bool(
            data.get("diagnostic_only")
            or data.get("python_only_diagnostic")
            or "diagnostic" in str(data.get("claim_status") or "").lower()
        ),
    }
    if not row["shared_oss_model_contract_required"]:
        errors.append(f"{label}: shared_oss_model_contract_not_required")
    if not row["shared_oss_model_contract_passed"]:
        errors.append(f"{label}: shared_oss_model_contract_not_passed")
    for key in CONTRACT_KEYS:
        if row.get(key) in ("", None, 0):
            errors.append(f"{label}: {key}_missing")
    if row.get("embedding_model") and row.get("encoding_model") and row["embedding_model"] != row["encoding_model"]:
        errors.append(
            f"{label}: embedding_encoding_model_mismatch "
            f"embedding={row.get('embedding_model')!r} encoding={row.get('encoding_model')!r}"
        )
    return row


def extract_contract(*sources: dict[str, Any]) -> dict[str, Any]:
    for source in sources:
        contract = source.get("benchmark_model_contract") if isinstance(source, dict) else None
        if isinstance(contract, dict):
            return contract
    return {}


def first_complete_contract(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in rows:
        if all(row.get(key) not in ("", None, 0) for key in CONTRACT_KEYS):
            return {key: row.get(key) for key in CONTRACT_KEYS}
    return None


def normalized(value: Any) -> str:
    return re.sub(r"[^a-z0-9._:-]+", "", str(value or "").strip().lower())


def to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float_policy(value: Any, data: dict[str, Any], key: str) -> float:
    if value is None:
        config = data.get("retrieval_budget_config")
        if isinstance(config, dict):
            value = config.get(key)
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return 0.0


if __name__ == "__main__":
    raise SystemExit(main())
