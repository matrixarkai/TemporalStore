#!/usr/bin/env python3
"""Validate C++/Rust shared-corpus feature execution parity status.

The shared corpus can temporarily prove some C++ families through static source
surface gates. This validator keeps that honest: every static or mixed gate must
have an explicit blocker and expected runner, and full feature parity cannot be
claimed until all product-behavior families have native C++ execution or an
approved native adapter contract.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "compat" / "temporalstore_cpp_rust_feature_execution_matrix.json"
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"

ALLOWED_STATUSES = {
    "native_executable",
    "native_adapter_contract",
    "mixed_native_and_static_surface_gate",
    "temporary_static_surface_gate",
}
COMPLETION_STATUSES = {"native_executable", "native_adapter_contract"}
STATIC_STATUSES = {"mixed_native_and_static_surface_gate", "temporary_static_surface_gate"}
REQUIRED_STATIC_RUNNER_TOKENS = (
    "tools/run_temporalstore_unified_tests.py",
    "--cpp",
    "--require-cpp-native",
    "--family",
    "{corpus}",
)
REQUIRED_STATIC_COMPARISON_TOKENS = (
    "tools/compare_unified_cpp_rust_case_reports.py",
    "--rust-report",
    "--cpp-report",
    "temporalstore_unified_case_report_v1",
)
REQUIRED_STATIC_EXIT_CRITERIA_TOKENS = (
    "native C++",
    "temporalstore_unified_case_report_v1",
    "selected shared cases",
    "no static surface gate",
)


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_strings(value: Any) -> list[str]:
    return [str(item) for item in _as_list(value)]


def _coverage_by_family(corpus: dict[str, Any]) -> dict[str, dict[str, Any]]:
    coverage = corpus.get("coverage") if isinstance(corpus.get("coverage"), dict) else {}
    rows = coverage.get("cpp_adapter_coverage")
    out: dict[str, dict[str, Any]] = {}
    for row in _as_list(rows):
        if not isinstance(row, dict):
            continue
        family = str(row.get("family") or "")
        if not family:
            continue
        target = out.setdefault(
            family,
            {
                "statuses": set(),
                "suites": set(),
                "has_blocker": False,
                "has_expected_runner": False,
                "static_rows_missing_comparison": 0,
                "static_rows_missing_exit_criteria": 0,
                "static_rows_missing_exit_tokens": set(),
                "static_rows_missing_comparison_tokens": set(),
            },
        )
        status = str(row.get("status") or "")
        if status:
            target["statuses"].add(status)
        for suite in _as_strings(row.get("suites")):
            target["suites"].add(suite)
        if row.get("suite"):
            target["suites"].add(str(row.get("suite")))
        if row.get("blocker"):
            target["has_blocker"] = True
        if row.get("expected_runner_command") or row.get("runner_command"):
            target["has_expected_runner"] = True
        if status in STATIC_STATUSES:
            comparison_command = str(row.get("comparison_command") or "")
            if not comparison_command:
                target["static_rows_missing_comparison"] += 1
            for token in REQUIRED_STATIC_COMPARISON_TOKENS:
                if token not in comparison_command:
                    target["static_rows_missing_comparison_tokens"].add(token)
            exit_criteria = _as_strings(row.get("exit_criteria"))
            if not exit_criteria:
                target["static_rows_missing_exit_criteria"] += 1
            joined_exit_criteria = " ".join(exit_criteria)
            for token in REQUIRED_STATIC_EXIT_CRITERIA_TOKENS:
                if token not in joined_exit_criteria:
                    target["static_rows_missing_exit_tokens"].add(token)
    return out


def _coverage_identity_failures(corpus: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    coverage = corpus.get("coverage") if isinstance(corpus.get("coverage"), dict) else {}
    rows = coverage.get("cpp_adapter_coverage")
    seen_families: set[str] = set()
    suite_owner: dict[str, str] = {}
    for index, row in enumerate(_as_list(rows)):
        if not isinstance(row, dict):
            continue
        family = str(row.get("family") or "")
        if not family:
            continue
        if family in seen_families:
            failures.append(f"coverage.cpp_adapter_coverage has duplicate family `{family}`")
        seen_families.add(family)
        for suite in _as_strings(row.get("suites")):
            previous = suite_owner.get(suite)
            if previous and previous != family:
                failures.append(
                    f"coverage.cpp_adapter_coverage suite `{suite}` is owned by both "
                    f"`{previous}` and `{family}`"
                )
            suite_owner[suite] = family
        if not _as_strings(row.get("suites")):
            failures.append(f"coverage.cpp_adapter_coverage[{index}] family `{family}` has no suites")
    return failures


def _case_matches_family(case: dict[str, Any], family: str, family_suites: set[str]) -> bool:
    if case.get("family") == family:
        return True
    for step in _as_list(case.get("steps")):
        if not isinstance(step, dict):
            continue
        command = step.get("command") if isinstance(step.get("command"), dict) else {}
        if command.get("suite") in family_suites:
            return True
    return False


def _case_count_by_family(corpus: dict[str, Any], coverage: dict[str, dict[str, Any]]) -> dict[str, int]:
    cases = [case for case in _as_list(corpus.get("cases")) if isinstance(case, dict)]
    counts: dict[str, int] = {}
    for family, source in coverage.items():
        family_suites = {str(suite) for suite in source["suites"]}
        counts[family] = sum(1 for case in cases if _case_matches_family(case, family, family_suites))
    return counts


def main() -> int:
    matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    failures: list[str] = []

    if matrix.get("schema") != "temporalstore_cpp_rust_feature_execution_matrix_v1":
        failures.append("unexpected schema")

    coverage = _coverage_by_family(corpus)
    failures.extend(_coverage_identity_failures(corpus))
    case_counts = _case_count_by_family(corpus, coverage)
    rows = matrix.get("rows")
    if not isinstance(rows, list):
        rows = []
        failures.append("rows must be a list")
    by_family = {str(row.get("family")): row for row in rows if isinstance(row, dict)}
    for family, source in coverage.items():
        row = by_family.get(family)
        if not isinstance(row, dict):
            failures.append(f"matrix missing family `{family}` from corpus coverage")
            continue
        status = str(row.get("status") or "")
        if status not in ALLOWED_STATUSES:
            failures.append(f"{family} invalid status `{status}`")
        corpus_statuses = source["statuses"]
        if status not in corpus_statuses and not (
            status == "native_executable" and corpus_statuses <= STATIC_STATUSES
        ):
            failures.append(f"{family} matrix status `{status}` does not match corpus statuses {sorted(corpus_statuses)}")
        matrix_suites = set(_as_strings(row.get("suites")))
        missing_suites = sorted(source["suites"] - matrix_suites)
        if missing_suites:
            failures.append(f"{family} missing suites from corpus coverage: {', '.join(missing_suites)}")
        selected_case_count = case_counts.get(family, 0)
        if selected_case_count <= 0:
            failures.append(f"{family} --family filter selects no shared corpus cases")
        declared_case_count = row.get("selected_case_count")
        if declared_case_count != selected_case_count:
            failures.append(
                f"{family} selected_case_count mismatch: matrix={declared_case_count!r} "
                f"computed={selected_case_count}"
            )
        if status in STATIC_STATUSES:
            if source["static_rows_missing_comparison"]:
                failures.append(
                    f"{family} corpus static coverage rows missing comparison_command: "
                    f"{source['static_rows_missing_comparison']}"
                )
            missing_comparison_tokens = sorted(source["static_rows_missing_comparison_tokens"])
            if missing_comparison_tokens:
                failures.append(
                    f"{family} corpus static comparison_command missing tokens: "
                    + ", ".join(missing_comparison_tokens)
                )
            if source["static_rows_missing_exit_criteria"]:
                failures.append(
                    f"{family} corpus static coverage rows missing exit_criteria: "
                    f"{source['static_rows_missing_exit_criteria']}"
                )
            missing_exit_tokens = sorted(source["static_rows_missing_exit_tokens"])
            if missing_exit_tokens:
                failures.append(
                    f"{family} corpus static exit_criteria missing tokens: "
                    + ", ".join(missing_exit_tokens)
                )
            if row.get("native_cpp_executable") is True:
                failures.append(f"{family} static gate cannot claim native_cpp_executable=true")
            if not row.get("blocker"):
                failures.append(f"{family} static gate requires blocker")
            expected_runner_command = str(row.get("expected_runner_command") or "")
            if not expected_runner_command:
                failures.append(f"{family} static gate requires expected_runner_command")
            for token in REQUIRED_STATIC_RUNNER_TOKENS:
                if token not in expected_runner_command:
                    failures.append(f"{family} static gate expected_runner_command missing `{token}`")
            if f"--family {family}" not in expected_runner_command:
                failures.append(
                    f"{family} static gate expected_runner_command must scope native proof "
                    f"with `--family {family}`"
                )
            comparison_command = str(row.get("comparison_command") or "")
            if not comparison_command:
                failures.append(f"{family} static gate requires comparison_command")
            for token in REQUIRED_STATIC_COMPARISON_TOKENS:
                if token not in comparison_command:
                    failures.append(f"{family} static gate comparison_command missing `{token}`")
            exit_criteria = _as_strings(row.get("exit_criteria"))
            if not exit_criteria:
                failures.append(f"{family} static gate requires exit_criteria")
            joined_exit_criteria = " ".join(exit_criteria)
            for token in REQUIRED_STATIC_EXIT_CRITERIA_TOKENS:
                if token not in joined_exit_criteria:
                    failures.append(f"{family} static gate exit_criteria missing `{token}`")
        if status in COMPLETION_STATUSES:
            if row.get("native_cpp_executable") is not True:
                failures.append(f"{family} completion status requires native_cpp_executable=true")
            if row.get("blocker"):
                failures.append(f"{family} completion status cannot have blocker")
            if row.get("exit_criteria"):
                failures.append(f"{family} completion status should not keep exit_criteria")

    unknown = sorted(set(by_family) - set(coverage))
    if unknown:
        failures.append(f"matrix has unknown families not present in corpus coverage: {', '.join(unknown)}")

    status = matrix.get("status") if isinstance(matrix.get("status"), dict) else {}
    blockers = _as_strings(status.get("open_blockers"))
    feature_correct = status.get("feature_correct") is True
    all_complete = all(
        isinstance(row, dict) and row.get("status") in COMPLETION_STATUSES
        for row in rows
    ) and bool(rows)
    static_families = sorted(
        str(row.get("family") or "")
        for row in rows
        if isinstance(row, dict) and row.get("status") in STATIC_STATUSES and row.get("family")
    )
    joined_blockers = " ".join(blockers)
    for family in static_families:
        if family not in joined_blockers:
            failures.append(f"status.open_blockers must name static/mixed family `{family}`")
    if feature_correct and blockers:
        failures.append("feature_correct cannot be true while open_blockers remain")
    if feature_correct and not all_complete:
        failures.append("feature_correct requires every row to be native_executable or native_adapter_contract")
    if all_complete and not feature_correct:
        failures.append("all rows complete but status.feature_correct is false")

    if failures:
        details = "\n".join(f"- {failure}" for failure in failures)
        raise SystemExit(f"TemporalStore C++/Rust feature execution matrix failed:\n{details}")

    static_count = sum(1 for row in rows if isinstance(row, dict) and row.get("status") in STATIC_STATUSES)
    print("TemporalStore C++/Rust feature execution matrix is explicit and fail-closed")
    print(f"- families={len(rows)}")
    print(f"- static_or_mixed_gates={static_count}")
    print(f"- selected_case_count={sum(case_counts.values())}")
    print(f"- feature_correct={feature_correct}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
