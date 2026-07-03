#!/usr/bin/env python3
"""Verify MatrixArk context backfill CI evidence manifests."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


Json = dict[str, Any]
SCHEMA = "matrixark_context_backfill_ci_evidence_manifest_v1"
DUAL_WRITE_SCHEMA = "matrixark_dual_write_readiness_evidence_v1"
REQUIRED_ARTIFACTS = {
    "matrixark_context_backfill_readiness_json",
    "dual_write_dual_write_readiness_json",
    "dual_write_dual_write_readiness_prom",
    "dual_write_manifest_json",
}
REQUIRED_DUAL_WRITE_MANIFEST_ARTIFACTS = {
    "dual_write_readiness_json",
    "dual_write_readiness_prometheus",
}
REQUIRED_READINESS_SECTIONS = {
    "baseline_gate",
    "benchmark",
    "cutover_gate",
    "dead_letter_gate",
    "dual_write_gate",
    "manifest_verification_gate",
    "partial_repair_gate",
    "prometheus_gate",
    "resume_gate",
    "source_scan_gate",
    "unvalidated_repair_gate",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prom_label_value(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def prom_labels(**labels: Any) -> str:
    return ",".join(f'{key}="{prom_label_value(value)}"' for key, value in labels.items())


def load_json_file(path: Path) -> tuple[bool, Json | list[Any] | None]:
    try:
        return True, json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False, None


def to_prometheus(summary: Json) -> str:
    manifest_path = str(summary.get("manifest_path") or "")
    lines = [
        "# HELP matrixark_context_backfill_ci_evidence_verification_status CI evidence manifest verification status.",
        "# TYPE matrixark_context_backfill_ci_evidence_verification_status gauge",
        f'matrixark_context_backfill_ci_evidence_verification_status{{{prom_labels(manifest_path=manifest_path, status=str(summary.get("status") or "unknown"))}}} 1',
        "# HELP matrixark_context_backfill_ci_evidence_verification_check CI evidence manifest verification check result, 1 for pass and 0 for fail.",
        "# TYPE matrixark_context_backfill_ci_evidence_verification_check gauge",
    ]
    checks = summary.get("checks") if isinstance(summary.get("checks"), dict) else {}
    for check_name, passed in sorted(checks.items()):
        lines.append(
            f'matrixark_context_backfill_ci_evidence_verification_check{{{prom_labels(manifest_path=manifest_path, check=check_name)}}} {1 if passed else 0}'
        )
    return "\n".join(lines) + "\n"


def verify_manifest(manifest_path: Path, *, require_relative_paths: bool = False) -> Json:
    checks: dict[str, bool] = {
        "manifest_found": manifest_path.exists(),
        "manifest_json_valid": False,
        "manifest_schema_supported": False,
        "manifest_status_ok": False,
        "required_artifacts_present": False,
        "artifact_paths_relative": not require_relative_paths,
        "artifact_paths_readable": False,
        "artifact_sizes_match": False,
        "artifact_sha256_match": False,
        "readiness_json_valid": False,
        "readiness_status_ok": False,
        "readiness_checks_all_passed": False,
        "readiness_required_sections_present": False,
        "readiness_required_sections_ok": False,
        "dual_write_readiness_json_valid": False,
        "dual_write_readiness_status_ok": False,
        "dual_write_manifest_json_valid": False,
        "dual_write_manifest_schema_supported": False,
        "dual_write_manifest_status_ok": False,
        "dual_write_manifest_required_artifacts_present": False,
        "dual_write_manifest_artifact_paths_relative": False,
        "dual_write_manifest_artifact_paths_within_dir": False,
        "dual_write_manifest_artifact_paths_readable": False,
        "dual_write_manifest_artifact_sizes_match": False,
        "dual_write_manifest_artifact_sha256_match": False,
    }
    summary: Json = {
        "status": "failed",
        "manifest_path": str(manifest_path),
        "schema": "",
        "artifact_count": 0,
        "verified_artifacts": [],
        "nested_verified_artifacts": [],
        "missing_artifacts": sorted(REQUIRED_ARTIFACTS),
        "checks": checks,
    }
    if not checks["manifest_found"]:
        summary["error"] = "manifest not found"
        return summary
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        checks["manifest_json_valid"] = True
    except json.JSONDecodeError as exc:
        summary["error"] = f"invalid manifest JSON: {exc}"
        return summary

    schema = str(manifest.get("schema") or "")
    status = str(manifest.get("status") or "")
    artifacts = manifest.get("artifacts") if isinstance(manifest.get("artifacts"), dict) else {}
    artifact_names = set(artifacts)
    missing = sorted(REQUIRED_ARTIFACTS - artifact_names)

    checks["manifest_schema_supported"] = schema == SCHEMA
    checks["manifest_status_ok"] = status == "ok"
    checks["required_artifacts_present"] = not missing
    summary.update({
        "schema": schema,
        "manifest_status": status,
        "artifact_count": len(artifacts),
        "missing_artifacts": missing,
    })

    readable = True
    paths_relative = True
    sizes_match = True
    hashes_match = True
    verified_artifacts: list[Json] = []
    resolved_artifacts: dict[str, Path] = {}
    manifest_dir = manifest_path.parent
    for name, artifact in sorted(artifacts.items()):
        if not isinstance(artifact, dict):
            readable = False
            sizes_match = False
            hashes_match = False
            verified_artifacts.append({"name": name, "status": "invalid_artifact_metadata"})
            continue
        stored_path = str(artifact.get("path") or "")
        artifact_path = Path(stored_path)
        paths_relative = paths_relative and not artifact_path.is_absolute()
        if not artifact_path.is_absolute():
            artifact_path = manifest_dir / artifact_path
        item: Json = {
            "name": name,
            "path": str(artifact_path),
            "stored_path": stored_path,
            "path_relative": not Path(stored_path).is_absolute(),
        }
        if not artifact_path.exists() or not artifact_path.is_file():
            readable = False
            sizes_match = False
            hashes_match = False
            item["status"] = "missing"
            verified_artifacts.append(item)
            continue
        resolved_artifacts[name] = artifact_path
        actual_bytes = artifact_path.stat().st_size
        expected_bytes = int(artifact.get("bytes", -1) or -1)
        actual_sha256 = sha256_file(artifact_path)
        expected_sha256 = str(artifact.get("sha256") or "")
        size_match = actual_bytes == expected_bytes
        hash_match = actual_sha256 == expected_sha256
        sizes_match = sizes_match and size_match
        hashes_match = hashes_match and hash_match
        item.update({
            "status": "ok" if size_match and hash_match else "mismatch",
            "bytes_match": size_match,
            "sha256_match": hash_match,
        })
        verified_artifacts.append(item)

    checks["artifact_paths_relative"] = paths_relative if require_relative_paths else True
    checks["artifact_paths_readable"] = readable
    checks["artifact_sizes_match"] = sizes_match
    checks["artifact_sha256_match"] = hashes_match

    readiness_path = resolved_artifacts.get("matrixark_context_backfill_readiness_json")
    if readiness_path is not None:
        valid, readiness = load_json_file(readiness_path)
        checks["readiness_json_valid"] = valid and isinstance(readiness, dict)
        if isinstance(readiness, dict):
            checks["readiness_status_ok"] = readiness.get("status") == "ok"
            readiness_checks = readiness.get("checks")
            checks["readiness_checks_all_passed"] = (
                isinstance(readiness_checks, list)
                and bool(readiness_checks)
                and all(isinstance(item, dict) and bool(item.get("passed")) for item in readiness_checks)
            )
            checks["readiness_required_sections_present"] = REQUIRED_READINESS_SECTIONS.issubset(set(readiness))
            checks["readiness_required_sections_ok"] = (
                checks["readiness_required_sections_present"]
                and all(
                    isinstance(readiness.get(section), dict)
                    and readiness[section].get("status") == "ok"
                    for section in REQUIRED_READINESS_SECTIONS
                )
            )

    dual_write_path = resolved_artifacts.get("dual_write_dual_write_readiness_json")
    if dual_write_path is not None:
        valid, dual_write = load_json_file(dual_write_path)
        checks["dual_write_readiness_json_valid"] = valid and isinstance(dual_write, dict)
        if isinstance(dual_write, dict):
            checks["dual_write_readiness_status_ok"] = dual_write.get("status") == "ok"

    dual_write_manifest_path = resolved_artifacts.get("dual_write_manifest_json")
    nested_verified_artifacts: list[Json] = []
    if dual_write_manifest_path is not None:
        valid, dual_write_manifest = load_json_file(dual_write_manifest_path)
        checks["dual_write_manifest_json_valid"] = valid and isinstance(dual_write_manifest, dict)
        if isinstance(dual_write_manifest, dict):
            nested_artifacts = (
                dual_write_manifest.get("artifacts")
                if isinstance(dual_write_manifest.get("artifacts"), dict)
                else {}
            )
            checks["dual_write_manifest_schema_supported"] = dual_write_manifest.get("schema") == DUAL_WRITE_SCHEMA
            checks["dual_write_manifest_status_ok"] = dual_write_manifest.get("status") == "ok"
            checks["dual_write_manifest_required_artifacts_present"] = REQUIRED_DUAL_WRITE_MANIFEST_ARTIFACTS.issubset(set(nested_artifacts))
            nested_paths_relative = True
            nested_paths_within_dir = True
            nested_paths_readable = True
            nested_sizes_match = True
            nested_hashes_match = True
            nested_manifest_dir = dual_write_manifest_path.parent.resolve()
            for nested_name, nested_artifact in sorted(nested_artifacts.items()):
                if not isinstance(nested_artifact, dict):
                    nested_paths_relative = False
                    nested_paths_within_dir = False
                    nested_paths_readable = False
                    nested_sizes_match = False
                    nested_hashes_match = False
                    nested_verified_artifacts.append({
                        "name": nested_name,
                        "status": "invalid_artifact_metadata",
                    })
                    continue
                nested_stored_path = str(nested_artifact.get("path") or "")
                nested_path = Path(nested_stored_path)
                nested_path_relative = not nested_path.is_absolute()
                nested_paths_relative = nested_paths_relative and nested_path_relative
                if not nested_path.is_absolute():
                    nested_path = dual_write_manifest_path.parent / nested_path
                nested_within_dir = True
                try:
                    nested_resolved_path = nested_path.resolve()
                    nested_resolved_path.relative_to(nested_manifest_dir)
                except (OSError, ValueError):
                    nested_within_dir = False
                    nested_paths_within_dir = False
                nested_item: Json = {
                    "name": nested_name,
                    "path": str(nested_path),
                    "stored_path": nested_stored_path,
                    "path_relative": nested_path_relative,
                    "path_within_manifest_dir": nested_within_dir,
                }
                if not nested_path.exists() or not nested_path.is_file():
                    nested_paths_readable = False
                    nested_sizes_match = False
                    nested_hashes_match = False
                    nested_item["status"] = "missing"
                    nested_verified_artifacts.append(nested_item)
                    continue
                nested_expected_bytes = int(nested_artifact.get("bytes", -1) or -1)
                nested_expected_sha256 = str(nested_artifact.get("sha256") or "")
                nested_actual_bytes = nested_path.stat().st_size
                nested_actual_sha256 = sha256_file(nested_path)
                nested_size_match = nested_actual_bytes == nested_expected_bytes
                nested_hash_match = nested_actual_sha256 == nested_expected_sha256
                nested_sizes_match = nested_sizes_match and nested_size_match
                nested_hashes_match = nested_hashes_match and nested_hash_match
                nested_item.update({
                    "status": "ok" if nested_within_dir and nested_size_match and nested_hash_match else "mismatch",
                    "bytes_match": nested_size_match,
                    "sha256_match": nested_hash_match,
                })
                nested_verified_artifacts.append(nested_item)
            checks["dual_write_manifest_artifact_paths_relative"] = bool(nested_artifacts) and nested_paths_relative
            checks["dual_write_manifest_artifact_paths_within_dir"] = bool(nested_artifacts) and nested_paths_within_dir
            checks["dual_write_manifest_artifact_paths_readable"] = bool(nested_artifacts) and nested_paths_readable
            checks["dual_write_manifest_artifact_sizes_match"] = bool(nested_artifacts) and nested_sizes_match
            checks["dual_write_manifest_artifact_sha256_match"] = bool(nested_artifacts) and nested_hashes_match

    summary["verified_artifacts"] = verified_artifacts
    summary["nested_verified_artifacts"] = nested_verified_artifacts
    summary["status"] = "ok" if all(checks.values()) else "failed"
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify MatrixArk context backfill CI evidence artifacts.")
    parser.add_argument("--manifest", required=True, help="path to matrixark_context_backfill_evidence/manifest.json")
    parser.add_argument("--require-relative-paths", type=int, choices=[0, 1], default=0, help="fail when manifest artifact paths are absolute")
    parser.add_argument("--prometheus-output", default="")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = verify_manifest(Path(args.manifest), require_relative_paths=bool(args.require_relative_paths))
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(to_prometheus(summary), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary.get("status") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
