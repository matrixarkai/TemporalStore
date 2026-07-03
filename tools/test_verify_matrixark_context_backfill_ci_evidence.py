#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import verify_matrixark_context_backfill_ci_evidence as verifier

DUAL_WRITE_READINESS_CONTENT = '{"status":"ok"}\n'
DUAL_WRITE_PROMETHEUS_CONTENT = "metric 1\n"


def readiness_payload(status: str = "ok", *, check_passed: bool = True, failed_section: str = "") -> str:
    payload = {
        "status": status,
        "checks": [{"name": "unit", "passed": check_passed}],
    }
    for section in verifier.REQUIRED_READINESS_SECTIONS:
        payload[section] = {"status": "ok"}
    if failed_section:
        payload[failed_section] = {"status": "failed"}
    return json.dumps(payload, sort_keys=True) + "\n"


def write_artifact(path: Path, content: str) -> dict[str, object]:
    path.write_text(content, encoding="utf-8")
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def dual_write_manifest_payload(
    status: str = "ok",
    *,
    schema: str = verifier.DUAL_WRITE_SCHEMA,
    readiness_path: str = "dual_write_readiness.json",
    prometheus_path: str = "dual_write_readiness.prom",
) -> str:
    readiness_bytes = len(DUAL_WRITE_READINESS_CONTENT.encode("utf-8"))
    readiness_sha256 = hashlib.sha256(DUAL_WRITE_READINESS_CONTENT.encode("utf-8")).hexdigest()
    prometheus_bytes = len(DUAL_WRITE_PROMETHEUS_CONTENT.encode("utf-8"))
    prometheus_sha256 = hashlib.sha256(DUAL_WRITE_PROMETHEUS_CONTENT.encode("utf-8")).hexdigest()
    return json.dumps({
        "schema": schema,
        "status": status,
        "artifacts": {
            "dual_write_readiness_json": {"path": readiness_path, "bytes": readiness_bytes, "sha256": readiness_sha256},
            "dual_write_readiness_prometheus": {"path": prometheus_path, "bytes": prometheus_bytes, "sha256": prometheus_sha256},
        },
    }, sort_keys=True) + "\n"


class VerifyMatrixArkContextBackfillCiEvidenceTest(unittest.TestCase):
    def test_verifies_required_artifacts_and_prometheus(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifacts = {
                "matrixark_context_backfill_readiness_json": write_artifact(root / "readiness.json", readiness_payload()),
                "dual_write_dual_write_readiness_json": write_artifact(root / "dual_write_readiness.json", DUAL_WRITE_READINESS_CONTENT),
                "dual_write_dual_write_readiness_prom": write_artifact(root / "dual_write_readiness.prom", DUAL_WRITE_PROMETHEUS_CONTENT),
                "dual_write_manifest_json": write_artifact(root / "dual_write_manifest.json", dual_write_manifest_payload()),
            }
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "schema": verifier.SCHEMA,
                "status": "ok",
                "artifacts": artifacts,
            }, sort_keys=True), encoding="utf-8")

            summary = verifier.verify_manifest(manifest)
            self.assertEqual(summary["status"], "ok")
            self.assertTrue(summary["checks"]["artifact_sha256_match"])
            self.assertEqual({item["name"] for item in summary["nested_verified_artifacts"]}, {
                "dual_write_readiness_json",
                "dual_write_readiness_prometheus",
            })
            self.assertTrue(all(item["status"] == "ok" for item in summary["nested_verified_artifacts"]))
            strict = verifier.verify_manifest(manifest, require_relative_paths=True)
            self.assertEqual(strict["status"], "failed")
            self.assertFalse(strict["checks"]["artifact_paths_relative"])
            prom = verifier.to_prometheus(summary)
            self.assertIn("matrixark_context_backfill_ci_evidence_verification_status", prom)
            self.assertIn('status="ok"', prom)
            self.assertIn('check="artifact_sha256_match"} 1', prom)

    def test_relative_paths_survive_bundle_relocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            evidence = source / "evidence"
            dual_write = evidence / "dual_write"
            dual_write.mkdir(parents=True)
            readiness = evidence / "readiness.json"
            artifacts = {
                "matrixark_context_backfill_readiness_json": write_artifact(readiness, readiness_payload()),
                "dual_write_dual_write_readiness_json": write_artifact(dual_write / "dual_write_readiness.json", DUAL_WRITE_READINESS_CONTENT),
                "dual_write_dual_write_readiness_prom": write_artifact(dual_write / "dual_write_readiness.prom", DUAL_WRITE_PROMETHEUS_CONTENT),
                "dual_write_manifest_json": write_artifact(dual_write / "manifest.json", dual_write_manifest_payload()),
            }
            manifest = evidence / "manifest.json"
            portable_artifacts = {}
            for name, artifact in artifacts.items():
                artifact_path = Path(str(artifact["path"]))
                portable_artifacts[name] = {
                    **artifact,
                    "path": str(artifact_path.relative_to(evidence)),
                }
            manifest.write_text(json.dumps({
                "schema": verifier.SCHEMA,
                "status": "ok",
                "artifacts": portable_artifacts,
            }, sort_keys=True), encoding="utf-8")

            relocated = root / "relocated"
            shutil.copytree(source, relocated)
            summary = verifier.verify_manifest(relocated / "evidence" / "manifest.json")
            self.assertEqual(summary["status"], "ok")
            self.assertTrue(summary["checks"]["artifact_paths_readable"])
            self.assertTrue(summary["checks"]["artifact_sha256_match"])
            strict = verifier.verify_manifest(relocated / "evidence" / "manifest.json", require_relative_paths=True)
            self.assertEqual(strict["status"], "ok")
            self.assertTrue(strict["checks"]["artifact_paths_relative"])
            self.assertTrue(strict["checks"]["artifact_paths_within_dir"])

    def test_rejects_top_level_manifest_artifact_path_escape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            evidence = root / "evidence"
            evidence.mkdir()
            outside_readiness = root / "readiness.json"
            artifacts = {
                "matrixark_context_backfill_readiness_json": write_artifact(outside_readiness, readiness_payload()),
                "dual_write_dual_write_readiness_json": write_artifact(evidence / "dual_write_readiness.json", DUAL_WRITE_READINESS_CONTENT),
                "dual_write_dual_write_readiness_prom": write_artifact(evidence / "dual_write_readiness.prom", DUAL_WRITE_PROMETHEUS_CONTENT),
                "dual_write_manifest_json": write_artifact(evidence / "dual_write_manifest.json", dual_write_manifest_payload()),
            }
            artifacts["matrixark_context_backfill_readiness_json"] = {
                **artifacts["matrixark_context_backfill_readiness_json"],
                "path": "../readiness.json",
            }
            for name, artifact in list(artifacts.items()):
                if name == "matrixark_context_backfill_readiness_json":
                    continue
                artifact_path = Path(str(artifact["path"]))
                artifacts[name] = {
                    **artifact,
                    "path": str(artifact_path.relative_to(evidence)),
                }
            manifest = evidence / "manifest.json"
            manifest.write_text(json.dumps({
                "schema": verifier.SCHEMA,
                "status": "ok",
                "artifacts": artifacts,
            }, sort_keys=True), encoding="utf-8")

            summary = verifier.verify_manifest(manifest, require_relative_paths=True)
            self.assertEqual(summary["status"], "failed")
            self.assertTrue(summary["checks"]["artifact_paths_relative"])
            self.assertFalse(summary["checks"]["artifact_paths_within_dir"])
            self.assertFalse(summary["checks"]["artifact_paths_readable"])
            verified = {item["name"]: item for item in summary["verified_artifacts"]}
            escaped = verified["matrixark_context_backfill_readiness_json"]
            self.assertEqual(escaped["status"], "outside_manifest_dir")
            self.assertFalse(escaped["path_within_manifest_dir"])
            prom = verifier.to_prometheus(summary)
            self.assertIn('check="artifact_paths_within_dir"} 0', prom)

    def test_rejects_tampered_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            readiness = root / "readiness.json"
            artifacts = {
                "matrixark_context_backfill_readiness_json": write_artifact(readiness, readiness_payload()),
                "dual_write_dual_write_readiness_json": write_artifact(root / "dual_write_readiness.json", DUAL_WRITE_READINESS_CONTENT),
                "dual_write_dual_write_readiness_prom": write_artifact(root / "dual_write_readiness.prom", DUAL_WRITE_PROMETHEUS_CONTENT),
                "dual_write_manifest_json": write_artifact(root / "dual_write_manifest.json", dual_write_manifest_payload()),
            }
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "schema": verifier.SCHEMA,
                "status": "ok",
                "artifacts": artifacts,
            }, sort_keys=True), encoding="utf-8")
            readiness.write_text('{"status":"tampered"}\n', encoding="utf-8")

            summary = verifier.verify_manifest(manifest)
            self.assertEqual(summary["status"], "failed")
            self.assertFalse(summary["checks"]["artifact_sha256_match"])
            prom = verifier.to_prometheus(summary)
            self.assertIn('status="failed"', prom)
            self.assertIn('check="artifact_sha256_match"} 0', prom)

    def test_rejects_failed_readiness_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifacts = {
                "matrixark_context_backfill_readiness_json": write_artifact(root / "readiness.json", readiness_payload(status="failed")),
                "dual_write_dual_write_readiness_json": write_artifact(root / "dual_write_readiness.json", DUAL_WRITE_READINESS_CONTENT),
                "dual_write_dual_write_readiness_prom": write_artifact(root / "dual_write_readiness.prom", DUAL_WRITE_PROMETHEUS_CONTENT),
                "dual_write_manifest_json": write_artifact(root / "dual_write_manifest.json", dual_write_manifest_payload()),
            }
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "schema": verifier.SCHEMA,
                "status": "ok",
                "artifacts": artifacts,
            }, sort_keys=True), encoding="utf-8")

            summary = verifier.verify_manifest(manifest)
            self.assertEqual(summary["status"], "failed")
            self.assertFalse(summary["checks"]["readiness_status_ok"])
            self.assertTrue(summary["checks"]["artifact_sha256_match"])
            prom = verifier.to_prometheus(summary)
            self.assertIn('check="readiness_status_ok"} 0', prom)

    def test_rejects_failed_required_readiness_section(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            failed_section = sorted(verifier.REQUIRED_READINESS_SECTIONS)[0]
            artifacts = {
                "matrixark_context_backfill_readiness_json": write_artifact(root / "readiness.json", readiness_payload(failed_section=failed_section)),
                "dual_write_dual_write_readiness_json": write_artifact(root / "dual_write_readiness.json", DUAL_WRITE_READINESS_CONTENT),
                "dual_write_dual_write_readiness_prom": write_artifact(root / "dual_write_readiness.prom", DUAL_WRITE_PROMETHEUS_CONTENT),
                "dual_write_manifest_json": write_artifact(root / "dual_write_manifest.json", dual_write_manifest_payload()),
            }
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "schema": verifier.SCHEMA,
                "status": "ok",
                "artifacts": artifacts,
            }, sort_keys=True), encoding="utf-8")

            summary = verifier.verify_manifest(manifest)
            self.assertEqual(summary["status"], "failed")
            self.assertTrue(summary["checks"]["readiness_required_sections_present"])
            self.assertFalse(summary["checks"]["readiness_required_sections_ok"])
            self.assertTrue(summary["checks"]["artifact_sha256_match"])
            prom = verifier.to_prometheus(summary)
            self.assertIn('check="readiness_required_sections_ok"} 0', prom)

    def test_rejects_failed_dual_write_manifest_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifacts = {
                "matrixark_context_backfill_readiness_json": write_artifact(root / "readiness.json", readiness_payload()),
                "dual_write_dual_write_readiness_json": write_artifact(root / "dual_write_readiness.json", DUAL_WRITE_READINESS_CONTENT),
                "dual_write_dual_write_readiness_prom": write_artifact(root / "dual_write_readiness.prom", DUAL_WRITE_PROMETHEUS_CONTENT),
                "dual_write_manifest_json": write_artifact(root / "dual_write_manifest.json", dual_write_manifest_payload(status="failed")),
            }
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "schema": verifier.SCHEMA,
                "status": "ok",
                "artifacts": artifacts,
            }, sort_keys=True), encoding="utf-8")

            summary = verifier.verify_manifest(manifest)
            self.assertEqual(summary["status"], "failed")
            self.assertTrue(summary["checks"]["dual_write_manifest_json_valid"])
            self.assertTrue(summary["checks"]["dual_write_manifest_schema_supported"])
            self.assertFalse(summary["checks"]["dual_write_manifest_status_ok"])
            self.assertTrue(summary["checks"]["dual_write_manifest_required_artifacts_present"])
            prom = verifier.to_prometheus(summary)
            self.assertIn('check="dual_write_manifest_status_ok"} 0', prom)

    def test_rejects_nested_dual_write_manifest_artifact_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifacts = {
                "matrixark_context_backfill_readiness_json": write_artifact(root / "readiness.json", readiness_payload()),
                "dual_write_dual_write_readiness_json": write_artifact(root / "dual_write_readiness.json", DUAL_WRITE_READINESS_CONTENT),
                "dual_write_dual_write_readiness_prom": write_artifact(root / "dual_write_readiness.prom", DUAL_WRITE_PROMETHEUS_CONTENT),
                "dual_write_manifest_json": write_artifact(root / "dual_write_manifest.json", dual_write_manifest_payload()),
            }
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "schema": verifier.SCHEMA,
                "status": "ok",
                "artifacts": artifacts,
            }, sort_keys=True), encoding="utf-8")
            (root / "dual_write_readiness.prom").write_text("tampered 1\n", encoding="utf-8")

            summary = verifier.verify_manifest(manifest)
            self.assertEqual(summary["status"], "failed")
            self.assertFalse(summary["checks"]["artifact_sha256_match"])
            self.assertTrue(summary["checks"]["dual_write_manifest_artifact_paths_relative"])
            self.assertTrue(summary["checks"]["dual_write_manifest_artifact_paths_readable"])
            self.assertFalse(summary["checks"]["dual_write_manifest_artifact_sizes_match"])
            self.assertFalse(summary["checks"]["dual_write_manifest_artifact_sha256_match"])
            nested = {item["name"]: item for item in summary["nested_verified_artifacts"]}
            self.assertEqual(nested["dual_write_readiness_prometheus"]["status"], "mismatch")
            self.assertFalse(nested["dual_write_readiness_prometheus"]["bytes_match"])
            self.assertFalse(nested["dual_write_readiness_prometheus"]["sha256_match"])
            prom = verifier.to_prometheus(summary)
            self.assertIn('check="dual_write_manifest_artifact_sha256_match"} 0', prom)

    def test_rejects_nested_dual_write_manifest_path_escape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            evidence = root / "evidence"
            dual_write = evidence / "dual_write"
            dual_write.mkdir(parents=True)
            (evidence / "dual_write_readiness.json").write_text(DUAL_WRITE_READINESS_CONTENT, encoding="utf-8")
            artifacts = {
                "matrixark_context_backfill_readiness_json": write_artifact(evidence / "readiness.json", readiness_payload()),
                "dual_write_dual_write_readiness_json": write_artifact(evidence / "dual_write_readiness.json", DUAL_WRITE_READINESS_CONTENT),
                "dual_write_dual_write_readiness_prom": write_artifact(dual_write / "dual_write_readiness.prom", DUAL_WRITE_PROMETHEUS_CONTENT),
                "dual_write_manifest_json": write_artifact(
                    dual_write / "manifest.json",
                    dual_write_manifest_payload(readiness_path="../dual_write_readiness.json"),
                ),
            }
            manifest = evidence / "manifest.json"
            portable_artifacts = {}
            for name, artifact in artifacts.items():
                artifact_path = Path(str(artifact["path"]))
                portable_artifacts[name] = {
                    **artifact,
                    "path": str(artifact_path.relative_to(evidence)),
                }
            manifest.write_text(json.dumps({
                "schema": verifier.SCHEMA,
                "status": "ok",
                "artifacts": portable_artifacts,
            }, sort_keys=True), encoding="utf-8")

            summary = verifier.verify_manifest(manifest)
            self.assertEqual(summary["status"], "failed")
            self.assertTrue(summary["checks"]["dual_write_manifest_artifact_paths_relative"])
            self.assertFalse(summary["checks"]["dual_write_manifest_artifact_paths_within_dir"])
            self.assertTrue(summary["checks"]["dual_write_manifest_artifact_paths_readable"])
            nested = {item["name"]: item for item in summary["nested_verified_artifacts"]}
            self.assertEqual(nested["dual_write_readiness_json"]["status"], "mismatch")
            self.assertFalse(nested["dual_write_readiness_json"]["path_within_manifest_dir"])
            self.assertTrue(nested["dual_write_readiness_json"]["path_relative"])
            prom = verifier.to_prometheus(summary)
            self.assertIn('check="dual_write_manifest_artifact_paths_within_dir"} 0', prom)


if __name__ == "__main__":
    unittest.main()
