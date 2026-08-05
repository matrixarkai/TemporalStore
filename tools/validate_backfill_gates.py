"""Split out of validate_matrixark_context_backfill_readiness.py; re-exported at that module's end via the dual
relative/absolute import pattern so the same module object is reused under both
the package path (tools.<mod>) and the top-level path. No import-time cycle.
__all__ lists every moved name for total re-export."""
import argparse
import hashlib
import json
import tempfile

try:  # package path (tools.validate_matrixark_context_backfill_readiness)
    from .validate_matrixark_context_backfill_readiness import (
        Json,
        Path,
        backfill,
        bench,
        dual_bench,
        int_or_default,
    )
except ImportError:  # top-level path (validate_matrixark_context_backfill_readiness)
    from validate_matrixark_context_backfill_readiness import (
        Json,
        Path,
        backfill,
        bench,
        dual_bench,
        int_or_default,
    )

__all__ = ['seed_legacy_raw_log', 'seed_scan_hash_raw_log', 'run_source_scan_gate', 'seed_partial_raw_log', 'run_partial_repair_gate', 'run_unvalidated_repair_gate', 'run_manifest_verification_gate', 'run_dead_letter_gate', 'run_resume_gate', 'run_prometheus_gate', 'dual_write_evidence_dir', 'sha256_file', 'run_dual_write_gate']


def seed_legacy_raw_log(kv: backfill.LocalJsonKV, *, prefix: str, records: int, payload_bytes: int) -> None:
    record_ids = []
    kv.begin_bulk()
    try:
        for sequence in range(records):
            record_id = f"legacy-{sequence:020d}"
            record_ids.append(record_id)
            kv.hset(
                f"{prefix}:records",
                record_id,
                json.dumps(bench.make_raw_record(sequence, payload_bytes=payload_bytes), sort_keys=True),
            )
        kv.put_string(f"{prefix}:record_index", json.dumps(record_ids, separators=(",", ":")))
    finally:
        kv.end_bulk()


def seed_scan_hash_raw_log(kv: backfill.LocalJsonKV, *, prefix: str, records: int, payload_bytes: int) -> None:
    kv.begin_bulk()
    try:
        for sequence in range(records):
            shard = sequence // backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            offset = sequence % backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            kv.hset(
                f"{prefix}:records:{shard:06d}",
                f"{offset:020d}",
                json.dumps(bench.make_raw_record(sequence, payload_bytes=payload_bytes), sort_keys=True),
            )
    finally:
        kv.end_bulk()


def run_source_scan_gate(args: argparse.Namespace) -> Json:
    results: list[Json] = []
    records = max(4, int(args.records))
    scenarios = [
        ("record_count", bench.seed_raw_log, None),
        ("record_index", seed_legacy_raw_log, None),
        ("scan_hash", seed_scan_hash_raw_log, records),
    ]
    for raw_backend in ["temporalstore", "matrixkv"]:
        for expected_scan_mode, seed_fn, end_seq in scenarios:
            with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_scan_{raw_backend}_{expected_scan_mode}_") as tmp:
                kv_path = Path(tmp) / "kv.json"
                kv = backfill.LocalJsonKV(kv_path)
                source_prefix = f"matrixark:mcp:readiness_scan:{expected_scan_mode}"
                seed_fn(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
                target_prefix = f"matrixark:context_backfill:readiness_scan:{raw_backend}:{expected_scan_mode}"
                job_id = f"readiness-scan-{raw_backend}-{expected_scan_mode}"
                run_args = bench.make_backfill_args(
                    kv_path=kv_path,
                    source_prefix=source_prefix,
                    target_prefix=target_prefix,
                    raw_backend=raw_backend,
                    job_id=job_id,
                    batch_size=args.batch_size,
                    end_seq=end_seq,
                )
                summary = backfill.run_backfill(run_args)
                validation = backfill.run_validate_shadow(bench.make_backfill_args(
                    kv_path=kv_path,
                    source_prefix=source_prefix,
                    target_prefix=target_prefix,
                    raw_backend=raw_backend,
                    job_id=job_id,
                    batch_size=args.batch_size,
                    mode="validate_shadow",
                    end_seq=end_seq,
                ))
                source_range = summary.get("source_range") if isinstance(summary.get("source_range"), dict) else {}
                metrics = summary.get("metrics") if isinstance(summary.get("metrics"), dict) else {}
                results.append({
                    "raw_backend": raw_backend,
                    "expected_scan_mode": expected_scan_mode,
                    "scan_mode": source_range.get("scan_mode"),
                    "status": summary.get("status"),
                    "records": records,
                    "scanned": int(metrics.get("scanned", 0) or 0),
                    "written": int(metrics.get("written", 0) or 0),
                    "failed": int(metrics.get("failed", 0) or 0),
                    "validation_status": validation.get("status"),
                    "validation_fingerprint_match": validation.get("checks", {}).get("serving_record_fingerprint_match"),
                    "source_record_count_estimated": bool(source_range.get("source_record_count_estimated")),
                    "source_high_watermark_seq": source_range.get("source_high_watermark_seq"),
                })
    status = "ok" if all(
        item["status"] == "ok"
        and item["validation_status"] == "ok"
        and item["validation_fingerprint_match"] is True
        and item["scan_mode"] == item["expected_scan_mode"]
        and item["scanned"] == item["records"]
        and item["written"] == item["records"]
        and item["failed"] == 0
        and item["source_high_watermark_seq"] == item["records"] - 1
        and (item["source_record_count_estimated"] is (item["expected_scan_mode"] == "scan_hash"))
        for item in results
    ) else "failed"
    return {"status": status, "results": results}


def seed_partial_raw_log(kv: backfill.LocalJsonKV, *, prefix: str, records: int, payload_bytes: int) -> int:
    expected = 0
    kv.begin_bulk()
    try:
        for sequence in range(records):
            record = bench.make_raw_record(sequence, payload_bytes=payload_bytes)
            if sequence % 4 == 0:
                record["scope"] = {
                    "tenant_id": "tenant-partial",
                    "user_id": f"user-partial-{sequence % 2}",
                    "session_id": "session-hot",
                    "team": "search",
                }
                record["kind"] = "message"
                expected += 1
            elif sequence % 4 == 1:
                record["scope"] = {
                    "tenant_id": "tenant-partial",
                    "user_id": "user-other",
                    "session_id": "session-cold",
                    "team": "search",
                }
                record["kind"] = "message"
            elif sequence % 4 == 2:
                record["scope"] = {
                    "tenant_id": "tenant-other",
                    "user_id": "user-other",
                    "session_id": "session-hot",
                    "team": "search",
                }
                record["kind"] = "message"
            else:
                record["scope"] = {
                    "tenant_id": "tenant-partial",
                    "user_id": "user-other",
                    "session_id": "session-hot",
                    "team": "billing",
                }
                record["kind"] = "metric"
            shard = sequence // backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            offset = sequence % backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            kv.hset(
                f"{prefix}:records:{shard:06d}",
                f"{offset:020d}",
                json.dumps(record, sort_keys=True),
            )
        kv.put_string(f"{prefix}:record_count", str(records))
    finally:
        kv.end_bulk()
    return expected


def run_partial_repair_gate(args: argparse.Namespace) -> Json:
    results: list[Json] = []
    records = max(8, int(args.records))
    partial_values = {
        "partial": True,
        "partial_record_types": "context_event",
        "partial_tenant_ids": "tenant-partial",
        "partial_session_ids": "session-hot",
        "partial_filter_json": json.dumps({"kind": "message", "scope": {"team": "search"}}, sort_keys=True),
    }
    for raw_backend in ["temporalstore", "matrixkv"]:
        with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_partial_{raw_backend}_") as tmp:
            kv_path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(kv_path)
            source_prefix = "matrixark:mcp:readiness_partial"
            expected_matches = seed_partial_raw_log(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
            active_key = "matrixark:context:active_prefix"
            active_prefix = f"matrixark:context:active:partial:{raw_backend}"
            repair_prefix = f"matrixark:context_repair:readiness_partial:{raw_backend}"
            job_id = f"readiness-partial-{raw_backend}"
            kv.put_string(active_key, active_prefix)
            shadow_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
            )
            for key, value in partial_values.items():
                setattr(shadow_args, key, value)
            shadow = backfill.run_backfill(shadow_args)
            validation_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                mode="validate_shadow",
                end_seq=records,
            )
            for key, value in partial_values.items():
                setattr(validation_args, key, value)
            validation = backfill.run_validate_shadow(validation_args)
            repair_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                end_seq=records,
                mode="incremental_repair",
                confirm_incremental_repair="YES",
                expect_active_prefix=active_prefix,
            )
            for key, value in partial_values.items():
                setattr(repair_args, key, value)
            repair = backfill.run_incremental_repair(repair_args)
            retried = backfill.run_incremental_repair(repair_args)
            kv_after = backfill.LocalJsonKV(kv_path)
            audit = kv_after.hget(f"{active_key}:incremental_repair_audit", job_id)
            consistency = repair.get("promotion_consistency") if isinstance(repair.get("promotion_consistency"), dict) else {}
            shadow_metrics = shadow.get("metrics") if isinstance(shadow.get("metrics"), dict) else {}
            promotion = repair.get("promotion") if isinstance(repair.get("promotion"), dict) else {}
            retry_promotion = retried.get("promotion") if isinstance(retried.get("promotion"), dict) else {}
            promotion_metrics = promotion.get("metrics") if isinstance(promotion.get("metrics"), dict) else {}
            retry_metrics = retry_promotion.get("metrics") if isinstance(retry_promotion.get("metrics"), dict) else {}
            target_count = int(kv_after.get_string(f"{active_prefix}:record_count") or 0)
            results.append({
                "raw_backend": raw_backend,
                "records": records,
                "expected_matches": expected_matches,
                "shadow_status": shadow.get("status"),
                "shadow_scanned": int(shadow_metrics.get("scanned", 0) or 0),
                "shadow_filtered": int(shadow_metrics.get("filtered", 0) or 0),
                "shadow_written": int(shadow_metrics.get("written", 0) or 0),
                "validation_status": validation.get("status"),
                "validation_expected_records": int(validation.get("expected_records", 0) or 0),
                "validation_fingerprint_match": validation.get("checks", {}).get("serving_record_fingerprint_match"),
                "repair_status": repair.get("status"),
                "repair_written": int(promotion_metrics.get("written", 0) or 0),
                "retry_duplicate": int(retry_metrics.get("duplicate", 0) or 0),
                "target_count": target_count,
                "partial_enabled": bool(shadow.get("partial", {}).get("enabled")),
                "promotion_partial_matches_validation": bool(consistency.get("checks", {}).get("promotion_partial_matches_validation")),
                "promotion_data_quality_status": consistency.get("promotion_data_quality_status"),
                "promotion_data_quality_clean": bool(consistency.get("checks", {}).get("promotion_data_quality_clean")),
                "promotion_source_range_matches_validation": bool(consistency.get("checks", {}).get("promotion_source_range_matches_validation")),
                "promotion_covered_expected_records": bool(consistency.get("checks", {}).get("promotion_covered_expected_records")),
                "audit_written": bool(audit),
            })
    status = "ok" if all(
        item["shadow_status"] == "ok"
        and item["validation_status"] == "ok"
        and item["repair_status"] == "ok"
        and item["partial_enabled"]
        and item["shadow_scanned"] == item["records"]
        and item["shadow_written"] == item["expected_matches"]
        and item["shadow_filtered"] == item["records"] - item["expected_matches"]
        and item["validation_expected_records"] == item["expected_matches"]
        and item["validation_fingerprint_match"] is True
        and item["repair_written"] == item["expected_matches"]
        and item["retry_duplicate"] == item["expected_matches"]
        and item["target_count"] == item["expected_matches"]
        and item["promotion_partial_matches_validation"]
        and item["promotion_data_quality_status"] == "clean"
        and item["promotion_data_quality_clean"]
        and item["promotion_source_range_matches_validation"]
        and item["promotion_covered_expected_records"]
        and item["audit_written"]
        for item in results
    ) else "failed"
    return {"status": status, "results": results}


def run_unvalidated_repair_gate(args: argparse.Namespace) -> Json:
    results: list[Json] = []
    source_prefix = "matrixark:mcp:readiness_unvalidated_repair"
    records = max(2, min(max(2, int(args.records)), max(2, int(args.incremental_records))))
    end_seq = min(records, max(2, int(args.batch_size)))
    for raw_backend in ["temporalstore", "matrixkv"]:
        with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_unvalidated_repair_{raw_backend}_") as tmp:
            kv_path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(kv_path)
            bench.seed_raw_log(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
            active_key = "matrixark:context:active_prefix"
            active_prefix = f"matrixark:context:active:unvalidated:{raw_backend}"
            repair_prefix = f"matrixark:context_repair:unvalidated:{raw_backend}"
            job_id = f"readiness-unvalidated-repair-{raw_backend}"
            kv.put_string(active_key, active_prefix)

            blocked_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                end_seq=end_seq,
                mode="incremental_repair",
                confirm_incremental_repair="YES",
                expect_active_prefix=active_prefix,
            )
            blocked_args.skip_validation = True
            blocked_args.confirm_skip_validation = "YES"
            blocked_without_target_state_confirmation = False
            blocked_error = ""
            try:
                backfill.run_incremental_repair(blocked_args)
            except backfill.BackfillError as exc:
                blocked_error = str(exc)
                blocked_without_target_state_confirmation = "confirm-unvalidated-target-state=YES" in blocked_error

            repair_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                end_seq=end_seq,
                mode="incremental_repair",
                confirm_incremental_repair="YES",
                expect_active_prefix=active_prefix,
            )
            repair_args.skip_validation = True
            repair_args.confirm_skip_validation = "YES"
            repair_args.confirm_unvalidated_target_state = "YES"
            repair = backfill.run_incremental_repair(repair_args)
            kv_after = backfill.LocalJsonKV(kv_path)
            audit_raw = kv_after.hget(f"{active_key}:incremental_repair_audit", job_id)
            audit = json.loads(audit_raw) if audit_raw else {}
            target_state = repair.get("validation_target_state") if isinstance(repair.get("validation_target_state"), dict) else {}
            audit_target_state = audit.get("validation_target_state") if isinstance(audit.get("validation_target_state"), dict) else {}
            promotion = repair.get("promotion") if isinstance(repair.get("promotion"), dict) else {}
            promotion_metrics = promotion.get("metrics") if isinstance(promotion.get("metrics"), dict) else {}
            results.append({
                "raw_backend": raw_backend,
                "records": records,
                "end_seq": end_seq,
                "blocked_without_target_state_confirmation": blocked_without_target_state_confirmation,
                "blocked_error": blocked_error,
                "status": repair.get("status"),
                "validation_status": repair.get("validation_status"),
                "validation_skipped": bool(repair.get("validation_skipped")),
                "unvalidated_target_state_confirmed": bool(repair.get("unvalidated_target_state_confirmed")),
                "validation_target_record_count": int_or_default(target_state.get("record_count")),
                "validation_target_healthy": bool(target_state.get("healthy_for_unvalidated_activation")),
                "promotion_written": int(promotion_metrics.get("written", 0) or 0),
                "promotion_failed": int(promotion_metrics.get("failed", 0) or 0),
                "promotion_dead_letter": int(promotion_metrics.get("dead_letter", 0) or 0),
                "audit_written": bool(audit_raw),
                "audit_unvalidated_target_state_confirmed": bool(audit.get("unvalidated_target_state_confirmed")),
                "audit_validation_target_record_count": int_or_default(audit_target_state.get("record_count")),
                "audit_validation_target_healthy": bool(audit_target_state.get("healthy_for_unvalidated_activation")),
            })
    status = "ok" if all(
        item["blocked_without_target_state_confirmation"]
        and item["status"] == "ok"
        and item["validation_status"] == "skipped"
        and item["validation_skipped"]
        and item["unvalidated_target_state_confirmed"]
        and item["validation_target_record_count"] == 0
        and item["validation_target_healthy"] is False
        and item["promotion_written"] > 0
        and item["promotion_failed"] == 0
        and item["promotion_dead_letter"] == 0
        and item["audit_written"]
        and item["audit_unvalidated_target_state_confirmed"]
        and item["audit_validation_target_record_count"] == 0
        and item["audit_validation_target_healthy"] is False
        for item in results
    ) else "failed"
    return {"status": status, "results": results}


def run_manifest_verification_gate(args: argparse.Namespace) -> Json:
    results: list[Json] = []
    records = max(2, min(max(2, int(args.records)), max(2, int(args.incremental_records))))
    source_prefix = "matrixark:mcp:readiness_manifest"
    for raw_backend in ["temporalstore", "matrixkv"]:
        with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_manifest_{raw_backend}_") as tmp:
            kv_path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(kv_path)
            bench.seed_raw_log(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
            target_prefix = f"matrixark:context_backfill:readiness_manifest:{raw_backend}"
            job_id = f"readiness-manifest-{raw_backend}"
            run_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                end_seq=records,
            )
            backfilled = backfill.run_backfill(run_args)
            verify_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                mode="verify_manifest",
            )
            verify_prom = Path(tmp) / "verify_manifest.prom"
            verify_args.prometheus_output = str(verify_prom)
            verified = backfill.run_verify_manifest(verify_args)
            verify_prom_text = verify_prom.read_text(encoding="utf-8")

            kv_after = backfill.LocalJsonKV(kv_path)
            manifest_key = f"{target_prefix}:backfill_manifest"
            manifest = json.loads(kv_after.hget(manifest_key, job_id))
            manifest.setdefault("source_range", {})["source_record_count"] = int(manifest.get("source_range", {}).get("source_record_count", 0) or 0) + 1
            kv_after.hset(manifest_key, job_id, json.dumps(manifest, sort_keys=True, separators=(",", ":")))
            tampered_prom = Path(tmp) / "verify_manifest_tampered.prom"
            verify_args.prometheus_output = str(tampered_prom)
            tampered = backfill.run_verify_manifest(verify_args)
            tampered_prom_text = tampered_prom.read_text(encoding="utf-8")

            results.append({
                "raw_backend": raw_backend,
                "records": records,
                "backfill_status": backfilled.get("status"),
                "manifest_schema": backfilled.get("manifest_schema"),
                "manifest_payload_sha256": backfilled.get("manifest_payload_sha256"),
                "verify_status": verified.get("status"),
                "verify_hash_match": bool(verified.get("checks", {}).get("manifest_payload_sha256_match")),
                "verify_schema_supported": bool(verified.get("checks", {}).get("manifest_schema_supported")),
                "verify_job_id_matches": bool(verified.get("checks", {}).get("manifest_job_id_matches")),
                "verify_target_prefix_matches": bool(verified.get("checks", {}).get("manifest_target_prefix_matches")),
                "verify_raw_backend_matches": bool(verified.get("checks", {}).get("manifest_raw_backend_matches")),
                "verify_prometheus_metrics_present": all(token in verify_prom_text for token in [
                    "matrixark_context_backfill_manifest_verification_status",
                    "matrixark_context_backfill_manifest_verification_check",
                    'status="ok"',
                    'check="manifest_payload_sha256_match"} 1',
                ]),
                "tampered_status": tampered.get("status"),
                "tampered_hash_match": bool(tampered.get("checks", {}).get("manifest_payload_sha256_match")),
                "tampered_prometheus_metrics_present": all(token in tampered_prom_text for token in [
                    "matrixark_context_backfill_manifest_verification_status",
                    "matrixark_context_backfill_manifest_verification_check",
                    'status="failed"',
                    'check="manifest_payload_sha256_match"} 0',
                ]),
            })
    status = "ok" if all(
        item["backfill_status"] == "ok"
        and item["manifest_schema"] == "matrixark_context_backfill_manifest_v1"
        and isinstance(item["manifest_payload_sha256"], str)
        and len(item["manifest_payload_sha256"]) == 64
        and item["verify_status"] == "ok"
        and item["verify_hash_match"]
        and item["verify_schema_supported"]
        and item["verify_job_id_matches"]
        and item["verify_target_prefix_matches"]
        and item["verify_raw_backend_matches"]
        and item["verify_prometheus_metrics_present"]
        and item["tampered_status"] == "failed"
        and item["tampered_hash_match"] is False
        and item["tampered_prometheus_metrics_present"]
        for item in results
    ) else "failed"
    return {"status": status, "results": results}


def run_dead_letter_gate(args: argparse.Namespace) -> Json:
    results: list[Json] = []
    source_prefix = "matrixark:mcp:readiness_dead_letter"
    records = max(3, int(args.records))
    missing_sequence = 1
    for raw_backend in ["temporalstore", "matrixkv"]:
        with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_dead_letter_{raw_backend}_") as tmp:
            kv_path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(kv_path)
            bench.seed_raw_log(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
            shard = missing_sequence // backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            offset = missing_sequence % backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            kv.data["hashes"].get(f"{source_prefix}:records:{shard:06d}", {}).pop(f"{offset:020d}", None)
            kv._flush()

            target_prefix = f"matrixark:context_backfill:readiness_dead_letter:{raw_backend}"
            job_id = f"readiness-dead-letter-{raw_backend}"
            run_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
            )
            summary = backfill.run_backfill(run_args)
            validation = backfill.run_validate_shadow(bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                mode="validate_shadow",
            ))
            kv_after = backfill.LocalJsonKV(kv_path)
            dead_letter_count = kv_after.get_string(f"{target_prefix}:dead_letter_count")
            dead_letter_preview = kv_after.hget(f"{target_prefix}:dead_letter", "00000000000000000000")
            partial = backfill.build_partial_spec(run_args)
            checkpoint_state = backfill.read_checkpoint_state(
                kv_after,
                backfill.checkpoint_key(
                    job_id=job_id,
                    source_prefix=source_prefix,
                    target_prefix=target_prefix,
                    raw_backend=raw_backend,
                    partial=partial,
                ),
            )
            results.append({
                "raw_backend": raw_backend,
                "records": records,
                "missing_sequence": missing_sequence,
                "status": summary.get("status"),
                "data_quality_status": summary.get("data_quality_status"),
                "failed": int(summary.get("metrics", {}).get("failed", 0) or 0),
                "dead_letter": int(summary.get("metrics", {}).get("dead_letter", 0) or 0),
                "written": int(summary.get("metrics", {}).get("written", 0) or 0),
                "dead_letter_count": int(dead_letter_count or 0),
                "dead_letter_preview_has_error": "missing sharded record" in dead_letter_preview,
                "checkpoint_last_sequence": checkpoint_state.get("checkpoint_last_sequence"),
                "validation_status": validation.get("status"),
                "validation_no_shadow_dead_letters": validation.get("checks", {}).get("no_shadow_dead_letters"),
                "validation_source_scan_had_no_failures": validation.get("checks", {}).get("source_scan_had_no_failures"),
            })
    status = "ok" if all(
        item["status"] == "ok"
        and item["data_quality_status"] == "completed_with_errors"
        and item["failed"] == 1
        and item["dead_letter"] == 1
        and item["dead_letter_count"] == 1
        and item["dead_letter_preview_has_error"]
        and item["written"] == item["records"] - 1
        and item["checkpoint_last_sequence"] == item["records"] - 1
        and item["validation_status"] == "failed"
        and item["validation_no_shadow_dead_letters"] is False
        and item["validation_source_scan_had_no_failures"] is False
        for item in results
    ) else "failed"
    return {"status": status, "results": results}


def run_resume_gate(args: argparse.Namespace) -> Json:
    results: list[Json] = []
    source_prefix = "matrixark:mcp:readiness_resume"
    records = max(2, int(args.records))
    first_end_seq = max(1, min(records - 1, records // 2))
    for raw_backend in ["temporalstore", "matrixkv"]:
        with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_resume_{raw_backend}_") as tmp:
            kv_path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(kv_path)
            bench.seed_raw_log(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
            target_prefix = f"matrixark:context_backfill:readiness_resume:{raw_backend}"
            job_id = f"readiness-resume-{raw_backend}"
            first_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                end_seq=first_end_seq,
            )
            first_args.resume = True
            first = backfill.run_backfill(first_args)
            second_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
            )
            second_args.resume = True
            second = backfill.run_backfill(second_args)
            incompatible_blocked = False
            incompatible_error = ""
            incompatible_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                start_seq=1,
                end_seq=records,
            )
            incompatible_args.resume = True
            try:
                backfill.run_backfill(incompatible_args)
            except backfill.BackfillError as exc:
                incompatible_blocked = "confirm-resume-range-change=YES" in str(exc)
                incompatible_error = str(exc)
            confirmed_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                start_seq=1,
                end_seq=records,
            )
            confirmed_args.resume = True
            confirmed_args.confirm_resume_range_change = "YES"
            confirmed = backfill.run_backfill(confirmed_args)
            validation = backfill.run_validate_shadow(bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                mode="validate_shadow",
            ))
            results.append({
                "raw_backend": raw_backend,
                "records": records,
                "first_end_seq": first_end_seq,
                "first_written": int(first["metrics"]["written"]),
                "second_written": int(second["metrics"]["written"]),
                "second_resume_state": second.get("resume_state", {}),
                "incompatible_resume_blocked": incompatible_blocked,
                "incompatible_resume_error": incompatible_error,
                "confirmed_resume_state": confirmed.get("resume_state", {}),
                "confirmed_scanned": int(confirmed.get("metrics", {}).get("scanned", 0) or 0),
                "confirmed_duplicate": int(confirmed.get("metrics", {}).get("duplicate", 0) or 0),
                "confirmed_written": int(confirmed.get("metrics", {}).get("written", 0) or 0),
                "confirmed_failed": int(confirmed.get("metrics", {}).get("failed", 0) or 0),
                "confirmed_dead_letter": int(confirmed.get("metrics", {}).get("dead_letter", 0) or 0),
                "validation_status": validation.get("status"),
                "actual_records": validation.get("actual_records"),
                "expected_records": validation.get("expected_records"),
                "serving_record_fingerprint_match": validation.get("checks", {}).get("serving_record_fingerprint_match"),
            })
    return {"status": "ok" if all(item["validation_status"] == "ok" for item in results) else "failed", "results": results}


def run_prometheus_gate(args: argparse.Namespace) -> Json:
    required_plan_metrics = [
        "matrixark_context_backfill_plan_status",
        "matrixark_context_backfill_plan_safety_check",
        "matrixark_context_backfill_plan_readiness_blocker",
        "matrixark_context_backfill_plan_source_range",
        "matrixark_context_backfill_plan_source_scan_mode",
        "matrixark_context_backfill_plan_target_records",
        "matrixark_context_backfill_plan_chunk_windows",
        "matrixark_context_backfill_plan_execution_readiness_status",
        "matrixark_context_backfill_plan_execution_readiness_blocker",
        "matrixark_context_backfill_plan_execution_readiness_count",
    ]
    required_shadow_metrics = [
        "matrixark_context_backfill_run_elapsed_ms",
        "matrixark_context_backfill_scan_qps",
        "matrixark_context_backfill_records_total",
        "matrixark_context_backfill_serving_records_total",
        "matrixark_context_backfill_serving_record_fingerprint_info",
        "matrixark_context_backfill_source_range",
        "matrixark_context_backfill_source_scan_mode",
    ]
    required_repair_metrics = [
        "matrixark_context_backfill_incremental_repair_status",
        "matrixark_context_backfill_incremental_repair_promotion_consistency_status",
        "matrixark_context_backfill_incremental_repair_promotion_consistency_check",
        "matrixark_context_backfill_incremental_repair_promotion_records",
        "matrixark_context_backfill_incremental_repair_promotion_data_quality_status",
        "matrixark_context_backfill_incremental_repair_promotion_source_range",
        "matrixark_context_backfill_incremental_repair_validation_status",
        "matrixark_context_backfill_incremental_repair_promotion_manifest_status",
        "matrixark_context_backfill_incremental_repair_promotion_manifest_check",
    ]
    required_validation_metrics = [
        "matrixark_context_backfill_validation_status",
        "matrixark_context_backfill_validation_check",
        "matrixark_context_backfill_promotion_readiness_status",
        "matrixark_context_backfill_validation_source_range",
        "matrixark_context_backfill_validation_source_scan_mode",
    ]
    required_activation_metrics = [
        "matrixark_context_backfill_activation_status",
        "matrixark_context_backfill_activation_validation_status",
        "matrixark_context_backfill_activation_guard_status",
        "matrixark_context_backfill_activation_target_records",
        "matrixark_context_backfill_activation_source_range",
    ]
    required_rollback_metrics = [
        "matrixark_context_backfill_rollback_status",
        "matrixark_context_backfill_rollback_guard_status",
        "matrixark_context_backfill_rollback_target_records",
        "matrixark_context_backfill_rollback_target_health",
    ]
    required_local_recovery_metrics = [
        "matrixark_context_local_recovery_status",
        "matrixark_context_local_recovery_check",
        "matrixark_context_local_recovery_blocker",
        "matrixark_context_local_recovery_serving_layers",
    ]
    results: list[Json] = []
    source_prefix = "matrixark:mcp:readiness_prometheus"
    records = max(2, int(args.records))
    incremental_records = max(1, min(int(args.incremental_records), records))
    incremental_start = records - incremental_records
    for raw_backend in ["temporalstore", "matrixkv"]:
        with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_prometheus_{raw_backend}_") as tmp:
            tmp_path = Path(tmp)
            kv_path = tmp_path / "kv.json"
            kv = backfill.LocalJsonKV(kv_path)
            bench.seed_raw_log(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
            recovery_records = [
                {
                    "record_type": "context_event",
                    "event_id_hash": backfill.stable_hash(f"{raw_backend}:local-recovery:event"),
                    "node_hash": backfill.stable_hash(f"{raw_backend}:local-recovery:node"),
                    "scope": {"tenant_id": "readiness", "user_id": raw_backend, "session_id": "local-recovery"},
                    "text": "local recovery readiness event",
                    "idempotency_key": f"{raw_backend}:local-recovery:event",
                },
                {
                    "record_type": "context_entity",
                    "entity_hash": backfill.stable_hash(f"{raw_backend}:local-recovery:entity"),
                    "node_hash": backfill.stable_hash(f"{raw_backend}:local-recovery:node"),
                    "scope": {"tenant_id": "readiness", "user_id": raw_backend, "session_id": "local-recovery"},
                    "entity_type": "recovery_gate",
                    "entity_name": "local_context_rebuild",
                    "state": "Local recovery can rebuild context events, entities, embeddings, and secondary indexes.",
                    "idempotency_key": f"{raw_backend}:local-recovery:entity",
                },
                {
                    "record_type": "context_embedding",
                    "embedding_type": "entity_state",
                    "ref_type": "entity",
                    "ref_hash": backfill.stable_hash(f"{raw_backend}:local-recovery:entity"),
                    "node_hash": backfill.stable_hash(f"{raw_backend}:local-recovery:node"),
                    "scope": {"tenant_id": "readiness", "user_id": raw_backend, "session_id": "local-recovery"},
                    "vector": [1.0, 0.0, 0.5],
                    "idempotency_key": f"{raw_backend}:local-recovery:embedding",
                },
                {
                    "record_type": "context_index",
                    "index_name": "entity",
                    "index_value": "local_context_rebuild",
                    "ref_hash": backfill.stable_hash(f"{raw_backend}:local-recovery:entity"),
                    "node_hash": backfill.stable_hash(f"{raw_backend}:local-recovery:node"),
                    "scope": {"tenant_id": "readiness", "user_id": raw_backend, "session_id": "local-recovery"},
                    "scope_key": f"tenant:readiness/user:{raw_backend}/session:local-recovery",
                    "idempotency_key": f"{raw_backend}:local-recovery:index",
                },
                {
                    "record_type": "context_summary",
                    "summary_type": "session_l1",
                    "summary_hash": backfill.stable_hash(f"{raw_backend}:local-recovery:summary"),
                    "node_hash": backfill.stable_hash(f"{raw_backend}:local-recovery:node"),
                    "scope": {"tenant_id": "readiness", "user_id": raw_backend, "session_id": "local-recovery"},
                    "summary_text": "Local recovery can rebuild summary serving records for retrieval.",
                    "idempotency_key": f"{raw_backend}:local-recovery:summary",
                },
                {
                    "record_type": "context_pack_telemetry",
                    "context_pack_id": f"{raw_backend}:local-recovery:pack",
                    "scope": {"tenant_id": "readiness", "user_id": raw_backend, "session_id": "local-recovery"},
                    "selected_ref_count": 2,
                    "used_remote_context_tokens": 24,
                    "remote_context_budget_tokens": 256,
                    "memory_layer_budget": {
                        "by_session_continuity": {"same_session": {"refs": 1, "tokens": 12}},
                        "by_memory_scope": {"session": {"refs": 1, "tokens": 12}},
                    },
                    "retrieval_request_metadata": {
                        "retrieval_source": "local_recovery_report",
                        "lifecycle_stage": "readiness_probe",
                    },
                    "idempotency_key": f"{raw_backend}:local-recovery:telemetry",
                },
            ]
            for offset, record in enumerate(recovery_records):
                sequence = records + offset
                shard = sequence // backfill.DIRECT_RECORD_LOG_SHARD_SIZE
                field = f"{sequence % backfill.DIRECT_RECORD_LOG_SHARD_SIZE:020d}"
                kv.hset(f"{source_prefix}:records:{shard:06d}", field, json.dumps(record, sort_keys=True))
            records += len(recovery_records)
            kv.put_string(f"{source_prefix}:record_count", str(records))
            kv.put_string("matrixark:context:active_prefix", f"matrixark:context:active:prometheus:{raw_backend}")
            backfill.MatrixKVBackfillTarget(
                kv,
                prefix=f"matrixark:context:active:prometheus:{raw_backend}",
                raw_backend=raw_backend,
            ).append_many([{
                "record_type": "context_event",
                "event_id_hash": f"previous-active-{raw_backend}",
            }])

            plan_prometheus = tmp_path / "plan.prom"
            plan_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=f"matrixark:context_backfill:readiness_prometheus:{raw_backend}:full",
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-plan",
                batch_size=args.batch_size,
                mode="plan",
                end_seq=records,
            )
            plan_args.plan_window_size = max(1, min(records, int(args.batch_size)))
            plan_args.prometheus_output = str(plan_prometheus)
            plan_summary = backfill.run_plan(plan_args)
            plan_text = plan_prometheus.read_text(encoding="utf-8") if plan_prometheus.exists() else ""

            shadow_prometheus = tmp_path / "shadow.prom"
            shadow_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=f"matrixark:context_backfill:readiness_prometheus:{raw_backend}:full",
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-full",
                batch_size=args.batch_size,
            )
            shadow_args.prometheus_output = str(shadow_prometheus)
            shadow_summary = backfill.run_backfill(shadow_args)
            shadow_text = shadow_prometheus.read_text(encoding="utf-8") if shadow_prometheus.exists() else ""
            validation_prometheus = tmp_path / "validation.prom"
            validation_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=f"matrixark:context_backfill:readiness_prometheus:{raw_backend}:full",
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-full",
                batch_size=args.batch_size,
                mode="validate_shadow",
            )
            validation_args.prometheus_output = str(validation_prometheus)
            validation_summary = backfill.run_validate_shadow(validation_args)
            validation_text = validation_prometheus.read_text(encoding="utf-8") if validation_prometheus.exists() else ""

            local_recovery_prometheus = tmp_path / "local_recovery.prom"
            local_recovery_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=f"matrixark:context_backfill:readiness_prometheus:{raw_backend}:full",
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-local-recovery",
                batch_size=args.batch_size,
                mode="local_recovery_report",
            )
            local_recovery_args.prometheus_output = str(local_recovery_prometheus)
            local_recovery_summary = backfill.run_local_recovery_report(local_recovery_args)
            local_recovery_text = local_recovery_prometheus.read_text(encoding="utf-8") if local_recovery_prometheus.exists() else ""

            activation_prometheus = tmp_path / "activation.prom"
            activation_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=f"matrixark:context_backfill:readiness_prometheus:{raw_backend}:full",
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-activate",
                batch_size=args.batch_size,
                mode="activate_shadow",
                expect_active_prefix=f"matrixark:context:active:prometheus:{raw_backend}",
            )
            activation_args.confirm_activate = "YES"
            activation_args.prometheus_output = str(activation_prometheus)
            activation_summary = backfill.run_activate_shadow(activation_args)
            activation_text = activation_prometheus.read_text(encoding="utf-8") if activation_prometheus.exists() else ""

            rollback_prometheus = tmp_path / "rollback.prom"
            rollback_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=f"matrixark:context_backfill:readiness_prometheus:{raw_backend}:full",
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-rollback",
                batch_size=args.batch_size,
                mode="rollback_activation",
                expect_active_prefix=f"matrixark:context_backfill:readiness_prometheus:{raw_backend}:full",
            )
            rollback_args.rollback_job_id = f"readiness-prometheus-{raw_backend}-activate"
            rollback_args.confirm_rollback = "YES"
            rollback_args.prometheus_output = str(rollback_prometheus)
            rollback_summary = backfill.run_rollback_activation(rollback_args)
            rollback_text = rollback_prometheus.read_text(encoding="utf-8") if rollback_prometheus.exists() else ""

            repair_prefix = f"matrixark:context_repair:readiness_prometheus:{raw_backend}"
            repair_shadow_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-repair-shadow",
                batch_size=args.batch_size,
                start_seq=incremental_start,
                end_seq=records,
            )
            backfill.run_backfill(repair_shadow_args)
            repair_prometheus = tmp_path / "repair.prom"
            repair_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-repair",
                batch_size=args.batch_size,
                start_seq=incremental_start,
                end_seq=records,
                mode="incremental_repair",
                confirm_incremental_repair="YES",
                expect_active_prefix=f"matrixark:context:active:prometheus:{raw_backend}",
            )
            repair_args.prometheus_output = str(repair_prometheus)
            repair_summary = backfill.run_incremental_repair(repair_args)
            repair_text = repair_prometheus.read_text(encoding="utf-8") if repair_prometheus.exists() else ""

            results.append({
                "raw_backend": raw_backend,
                "plan_status": plan_summary.get("status"),
                "plan_execution_readiness_status": (plan_summary.get("execution_readiness") or {}).get("status"),
                "plan_execution_readiness_ready": bool((plan_summary.get("execution_readiness") or {}).get("ready")),
                "shadow_status": shadow_summary.get("status"),
                "validation_status": validation_summary.get("status"),
                "local_recovery_status": local_recovery_summary.get("status"),
                "local_recovery_ready": bool(local_recovery_summary.get("ready")),
                "activation_status": activation_summary.get("status"),
                "rollback_status": rollback_summary.get("status"),
                "repair_status": repair_summary.get("status"),
                "repair_manifest_verification_status": (repair_summary.get("promotion_manifest_verification") or {}).get("status"),
                "plan_prometheus_output": str(plan_prometheus),
                "shadow_prometheus_output": str(shadow_prometheus),
                "validation_prometheus_output": str(validation_prometheus),
                "local_recovery_prometheus_output": str(local_recovery_prometheus),
                "activation_prometheus_output": str(activation_prometheus),
                "rollback_prometheus_output": str(rollback_prometheus),
                "repair_prometheus_output": str(repair_prometheus),
                "plan_metric_count": sum(1 for line in plan_text.splitlines() if line and not line.startswith("#")),
                "shadow_metric_count": sum(1 for line in shadow_text.splitlines() if line and not line.startswith("#")),
                "validation_metric_count": sum(1 for line in validation_text.splitlines() if line and not line.startswith("#")),
                "local_recovery_metric_count": sum(1 for line in local_recovery_text.splitlines() if line and not line.startswith("#")),
                "activation_metric_count": sum(1 for line in activation_text.splitlines() if line and not line.startswith("#")),
                "rollback_metric_count": sum(1 for line in rollback_text.splitlines() if line and not line.startswith("#")),
                "repair_metric_count": sum(1 for line in repair_text.splitlines() if line and not line.startswith("#")),
                "plan_metrics_present": {metric: metric in plan_text for metric in required_plan_metrics},
                "shadow_metrics_present": {metric: metric in shadow_text for metric in required_shadow_metrics},
                "validation_metrics_present": {metric: metric in validation_text for metric in required_validation_metrics},
                "local_recovery_metrics_present": {metric: metric in local_recovery_text for metric in required_local_recovery_metrics},
                "activation_metrics_present": {metric: metric in activation_text for metric in required_activation_metrics},
                "rollback_metrics_present": {metric: metric in rollback_text for metric in required_rollback_metrics},
                "repair_metrics_present": {metric: metric in repair_text for metric in required_repair_metrics},
            })
    status = "ok" if all(
        item["plan_status"] == "ok"
        and item["plan_execution_readiness_status"] == "ready"
        and item["plan_execution_readiness_ready"]
        and item["shadow_status"] == "ok"
        and item["validation_status"] == "ok"
        and item["local_recovery_status"] == "ok"
        and item["local_recovery_ready"]
        and item["activation_status"] == "ok"
        and item["rollback_status"] == "ok"
        and item["repair_status"] == "ok"
        and item["repair_manifest_verification_status"] == "ok"
        and all(item["plan_metrics_present"].values())
        and all(item["shadow_metrics_present"].values())
        and all(item["validation_metrics_present"].values())
        and all(item["local_recovery_metrics_present"].values())
        and all(item["activation_metrics_present"].values())
        and all(item["rollback_metrics_present"].values())
        and all(item["repair_metrics_present"].values())
        for item in results
    ) else "failed"
    return {"status": status, "results": results}


def dual_write_evidence_dir(args: argparse.Namespace) -> Path | None:
    configured = str(getattr(args, "dual_write_evidence_dir", "") or "").strip()
    if configured:
        path = Path(configured)
        path.mkdir(parents=True, exist_ok=True)
        return path
    if str(getattr(args, "json_output", "") or "").strip():
        path = Path(args.json_output).resolve().parent / "matrixark_context_backfill_dual_write_evidence"
        path.mkdir(parents=True, exist_ok=True)
        return path
    path = Path(tempfile.gettempdir()) / "matrixark_context_backfill_dual_write_evidence"
    path.mkdir(parents=True, exist_ok=True)
    return path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_dual_write_gate(args: argparse.Namespace) -> Json:
    required_metrics = [
        "matrixark_dual_write_ingestion_status",
        "matrixark_dual_write_ingestion_qps",
        "matrixark_dual_write_ingestion_batch_latency_ms",
        "matrixark_dual_write_ingestion_records_total",
        "matrixark_dual_write_ingestion_counts_validated",
        "matrixark_dual_write_ingestion_backend_qps_ratio",
        "matrixark_dual_write_ingestion_performance_gate_status",
    ]
    evidence_dir = dual_write_evidence_dir(args)
    tmp_path = evidence_dir
    evidence_persistent = True
    summary_path = tmp_path / "dual_write_readiness.json"
    prometheus_path = tmp_path / "dual_write_readiness.prom"
    manifest_path = tmp_path / "manifest.json"
    records = max(2, int(args.records))
    batch_size = max(1, min(int(args.batch_size), records))
    bench_args = argparse.Namespace(
        mode="local",
        records=records,
        workers=2,
        batch_size=batch_size,
        payload_bytes=args.payload_bytes,
        scope_key="readiness:dual-write",
        local_write_delay_us=0,
        storage_prefix="matrixark:mcp:readiness_dual_write",
        raw_storage_prefix="",
        raw_backend="temporalstore",
        raw_backends="both",
        shard_size=4096,
        metaserver="unused",
        namespace="unused",
        table="unused",
        library_path="",
        request_timeout_ms=1000,
        io_timeout_ms=1000,
        min_ingestion_qps=1.0,
        max_batch_p95_ms=1000.0,
        min_backend_qps_ratio=0.000001,
        require_dual_write_counts=1,
        json_output=str(summary_path),
        prometheus_output=str(prometheus_path),
    )
    summary = dual_bench.run_backend_sweep(bench_args)
    prometheus_text = dual_bench.render_prometheus(summary)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    prometheus_path.write_text(prometheus_text, encoding="utf-8")
    artifact_manifest = {
        "schema": "matrixark_dual_write_readiness_evidence_v1",
        "status": "ok" if summary.get("status") == "ok" else "failed",
        "generated_by": "tools/validate_matrixark_context_backfill_readiness.py",
        "raw_backends": summary.get("raw_backends", []),
        "records_per_backend": summary.get("records_per_backend"),
        "artifacts": {
            "dual_write_readiness_json": {
                "path": summary_path.name,
                "bytes": summary_path.stat().st_size,
                "sha256": sha256_file(summary_path),
            },
            "dual_write_readiness_prometheus": {
                "path": prometheus_path.name,
                "bytes": prometheus_path.stat().st_size,
                "sha256": sha256_file(prometheus_path),
            },
        },
    }
    manifest_path.write_text(json.dumps(artifact_manifest, indent=2, sort_keys=True), encoding="utf-8")
    metric_samples = [
        line
        for line in prometheus_text.splitlines()
        if line and not line.startswith("#")
    ]
    metrics_present = {metric: metric in prometheus_text for metric in required_metrics}
    raw_backends = set(summary.get("raw_backends") or [])
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    performance_gate = summary.get("performance_gate") if isinstance(summary.get("performance_gate"), dict) else {}
    gate_checks = performance_gate.get("checks") if isinstance(performance_gate.get("checks"), list) else []
    ratio_checked = any(item.get("metric") == "backend_ingestion_qps_ratio" and item.get("passed") for item in gate_checks)
    counts_validated = bool(results) and all(bool(item.get("dual_write_counts_validated")) for item in results)
    status_ok = (
        summary.get("status") == "ok"
        and raw_backends == {"temporalstore", "matrixkv"}
        and counts_validated
        and ratio_checked
        and bool(performance_gate.get("passed"))
        and all(metrics_present.values())
        and len(metric_samples) > 0
        and 'raw_backend="temporalstore"' in prometheus_text
        and 'raw_backend="matrixkv"' in prometheus_text
    )
    return {
        "status": "ok" if status_ok else "failed",
        "evidence_persistent": evidence_persistent,
        "raw_backends": summary.get("raw_backends", []),
        "records_per_backend": summary.get("records_per_backend"),
        "batch_size": summary.get("batch_size"),
        "results": [
            {
                "raw_backend": item.get("raw_backend"),
                "status": item.get("status"),
                "records": item.get("records"),
                "ingestion_qps": item.get("ingestion_qps"),
                "dual_write_counts_validated": item.get("dual_write_counts_validated"),
            }
            for item in results
        ],
        "summary": summary.get("summary", {}),
        "performance_gate": performance_gate,
        "prometheus_metrics_present": metrics_present,
        "prometheus_metric_count": len(metric_samples),
        "prometheus_output": str(prometheus_path),
        "summary_output": str(summary_path),
        "evidence_manifest": str(manifest_path),
        "evidence_checksums": {
            "dual_write_readiness_json": artifact_manifest["artifacts"]["dual_write_readiness_json"]["sha256"],
            "dual_write_readiness_prometheus": artifact_manifest["artifacts"]["dual_write_readiness_prometheus"]["sha256"],
            "manifest": sha256_file(manifest_path),
        },
    }
