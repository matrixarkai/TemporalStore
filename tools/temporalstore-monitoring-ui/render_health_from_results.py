#!/usr/bin/env python3
"""Render monitoring-ui health.json from TemporalStore test artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any


def human_bytes(value: str | None) -> str:
    if not value:
        return "-"
    try:
        size = float(value)
    except ValueError:
        return value
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    return f"{size:.0f} {units[idx]}" if idx else f"{int(size)} B"


def status_from_exit(result_dir: Path, name: str) -> str:
    path = result_dir / f"{name}.exit_code"
    if not path.exists():
        return "pending"
    return "ok" if path.read_text(encoding="utf-8").strip() == "0" else "failed"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def best_temporalstore_case(result_dir: Path) -> dict[str, str]:
    rows = read_csv_rows(result_dir / "temporalstore.csv")
    candidates = [row for row in rows if row.get("phase", "").lower() in {"read", "get", "mixed", "write"}]
    if not candidates:
        return {}
    return max(candidates, key=lambda row: float(row.get("qps") or 0))


def module_latency_map(result_dir: Path) -> dict[str, dict[str, str]]:
    rows = read_csv_rows(result_dir / "module_latency.csv")
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        module = row.get("module") or row.get("name") or row.get("case") or ""
        if module:
            result[module.lower()] = row
    return result


def load_existing(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def runtime_config_from_env() -> dict[str, str]:
    env = os.environ
    return {
        "storage_zone_size": human_bytes(env.get("TEMPORALSTORE_STORAGE_ZONE_SIZE", "268435456")),
        "stream_max_blob_size": human_bytes(env.get("TEMPORALSTORE_STREAM_MAX_BLOB_SIZE", "268435456")),
        "storage_oplog_delay_dump_length": env.get("TEMPORALSTORE_STORAGE_OPLOG_DELAY_DUMP_LENGTH", "0"),
        "replicator_loop_interval_us": env.get("TEMPORALSTORE_REPLICATOR_LOOP_INTERVAL_US", "1000"),
        "replicator_max_oplog_per_loop": env.get("TEMPORALSTORE_REPLICATOR_MAX_OPLOG_PER_LOOP", "20000"),
        "replicator_update_remote_interval_ms": env.get("TEMPORALSTORE_REPLICATOR_UPDATE_REMOTE_INTERVAL_MS", "20"),
        "blockcache_dram_capacity": human_bytes(env.get("TEMPORALSTORE_BLOCKCACHE_DRAM_CAPACITY", str(64 * 1024 * 1024))),
        "blockcache_ssd_capacity": human_bytes(env.get("TEMPORALSTORE_BLOCKCACHE_SSD_CAPACITY", str(2 * 1024 * 1024 * 1024))),
    }


def build_health(args: argparse.Namespace) -> dict[str, Any]:
    result_dir = Path(args.result_dir).resolve()
    base = load_existing(Path(args.template).resolve()) if args.template else {}
    temporal_case = best_temporalstore_case(result_dir)
    module_rows = module_latency_map(result_dir)

    cluster_status = "ok" if status_from_exit(result_dir, "replication_smoke") == "ok" else "pending"
    write_qps = temporal_case.get("qps", "-")
    p50 = temporal_case.get("p50_us", "-")
    p99 = temporal_case.get("p99_us", "-")

    health = {
        "cluster": {
            "name": args.cluster_name,
            "status": cluster_status,
            "environment": args.environment,
            "metaservers": args.metaservers,
            "data_nodes": args.data_nodes,
        },
        "health": {
            "metaserver": {"status": "ok", "detail": "dashboard generated from test results"},
            "proxy": {"status": status_from_exit(result_dir, "proxy_smoke"), "detail": "proxy smoke"},
            "exporter": {"status": "pending", "detail": "metrics endpoint"},
            "data_nodes": {"status": cluster_status, "detail": "primary and secondary"},
            "efs": {"status": "ok" if args.shared_store else "pending", "detail": args.shared_store or "shared store"},
            "blockcache": {"status": "ok" if args.blockcache else "pending", "detail": args.blockcache or "DRAM + SSD cache"},
        },
        "runtime_config": runtime_config_from_env(),
        "nodes": base.get("nodes", []),
        "replication": {
            "mode": args.replication_mode,
            "secondary_lag_ms": args.secondary_lag_ms,
            "replay_source": args.replay_source,
            "visibility": "ok" if cluster_status == "ok" else "pending",
        },
        "scale_tests": [
            {
                "name": "TemporalAggregate high-cardinality",
                "status": status_from_exit(result_dir, "temporal_aggregate_lag"),
                "write_qps": args.temporalaggregate_qps,
                "read_p50_ms": args.temporalaggregate_p50_ms,
                "read_p99_ms": args.temporalaggregate_p99_ms,
                "secondary_lag_ms": args.secondary_lag_ms,
                "workload": "feature x bucket increments with window query",
            },
            {
                "name": "STRING primary / replica",
                "status": status_from_exit(result_dir, "string_primary"),
                "write_qps": write_qps,
                "read_p50_ms": f"{float(p50) / 1000:.2f}" if p50 not in {"", "-"} else "-",
                "read_p99_ms": f"{float(p99) / 1000:.2f}" if p99 not in {"", "-"} else "-",
                "secondary_lag_ms": args.secondary_lag_ms,
                "workload": "plain SET/GET baseline",
            },
            {
                "name": "Sequence feature",
                "status": status_from_exit(result_dir, "sequence_primary"),
                "write_qps": "-",
                "read_p50_ms": "-",
                "read_p99_ms": "-",
                "secondary_lag_ms": args.secondary_lag_ms,
                "workload": "long behavior sequence window scan",
            },
        ],
        "module_tests": module_tests(module_rows, result_dir),
        "diagnostics": {
            "last_result_dir": str(result_dir),
            "release_build": args.release_build,
            "proxy_sdk": status_from_exit(result_dir, "proxy_smoke"),
            "direct_sdk": status_from_exit(result_dir, "module_ingest"),
        },
    }
    if not health["nodes"]:
        health["nodes"] = base.get("nodes", [])
    return health


def module_tests(rows: dict[str, dict[str, str]], result_dir: Path) -> list[dict[str, str]]:
    specs = [
        ("TemporalAggregate", "high-cardinality window aggregate", "temporalaggregate"),
        ("Feature", "module ingest/query", "feature"),
        ("IPS", "risk/frequency cap sample", "ips"),
        ("STRING", "SET/GET baseline", "string"),
    ]
    tests = []
    for module, test, key in specs:
        row = rows.get(key, {})
        p99 = row.get("p99_us") or row.get("p99") or "-"
        latency = f"p99 {float(p99) / 1000:.2f} ms" if p99 not in {"", "-"} else "-"
        tests.append(
            {
                "module": module,
                "test": test,
                "status": "ok" if row else status_from_exit(result_dir, "module_ingest"),
                "write_path": "direct SDK",
                "read_path": "primary and replica-eligible" if module in {"TemporalAggregate", "STRING"} else "primary",
                "latency": latency,
                "notes": row.get("notes") or row.get("phase") or "covered by module test harness",
            }
        )
    return tests


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--template", default="")
    parser.add_argument("--cluster-name", default="aws-scale")
    parser.add_argument("--environment", default="AWS test cluster")
    parser.add_argument("--metaservers", type=int, default=1)
    parser.add_argument("--data-nodes", type=int, default=2)
    parser.add_argument("--shared-store", default="EFS shared file store")
    parser.add_argument("--blockcache", default="DRAM + SSD cache")
    parser.add_argument("--replication-mode", default="shared file store + primary-pull fallback")
    parser.add_argument("--replay-source", default="EFS or primary stream")
    parser.add_argument("--secondary-lag-ms", default="-")
    parser.add_argument("--temporalaggregate-qps", default="-")
    parser.add_argument("--temporalaggregate-p50-ms", default="-")
    parser.add_argument("--temporalaggregate-p99-ms", default="-")
    parser.add_argument("--release-build", default="pending")
    args = parser.parse_args()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(build_health(args), indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
